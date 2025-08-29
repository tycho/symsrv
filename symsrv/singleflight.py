import asyncio, errno, hashlib, os, socket, time
from contextlib import suppress, asynccontextmanager
from typing import Protocol, Optional, AsyncContextManager

from .cachekey import CacheKey

# Per-process reentrancy guard
# key -> (owning_task, depth, _UDSOwner)
_REENTRANT: dict[bytes, tuple[asyncio.Task, int, "_UDSOwner"]] = {}


def _addr_for_key(key: CacheKey) -> bytes:
    # abstract namespace: b"\0" prefix keeps it out of the filesystem
    addr = (
        b"\x00sf:" + key.hash_key_raw[:16]
    )  # short, fixed-length; well under sockaddr_un limit
    return addr


class _UDSOwner:
    """Holds the listening socket and accept loop for the lock owner."""

    __slots__ = ("addr", "srv", "accept_task", "conns")

    def __init__(self, addr: bytes):
        self.addr = addr
        self.srv: Optional[socket.socket] = None
        self.accept_task: Optional[asyncio.Task] = None
        self.conns: set[socket.socket] = set()

    async def start(self):
        loop = asyncio.get_running_loop()
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.bind(self.addr)  # success => we own the lock
            s.listen(256)
            s.setblocking(False)
        except:
            s.close()
            raise
        self.srv = s
        self.accept_task = asyncio.create_task(self._accept_loop(loop))

    async def _accept_loop(self, loop: asyncio.AbstractEventLoop):
        assert self.srv is not None
        try:
            while True:
                conn, _ = await loop.sock_accept(self.srv)  # back-pressures waiters
                conn.setblocking(False)
                self.conns.add(conn)
                # We don't need to read/write. Presence of the listener "holds" the lock.
                # Waiters block on recv() until we close (release).
        except asyncio.CancelledError:
            pass
        finally:
            for c in list(self.conns):
                with suppress(Exception):
                    c.close()
            self.conns.clear()

    async def stop(self):
        # Close all waiter connections, then the listener.
        if self.accept_task:
            self.accept_task.cancel()
            self.accept_task = None
        if self.srv:
            self.srv.close()
            self.srv = None


class SingleFlight(Protocol):
    async def acquire(self, timeout: Optional[float] = None) -> None: ...
    async def release(self) -> None: ...
    async def transfer(self, new_owner: Optional[asyncio.Task] = None): ...
    async def is_locked(self) -> bool: ...
    def locked(self, timeout: Optional[float] = None) -> AsyncContextManager[None]: ...


class UDSSingleFlight:
    """
    Cross-process single-flight lock using abstract UDS.
    Re-entrant within a process (task-scoped).
    """

    def __init__(self, key: CacheKey):
        self.addr = _addr_for_key(key)

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.shield(self.release())
        return False

    async def transfer(self, new_owner: Optional[asyncio.Task] = None):
        if new_owner is None:
            new_owner = asyncio.current_task()
        owner = _REENTRANT.get(self.addr)
        assert owner is not None
        assert new_owner is not None
        if owner:
            _REENTRANT[self.addr] = (new_owner, owner[1], owner[2])

    async def is_locked(self) -> bool:
        # Try to become owner (bind/listen)
        owner_obj = _UDSOwner(self.addr)
        try:
            await owner_obj.start()
            return False
        except OSError as e:
            # Address in use => someone else owns it. Go wait.
            if e.errno != errno.EADDRINUSE:
                raise
            return True
        finally:
            await owner_obj.stop()

    async def acquire(self, timeout: Optional[float] = None):
        # Re-entrancy: same task acquiring the same key nests.
        cur = asyncio.current_task()
        owner = _REENTRANT.get(self.addr)
        if owner and owner[0] is cur:
            _REENTRANT[self.addr] = (owner[0], owner[1] + 1, owner[2])
            return
        assert cur is not None

        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else (loop.time() + timeout)
        delay = 0.05

        while True:
            # Try to become owner (bind/listen)
            owner_obj = _UDSOwner(self.addr)
            try:
                await owner_obj.start()
                # success: record ownership for reentrancy and return
                _REENTRANT[self.addr] = (cur, 1, owner_obj)
                return
            except OSError as e:
                # Address in use => someone else owns it. Go wait.
                if e.errno != errno.EADDRINUSE:
                    raise

            # Connect to the current owner and wait for EOF (release/crash)
            cs = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            cs.setblocking(False)
            try:
                # connect may fail transiently if owner just died; retry with backoff
                try:
                    await loop.sock_connect(cs, self.addr)
                except OSError as ce:
                    # ECONNREFUSED/ENOENT can happen if race between owners; backoff and retry
                    if ce.errno not in (errno.ECONNREFUSED, errno.ENOENT, errno.EAGAIN):
                        raise
                else:
                    # Connected: block until remote closes (release)
                    while True:
                        # recv returns b'' when the owner closes.
                        data = await loop.sock_recv(cs, 1)
                        if not data:
                            break
            except ConnectionResetError:
                # Means the holder went away, no problem for us
                pass
            finally:
                with suppress(Exception):
                    cs.close()

            if deadline is not None and loop.time() >= deadline:
                raise TimeoutError("UDS single-flight acquire timed out")

            await asyncio.sleep(delay + (os.getpid() % 7) * 0.001)  # tiny jitter

    async def release(self):
        # Re-entrancy: unwind nesting
        owner = _REENTRANT.get(self.addr)
        if not owner:
            return
        task, depth, owner_obj = owner
        if task is not asyncio.current_task():
            # Different task trying to release — ignore (or raise if you prefer).
            return
        if depth > 1:
            _REENTRANT[self.addr] = (task, depth - 1, owner_obj)
            return
        # Depth == 1: actually release
        _REENTRANT.pop(self.addr, None)
        await owner_obj.stop()

    @asynccontextmanager
    async def locked(self, timeout: Optional[float] = None):
        await self.acquire(timeout)
        try:
            yield
        finally:
            await self.release()


class NoOpSingleFlight:
    """A do-nothing single-flight lock for keys we never cache."""

    __slots__ = ("key",)

    def __init__(self, key: CacheKey):
        self.key = key

    async def acquire(self, timeout: Optional[float] = None) -> None:
        return  # no-op

    async def release(self) -> None:
        return  # no-op

    async def transfer(self, new_owner: Optional[asyncio.Task] = None):
        return

    async def is_locked(self) -> bool:
        return False

    @asynccontextmanager
    async def locked(self, timeout: Optional[float] = None):
        yield  # immediate pass-through

    # Optional: support `async with NoOpSingleFlight(key): ...`
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False
