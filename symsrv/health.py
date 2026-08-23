#!/usr/bin/python
# pylint: disable=C0114,C0115,C0116,C0301

"""
Cross-worker upstream health tracking -- a simple circuit breaker.

An upstream that fails to complete a request/response cycle (connection refused,
connect/read timeout, reset, ...) is *suspended*: it is dropped from the live
fan-out so a single stalled server can't block clients or be retried on every
request. A background task periodically probes suspended upstreams with a HEAD
to their base URL; any complete HTTP response (any status) restores them.

State is shared across all uvicorn worker processes via a small mmap'd file, so
the stall is paid ~once total rather than once per worker:

    [0:4]   magic  b"SVH1"
    [4:36]  sha256 of the sorted upstream-name list (config-state hash)
    [36:..] one byte per upstream, indexed by sorted position
            (0 = healthy, 1 = suspended)

Slots are assigned by sorted position (collision-free, unlike hashing). The
header hash lets a worker discard and reinitialize the file when the upstream
set has changed since it was created. A one-byte read/write is atomic, so the
hot-path availability check needs no lock.

NOTE: state deliberately persists across restarts -- an upstream that was dead
at shutdown stays suspended until the probe confirms recovery (<= one interval),
rather than eating a fresh stall on every restart. Stale state self-heals.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import mmap
import os
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

import aiohttp

try:
    # POSIX only; used to serialize first-time file init across workers.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX (dev/test on Windows)
    fcntl = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from .upstream import Upstream

logger = logging.getLogger("uvicorn.error")

_MAGIC = b"SVH1"
_HASH_LEN = 32
_HEADER_SIZE = len(_MAGIC) + _HASH_LEN  # 36

_HEALTHY = 0
_HEALTHY_B = b"\x00"
_SUSPENDED_B = b"\x01"


def _config_hash(names: list[str]) -> bytes:
    # The slot mapping is "sorted position -> byte offset", so the file is
    # reusable iff the sorted name list is identical. base_url changes don't
    # move slots, so they don't invalidate the file.
    return hashlib.sha256("\n".join(names).encode("utf-8")).digest()


class UpstreamHealth:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        upstreams: "dict[str, Upstream]",
        state_path: Union[str, os.PathLike],
        probe_interval: float = 30.0,
        probe_timeout: float = 15.0,
    ):
        self._session = session
        self._upstreams = dict(upstreams)
        # Stable, collision-free slot assignment: sorted position. Must stay in
        # sync with the input to _config_hash().
        self._names = sorted(self._upstreams)
        self._slot = {name: _HEADER_SIZE + i for i, name in enumerate(self._names)}
        self._path = Path(state_path)
        self._interval = float(probe_interval)
        self._timeout = float(probe_timeout)
        self._size = _HEADER_SIZE + len(self._names)
        self._fd: Optional[int] = None
        self._mm: Optional[mmap.mmap] = None
        self._task: Optional[asyncio.Task] = None
        self._closing = False

    async def start(self) -> "UpstreamHealth":
        self._open_state()
        self._task = asyncio.create_task(self._probe_loop())
        return self

    # --- shared-state file setup ---

    def _open_state(self) -> None:
        cfg_hash = _config_hash(self._names)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            self._lock(fd)
            try:
                self._init_locked(fd, cfg_hash)
            finally:
                self._unlock(fd)
            self._mm = mmap.mmap(fd, self._size)
            self._fd = fd
        except BaseException:
            os.close(fd)
            raise

    def _init_locked(self, fd: int, cfg_hash: bytes) -> None:
        reuse = False
        if os.fstat(fd).st_size == self._size:
            os.lseek(fd, 0, os.SEEK_SET)
            header = os.read(fd, _HEADER_SIZE)
            if header[: len(_MAGIC)] == _MAGIC and header[len(_MAGIC) :] == cfg_hash:
                reuse = True
        if reuse:
            logger.debug("Reusing upstream health state at %s", self._path)
            return
        logger.info(
            "Initializing upstream health state at %s (%d upstreams)",
            self._path,
            len(self._names),
        )
        # Truncate to zero then grow (zero-fills), then stamp the header. Slots
        # are left zeroed (all healthy).
        os.ftruncate(fd, 0)
        os.ftruncate(fd, self._size)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, _MAGIC + cfg_hash)

    @staticmethod
    def _lock(fd: int) -> None:
        if fcntl is not None:
            fcntl.flock(fd, fcntl.LOCK_EX)  # type: ignore[attr-defined]

    @staticmethod
    def _unlock(fd: int) -> None:
        if fcntl is not None:
            fcntl.flock(fd, fcntl.LOCK_UN)  # type: ignore[attr-defined]

    # --- hot path (single-byte, lock-free) ---

    def is_available(self, name: str) -> bool:
        mm = self._mm
        slot = self._slot.get(name)
        if mm is None or slot is None:
            # Unknown upstream or not initialized: never block it.
            return True
        return mm[slot] == _HEALTHY

    def trip(self, name: str) -> None:
        mm = self._mm
        slot = self._slot.get(name)
        if mm is None or slot is None:
            return
        if mm[slot] == _HEALTHY:
            logger.warning(
                "Upstream '%s' suspended after a transport failure; "
                "a background probe will restore it once it responds again",
                name,
            )
        mm[slot : slot + 1] = _SUSPENDED_B

    def _restore(self, name: str) -> None:
        mm = self._mm
        slot = self._slot.get(name)
        if mm is None or slot is None:
            return
        mm[slot : slot + 1] = _HEALTHY_B

    # --- background probing ---

    async def probe_once(self) -> None:
        for name in self._names:
            if self.is_available(name):
                continue
            up = self._upstreams.get(name)
            if up is None:
                continue
            if await self._probe(up):
                self._restore(name)
                logger.info("Upstream '%s' restored (liveness probe succeeded)", name)

    async def _probe(self, up: "Upstream") -> bool:
        timeout = aiohttp.ClientTimeout(sock_connect=5.0, total=self._timeout)
        try:
            async with self._session.head(
                up.base_url, timeout=timeout, allow_redirects=False
            ):
                # Any complete HTTP response (any status) means it's alive.
                return True
        except asyncio.CancelledError:
            raise
        except Exception:
            return False

    async def _probe_loop(self) -> None:
        while not self._closing:
            try:
                await asyncio.sleep(self._interval)
                await self.probe_once()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in upstream health probe loop")

    async def stop(self) -> None:
        self._closing = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._mm is not None:
            self._mm.close()
            self._mm = None
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
