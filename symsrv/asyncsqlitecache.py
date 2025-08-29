#!/usr/bin/python
# pylint: disable=C0114,C0115,C0116,C0301

from __future__ import annotations

import asyncio
import errno
import fcntl
import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import aiosqlite
import aiofiles
import aiofiles.os

from .cachekey import CacheKey, cache_key_from_digest, cache_key_from_hexdigest
from .singleflight import UDSSingleFlight
from .common import StreamingWriter

logger = logging.getLogger("uvicorn.error")

CHUNK_SIZE = 8 * 1024 * 1024

import asyncio
import contextlib
from typing import Awaitable, Callable, Optional

import aiosqlite

InitHook = Optional[Callable[[aiosqlite.Connection], Awaitable[None]]]


def _is_aiosqlite_valueerror(e: ValueError):
    known = {
        "no active connection",
        "Connection closed",
    }
    if e.args and e.args[0] in known:
        return True
    return False


class AioSqlitePool:
    """
    An asyncio-friendly connection pool for aiosqlite.

    Notes:
    - Connections are independent threads internally (aiosqlite runs sqlite3 ops
      in a thread), so concurrency is achieved by multiple connections.
    - SQLite still serializes writers; use WAL + busy_timeout to reduce lock pain.
    - Prefer .transaction(write=True) for writes (BEGIN IMMEDIATE) to fail fast
      on lock contention and avoid long tail deadlocks.
    """

    def __init__(
        self,
        database: str,
        *,
        min_size: int = 1,
        max_size: int = 8,
        uri: bool = False,
        init: InitHook = None,
        **connect_kwargs,
    ):
        assert max_size >= 1 and 0 <= min_size <= max_size
        self._db = database
        self._uri = uri
        self._init = init
        self._connect_kwargs = connect_kwargs
        self._min_size = min_size
        self._max_size = max_size

        self._queue: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue()
        self._total = 0
        self._total_lock = asyncio.Lock()
        self._closed = False
        self._all_conns: set[aiosqlite.Connection] = set()

    async def start(self) -> "AioSqlitePool":
        for _ in range(self._min_size):
            conn = await self._new_connection()
            await self._queue.put(conn)
        return self

    async def close(self) -> None:
        self._closed = True
        # Drain whatever is idle first
        while not self._queue.empty():
            conn = self._queue.get_nowait()
            await self._really_close(conn)
        # Anything still checked-out will be closed on release
        # Wait for all to close
        while self._all_conns:
            await asyncio.sleep(0)  # let releasers run

    async def _new_connection(self) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(self._db, uri=self._uri, **self._connect_kwargs)
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA cache_size = -8000;")  # 8MB per connection
        await conn.execute("PRAGMA temp_store = MEMORY;")
        await conn.execute("PRAGMA busy_timeout = 5000;")  # ms
        # Optional user hook (e.g., row_factory, temp_store, cache_size, etc.)
        if self._init:
            await self._init(conn)
        await conn.commit()
        self._all_conns.add(conn)
        return conn

    async def _maybe_create(self) -> Optional[aiosqlite.Connection]:
        async with self._total_lock:
            if self._total < self._max_size:
                self._total += 1
                return await self._new_connection()
        return None

    @contextlib.asynccontextmanager
    async def acquire(self) -> AsyncIterator[aiosqlite.Connection]:
        """
        Acquire a connection; returns an aiosqlite.Connection.
        Always release via async with.
        """
        if self._closed:
            raise RuntimeError("Pool is closed")

        conn: Optional[aiosqlite.Connection] = None

        try:
            conn = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            conn = await self._maybe_create()
            if conn is None:  # pool at capacity - wait
                conn = await self._queue.get()

        try:
            yield conn
        finally:
            # If pool is closed, actually close the connection
            if self._closed:
                await self._really_close(conn)
            else:
                await self._queue.put(conn)

    async def _really_close(self, conn: aiosqlite.Connection) -> None:
        if conn in self._all_conns:
            self._all_conns.remove(conn)
        try:
            await conn.close()
        finally:
            async with self._total_lock:
                self._total -= 1

    @contextlib.asynccontextmanager
    async def transaction(
        self, *, write: bool = False
    ) -> AsyncIterator[aiosqlite.Connection]:
        """
        Convenience transaction context:
          - write=False: BEGIN (deferred)
          - write=True : BEGIN IMMEDIATE (locks for write up-front)
        Usage:
            async with pool.transaction(write=True) as conn:
                await conn.execute(...)
        """
        async with self.acquire() as conn:
            try:
                if write:
                    await conn.execute("BEGIN IMMEDIATE;")
                else:
                    await conn.execute("BEGIN;")
                yield conn
                await conn.commit()
            except Exception:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise


@dataclass
class _SqliteStreamState:
    tmp_path: Path
    final_path: Path
    f: aiofiles.threadpool.binary.AsyncFileIO
    total: int
    etag: Optional[str]
    last_modified: Optional[str]
    ttl: Optional[float]
    status_code: int
    sha256: hashlib._Hash


class _SqliteStreamingWriter:
    def __init__(
        self, cache: "AsyncSqliteCache", key: CacheKey, st: _SqliteStreamState
    ):
        self._cache = cache
        self._key = key
        self._st = st
        self._closed = False

    async def write(self, chunk: bytes) -> None:
        if self._closed:
            return
        await self._st.f.write(chunk)
        self._st.total += len(chunk)
        self._st.sha256.update(chunk)

    async def commit(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._st.f.flush()
            fd = self._st.f.fileno()
            await asyncio.to_thread(os.fsync, fd)
        finally:
            await self._st.f.close()

        self._st.final_path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(os.replace, self._st.tmp_path, self._st.final_path)

        content_sha256 = self._st.sha256.digest()
        etag = self._st.etag or '"' + self._st.sha256.hexdigest() + '"'
        now = time.time()

        # Single transaction to upsert cache row
        async with self._cache.connection_pool().acquire() as conn:
            await conn.execute(
                """
                INSERT OR REPLACE INTO cache_entries (
                    cache_key, url, upstream_name, status_code,
                    created_at, accessed_at, ttl, content_length,
                    etag, last_modified, content_sha256
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self._key.hash_key_raw,
                    self._key.full_url,
                    self._key.upstream_name,
                    self._st.status_code,
                    now,
                    now,
                    (
                        self._st.ttl
                        if self._st.ttl is not None
                        else self._cache.default_ttl
                    ),
                    self._st.total,
                    etag,
                    await self._cache._parse_http_date(self._st.last_modified),
                    content_sha256,
                ),
            )
            await conn.commit()

    async def abort(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._st.f.close()
        except Exception:
            logger.exception("Can't close file")
            pass
        try:
            await aiofiles.os.remove(self._st.tmp_path)
        except Exception:
            logger.exception("Can't remove temporary file")
            pass


async def _remove_files(files: list[Path]) -> None:
    for filepath in files:
        if not filepath.is_file():
            continue
        try:
            await aiofiles.os.unlink(filepath)
        except (FileNotFoundError, OSError) as e:
            logger.error(f"Could not remove {filepath}: {e}")


@dataclass
class CacheMetadata:
    key: CacheKey
    created_at: float
    accessed_at: float
    status_code: int
    etag: Optional[str]
    ttl: float
    last_modified: Optional[str]
    size: int = 0  # File size in bytes


class AsyncSqliteCache:
    def __init__(
        self,
        cache_dir: str,
        default_ttl: Optional[float] = None,
        connection_pool_size_per_worker: int = 3,
        batch_update_interval: float = 30,
        cleanup_interval: float = 300,
        subdirectory_depth: int = 2,
        subdirectory_width: int = 2,
    ):
        """
        Initialize the async SQLite cache.

        Args:
            cache_dir: Directory to store cache files and database
            default_ttl: Default time-to-live in seconds for cached items
            connection_pool_size_per_worker: Number of connections per worker process
            batch_update_interval: How often to batch update access times (seconds)
            subdirectory_depth: Number of subdirectory levels for data files
            subdirectory_width: Number of hex characters per subdirectory level
        """
        self.cache_dir = Path(cache_dir)
        self.db_path = self.cache_dir / "metadata.db"
        self.data_dir = self.cache_dir / "data"
        self.temp_dir = self.cache_dir / "temp"
        self.default_ttl = float(default_ttl if default_ttl else 8 * 60 * 60)
        self.connection_pool_size = int(connection_pool_size_per_worker)
        self.batch_update_interval = float(batch_update_interval)
        self.cleanup_interval = float(cleanup_interval)
        self.subdirectory_depth = int(subdirectory_depth)
        self.subdirectory_width = int(subdirectory_width)

        # Connection pool and batch update tracking
        self._connection_pool: Optional[AioSqlitePool] = None
        self._closing = False

        # Batch access time updates
        self._lock_path = Path(cache_dir) / ".cleanup_lock"
        self._access_updates: Dict[bytes, float] = {}
        self._access_lock = asyncio.Lock()
        self._batch_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self):
        """Initialize database and connection pool if not already done."""
        # Create directories
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        self.temp_dir.mkdir(exist_ok=True)

        # Initialize database schema if it doesn't exist
        await self._init_database()

        # Create connection pool (using a simple list for now)
        self._connection_pool = await AioSqlitePool(
            str(self.db_path),
            min_size=self.connection_pool_size,
            max_size=self.connection_pool_size,
        ).start()

        # Per-worker access update loop
        self._batch_task = asyncio.create_task(self._batch_update_loop())

        if self._try_acquire_cleanup_lock():
            # Singleton cleanup
            self._cleanup_task = asyncio.create_task(self._singleton_periodic())

            # Remove any stale temporaries
            temp_files = list(
                filter(lambda x: x.is_file(), list(self.temp_dir.rglob("*")))
            )
            if temp_files:
                logger.info("Removed %d stale temporary files", len(temp_files))
                await _remove_files(temp_files)

        return self

    def connection_pool(self) -> AioSqlitePool:
        assert self._connection_pool is not None
        return self._connection_pool

    def _try_acquire_cleanup_lock(self) -> bool:
        """
        Try to acquire the cleanup lock using fcntl.
        Returns True if we got the lock, False otherwise.
        """
        try:
            # Open or create the lock file
            self._lock_file = open(self._lock_path, "w")

            # Try to acquire the lock without blocking
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            # If we get here, we got the lock
            # Write our PID to the file for debugging
            self._lock_file.seek(0)
            self._lock_file.write(str(os.getpid()))
            self._lock_file.flush()

            return True

        except (IOError, OSError) as e:
            # errno.EACCES or errno.EAGAIN mean the file is locked
            if e.errno in (errno.EACCES, errno.EAGAIN):
                if hasattr(self, "_lock_file"):
                    self._lock_file.close()
                return False
            raise  # Re-raise other errors

    async def _init_database(self):
        """Initialize the database schema."""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS cache_entries (
            cache_key BLOB PRIMARY KEY,
            url TEXT NOT NULL,
            upstream_name TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            created_at REAL NOT NULL,
            accessed_at REAL NOT NULL,
            ttl REAL NOT NULL,
            content_length INTEGER DEFAULT 0,
            etag TEXT,
            last_modified REAL,
            content_sha256 BLOB,

            CHECK (length(cache_key) = 32),
            CHECK (status_code BETWEEN 100 AND 599),
            CHECK (created_at > 0),
            CHECK (accessed_at >= created_at),
            CHECK (ttl > 0),
            CHECK (content_length >= 0),
            CHECK (content_sha256 IS NULL OR length(content_sha256) = 32),
            CHECK ((status_code = 200 AND content_length > 0) OR
                   (status_code != 200 AND content_length = 0))
        );

        CREATE INDEX IF NOT EXISTS idx_expiration ON cache_entries(
            CASE WHEN status_code = 200 THEN accessed_at + ttl
                 ELSE created_at + ttl END
        );
        CREATE INDEX IF NOT EXISTS idx_upstream_status ON cache_entries(upstream_name, status_code);
        CREATE INDEX IF NOT EXISTS idx_accessed_at ON cache_entries(accessed_at);
        CREATE INDEX IF NOT EXISTS idx_cache_key ON cache_entries(cache_key);
        """

        # Use a temporary connection for schema setup
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.executescript(schema_sql)
            await conn.commit()

    def _get_subdirectory_path(self, key_hash: str, base_dir: Path) -> Path:
        """Creates a subdirectory path based on the key hash."""
        w = self.subdirectory_width
        d = self.subdirectory_depth

        # Build the full path
        path = base_dir.joinpath(*(key_hash[i : i + w] for i in range(0, d * w, w)))

        # Create if it doesn't exist
        if not path.is_dir():
            path.mkdir(parents=True, exist_ok=True)
        return path

    def _get_data_file_path(self, key: CacheKey) -> Path:
        """Get the data file path for a given cache key."""
        data_subdir = self._get_subdirectory_path(key.hash_key, self.data_dir)
        return data_subdir / key.hash_key

    async def _parse_http_date(self, http_date: Optional[str]) -> Optional[float]:
        """Parse HTTP date string to Unix timestamp."""
        if not http_date:
            return None

        try:
            # Parse RFC 2822 format: "Wed, 23 Apr 2025 22:49:15 GMT"
            dt = datetime.strptime(http_date, "%a, %d %b %Y %H:%M:%S GMT")
            return dt.timestamp()
        except ValueError:
            logger.warning(f"Failed to parse HTTP date: {http_date}")
            return None

    async def _format_http_date(self, timestamp: Optional[float]) -> Optional[str]:
        """Format Unix timestamp to HTTP date string."""
        if timestamp is None:
            return None
        try:
            dt = datetime.fromtimestamp(timestamp)
            return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
        except (ValueError, OSError):
            return None

    async def get_many_metadata(self, keys: List[CacheKey]) -> List[CacheMetadata]:
        """Retrieve metadata for one or more cached items"""
        if not keys:
            return []

        try:
            async with self.connection_pool().acquire() as conn:
                placeholders = ",".join("?" for _ in keys)
                rows = await conn.execute_fetchall(
                    f"""
                    SELECT cache_key, url, upstream_name, status_code,
                           created_at, accessed_at, ttl, content_length, etag,
                           last_modified
                    FROM cache_entries
                    WHERE cache_key IN ({placeholders})
                    """,
                    [key.hash_key_raw for key in keys],
                )

                if not rows:
                    return []

            metas = []
            for row in rows:
                (
                    cache_key,
                    url,
                    upstream_name,
                    status_code,
                    created_at,
                    accessed_at,
                    ttl,
                    content_length,
                    etag,
                    last_modified_ts,
                ) = row

                key = CacheKey(upstream_name, url, cache_key, cache_key.hex())

                last_modified = await self._format_http_date(last_modified_ts)

                metas.append(
                    CacheMetadata(
                        key=key,
                        created_at=created_at,
                        accessed_at=accessed_at,
                        status_code=status_code,
                        ttl=ttl,
                        etag=etag,
                        last_modified=last_modified,
                        size=content_length,
                    )
                )

            return metas
        except Exception as e:
            logger.exception(f"Error getting metadata: {e}")
            return []

    async def get_metadata(self, key: CacheKey) -> Optional[CacheMetadata]:
        """Retrieve metadata for a cached item if it exists and is not expired."""
        try:
            async with self.connection_pool().acquire() as conn:
                rows = await conn.execute_fetchall(
                    """
                    SELECT url, status_code, created_at, accessed_at, ttl,
                           content_length, etag, last_modified
                    FROM cache_entries
                    WHERE cache_key = ?
                """,
                    (key.hash_key_raw,),
                )

            if not rows:
                return None

            (
                url,
                status_code,
                created_at,
                accessed_at,
                ttl,
                content_length,
                etag,
                last_modified_ts,
            ) = list(rows)[0]

            assert url == key.full_url

            # Check expiration
            now = time.time()
            if status_code == 200:
                # For 200 responses, use access time + TTL
                if (now - accessed_at) > ttl:
                    return None
            else:
                # For non-200 responses, use create time + TTL
                if (now - created_at) > ttl:
                    return None

            # Format last_modified back to HTTP date
            last_modified = await self._format_http_date(last_modified_ts)

            return CacheMetadata(
                key=key,
                created_at=created_at,
                accessed_at=accessed_at,
                status_code=status_code,
                etag=etag,
                ttl=ttl,
                last_modified=last_modified,
                size=content_length,
            )
        except Exception as e:
            logger.exception(f"Error getting metadata for {key.full_url}: {e}")
            return None

    async def open_write_stream(
        self,
        key: CacheKey,
        status_code: int,
        etag: Optional[str],
        last_modified: Optional[str],
        ttl: Optional[float],
    ) -> StreamingWriter:
        if status_code != 200:
            raise RuntimeError(
                "begin_streaming is intended for 200 responses with bodies"
            )

        # Ensure directories + compute final paths
        data_path = (
            self._get_subdirectory_path(key.hash_key, self.data_dir) / key.hash_key
        )
        data_path.parent.mkdir(parents=True, exist_ok=True)

        tmp = self.temp_dir / (
            key.hash_key + f".tmp-{os.getpid()}-{int(time.time() * 1e6)}"
        )
        tmp.parent.mkdir(parents=True, exist_ok=True)

        f = await aiofiles.open(tmp, "wb", buffering=0)

        st = _SqliteStreamState(
            tmp_path=tmp,
            final_path=data_path,
            f=f,
            total=0,
            etag=etag,
            last_modified=last_modified,
            ttl=ttl,
            status_code=status_code,
            sha256=hashlib.sha256(),
        )
        return _SqliteStreamingWriter(self, key, st)

    async def set_negative_cache(
        self, key: CacheKey, status_code: int, ttl: Optional[float] = None
    ) -> None:
        """Store data in the cache from an async stream."""
        assert status_code != 200

        # Store metadata in database
        now = time.time()
        try:
            async with self.connection_pool().acquire() as conn:
                await conn.execute(
                    """
                    INSERT OR REPLACE INTO cache_entries (
                        cache_key, url, upstream_name, status_code,
                        created_at, accessed_at, ttl, content_length,
                        etag, last_modified, content_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        key.hash_key_raw,
                        key.full_url,
                        key.upstream_name,
                        status_code,
                        now,
                        now,
                        ttl or self.default_ttl,
                        0,
                        None,
                        None,
                        None,
                    ),
                )
                await conn.commit()
        except Exception as e:
            logger.error(f"Error storing negative cache entry for {key.full_url}: {e}")
            raise

    async def get_nginx_redirect_path(self, metadata: CacheMetadata) -> Optional[str]:
        """Get the NGINX X-Accel-Redirect response."""
        try:
            # Check expiration and file existence
            now = time.time()
            if metadata.status_code == 200:
                if (now - metadata.accessed_at) > metadata.ttl:
                    return None
            else:
                if (now - metadata.created_at) > metadata.ttl:
                    return None

            full_data_path = self._get_data_file_path(metadata.key)
            data_file_path = full_data_path.relative_to(self.data_dir)

            # Verify data file exists
            if not full_data_path.exists():
                return None

            # Mark as accessed
            await self._mark_accessed(metadata.key)

            # Return nginx redirect path
            redirect_path = f"/_symsrv_cache/{data_file_path}"
            return redirect_path

        except Exception as e:
            logger.error(
                f"Error getting nginx redirect for {metadata.key.full_url}: {e}"
            )
            return None

    async def get_streaming(self, key: CacheKey) -> Optional[AsyncIterator[bytes]]:
        """Retrieve data from cache as an async stream."""
        try:
            async with self.connection_pool().acquire() as conn:
                rows = await conn.execute_fetchall(
                    """
                    SELECT status_code, created_at, accessed_at, ttl
                    FROM cache_entries
                    WHERE cache_key = ?
                """,
                    (key.hash_key_raw,),
                )

                if not rows:
                    return None

            status_code, created_at, accessed_at, ttl = list(rows)[0]

            # Check expiration
            now = time.time()
            if status_code == 200:
                if (now - accessed_at) > ttl:
                    return None
            else:
                if (now - created_at) > ttl:
                    return None

            full_data_path = self._get_data_file_path(key)

            # Get full path and verify file exists
            if not full_data_path.exists():
                return None

            # Mark as accessed
            await self._mark_accessed(key)

            # Return stream generator
            async def stream_generator():
                async with aiofiles.open(full_data_path, "rb") as f:
                    while chunk := await f.read(CHUNK_SIZE):
                        yield chunk

            return stream_generator()

        except Exception as e:
            logger.error(f"Error getting stream for {key.full_url}: {e}")
            return None

    async def delete(self, key: CacheKey) -> None:
        """Delete a cached item and its metadata."""
        try:
            async with self.connection_pool().acquire() as conn:
                await conn.execute(
                    "DELETE FROM cache_entries WHERE cache_key = ?", (key.hash_key_raw,)
                )

            # Delete data file if it exists
            data_file_path = self._get_data_file_path(key)
            if data_file_path:
                full_data_path = self.data_dir / data_file_path
                await _remove_files([full_data_path])

        except Exception as e:
            logger.error(f"Error deleting cache entry for {key.full_url}: {e}")

    async def _mark_accessed(self, key: CacheKey) -> None:
        """Mark a cache entry as accessed (batched update)."""
        async with self._access_lock:
            self._access_updates[key.hash_key_raw] = time.time()

    async def _commit_atime_updates(self) -> None:
        # Get current batch and clear the queue
        async with self._access_lock:
            updates = dict(self._access_updates)
            self._access_updates.clear()

        if not updates:
            return

        try:
            async with self.connection_pool().transaction(write=True) as conn:
                # Batch update access times
                await conn.executemany(
                    """
                                       UPDATE cache_entries
                                       SET accessed_at = ?
                                       WHERE cache_key = ?
                                       """,
                    [
                        (timestamp, cache_key)
                        for cache_key, timestamp in updates.items()
                    ],
                )
            logger.debug(f"Updated access times for {len(updates)} entries")
        except asyncio.CancelledError:
            raise
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error updating access times: {e}")
            raise

    async def _cleanup_missing_metadata(self) -> None:
        """Clean up items with invalid metadata from the cache."""
        # Process files in smaller batches
        batch_size = 500
        total_removed = 0

        try:
            # Check for data files with missing metadata
            data_files = list(
                filter(lambda x: x.is_file(), list(self.data_dir.rglob("*")))
            )
            logger.debug(
                "Found %d data files, checking for missing metadata...", len(data_files)
            )

            for i in range(0, len(data_files), batch_size):
                batch = data_files[i : i + batch_size]
                remove_queue = []
                key_data_pairs = []

                for data_path in batch:
                    try:
                        cache_key = cache_key_from_hexdigest(data_path.stem)
                    except ValueError as e:
                        logger.error("Invalid cache file %s: %s", data_path, e)
                        remove_queue.append(data_path)
                        continue
                    lock = UDSSingleFlight(cache_key)
                    if await lock.is_locked():
                        continue
                    key_data_pairs.append((cache_key.hash_key_raw, str(data_path)))

                async with self.connection_pool().acquire() as conn:
                    await conn.execute(
                        """
                        CREATE TEMP TABLE IF NOT EXISTS _to_check(
                            key BLOB,
                            path TEXT NOT NULL
                        );
                    """
                    )
                    await conn.execute("DELETE FROM _to_check;")
                    await conn.executemany(
                        "INSERT INTO _to_check(key, path) VALUES (?, ?)", key_data_pairs
                    )
                    sql = """
                    SELECT t.path
                    FROM _to_check as t
                    LEFT JOIN cache_entries as c
                        ON c.cache_key = t.key
                    WHERE c.cache_key IS NULL
                    """
                    rows = await conn.execute_fetchall(sql)
                    await conn.commit()

                if not rows:
                    continue

                remove_queue.extend([Path(r[0]) for r in rows])

                if remove_queue:
                    # Remove anything already deemed invalid
                    await _remove_files(remove_queue)
                    total_removed += len(remove_queue)

                await asyncio.sleep(0.05)

        except asyncio.CancelledError:
            raise
        except ValueError:
            raise
        except Exception as e:
            logger.error("Error during missing metadata sweep: %s", e, exc_info=True)
            raise

        if total_removed:
            logger.info("Removed %d invalid cache data files", total_removed)

    async def _cleanup_missing_data(self) -> None:
        """Clean up items with missing data payloads from the cache."""
        batch_size = 500
        try:
            bad_keys = []
            async with self.connection_pool().acquire() as conn:
                sql = """
                SELECT cache_key FROM cache_entries WHERE content_length > 0
                """
                async with conn.execute(sql) as cur:
                    while True:
                        rows = await cur.fetchmany(batch_size)
                        if not rows:
                            break

                        all_keys = [cache_key_from_digest(row[0]) for row in rows]
                        entries = [
                            (cache_key, self._get_data_file_path(cache_key))
                            for cache_key in all_keys
                        ]

                        if entries:
                            for cache_key, path in entries:
                                if not path.is_file():
                                    bad_keys.append((cache_key.hash_key_raw,))

                        await asyncio.sleep(0.1)
                await conn.commit()

            if bad_keys:
                for i in range(0, len(bad_keys), batch_size):
                    batch = bad_keys[i : i + batch_size]
                    async with self.connection_pool().transaction(write=True) as conn:
                        sql = """
                        DELETE FROM cache_entries WHERE cache_key IN (?)
                        """
                        await conn.executemany(sql, batch)
                        await conn.commit()
                    await asyncio.sleep(0.1)

                logger.info("Removed %d entries with missing data", len(bad_keys))

        except asyncio.CancelledError:
            raise
        except ValueError:
            raise
        except Exception as e:
            logger.error("Error during missing data sweep: %s", e, exc_info=True)
            raise

    async def _cleanup_expired(self) -> None:
        """Clean up expired items from the cache."""
        batch_size = 500
        now = time.time()

        try:
            while True:
                async with self.connection_pool().acquire() as conn:
                    sql = """
                    WITH expired AS (
                        SELECT rowid FROM cache_entries
                        WHERE (status_code = 200  AND accessed_at + ttl < ?)
                           OR (status_code != 200 AND created_at  + ttl < ?)
                        LIMIT ?
                    )
                    DELETE FROM cache_entries
                    WHERE rowid IN (SELECT rowid FROM expired)
                    RETURNING cache_key
                    """
                    rows = await conn.execute_fetchall(sql, (now, now, batch_size))
                    await conn.commit()

                if rows is None:
                    rows = []

                remove_keys = [cache_key_from_digest(row[0]) for row in rows]
                remove_queue = [self._get_data_file_path(key) for key in remove_keys]

                if remove_queue:
                    # Remove anything already deemed invalid
                    await _remove_files(remove_queue)
                    logger.info(
                        "Removed %d expired cache data files", len(remove_queue)
                    )
                else:
                    # No batches, break out of this run
                    break

                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            raise
        except ValueError:
            raise
        except Exception as e:
            logger.error("Error during expired key sweep: %s", e, exc_info=True)
            raise

    async def _singleton_periodic(self) -> None:
        """Periodically clean up expired/invalid items."""
        while not self._closing:
            try:
                logger.debug("Starting cleanup tasks")
                await self._cleanup_missing_metadata()
                await self._cleanup_missing_data()
                await self._cleanup_expired()
                logger.debug("Cleanup tasks complete")
            except asyncio.CancelledError:
                break
            except ValueError as e:
                if _is_aiosqlite_valueerror(e):
                    logger.error("SQLite connection died during cleanup task")
                else:
                    logger.exception("Error during cache cleanup: %s", e)
                break
            except Exception as e:
                logger.exception("Error during cache cleanup: %s", e)
                break

            await asyncio.sleep(self.cleanup_interval)

        logger.info("Cleanup task exiting")

    async def _batch_update_loop(self) -> None:
        """Background task to batch update access times."""
        while not self._closing:
            try:
                await asyncio.sleep(self.batch_update_interval)
                await self._commit_atime_updates()
            except asyncio.CancelledError:
                break
            except ValueError as e:
                if _is_aiosqlite_valueerror(e):
                    logger.error("SQLite connection died during access time update")
                else:
                    logger.exception("Error during access time update: %s", e)
                break
            except Exception as e:
                logger.exception("Error during access time update: %s", e)

    async def close(self) -> None:
        """Gracefully shut down the cache."""
        self._closing = True

        # Cancel batch update task
        if self._batch_task:
            self._batch_task.cancel()
            try:
                await self._batch_task
            except asyncio.CancelledError:
                pass

        # Process any remaining access updates
        await self._commit_atime_updates()

        if self._connection_pool is not None:
            await self._connection_pool.close()
