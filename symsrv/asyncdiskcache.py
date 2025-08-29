import asyncio
import aiofiles
import aiofiles.os
import errno
import fcntl
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional, Dict, AsyncIterator, Tuple
from dataclasses import dataclass
import hashlib

logger = logging.getLogger("uvicorn.error")

@dataclass
class CacheMetadata:
    created_at: float
    ttl: Optional[float]
    tags: Dict[str, Any]
    size: int = 0  # File size in bytes

class AsyncDiskCache:
    _cleanup_task_created = False

    def __init__(
        self,
        cache_dir: str,
        default_ttl: Optional[float] = None,
        cleanup_interval: float = 28800, # Run cleanup every hour by default
        subdirectory_depth: int = 2,     # Number of subdirectory levels
        subdirectory_width: int = 2      # Characters per subdirectory level
    ):
        """
        Initialize the async disk cache.

        Args:
            cache_dir: Directory to store cache files
            default_ttl: Default time-to-live in seconds for cached items
            cleanup_interval: How often to run background cleanup (in seconds)
            subdirectory_depth: Number of subdirectory levels to create
            subdirectory_width: Number of hex characters to use per subdirectory
        """
        self.cache_dir = Path(cache_dir)
        self.default_ttl = default_ttl
        self.subdirectory_depth = subdirectory_depth
        self.subdirectory_width = subdirectory_width

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir = self.cache_dir / "metadata"
        self.metadata_dir.mkdir(exist_ok=True)
        self.data_dir = self.cache_dir / "data"
        self.data_dir.mkdir(exist_ok=True)

        self._closing = False
        self._atime_queue = asyncio.Queue()
        self._atime_task = asyncio.create_task(self._background_atime())

        # Create task for background cleanup
        self.lock_path = Path(cache_dir) / ".cleanup_lock"
        if self._try_acquire_cleanup_lock():
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup(cleanup_interval))
        else:
            self._cleanup_task = None

    def __del__(self):
        self._closing = True
        self._atime_queue.shutdown()
        if hasattr(self, '_lock_file'):
            try:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                self._lock_file.close()
            except:
                pass

    def _verify_lock(self) -> bool:
        """Verify we still hold the lock."""
        try:
            # Try to get the lock again - will fail if someone else has it
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (IOError, OSError) as e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                return False
            raise

    def _try_acquire_cleanup_lock(self) -> bool:
        """
        Try to acquire the cleanup lock using fcntl.
        Returns True if we got the lock, False otherwise.
        """
        try:
            # Open or create the lock file
            self._lock_file = open(self.lock_path, 'w')

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
                if hasattr(self, '_lock_file'):
                    self._lock_file.close()
                return False
            raise  # Re-raise other errors

    def _get_subdirectory_path(self, key_hash: str, base_dir: Path) -> Path:
        """Creates a subdirectory path based on the key hash."""
        parts = [
            key_hash[i:i + self.subdirectory_width]
            for i in range(0, self.subdirectory_depth * self.subdirectory_width, self.subdirectory_width)
        ]

        current_path = base_dir
        for part in parts:
            current_path = current_path / part
            current_path.mkdir(exist_ok=True)

        return current_path

    def _get_paths(self, key: str) -> tuple[Path, Path]:
        """Get the paths for data and metadata files for a given key."""
        key_hash = hashlib.sha256(key.encode()).hexdigest()

        data_subdir = self._get_subdirectory_path(key_hash, self.data_dir)
        meta_subdir = self._get_subdirectory_path(key_hash, self.metadata_dir)

        return (
            data_subdir / key_hash,
            meta_subdir / f"{key_hash}.json"
        )

    def _get_paths_from_hash(self, key_hash: str) -> tuple[Path, Path]:
        """Get the paths for data and metadata files for a given key hash."""
        data_subdir = self._get_subdirectory_path(key_hash, self.data_dir)
        meta_subdir = self._get_subdirectory_path(key_hash, self.metadata_dir)

        return (
            data_subdir / key_hash,
            meta_subdir / f"{key_hash}.json"
        )

    async def get_metadata(self, key: str) -> Optional[CacheMetadata]:
        """Retrieve metadata for a cached item if it exists and is not expired."""
        _, meta_path = self._get_paths(key)

        try:
            async with aiofiles.open(meta_path, 'r') as f:
                data = json.loads(await f.read())
                metadata = CacheMetadata(**data)
                return metadata
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    async def get_file_info(self, key: str) -> Optional[Tuple[float, int]]:
        """Get modification time and size for a cached item."""
        data_path, _ = self._get_paths(key)
        try:
            stat = await asyncio.to_thread(os.stat, data_path)
            return (stat.st_mtime, stat.st_size)
        except FileNotFoundError:
            return None

    async def set_streaming(
        self,
        key: str,
        data_stream: AsyncIterator[bytes],
        ttl: Optional[float] = None,
        tags: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Store data in the cache from an async stream.

        Args:
            key: Cache key
            data_stream: Async iterator yielding bytes
            ttl: Time-to-live in seconds
            tags: Optional metadata tags to store with the cached item
        """
        data_path, meta_path = self._get_paths(key)

        # Stream data to file first to get final size
        total_size = 0
        async with aiofiles.open(data_path, 'wb') as f:
            async for chunk in data_stream:
                await f.write(chunk)
                total_size += len(chunk)

        # Write metadata after we have the final size
        metadata = CacheMetadata(
            created_at=time.time(),
            ttl=ttl if ttl is not None else self.default_ttl,
            tags=tags or {},
            size=total_size
        )

        meta_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(meta_path, 'w') as f:
            await f.write(json.dumps(vars(metadata)))

    async def get_nginx_redirect_path(self, key: str) -> str:
        """
        Get the NGINX X-Accel-Redirect response.

        Returns None if item doesn't exist or is expired.
        """
        data_path, meta_path = self._get_paths(key)

        # Is the data file missing?
        if not data_path.is_file():
            return None

        # Touch the metadata file to update its modify timestamp
        await self._atime_queue.put(meta_path)

        redirect_path = Path("/_symsrv_cache/") / data_path.relative_to(self.data_dir)

        return str(redirect_path)

    async def get_streaming(self, key: str) -> Optional[AsyncIterator[bytes]]:
        """
        Retrieve data from cache as an async stream.

        Returns None if item doesn't exist or is expired.
        """
        data_path, meta_path = self._get_paths(key)

        # Is the data file missing?
        if not data_path.is_file():
            return None

        # Touch the metadata file to update its modify timestamp
        await self._atime_queue.put(meta_path)

        # Open the data file and stream it
        async def stream_generator():
            async with aiofiles.open(data_path, 'rb') as f:
                while chunk := await f.read(8192):  # 8KB chunks
                    yield chunk

        return stream_generator()

    async def delete(self, key: str) -> None:
        """Delete a cached item and its metadata."""
        data_path, meta_path = self._get_paths(key)

        try:
            await asyncio.gather(
                asyncio.to_thread(os.unlink, data_path),
                asyncio.to_thread(os.unlink, meta_path)
            )

            # Try to remove empty parent directories
            for path in (data_path.parent, meta_path.parent):
                try:
                    if not any(path.iterdir()):
                        await asyncio.to_thread(os.rmdir, path)
                except (FileNotFoundError, OSError):
                    pass

        except FileNotFoundError:
            pass

    async def _background_atime(self) -> None:
        while not self._closing:
            try:
                file_path = await self._atime_queue.get()
            except asyncio.QueueShutDown:
                logger.info("Access time queue shut down")
                break

            if file_path is None:
                continue

            try:
                async with aiofiles.open(file_path, 'a') as f:
                    pass
            except Exception as e:
                logger.warning(f"Could not update access time for {file_path}: {e}")

        logger.info("Access time task exiting")

    async def _periodic_cleanup(self, interval: float) -> None:
        """Periodically clean up expired items."""
        while not self._closing:
            try:
                # Verify we still have the lock before cleaning
                if not self._verify_lock():
                    logger.error("Lost cleanup lock, stopping cleanup task")
                    break

                logger.info("Starting cleanup tasks")
                await self._cleanup_invalid_metadata()
                await self._cleanup_expired()
                logger.info("Cleanup tasks complete")
            except Exception as e:
                logger.error(f"Error during cache cleanup: {e}")

            await asyncio.sleep(interval)
        logger.info("Cleanup task exiting")

    async def _remove_files(self, files: list[Path]) -> None:
        for filepath in files:
            if not filepath.is_file():
                continue
            try:
                await aiofiles.os.unlink(filepath)
            except (FileNotFoundError, OSError) as e:
                logger.error(f"Could not remove {filepath}: {e}")

    async def _cleanup_invalid_metadata(self) -> None:
        """Clean up items with invalid metadata from the cache."""
        now = time.time()

        # Process files in smaller batches
        batch_size = 50  # Reduced from previous 100

        try:
            # Check for data files with missing metadata
            data_files = list(filter(lambda x: x.is_file(), list(self.data_dir.rglob("*"))))
            logger.info(f"Found {len(data_files)} data files, checking for missing metadata...")
            for i in range(0, len(data_files), batch_size):
                batch = data_files[i:i + batch_size]
                # Process one file at a time instead of gathering
                for data_path in batch:
                    try:
                        meta_path = self._get_paths_from_hash(data_path.stem)[1]

                        if not meta_path.is_file():
                            logger.info(f"Removing cache entry with missing metadata {data_path}")
                            await self._remove_files([data_path, meta_path])

                    except (FileNotFoundError, OSError) as e:
                        logger.error(f"Error processing cache file {data_path}: {e}")
                        continue

                # Add a small delay between batches
                await asyncio.sleep(0.01)

        except Exception as e:
            logger.error(f"Error during missing metadata sweep: {e}", exc_info=True)

        try:
            # Check for metadata corruption
            meta_files = list(self.metadata_dir.rglob("*.json"))
            logger.info(f"Found {len(meta_files)} cache files, checking for corrupt metadata...")
            for i in range(0, len(meta_files), batch_size):
                batch = meta_files[i:i + batch_size]
                # Process one file at a time instead of gathering
                for meta_path in batch:
                    try:
                        async with aiofiles.open(meta_path, 'r') as f:
                            remove = False

                            if not remove:
                                try:
                                    data = json.loads(await f.read())
                                except json.JSONDecodeError as e:
                                    remove = True
                                    logger.error(f"Removing unparseable cache entry {meta_path}: {e}")

                            if not remove:
                                try:
                                    metadata = CacheMetadata(**data)
                                except TypeError as e:
                                    remove = True
                                    logger.error(f"Removing incomplete cache entry {meta_path}: {e}")

                            data_path = self._get_paths_from_hash(meta_path.stem)[0]
                            if not remove:
                                if not data_path.is_file():
                                    logger.error(f"Removing cache entry with metadata but no data: {meta_path}")
                                    remove = True

                            if remove:
                                await self._remove_files([data_path, meta_path])

                    except (FileNotFoundError, OSError) as e:
                        logger.error(f"Error processing cache file {meta_path}: {e}")
                        continue

                # Add a small delay between batches
                await asyncio.sleep(0.01)

        except Exception as e:
            logger.error(f"Error during metadata integrity sweep: {e}", exc_info=True)

    async def _cleanup_expired(self) -> None:
        """Clean up all expired items from the cache."""
        now = time.time()

        # Process files in smaller batches
        batch_size = 50  # Reduced from previous 100

        try:
            # Get list of files first
            meta_files = list(self.metadata_dir.rglob("*.json"))
            logger.info(f"Found {len(meta_files)} cache files, checking for expired entries...")

            num_removed = 0

            for i in range(0, len(meta_files), batch_size):
                batch = meta_files[i:i + batch_size]
                # Process one file at a time instead of gathering
                for meta_path in batch:
                    try:
                        async with aiofiles.open(meta_path, 'r') as f:
                            data = json.loads(await f.read())
                            metadata = CacheMetadata(**data)

                            # Metadata TTL, or 365 days if not specified
                            ttl = metadata.ttl if metadata.ttl is not None else (365.0 * 24.0 * 60.0 * 60.0)

                            create_time = metadata.created_at
                            modify_time = meta_path.stat().st_mtime

                            create_expired = (now - create_time) > metadata.ttl
                            modify_expired = (now - modify_time) > metadata.ttl

                            if create_expired and modify_expired:
                                data_path = self._get_paths_from_hash(meta_path.stem)[0]
                                num_removed += 1
                                await self._remove_files([data_path, meta_path])
                    except (FileNotFoundError, json.JSONDecodeError, TypeError, OSError) as e:
                        logger.error(f"Error processing cache file {meta_path}: {e}")
                        continue

                # Add a small delay between batches
                await asyncio.sleep(0.01)

            if num_removed > 0:
                logger.info(f"Removed {num_removed} expired entries")

        except Exception as e:
            logger.error(f"Error during cache cleanup: {e}", exc_info=True)

    async def close(self) -> None:
        """Gracefully shut down the cache."""
        self._closing = True
        if hasattr(self, '_cleanup_task') and self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

            # Release the lock and close the file
            if hasattr(self, '_lock_file'):
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                self._lock_file.close()
