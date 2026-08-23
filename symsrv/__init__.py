#!/usr/bin/python
# pylint: disable=C0114,C0115,C0116,C0301

import asyncio
import os
import ssl, socket
import time
from dataclasses import dataclass
from typing import AsyncGenerator, Dict, List, Optional, Set, Tuple

import aiohttp
import uvicorn
from fastapi import FastAPI, Request, Response
from starlette.responses import StreamingResponse
import tomllib  # Python 3.11+

from .cachekey import CacheKey, make_cache_key
from .asyncsqlitecache import AsyncSqliteCache
from .singleflight import SingleFlight, UDSSingleFlight, NoOpSingleFlight
from .log_queue import install_queue_logging, shutdown_queue_logging
from .config import AppConfig
from .health import UpstreamHealth
from .layout import classify_request
from .upstream import Upstream

import logging

logger = logging.getLogger("uvicorn.error")

app = FastAPI()

# =====================
# Config / constants
# =====================

# TTLs (seconds)
TTL_SUCCESS_DEFAULT = 8 * 60 * 60  # 8h
TTL_FAILURE_DEFAULT = 60 * 60  # 1h for 4xx negative cache

# Upstream read chunk size (aiohttp)
READ_CHUNK = 1 << 23  # 8 MiB

# Log filtering. The access-log filter runs in logging's process-global context,
# so this is populated from AppConfig at startup and kept as a module global.
ignore_access: Set[Tuple[int, str]] = set()


@dataclass(frozen=True)
class Candidate:
    """An upstream paired with the cache key for a specific request."""

    upstream: Upstream
    key: CacheKey


# =====================
# Helpers
# =====================


def _httpdate_now_gmt() -> str:
    return time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())


def is_blacklisted_path(path: str, config: AppConfig) -> bool:
    for part in config.substring_blacklist:
        if part in path:
            logger.debug(
                "Path '%s' rejected; matched substring blacklist '%s'", path, part
            )
            return True
    for suffix in config.suffix_blacklist:
        if path.endswith(suffix):
            logger.debug("Path '%s' rejected; endswith blacklist '%s'", path, suffix)
            return True
    file0 = path.split("/")[1] if "/" in path else path
    if file0 in config.first_path_component_blacklist:
        logger.debug(
            "Path '%s' rejected; first component '%s' in blacklist", path, file0
        )
        return True
    return False


def _build_client_headers(
    etag: Optional[str], last_modified: Optional[str], content_length: Optional[int]
) -> Dict[str, str]:
    h: Dict[str, str] = {}
    if etag:
        h["ETag"] = etag
    if last_modified:
        h["Last-Modified"] = last_modified
    if content_length is not None:
        h["Content-Length"] = str(content_length)
    h.setdefault("Content-Type", "application/octet-stream")
    return h


def _content_length(resp: aiohttp.ClientResponse):
    encoding: Optional[str] = resp.headers.get("Content-Encoding", None)
    length: Optional[str] = resp.headers.get("Content-Length", None)

    if encoding is not None:
        return None

    if length is None:
        return None

    try:
        return int(length)
    except ValueError:
        return None


async def empty_body() -> AsyncGenerator[bytes, None]:
    if False:
        yield b""


async def _close_response(resp: Optional[aiohttp.ClientResponse]):
    if resp is None:
        return
    try:
        resp.release()
    except Exception:
        try:
            resp.close()
        except Exception:
            pass


class CachePump:
    """
    Reads upstream -> writes to cache (writer) -> optionally fans out to a single client via a queue.
    Never blocks on the client. If the client is slow or disconnects, we detach it but continue
    caching to completion, then commit() under shield.
    """

    __slots__ = ("resp", "writer", "read_chunk", "q", "client_alive", "lock", "task")

    def __init__(
        self, resp: aiohttp.ClientResponse, writer, lock: SingleFlight, read_chunk: int
    ):
        self.resp = resp
        self.writer = writer
        self.read_chunk = read_chunk
        self.q: asyncio.Queue[Optional[bytes]] = asyncio.Queue()
        self.lock: SingleFlight = lock
        self.client_alive: Optional[bool] = True

    async def start(self):
        self.task = asyncio.create_task(self._run())
        await self.lock.transfer(self.task)
        await app.state.task_manager.submit_task(self.task)
        return self

    async def _run(self):
        try:
            async for chunk in self.resp.content.iter_chunked(self.read_chunk):
                if self.writer is not None:
                    await self.writer.write(chunk)
                if self.client_alive:
                    await self.q.put(chunk)
                elif self.client_alive == False:
                    # Client went away, but we can continue the caching
                    # process. Drain the queue entirely, once, to release
                    # memory.
                    self.q.shutdown(immediate=True)
                    self.client_alive = None

            if self.writer is not None:
                await self.writer.commit()
        except Exception:
            logging.exception("Unrecognized exception")
            if self.writer is not None:
                try:
                    await self.writer.abort()
                except Exception:
                    pass
            raise
        finally:
            if self.lock:
                await self.lock.release()
            if self.client_alive:
                # Normal completion: signal EOF to client
                await self.q.put(None)
            # Always release the upstream response
            await _close_response(self.resp)

    async def body(self):
        try:
            while True:
                item = await self.q.get()
                if item is None:
                    break
                yield item
        except asyncio.CancelledError:
            # Client went away; detach but keep caching
            self.client_alive = False
            # Don't cancel the pump task; it will finish and commit().
            raise


# =====================
# Startup / shutdown
# =====================


class FilteredAccessLog(logging.Filter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def filter(self, record: logging.LogRecord) -> bool:
        assert isinstance(record.args, tuple)
        filter_match = (record.args[4], record.args[2])
        if filter_match in ignore_access:
            return False
        return True


class BackgroundTaskManager:
    def __init__(self):
        self.task_queue: asyncio.Queue[asyncio.Task] = asyncio.Queue()
        self.active_tasks: Set[asyncio.Task] = set()
        self.processor_task: Optional[asyncio.Task] = None
        self.running: bool = False

    async def start(self):
        self.running = True
        self.processor_task = asyncio.create_task(self._processor())

    async def stop(self):
        self.running = False
        if self.processor_task is not None:
            self.processor_task.cancel()
        self.task_queue.shutdown(True)

    async def submit_task(self, coro):
        await self.task_queue.put(coro)

    async def _pull_available_tasks(self, blocking: bool):
        # No active tasks, wait for new ones
        if blocking:
            coro = await self.task_queue.get()
        else:
            try:
                coro = await self.task_queue.get_nowait()
            except asyncio.QueueEmpty:
                return False
        assert coro is not None
        self.active_tasks.add(coro)
        self.task_queue.task_done()
        return True

    async def _processor(self):
        while self.running:
            try:
                # Always try to get new tasks first
                while await self._pull_available_tasks(blocking=False):
                    pass

                if self.active_tasks:
                    # Wait for any active task to complete
                    done, pending = await asyncio.wait(
                        self.active_tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                        timeout=0.1,  # Short timeout to check for new tasks
                    )

                    # Remove completed tasks
                    self.active_tasks -= done

                    # Process results
                    for task in done:
                        try:
                            await task
                        except Exception as e:
                            logger.exception(f"Task failed: {e}")
                else:
                    await self._pull_available_tasks(blocking=True)

            except asyncio.CancelledError:
                break


@app.on_event("startup")
async def _setup_logging():
    install_queue_logging(microseconds=False)


@app.on_event("startup")
async def _warm_streaming_path():
    """
    Warm up the StreamingResponse path so we don't have imports that get
    delayed until our first StreamingResponse.
    """
    # Touch Starlette helpers that pull in anyio under the hood
    from starlette import concurrency as _c  # noqa: F401
    import anyio  # noqa: F401
    import anyio._backends._asyncio as _backend  # noqa: F401
    import array  # noqa: F401


@app.on_event("startup")
async def startup_event():
    global ignore_access

    logger.debug("Running startup event")

    config = AppConfig.load()
    app.state.config = config

    # HTTP client
    timeouts = aiohttp.ClientTimeout(sock_connect=5.0, sock_read=20.0)
    app.state.session_ssl_ctx = ssl.create_default_context()
    app.state.session_connector = aiohttp.TCPConnector(
        ssl=app.state.session_ssl_ctx,
        use_dns_cache=True,
        ttl_dns_cache=300,
        keepalive_timeout=30,
        limit_per_host=100,
        limit=0,
        family=socket.AF_INET,
        happy_eyeballs_delay=None,
        enable_cleanup_closed=True,
    )
    app.state.session = aiohttp.ClientSession(
        connector=app.state.session_connector, timeout=timeouts
    )

    # Filter out some noisy paths (logging filter runs process-global).
    ignore_access = config.ignore_access
    logging.getLogger("uvicorn.access").addFilter(FilteredAccessLog())

    # Cache backend + mode
    if config.cache_backend_type == "sqlite":
        app.state.cache = await AsyncSqliteCache(
            config.cache_path, **dict(config.main.get("sqlite", {}))
        ).start()
    else:
        raise ValueError(f"Unknown cache backend type: {config.cache_backend_type}")

    # Upstream circuit breaker (shared across workers via an mmap'd file).
    health_state_path = os.path.join(config.cache_path, "upstream_health.mmap")
    app.state.health = await UpstreamHealth(
        app.state.session,
        config.upstreams,
        health_state_path,
        probe_interval=config.health_probe_interval,
        probe_timeout=config.health_probe_timeout,
    ).start()

    app.state.task_manager = BackgroundTaskManager()
    await app.state.task_manager.start()

    logger.debug("Startup event complete")


@app.on_event("shutdown")
async def shutdown_event():
    await app.state.task_manager.stop()
    # Stop the health probe before closing the session it uses.
    await app.state.health.stop()
    await app.state.session.close()
    await app.state.cache.close()
    shutdown_queue_logging()


# =====================
# Core streaming logic
# =====================

Body = AsyncGenerator[bytes, None]
Result = Tuple[int, Dict[str, str], Body]


async def _cache_hit_result(
    metadata, cache, method: str, cache_mode: str
) -> Optional[Result]:
    """
    Return a Result if we can satisfy from cache; else None.
    Honors cache_mode: X-Accel-Redirect only when mode == 'nginx'.
    """
    if metadata is None:
        return None

    if metadata.status_code != 200:
        return (metadata.status_code, {}, empty_body())

    headers = _build_client_headers(
        metadata.etag, metadata.last_modified, metadata.size
    )

    # nginx offload first (only in nginx mode)
    if cache_mode == "nginx":
        redirect_path = await cache.get_nginx_redirect_path(metadata)
        if redirect_path:
            # Let nginx fill in content length -- *we* aren't giving back
            # a body here.
            del headers["Content-Length"]
            headers["X-Accel-Redirect"] = redirect_path
            return (200, headers, empty_body())

    if method == "HEAD":
        return (200, headers, empty_body())

    stream = await cache.get_streaming(metadata.key)
    if stream:

        async def body():
            async for chunk in stream:
                yield chunk

        return (200, headers, body())

    return None


async def _attempt_upstream(
    session: aiohttp.ClientSession,
    request: Request,
    cache,
    candidate: Candidate,
    method: str,
) -> Optional[Result]:
    """
    Try one upstream:
    - If cacheable and hit: return cache result.
    - Else request upstream; on 200, write-while-send via open_write_stream; on 4xx, set negative cache.
    Returns None on network failure (so hedger can try others).
    """
    allow_cache = cache is not None

    upstream = candidate.upstream
    key = candidate.key
    assert key.full_url is not None

    config: AppConfig = request.app.state.config
    health = request.app.state.health

    single = UDSSingleFlight(key) if allow_cache else NoOpSingleFlight(key)

    # We don't need to do an initial cache lookup right here, because we
    # already did a mass lookup earlier. We only need to worry about multiple
    # clients trying to fill the cache for the same key, so we do need to check
    # within the lock.

    async with single.locked(timeout=30.0):
        # Re-check the cache, we may have raced with another "first hit" and lost
        if allow_cache:
            metadata = await asyncio.shield(cache.get_metadata(key))
            hit = await _cache_hit_result(metadata, cache, method, config.cache_mode)
            if hit is not None:
                # Release the lock ASAP, we probably have other waiters
                await single.release()
                return hit

        # The upstream may have been suspended while we waited on the lock (e.g.
        # a sibling request just tripped it). Bail out rather than pile onto a
        # known-bad upstream. This is a cheap flag check, not a liveness probe.
        if not health.is_available(upstream.name):
            return None

        logger.debug("Cache miss for %s", key.full_url)

        # Upstream fetch. An exception here means we never completed a
        # request/response cycle (connect/read timeout, refused, reset, ...),
        # so suspend the upstream. A complete HTTP response -- including 5xx --
        # returns normally and never trips the breaker.
        req_headers = upstream.passthrough_headers(request)
        try:
            resp = await session.request(method, key.full_url, headers=req_headers)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            health.trip(upstream.name)
            logger.warning(
                "Upstream request to %s failed at the transport level: %s",
                key.full_url,
                e,
            )
            return None

        try:
            status = resp.status
            etag = resp.headers.get("ETag")
            last_mod = resp.headers.get("Last-Modified", _httpdate_now_gmt())
            content_length = _content_length(resp)

            # HEAD or 304 => header-only response; do not fill cache
            if method == "HEAD" or status == 304:
                headers = _build_client_headers(etag, last_mod, content_length)
                await _close_response(resp)
                return (status, headers, empty_body())

            if status == 200:
                headers = _build_client_headers(etag, last_mod, content_length)
                writer = None
                if allow_cache:
                    writer = await cache.open_write_stream(
                        key,
                        status_code=200,
                        etag=etag,
                        last_modified=last_mod,
                        ttl=config.cache_ttl.get(200, TTL_SUCCESS_DEFAULT),
                    )

                pump = await CachePump(resp, writer, single, READ_CHUNK).start()
                return (200, headers, pump.body())

            if allow_cache and 400 <= status < 500:
                # Negative cache for 400-499 errors
                await asyncio.shield(
                    cache.set_negative_cache(
                        key,
                        status_code=status,
                        ttl=config.cache_ttl.get(status, TTL_FAILURE_DEFAULT),
                    )
                )

            headers = _build_client_headers(etag, last_mod, content_length)
            await _close_response(resp)
            return (status, headers, empty_body())

        except asyncio.CancelledError:
            raise
        except Exception as e:
            await _close_response(resp)
            logger.exception(
                "Error handling upstream response from %s: %s", key.full_url, e
            )
            return None


async def _handle_upstream_group(
    session: aiohttp.ClientSession,
    request: Request,
    cache,
    candidates: List[Candidate],
    method: str,
) -> Optional[Result]:
    """
    Within a single group, try all upstreams at once.
    Accept the first successful result.
    If all attempts fail or return None, the tier fails -> try next tier.
    """
    if not candidates:
        return None

    async def attempt(candidate: Candidate):
        try:
            cache_for = cache if candidate.upstream.cachable else None
            res = await _attempt_upstream(
                session,
                request,
                cache_for,
                candidate,
                method,
            )

            # Any non-None result is a 'decisive' response (200, 4xx, etc.)
            if res is not None and res[0] == 200:
                return res

            return None
        except Exception as exc:
            # Keep searching within tier
            logger.exception("Unhandled exception")
            return None

    tasks = [asyncio.create_task(attempt(c)) for c in candidates]

    try:
        for done_task in asyncio.as_completed(tasks):
            resp = await done_task
            if resp is None:
                # Failed, try next completed task
                continue
            return resp
        return None
    finally:
        remaining = [t for t in tasks if not t.done()]
        for t in remaining:
            t.cancel()
        await asyncio.gather(*remaining, return_exceptions=True)


# =====================
# Routes
# =====================


@app.head("/{path:path}")
@app.get("/{path:path}")
async def proxy(path: str, request: Request):
    # Normalize path to exactly one leading '/'
    path = "/" + path.lstrip("/")

    config: AppConfig = request.app.state.config

    if is_blacklisted_path(path, config):
        logger.debug("Blacklisted: '%s', rejecting with 404", path)
        return Response(status_code=404)

    method = request.method.upper()
    if method not in ("GET", "HEAD"):
        return Response(status_code=405)

    # Parse the request into a layout-independent description.
    query = classify_request(path)
    if query is None:
        # Nothing understands this request; 404 without touching the cache.
        logger.debug("Unrecognized request '%s', rejecting with 404", path)
        return Response(status_code=404)

    session: aiohttp.ClientSession = request.app.state.session
    cache = request.app.state.cache
    health = request.app.state.health

    # Build the set of upstreams that can serve this query, each with the
    # canonical URL + cache key it would use.
    candidates: List[Candidate] = []
    for upstream in config.upstreams.values():
        url = upstream.candidate_url(query)
        if url is None:
            continue
        candidates.append(Candidate(upstream, make_cache_key(upstream.name, url)))

    if not candidates:
        # No upstream can handle this; 404 without a negative-cache write.
        logger.debug("No upstream can serve '%s', rejecting with 404", path)
        return Response(status_code=404)

    # Frontload cache lookups for the cachable upstreams.
    cachable = [c for c in candidates if c.upstream.cachable]

    metas = await cache.get_many_metadata([c.key for c in cachable])
    if metas:
        logger.debug(
            "Found cache metadata from upstreams %s for path %s",
            list([m.key.upstream_name for m in metas]),
            path,
        )

    negative_keys: Set[bytes] = set()
    for meta in metas:
        hit = await _cache_hit_result(meta, cache, method, config.cache_mode)

        # Check if this is either a positive or negative cache hit
        if hit is not None:

            # Found a hit, check if it's a positive hit
            status, headers, body_gen = hit
            if status == 200:
                # Positive hit, finish out our response immediately
                return StreamingResponse(
                    body_gen,
                    status_code=status,
                    headers=headers,
                    media_type=headers.get("Content-Type", "application/octet-stream"),
                )

            # Negative hit, we can exclude this upstream from the live query
            negative_keys.add(meta.key.hash_key_raw)

    # Remaining = cachable upstreams without a decisive cache hit, plus the
    # uncachable ones (which we always query live). Suspended upstreams are
    # dropped from the live fan-out here (a cheap flag check, no liveness probe)
    # -- but note they still served cache hits in the loop above.
    remaining = [
        c
        for c in cachable
        if c.key.hash_key_raw not in negative_keys
        and health.is_available(c.upstream.name)
    ]
    remaining += [
        c
        for c in candidates
        if not c.upstream.cachable and health.is_available(c.upstream.name)
    ]

    if remaining:
        logger.debug(
            "No positive cache hits, asking remaining upstreams %s for path %s",
            list([c.upstream.name for c in remaining]),
            path,
        )

    # For all remaining candidates, try an HTTP request to the upstream.
    result = await _handle_upstream_group(session, request, cache, remaining, method)
    if result is not None:
        status, headers, body_gen = result
        return StreamingResponse(
            body_gen,
            status_code=status,
            headers=headers,
            media_type=headers.get("Content-Type", "application/octet-stream"),
        )

    logger.debug("All upstreams exhausted for %s", path)
    return Response(status_code=404)


def main():
    with open("config/main.toml", "rb") as fd:
        main_config = tomllib.load(fd)
    uvicorn.run("symsrv:app", **main_config["uvicorn"])


if __name__ == "__main__":
    main()
