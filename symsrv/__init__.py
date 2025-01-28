#!/usr/bin/python
# pylint: disable=C0114,C0115,C0116
from contextlib import asynccontextmanager
from email.utils import formatdate
import typing
import datetime as dt
import hashlib
import os
import time
import logging
import asyncio

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from starlette.types import Scope, Receive, Send
from tomlkit.toml_file import TOMLFile
import aiohttp
import anyio
import uvicorn

from .asyncdiskcache import AsyncDiskCache

# Set up logging
logger = logging.getLogger("uvicorn.error")

TTL_SUCCESS_DEFAULT = 8 * 60 * 60  # 8 hours
TTL_FAILURE_DEFAULT = 1 * 60 * 60  # 1 hour

CHUNK_SIZE = 256 * 1024
PROFILING_ENABLED = False

cache_ttl: dict[int, int] = {}

upstreams = []

substring_blacklist: list[str] = []
first_path_component_blacklist: set[str] = set()
suffix_blacklist: list[str] = []
ignore_access: set[typing.Dict[str, typing.Any]] = set()

app = FastAPI()

def register_middleware(app: FastAPI):
    if PROFILING_ENABLED == True:
        from pyinstrument import Profiler
        from pyinstrument.renderers.html import HTMLRenderer
        from pyinstrument.renderers.speedscope import SpeedscopeRenderer

        @app.middleware("http")
        async def profile_request(request: Request, call_next: typing.Callable):
            """Profile the current request

            Taken from https://pyinstrument.readthedocs.io/en/latest/guide.html#profile-a-web-request-in-fastapi
            with small improvements.

            """
            # we map a profile type to a file extension, as well as a pyinstrument profile renderer
            profile_type_to_ext = {"html": "html", "speedscope": "speedscope.json"}
            profile_type_to_renderer = {
                "html": HTMLRenderer,
                "speedscope": SpeedscopeRenderer,
            }

            # if the `profile=true` HTTP query argument is passed, we profile the request
            if request.query_params.get("profile", False):

                # The default profile format is speedscope
                profile_type = request.query_params.get("profile_format", "speedscope")

                # we profile the request along with all additional middlewares, by interrupting
                # the program every 1ms1 and records the entire stack at that point
                with Profiler(interval=0.001, async_mode="enabled") as profiler:
                    response = await call_next(request)

                # we dump the profiling into a file
                extension = profile_type_to_ext[profile_type]
                renderer = profile_type_to_renderer[profile_type]()
                with open(f"profile.{extension}", "w") as out:
                    out.write(profiler.output(renderer=renderer))
                return response

            # Proceed without profiling
            return await call_next(request)
register_middleware(app)

class FilteredAccessLog(logging.Filter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def filter(self, record: logging.LogRecord) -> bool:
        filter_match = (record.args[4], record.args[2])
        if filter_match in ignore_access:
            return False
        return True

@app.on_event("startup")
async def startup_event():
    # Load main config
    global cache_ttl
    main_config = TOMLFile("config/main.toml").read()
    for key, value in main_config["cache_ttl"].items():
        cache_ttl[int(key)] = value
    cache_path = main_config["disk_cache"]["path"]

    global ignore_access
    for filter_spec in main_config["log_filters"]["ignore_access"]:
        ignore_access.add((int(filter_spec["status"]), str(filter_spec["path"])))
    logging.getLogger("uvicorn.access").addFilter(FilteredAccessLog())

    # Load blacklist config
    global substring_blacklist
    global first_path_component_blacklist
    global suffix_blacklist
    bl_config = TOMLFile("config/blacklist.toml").read()
    substring_blacklist = bl_config["substring_blacklist"]["patterns"]
    first_path_component_blacklist = set(
        bl_config["first_path_component_blacklist"]["names"]
    )
    suffix_blacklist = bl_config["suffix_blacklist"]["suffixes"]

    # Load upstream config
    global upstreams
    upstream_config = TOMLFile("config/upstreams.toml").read()
    upstreams = upstream_config["upstream"]

    app.state.cache = AsyncDiskCache(cache_path)
    app.state.session = aiohttp.ClientSession()


@app.on_event("shutdown")
async def shutdown_event():
    await app.state.cache.close()
    await app.state.session.close()


async def chunks_to_async_gen(chunks):
    for chunk in chunks:
        yield chunk


async def empty_async_gen():
    yield b''


async def fetch_from_upstream(
    session: aiohttp.ClientSession,
    cache: AsyncDiskCache,
    cache_key: str,
    full_url: str,
    method: str
) -> typing.AsyncIterator[typing.Tuple[bytes, int, typing.Dict[str, str]]]:
    """
    Fetch from upstream and handle caching based on response status.
    Yields tuples of (chunk, status_code).

    For 200 responses, yields and caches the response chunks.
    For 4xx responses, yields nothing but caches an empty response.
    For other responses, yields nothing and doesn't cache.
    """
    try:
        async with session.request(method, full_url) as response:
            status_code = response.status

            if status_code == 200:
                # Create a list to store chunks for caching
                chunks = []

                # Get an upstream ETag if possible
                response_headers = {}
                etag = response.headers.get('ETag')

                if etag:
                    response_headers['ETag'] = etag

                for header in ['Content-Type', 'Cache-Control', 'Last-Modified']:
                    if value := response.headers.get(header):
                        response_headers[header] = value

                # Stream the response while collecting chunks
                async for chunk in response.content.iter_chunks():
                    chunk_data = chunk[0]  # aiohttp returns tuples of (data, end_of_chunk)
                    chunks.append(chunk_data)
                    yield chunk_data, 200, response_headers

                if not etag:
                    hasher = hashlib.md5()
                    total_size = sum(len(chunk) for chunk in chunks)
                    hasher.update(str(total_size).encode())
                    hasher.update(str(int(time.time())).encode())
                    if chunks:
                        hasher.update(chunks[0][:1024])
                    etag = f'"{hasher.hexdigest()}"'

                # Cache the successful response
                await cache.set_streaming(
                    cache_key,
                    chunks_to_async_gen(chunks),
                    ttl=cache_ttl.get(200, TTL_SUCCESS_DEFAULT),
                    tags={
                        'status_code': 200,
                        'etag': etag,
                        'headers': response_headers
                    }
                )

            elif 400 <= status_code < 500:
                response_headers = {}
                for header in ['Cache-Control']:
                    if value := response.headers.get(header):
                        response_headers[header] = value

                # Create a list to store chunks for caching
                # Cache client errors with empty content
                await cache.set_streaming(
                    cache_key,
                    empty_async_gen(),  # Empty content
                    ttl=cache_ttl.get(status_code, TTL_FAILURE_DEFAULT),
                    tags={
                        'status_code': status_code,
                        'headers': response_headers
                    }
                )
                # Don't yield anything for 4xx errors

            else:
                logger.error(
                    "Request to %s failed with status %d",
                    full_url,
                    status_code
                )
                # Don't yield anything for other status codes

    except aiohttp.ClientError as exc:
        logger.error("Request to %s failed: %s", full_url, exc)
        # Don't yield anything for connection errors


async def fetch_cached_or_live(
    session: aiohttp.ClientSession,
    cache: AsyncDiskCache,
    cache_key: str,
    full_url: str,
    method: str,
    request_headers: typing.Optional[typing.Dict[str, str]] = None
) -> typing.AsyncIterator[typing.Tuple[bytes, int, typing.Dict[str, str]]]:
    """
    Try to fetch from cache first, fall back to live request if needed.
    Yields tuples of (chunk, status_code).
    """
    # Check cache first
    if metadata := await cache.get_metadata(cache_key):
        status_code = metadata.tags.get('status_code', None)
        cached_headers = metadata.tags.get('headers', {})
        response_headers = {}

        # In a cached response that meets the 'If-None-Match' or
        # 'If-Modified-Since' conditions, we provide ETag and Last-Modified.
        if last_modified := cached_headers.get('Last-Modified'):
            response_headers['Last-Modified'] = last_modified
        if cached_etag := cached_headers.get('ETag'):
            response_headers['ETag'] = cached_etag

        # Check if we have an ETag match
        if (if_none_match := request_headers.get('if-none-match')) and cached_etag:
            if if_none_match == cached_etag:
                # Return just headers with 304 status
                yield b'', 304, response_headers
                return

        # Check If-Modified-Since
        if (if_modified_since := request_headers.get('if-modified-since')) and last_modified:
            try:
                # Parse both dates and compare
                req_date = time.strptime(if_modified_since, "%a, %d %b %Y %H:%M:%S GMT")
                cache_date = time.strptime(last_modified, "%a, %d %b %Y %H:%M:%S GMT")
                if cache_date <= req_date:
                    yield b'', 304, cached_headers
                    return
            except ValueError:
                # If we can't parse the dates, ignore the header
                pass

        if cached_stream := await cache.get_streaming(cache_key):
            async for chunk in cached_stream:
                yield chunk, status_code, cached_headers
            return

    # If not in cache or expired, fetch from upstream
    async for chunk, status, headers in fetch_from_upstream(
        session,
        cache,
        cache_key,
        full_url,
        method
    ):
        yield chunk, status, headers


def get_extension(path: str):
    _, ext = os.path.splitext(path)
    return ext


def choose_upstreams(path: str):
    ext = get_extension(path).lower()

    if ext == ".pd_":
        ext = ".pdb"

    if ext == ".dl_":
        ext = ".dll"

    if ext == ".ptr":
        # Don't warn on this guy, it's commonly requested
        ext = None

    if ext not in [None, ".pdb", ".dll"]:
        logger.warning(
            "Path '%s' doesn't have a recognized file extension (%s)", path, ext
        )
        ext = None

    target_upstreams = []

    for upstream in upstreams:
        extensions = upstream.get("extensions", [])

        include = False
        if ext is None or not extensions:
            include = True
        elif ext in upstream["extensions"]:
            include = True

        if include:
            target_upstreams.append(upstream["base_url"])

    return target_upstreams


def is_blacklisted_path(path: str):
    for part in substring_blacklist:
        if part in path:
            logger.debug(
                "Path '%s' rejected, matched '%s' in substring blacklist", path, part
            )
            return True

    for suffix in suffix_blacklist:
        if path.endswith(suffix):
            logger.debug(
                "Path '%s' rejected, ends with '%s' in suffix blacklist", path, suffix
            )
            return True

    file = path.split("/")[1]

    if file in first_path_component_blacklist:
        logger.debug(
            "Path '%s' rejected, first path component '%s' in blacklist", path, file
        )
        return True

    return False


@app.head("/{path:path}")
@app.get("/{path:path}")
async def proxy(path: str, request: Request):
    # Ensure path begins with exactly one forward slash
    path = "/" + path.lstrip("/")

    if is_blacklisted_path(path):
        logger.debug(f"Blacklisted: '{path}', rejecting with 404 response")
        return Response(status_code=404)

    target_upstreams = choose_upstreams(path)
    session = request.app.state.session  # We'll need to change this to aiohttp.ClientSession
    cache = request.app.state.cache      # Our AsyncDiskCache instance

    async def stream_from_upstream(base_url: str):
        """Try a single upstream, return (success, stream) tuple"""
        full_url = f"{base_url.rstrip('/')}{path}"
        cache_key = f"{request.method}:{full_url}"

        stream = fetch_cached_or_live(
            session,
            cache,
            cache_key,
            full_url,
            request.method,
            dict(request.headers)
        )

        try:
            chunk, status, headers = await anext(stream)
            if status in (200, 304):
                return True, status, headers, chunk, stream
        except StopAsyncIteration:
            pass

        return False, None, None, None, None

    # Try each upstream until success
    for base_url in target_upstreams:
        success, status_code, response_headers, first_chunk, stream = \
            await stream_from_upstream(base_url)

        if success:
            async def response_stream():
                yield first_chunk
                async for chunk, _, _ in stream:
                    yield chunk

            return StreamingResponse(
                response_stream(),
                status_code=status_code,
                media_type=response_headers.get('Content-Type', 'application/octet-stream'),
                headers=response_headers
            )

    return Response(status_code=404)


def main():
    main_config = TOMLFile("config/main.toml").read()
    uvicorn.run("symsrv:app", **main_config["uvicorn"])


if __name__ == "__main__":
    main()
