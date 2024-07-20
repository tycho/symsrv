from contextlib import asynccontextmanager
from email.utils import formatdate
import typing
import tomllib
import datetime as dt
import os
import time
import logging
import asyncio

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
from starlette._compat import md5_hexdigest
from starlette.background import BackgroundTask
from starlette.types import Scope, Receive, Send
import anyio
import diskcache as dc
import httpx
import humanfriendly
import humanize
import uvicorn

# Set up logging
logger = logging.getLogger("uvicorn.error")

TTL_SUCCESS_DEFAULT = 8 * 60 * 60 # 8 hours
TTL_FAILURE_DEFAULT = 1 * 60 * 60 # 1 hour

CHUNK_SIZE = 256 * 1024

cache: dc.FanoutCache = None
cache_ttl: dict[int, int] = {}

upstreams = []

substring_blacklist: list[str] = []
first_path_component_blacklist: set[str] = set()
suffix_blacklist: list[str] = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load main config
    global cache
    global cache_ttl
    with open("config/main.toml", "rb") as f:
        main_config = tomllib.load(f)
        for key, value in main_config['cache_ttl'].items():
            cache_ttl[int(key)] = value

        cache_size = humanfriendly.parse_size(main_config['disk_cache']['size'], binary=True)
        cache_path = main_config['disk_cache']['path']
        cache = dc.FanoutCache(cache_path, size_limit=cache_size)

    # Load blacklist config
    global substring_blacklist
    global first_path_component_blacklist
    global suffix_blacklist
    with open("config/blacklist.toml", "rb") as f:
        bl_config = tomllib.load(f)
        substring_blacklist = bl_config['substring_blacklist']['patterns']
        first_path_component_blacklist = set(bl_config['first_path_component_blacklist']['names'])
        suffix_blacklist = bl_config['suffix_blacklist']['suffixes']

    # Load upstream config
    global upstreams
    with open("config/upstreams.toml", "rb") as f:
        upstream_config = tomllib.load(f)
        upstreams = upstream_config['upstream']

    async with httpx.AsyncClient(follow_redirects=True) as client:
        yield {'client': client}

app = FastAPI(lifespan=lifespan)

# Wrap an async response in a file-like object for response-to-disk
# serialization
class StreamingToFileSyncAdapter:
    chunk_size = CHUNK_SIZE

    def __init__(self, response: httpx.Response):
        self.streaming_source = response.iter_bytes(self.chunk_size)
        self.buffer = b""
        self.buffer_offset = 0

    def read(self, num_bytes: int):
        while len(self.buffer) - self.buffer_offset < num_bytes:
            try:
                chunk = next(self.streaming_source)
                self.buffer += chunk
            except StopIteration:
                break

        if len(self.buffer) - self.buffer_offset >= num_bytes:
            data = self.buffer[self.buffer_offset:self.buffer_offset + num_bytes]
            self.buffer_offset += num_bytes
            return data
        else:
            data = self.buffer[self.buffer_offset:]
            self.buffer = b""
            self.buffer_offset = 0
            return data

# Response object with an already-open file handle
class OpenFileResponse(Response):
    chunk_size = CHUNK_SIZE

    def __init__(
        self,
        file: typing.BinaryIO,
        status_code: int = 200,
        headers: typing.Mapping[str, str] | None = None,
        media_type: str = "application/octet-stream",
        background: BackgroundTask | None = None,
    ) -> None:
        self.file = file
        self.background = None
        self.status_code = status_code
        self.media_type = media_type
        self.init_headers(headers)
        self.stat_result = os.fstat(self.file.fileno())
        self.set_stat_headers(self.stat_result)

    def set_stat_headers(self, stat_result: os.stat_result) -> None:
        content_length = str(stat_result.st_size)
        last_modified = formatdate(stat_result.st_mtime, usegmt=True)
        etag_base = str(stat_result.st_mtime) + "-" + str(stat_result.st_size)
        etag = f'"{md5_hexdigest(etag_base.encode(), usedforsecurity=False)}"'

        self.headers.setdefault("content-length", content_length)
        self.headers.setdefault("last-modified", last_modified)
        self.headers.setdefault("etag", etag)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self.raw_headers,
            }
        )
        if scope["method"].upper() == "HEAD":
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        else:
            async with anyio.wrap_file(self.file) as file:
                more_body = True
                while more_body:
                    chunk = await file.read(self.chunk_size)
                    more_body = len(chunk) == self.chunk_size
                    await send(
                        {
                            "type": "http.response.body",
                            "body": chunk,
                            "more_body": more_body,
                        }
                    )

        if self.background is not None:
            await self.background()

async def fetch_from_upstream(client: httpx.AsyncClient, url: str) -> httpx.Response:
    response = await client.get(url)
    response.raise_for_status()  # Raises HTTPStatusError for 4xx/5xx responses
    return response

def human_ttl(ttl: int):
    return humanize.naturaldelta(dt.timedelta(seconds=ttl))

async def cache_response(full_url: str, cache_key: str, response: httpx.Response, default_ttl: int):
    """
    Stream the response payload to the cache file, then retrieve a file handle
    to it from the cache
    """
    ttl = cache_ttl.get(response.status_code, default_ttl)
    ttl_human = human_ttl(ttl)
    logger.info(f"Cache miss, storing {response.status_code} from {full_url} for {ttl_human}")
    cache.set(cache_key, StreamingToFileSyncAdapter(response), read=True, tag=response.status_code, expire=ttl)  # Cache for 365 days
    return cache.get(cache_key, read=True, tag=True, expire_time=True)

async def single_fetch(client: httpx.AsyncClient, cache_key: str, full_url: str, method: str):
    try:
        result = await fetch_from_upstream(client, full_url)
        if result.status_code == 200:
            return await cache_response(full_url, cache_key, result, TTL_SUCCESS_DEFAULT)
        else:
            logger.warning(f"Unrecognized response code {result.status_code}, treating as uncacheable 404")
    except httpx.RequestError as exc:
        logger.error(f"Request to {full_url} failed: {exc}")
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code
        if status_code >= 500 and status_code < 600:
            # Do not cache server errors
            logger.error(f"Request to {full_url} failed with status {status_code}")
        elif status_code >= 400 and status_code < 500:
            # Cache client errors, as specified in cache_ttls, or 1 hour if not found
            return await cache_response(full_url, cache_key, exc.response, TTL_FAILURE_DEFAULT)

    return None, 0, 404

def get_extension(path: str):
    _, ext = os.path.splitext(path)
    return ext;

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
        logger.warning(f"Path '{path}' doesn't have a recognized file extension ({ext})")
        ext = None

    target_upstreams = []

    for upstream in upstreams:
        extensions = upstream.get('extensions', [])

        include = False
        if ext is None or not extensions:
            include = True
        elif ext in upstream['extensions']:
            include = True

        if include:
            target_upstreams.append(upstream['base_url'])

    return target_upstreams

def is_blacklisted_path(path: str):
    for part in substring_blacklist:
        if part in path:
            logging.debug(f"Path '{path}' rejected, matched '{part}' in substring blacklist")
            return True

    for suffix in suffix_blacklist:
        if path.endswith(suffix):
            logging.debug(f"Path '{path}' rejected, ends with '{suffix}' in suffix blacklist")
            return True

    file = path.split('/')[1]

    if file in first_path_component_blacklist:
        logging.debug(f"Path '{path}' rejected, first path component '{file}' in blacklist")
        return True

    return False

@app.get("/{path:path}")
async def proxy(path: str, request: Request):
    # Ensure path begins with exactly one forward slash
    path = '/' + path.lstrip('/')

    client = request.state.client

    if is_blacklisted_path(path):
        logger.info(f"Requested path is blacklisted, rejecting with 404 response")
        return Response(status_code=404)

    target_upstreams = choose_upstreams(path)

    # Check cache first
    pending = []
    for base_url in target_upstreams:
        base_url = base_url.rstrip('/')
        full_url = f"{base_url}{path}"
        cache_key = f"{request.method}:{full_url}"
        content, expire_time, status_code = cache.get(cache_key, read=True, tag=True, expire_time=True)
        if content is not None:
            time_left = human_ttl(expire_time - time.time())
            logger.info(f"Cache hit for {cache_key} with status {status_code}, expires in {time_left}")
            if status_code == 200:
                return OpenFileResponse(content, status_code=status_code, media_type="application/octet-stream")
        else:
            pending.append((cache_key, full_url))

    # Perform parallel fetch from upstreams
    for cache_key, full_url in pending:
        content, expire_time, status_code = await single_fetch(client, cache_key, full_url, request.method)
        if content is not None and status_code == 200:
            return OpenFileResponse(content, status_code=status_code, media_type="application/octet-stream")

    logger.info(f"No valid content for {path}, returning 404")

    return Response(status_code=404)

def main():
    with open("config/main.toml", "rb") as f:
        main_config = tomllib.load(f)

    uvicorn.run("symsrv:app", **main_config['uvicorn'])

if __name__ == "__main__":
    main()

