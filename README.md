# Symbol Server Caching Proxy

## Overview

Fetching symbols from upstream symbol servers can be slow, and the symbol
download process benefits significantly from caching. On a network used by
developers (or a company full of developers), multiple machines will likely
make the same symbol server requests. This caching proxy helps by storing
symbols locally and attempting to query multiple upstream symbol servers to
find the required symbol, thus improving the speed and efficiency of
development tools.

## Features

- **Efficient Symbol Fetching**: Caches symbols locally to reduce latency and
  network traffic.

- **Multiple Upstream Servers**: Attempts to fetch symbols from multiple
  upstream servers, improving the chances of finding the required symbol.

- **Negative Caching**: Caches 404 responses for symbols that do not exist
  upstream, reducing unnecessary outbound HTTP requests.

- **Support for Common Development Tools**: Works seamlessly with Windows
  development tools like Visual Studio, WinDbg, and Windows Performance Analyzer
  via the `_NT_SYMBOL_PATH` environment variable.

## Usage

### Prerequisites

- Python 3.12+ (will likely work on earlier versions, but not tested)
- [Poetry](https://pypi.org/project/poetry/)
- [FastAPI](https://pypi.org/project/fastapi/)
- [Uvicorn](https://pypi.org/project/uvicorn/)
- [HTTPX](https://pypi.org/project/httpx/)
- [DiskCache](https://pypi.org/project/diskcache/)
- [Humanize](https://pypi.org/project/humanize/)
- [HumanFriendly](https://pypi.org/project/humanfriendly/)

### Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/tycho/symsrv.git
   cd symsrv
   ```

2. Install the required dependencies:
   ```sh
   make poetry-install
   ```

3. Edit config files in the `config` subdirectory to match your needs.

### Running the Server

1. Ensure the cache directory (as specified in `config/main.toml`) exists:
   ```sh
   mkdir -p /srv/cache/ntsymbols/data
   ```

2. Run the FastAPI server:
   ```sh
   .venv/bin/poetry run serve
   ```

The server will listen on a UNIX socket by default. You can use an nginx
configuration like this to expose it to the world with HTTP:

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    '' close;
}

upstream symsrv {
    server unix:/srv/cache/ntsymbols/run/symsrv.sock;
}

server {
    listen 80;
    listen 443 ssl;

    server_name symbols.example.com;

    access_log /srv/cache/ntsymbols/logs/access.log;
    error_log /srv/cache/ntsymbols/logs/error.log;

    location / {
        client_max_body_size 4g;
        proxy_redirect off;
        proxy_buffering off;
        proxy_request_buffering off;
        proxy_http_version 1.1;
        proxy_set_header Host $http_host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_pass http://symsrv;
    }
}
```

### Setting Up Your Development Environment

Set the `_NT_SYMBOL_PATH` environment variable on your system to use the
caching proxy:

```cmd
set _NT_SYMBOL_PATH=SRV*C:\SymbolCache;http://symbols.example.com/
```

Replace `C:\SymbolCache` with your local cache directory and
`http://symbols.example.com/` with the URL of your caching proxy.

## Additional Features

### Negative Caching

Some symbols may never exist on any upstream. The proxy filters these up front
and returns a 404 result without making outbound HTTP requests if they have
been previously identified as non-existent. This feature helps to reduce
unnecessary network traffic and speeds up the response time for known missing
symbols.

### Logging

The proxy logs client IP addresses, cache hits, successful fetches, and errors
for debugging and monitoring purposes. This helps in identifying performance
bottlenecks and ensuring that the proxy operates efficiently.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue to
discuss your ideas or report bugs.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file
for details.
