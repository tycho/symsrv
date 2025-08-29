#!/usr/bin/python
# pylint: disable=C0114,C0115,C0116,C0301

from dataclasses import dataclass
from typing import Optional, Protocol
from pathlib import Path


@dataclass
class StreamingOpenParams:
    status_code: int
    etag: Optional[str]
    last_modified: Optional[str]
    ttl: Optional[float]


class StreamingWriter(Protocol):
    async def write(self, chunk: bytes) -> None: ...
    async def commit(self) -> None: ...
    async def abort(self) -> None: ...
