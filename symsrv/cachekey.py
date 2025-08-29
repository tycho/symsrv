import hashlib
import sys
from dataclasses import dataclass, field
from typing import Optional


def _hash_from_digest(digest: bytes, *, byteorder="little") -> int:
    """Convert a cryptographic digest to a Python hash() result."""
    w = sys.hash_info.width  # 32 or 64 (almost always 64)
    n = w // 8
    # Truncate or right-pad the digest to exactly n bytes
    d = (digest[:n]) if len(digest) >= n else (digest + b"\x00" * (n - len(digest)))
    h = int.from_bytes(d, byteorder=byteorder, signed=False)

    # Map to signed Py_ssize_t range and avoid -1 (reserved by CPython)
    if h >= 1 << (w - 1):
        h -= 1 << w
    return -2 if h == -1 else h


@dataclass(eq=True, frozen=True)
class CacheKey:
    upstream_name: Optional[str] = field(compare=False)
    full_url: Optional[str] = field(compare=False)
    hash_key_raw: bytes = field(compare=True)
    hash_key: str = field(compare=False)

    def __hash__(self) -> int:
        return _hash_from_digest(self.hash_key_raw)


def cache_key_from_digest(digest: bytes) -> CacheKey:
    # Must be a SHA256 digest
    if len(digest) != 32:
        raise ValueError("Hash digest must be 32 bytes")
    return CacheKey(None, None, digest, digest.hex())


def cache_key_from_hexdigest(digest: str) -> CacheKey:
    # Must be a SHA256 digest
    if len(digest) != 64:
        raise ValueError("Hash digest string must be 64 characters")
    return CacheKey(None, None, bytes.fromhex(digest), digest)


def make_cache_key(upstream_name: str, full_url: str):
    hashed_url = hashlib.sha256(full_url.encode("utf-8"))
    return CacheKey(
        upstream_name, full_url, hashed_url.digest(), hashed_url.hexdigest()
    )
