#!/usr/bin/python
# pylint: disable=C0114,C0115,C0116,C0301

"""
Typed upstream symbol servers.

An :class:`Upstream` bundles a base URL with three orthogonal admission axes --

- ``binformat``: which binary/symbol families it hosts (pe / elf / macho)
- ``provides``:  which roles it hosts (image / debug)
- ``extensions``: optional finer, symstore-only file-extension narrowing

-- plus the URI ``layout`` it speaks. Given a :class:`~symsrv.layout.SymbolQuery`
it decides whether it can serve the request and, if so, renders the canonical
URL to fetch (via its layout). This replaces the old raw-dict-in-a-global model
and centralizes all "which upstream, which URL" logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Optional, Union

from .layout import LAYOUTS, Family, Role, SymbolQuery

if TYPE_CHECKING:  # avoid importing fastapi at module import time / in tests
    from fastapi import Request


def _as_family_set(value: Union[None, str, Iterable[str]]) -> frozenset[Family]:
    if value is None:
        return frozenset({Family.PE})
    if isinstance(value, str):
        value = [value]
    return frozenset(Family(v.lower()) for v in value)


def _as_role_set(value: Union[None, str, Iterable[str]]) -> frozenset[Role]:
    if value is None:
        return frozenset({Role.IMAGE, Role.DEBUG})
    if isinstance(value, str):
        value = [value]
    return frozenset(Role(v.lower()) for v in value)


def _as_ext_set(value: Union[None, Iterable[str]]) -> Optional[frozenset[str]]:
    if not value:
        return None
    return frozenset(e.lower() for e in value)


@dataclass(frozen=True)
class Upstream:
    name: str
    base_url: str
    layout: str = "symstore"
    binformat: frozenset[Family] = frozenset({Family.PE})
    provides: frozenset[Role] = frozenset({Role.IMAGE, Role.DEBUG})
    extensions: Optional[frozenset[str]] = None
    priority: int = 10
    nocache: bool = False
    requires_symbolchecksum: bool = False

    @classmethod
    def from_config(cls, cfg: dict) -> "Upstream":
        layout = cfg.get("layout", "symstore")
        if layout not in LAYOUTS:
            raise ValueError(
                f"Upstream {cfg.get('name')!r}: unknown layout {layout!r} "
                f"(known: {sorted(LAYOUTS)})"
            )
        return cls(
            name=cfg["name"],
            base_url=cfg["base_url"],
            layout=layout,
            binformat=_as_family_set(cfg.get("binformat")),
            provides=_as_role_set(cfg.get("provides")),
            extensions=_as_ext_set(cfg.get("extensions")),
            priority=int(cfg.get("priority", 10)),
            nocache=bool(cfg.get("nocache", False)),
            requires_symbolchecksum=bool(cfg.get("requires_symbolchecksum", False)),
        )

    @property
    def cachable(self) -> bool:
        return not self.nocache

    def accepts(self, query: SymbolQuery) -> bool:
        """Whether this upstream could plausibly hold the requested artifact."""
        if query.family not in self.binformat:
            return False
        if query.role is not None and query.role not in self.provides:
            return False
        if self.extensions is not None and query.ext is not None:
            if query.ext not in self.extensions:
                return False
        return True

    def candidate_url(self, query: SymbolQuery) -> Optional[str]:
        """
        The canonical URL to fetch for this query, or None if this upstream
        can't/shouldn't serve it.
        """
        if not self.accepts(query):
            return None
        return LAYOUTS[self.layout].render(self.base_url, query)

    def passthrough_headers(self, request: "Request") -> dict[str, str]:
        # Keep minimal; these servers are picky.
        out: dict[str, str] = {}
        if "symbolchecksum" in request.headers:
            out["SymbolChecksum"] = request.headers["symbolchecksum"]
        elif self.requires_symbolchecksum:
            out["SymbolChecksum"] = "invalid"
        return out
