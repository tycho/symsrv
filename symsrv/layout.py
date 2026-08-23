#!/usr/bin/python
# pylint: disable=C0114,C0115,C0116,C0301

"""
Symbol server URI layouts.

A *layout* is a bidirectional codec between raw request URL paths and a
layout-independent description of the symbol being requested (:class:`SymbolQuery`):

- ``classify(path)`` parses an inbound request into a :class:`SymbolQuery`
  (or ``None`` if this layout doesn't recognize it).
- ``render(base_url, query)`` produces the URL to fetch from an upstream that
  speaks this layout (or ``None`` if it can't serve that query).

This is the single seam through which upstream request URLs are produced. It
keeps layout-specific knowledge (path shapes, extension handling, and -- in
later phases -- build-id/UUID translation) out of the request handler.

Phase 1 implements only the Microsoft SymbolStore / SSQP layout, and only for
PE/COFF binaries, which are passed through verbatim. The :class:`Family` /
:class:`Role` vocabulary and the :data:`LAYOUTS` registry are deliberately
open-ended so that ELF (debuginfod) and Mach-O (lldb) layouts can be added
without reworking the request path.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from enum import StrEnum
from typing import Optional, Protocol

logger = logging.getLogger("uvicorn.error")


class Family(StrEnum):
    """The binary/symbol family a request refers to."""

    PE = "pe"  # PE/COFF/PDB -- the Windows world
    ELF = "elf"
    MACHO = "macho"


class Role(StrEnum):
    """What kind of artifact a request is for."""

    IMAGE = "image"  # the binary itself (.dll/.exe/.sys, ELF executable, ...)
    DEBUG = "debug"  # debug info / symbols (.pdb/.snupkg, ELF debuginfo, ...)


@dataclass(frozen=True)
class SymbolQuery:
    """A layout-independent description of the symbol a client asked for."""

    family: Family
    # None means "unknown / any role" -- e.g. SymbolStore index (.ptr) files,
    # which should be offered to every otherwise-eligible upstream.
    role: Optional[Role]
    # Normalized file extension used for symstore fine-grained narrowing.
    # None means "don't narrow on extension".
    ext: Optional[str]
    # The normalized inbound request path, e.g. "/foo.pdb/HASH/foo.pdb".
    raw_path: str
    # The layout the request arrived in (registry key).
    inbound_layout: str


class Layout(Protocol):
    name: str

    def classify(self, path: str) -> Optional[SymbolQuery]: ...

    def render(self, base_url: str, query: SymbolQuery) -> Optional[str]: ...


# ---------------------------------------------------------------------------
# Microsoft SymbolStore / SSQP layout
# ---------------------------------------------------------------------------

# Compressed ("last character replaced with '_'") -> canonical extension. The
# upstream URL is still fetched verbatim (with the compressed extension); this
# mapping only drives extension-based narrowing and role classification.
_SYMSTORE_COMPRESSED = {
    ".pd_": ".pdb",
    ".dl_": ".dll",
    ".ex_": ".exe",
    ".sy_": ".sys",
    ".db_": ".dbg",
}

# Recognized (canonical) symstore extensions and the role each implies.
_SYMSTORE_ROLES = {
    ".pdb": Role.DEBUG,
    ".snupkg": Role.DEBUG,
    ".dbg": Role.DEBUG,
    ".dll": Role.IMAGE,
    ".exe": Role.IMAGE,
    ".sys": Role.IMAGE,
}


class SymStoreLayout:
    """Microsoft SymbolStore / SSQP URI layout: ``/<name>/<id>/<name>``."""

    name = "symstore"

    def classify(self, path: str) -> Optional[SymbolQuery]:
        ext = os.path.splitext(path)[1].lower()
        ext = _SYMSTORE_COMPRESSED.get(ext, ext)

        if ext == ".ptr":
            # Index pointer files carry no specific file type; let every
            # eligible upstream try them.
            return SymbolQuery(
                family=Family.PE,
                role=None,
                ext=None,
                raw_path=path,
                inbound_layout=self.name,
            )

        role = _SYMSTORE_ROLES.get(ext)
        if role is None:
            logger.warning(
                "Path '%s' doesn't have a recognized file extension (%s)", path, ext
            )
            return None

        return SymbolQuery(
            family=Family.PE,
            role=role,
            ext=ext,
            raw_path=path,
            inbound_layout=self.name,
        )

    def render(self, base_url: str, query: SymbolQuery) -> Optional[str]:
        # PE is single-layout (nothing but symstore ever carries it), so there
        # is no cross-layout translation to do -- pass the request through
        # verbatim, preserving today's exact URLs (and cache keys).
        return base_url.rstrip("/") + query.raw_path


# Registry of known layouts, keyed by the name used in upstream config.
LAYOUTS: dict[str, Layout] = {
    SymStoreLayout.name: SymStoreLayout(),
}


def classify_request(path: str) -> Optional[SymbolQuery]:
    """
    Classify an inbound request path into a layout-independent SymbolQuery.

    Returns ``None`` if no known inbound layout recognizes the path; the caller
    turns that into a 404 without touching the cache.
    """
    # Phase 1: only the symstore layout is understood on the way in.
    return LAYOUTS[SymStoreLayout.name].classify(path)
