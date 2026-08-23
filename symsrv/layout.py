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
keeps layout-specific knowledge (path shapes, extension handling, build-id/UUID
translation) out of the request handler.

Two layouts are implemented:

- :class:`SymStoreLayout` -- Microsoft SymbolStore / SSQP, ``/<name>/<id>/<name>``.
  PE/COFF requests are passed through verbatim. ELF/Mach-O requests carry their
  identity in the middle ``<id>`` field (``elf-buildid[-sym]-<hex>`` /
  ``mach-uuid[-sym]-<hex>``), which we decode so other layouts can render them.
- :class:`DebuginfodLayout` -- ``/buildid/<hex>/{debuginfo,executable,source/...}``.
  Renders/accepts ELF requests by build-id.

Cross-layout translation falls out of this: a Sentry SymbolStore ELF request and
a native debuginfod request both classify to the same ELF :class:`SymbolQuery`,
and the debuginfod renderer produces the same canonical URL for both -- so they
share one cache entry per upstream.

The Mach-O (``lldb``) render side is not implemented yet; ``mach-uuid-*`` requests
decode to a Mach-O query that currently no upstream can render (-> 404, no cache
write), and will "light up" once an lldb layout + Mach-O upstream are added.
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
    SOURCE = "source"  # source file (debuginfod only)


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
    # Decoded identity for translatable families: the full lowercase-hex GNU
    # build-id (ELF) or UUID (Mach-O). None for PE (never decoded) and for
    # ELF/Mach-O forms that don't carry a full id (e.g. the lossy SymStore GUID).
    ident: Optional[str] = None
    # For debuginfod source requests, the source path after ".../source/".
    source_path: Optional[str] = None


class Layout(Protocol):
    name: str

    def classify(self, path: str) -> Optional[SymbolQuery]: ...

    def render(self, base_url: str, query: SymbolQuery) -> Optional[str]: ...


def _is_hex(s: str) -> bool:
    return bool(s) and all(c in "0123456789abcdefABCDEF" for c in s)


# ---------------------------------------------------------------------------
# Microsoft SymbolStore / SSQP layout
# ---------------------------------------------------------------------------

# SSQP middle-field markers -> (family, role). Ordered longest-first so the
# "-sym-" variants match before their bare prefixes.
_SSQP_MARKERS: tuple[tuple[str, Family, Role], ...] = (
    ("elf-buildid-sym-", Family.ELF, Role.DEBUG),
    ("elf-buildid-", Family.ELF, Role.IMAGE),
    ("mach-uuid-sym-", Family.MACHO, Role.DEBUG),
    ("mach-uuid-", Family.MACHO, Role.IMAGE),
)

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
        # ELF / Mach-O requests carry their identity in the middle <id> field.
        # Decode those first (family/role/build-id); PE keys off the extension.
        parts = path.split("/")
        if len(parts) >= 3:
            ssqp = self._classify_ssqp(parts[2], path)
            if ssqp is not None:
                return ssqp

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

    def _classify_ssqp(self, id_field: str, path: str) -> Optional[SymbolQuery]:
        """Decode an ELF/Mach-O SSQP <id> field, or None if it isn't one."""
        for prefix, family, role in _SSQP_MARKERS:
            if not id_field.startswith(prefix):
                continue
            ident = id_field[len(prefix) :]
            if not _is_hex(ident):
                # Marked as ELF/Mach-O but malformed: reject (no fallback to PE).
                logger.warning("Malformed SSQP id field '%s' in '%s'", id_field, path)
                return None
            return SymbolQuery(
                family=family,
                role=role,
                ext=None,
                raw_path=path,
                inbound_layout=self.name,
                ident=ident.lower(),
            )
        return None

    def render(self, base_url: str, query: SymbolQuery) -> Optional[str]:
        # A symstore upstream can only re-emit a request it received in symstore
        # form (it has no way to reconstruct the SSQP <name> otherwise). PE is
        # single-layout so this is always the case in practice; passing the path
        # through verbatim preserves today's exact URLs (and cache keys).
        if query.inbound_layout != self.name:
            return None
        return base_url.rstrip("/") + query.raw_path


# ---------------------------------------------------------------------------
# debuginfod layout
# ---------------------------------------------------------------------------


class DebuginfodLayout:
    """debuginfod URI layout: ``/buildid/<hex>/{debuginfo,executable,source/...}``."""

    name = "debuginfod"

    _ARTIFACT_ROLES = {"debuginfo": Role.DEBUG, "executable": Role.IMAGE}

    def classify(self, path: str) -> Optional[SymbolQuery]:
        # Keep the source remainder (which may itself contain slashes) intact.
        parts = path.split("/", 4)
        if len(parts) < 4 or parts[0] != "" or parts[1] != "buildid":
            return None

        ident = parts[2]
        if not _is_hex(ident):
            return None

        artifact = parts[3]
        if artifact == "source":
            source_path = parts[4] if len(parts) == 5 else ""
            if not source_path:
                return None
            return SymbolQuery(
                family=Family.ELF,
                role=Role.SOURCE,
                ext=None,
                raw_path=path,
                inbound_layout=self.name,
                ident=ident.lower(),
                source_path=source_path,
            )

        role = self._ARTIFACT_ROLES.get(artifact)
        if role is None:
            return None
        # Reject trailing junk after the artifact (e.g. /buildid/id/debuginfo/x).
        if len(parts) == 5 and parts[4] != "":
            return None

        return SymbolQuery(
            family=Family.ELF,
            role=role,
            ext=None,
            raw_path=path,
            inbound_layout=self.name,
            ident=ident.lower(),
        )

    def render(self, base_url: str, query: SymbolQuery) -> Optional[str]:
        # Render canonically from the decoded identity (never from raw_path), so
        # a SymbolStore-ELF request and a native debuginfod request converge on
        # the same URL -> the same cache entry.
        if query.family is not Family.ELF or not query.ident:
            return None
        base = f"{base_url.rstrip('/')}/buildid/{query.ident}"
        if query.role is Role.DEBUG:
            return f"{base}/debuginfo"
        if query.role is Role.IMAGE:
            return f"{base}/executable"
        if query.role is Role.SOURCE and query.source_path:
            return f"{base}/source/{query.source_path}"
        return None


# Registry of known layouts, keyed by the name used in upstream config.
LAYOUTS: dict[str, Layout] = {
    SymStoreLayout.name: SymStoreLayout(),
    DebuginfodLayout.name: DebuginfodLayout(),
}

# Inbound layouts, tried in order. debuginfod is strict (`/buildid/...`) so it
# never swallows a symstore path; symstore is the general fallback.
_INBOUND_LAYOUTS = (DebuginfodLayout.name, SymStoreLayout.name)


def classify_request(path: str) -> Optional[SymbolQuery]:
    """
    Classify an inbound request path into a layout-independent SymbolQuery.

    Returns ``None`` if no known inbound layout recognizes the path; the caller
    turns that into a 404 without touching the cache.
    """
    for name in _INBOUND_LAYOUTS:
        query = LAYOUTS[name].classify(path)
        if query is not None:
            return query
    return None
