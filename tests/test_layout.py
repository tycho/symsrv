#!/usr/bin/python
# pylint: disable=C0114,C0115,C0116,C0301

"""
Unit tests for the layout codec and upstream admission logic.

These are pure functions (no server, no cache, no network), so they run either
under pytest or standalone:  python tests/test_layout.py
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

# Load the pure modules (layout, upstream) in isolation, WITHOUT executing
# symsrv/__init__.py -- that pulls in the whole server, including the Unix-only
# `fcntl`, which makes the package unimportable on Windows. We register a
# lightweight namespace package so the modules' relative imports still resolve.
_SYMSRV_DIR = Path(__file__).resolve().parent.parent / "symsrv"

if "symsrv" not in sys.modules:
    _pkg = types.ModuleType("symsrv")
    _pkg.__path__ = [str(_SYMSRV_DIR)]
    _pkg.__package__ = "symsrv"
    sys.modules["symsrv"] = _pkg


def _load(name: str):
    full = f"symsrv.{name}"
    if full in sys.modules:
        return sys.modules[full]
    spec = importlib.util.spec_from_file_location(full, _SYMSRV_DIR / f"{name}.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[full] = mod
    spec.loader.exec_module(mod)
    return mod


_layout = _load("layout")
_upstream = _load("upstream")

Family = _layout.Family
Role = _layout.Role
SymStoreLayout = _layout.SymStoreLayout
SymbolQuery = _layout.SymbolQuery
classify_request = _layout.classify_request
Upstream = _upstream.Upstream


# ---------------------------------------------------------------------------
# classify_request (symstore inbound)
# ---------------------------------------------------------------------------


def test_classify_pdb_is_debug():
    q = classify_request("/foo.pdb/AABBCCDD1/foo.pdb")
    assert q is not None
    assert q.family is Family.PE
    assert q.role is Role.DEBUG
    assert q.ext == ".pdb"
    assert q.raw_path == "/foo.pdb/AABBCCDD1/foo.pdb"
    assert q.inbound_layout == "symstore"


def test_classify_image_extensions_are_image():
    for name, ext in (("foo.dll", ".dll"), ("foo.exe", ".exe"), ("foo.sys", ".sys")):
        q = classify_request(f"/{name}/HASH/{name}")
        assert q is not None, ext
        assert q.role is Role.IMAGE, ext
        assert q.ext == ext


def test_classify_snupkg_is_debug():
    q = classify_request("/foo.snupkg/HASH/foo.snupkg")
    assert q is not None
    assert q.role is Role.DEBUG
    assert q.ext == ".snupkg"


def test_classify_compressed_normalizes_ext_but_keeps_raw_path():
    # ".pd_" is the compressed form of ".pdb"; the fetched URL must stay verbatim
    # (with .pd_) while narrowing/role use the canonical ".pdb".
    q = classify_request("/foo.pd_/HASH/foo.pd_")
    assert q is not None
    assert q.ext == ".pdb"
    assert q.role is Role.DEBUG
    assert q.raw_path == "/foo.pd_/HASH/foo.pd_"


def test_classify_ptr_allows_all():
    q = classify_request("/foo.ptr")
    assert q is not None
    assert q.role is None
    assert q.ext is None


def test_classify_uppercase_extension():
    q = classify_request("/FOO.PDB/HASH/FOO.PDB")
    assert q is not None
    assert q.ext == ".pdb"
    assert q.role is Role.DEBUG


def test_classify_unrecognized_returns_none():
    # ELF/Mach-O and unknown extensions are not understood in Phase 1.
    assert classify_request("/libpulse.so/HASH/libpulse.so") is None
    assert classify_request("/foo/HASH/foo") is None
    assert classify_request("/foo.txt/HASH/foo.txt") is None


# ---------------------------------------------------------------------------
# SymStoreLayout.render (verbatim passthrough)
# ---------------------------------------------------------------------------


def test_render_is_verbatim():
    layout = SymStoreLayout()
    q = classify_request("/foo.pdb/HASH/foo.pdb")
    assert q is not None
    assert (
        layout.render("http://msdl.microsoft.com/download/symbols", q)
        == "http://msdl.microsoft.com/download/symbols/foo.pdb/HASH/foo.pdb"
    )


def test_render_strips_trailing_slash():
    layout = SymStoreLayout()
    q = classify_request("/foo.pdb/HASH/foo.pdb")
    assert q is not None
    assert layout.render("https://example.com/", q) == (
        "https://example.com/foo.pdb/HASH/foo.pdb"
    )


# ---------------------------------------------------------------------------
# Upstream admission (binformat / provides / extensions gates)
# ---------------------------------------------------------------------------


def _q(path):
    q = classify_request(path)
    assert q is not None, path
    return q


def test_provides_image_only_rejects_debug():
    # nvidia-style: images only.
    up = Upstream.from_config(
        {"name": "nvidia", "base_url": "https://nv", "provides": ["image"]}
    )
    assert up.candidate_url(_q("/foo.dll/HASH/foo.dll")) is not None
    assert up.candidate_url(_q("/foo.sys/HASH/foo.sys")) is not None
    assert up.candidate_url(_q("/foo.pdb/HASH/foo.pdb")) is None
    assert up.candidate_url(_q("/foo.snupkg/HASH/foo.snupkg")) is None
    # .ptr index files (role None) are still offered.
    assert up.candidate_url(_q("/foo.ptr")) is not None


def test_extensions_narrowing_excludes_sys():
    # mozilla-style: has debug + image, but no kernel drivers.
    up = Upstream.from_config(
        {
            "name": "mozilla",
            "base_url": "https://moz",
            "extensions": [".pdb", ".dll", ".exe"],
        }
    )
    assert up.candidate_url(_q("/foo.pdb/HASH/foo.pdb")) is not None
    assert up.candidate_url(_q("/foo.dll/HASH/foo.dll")) is not None
    assert up.candidate_url(_q("/foo.sys/HASH/foo.sys")) is None
    # .ptr bypasses extension narrowing (query.ext is None).
    assert up.candidate_url(_q("/foo.ptr")) is not None


def test_default_upstream_serves_everything_pe():
    # microsoft/nuget-style: no narrowing.
    up = Upstream.from_config({"name": "microsoft", "base_url": "https://msdl"})
    for path in (
        "/foo.pdb/HASH/foo.pdb",
        "/foo.dll/HASH/foo.dll",
        "/foo.sys/HASH/foo.sys",
        "/foo.snupkg/HASH/foo.snupkg",
        "/foo.ptr",
    ):
        assert up.candidate_url(_q(path)) is not None, path


def test_binformat_gate_rejects_wrong_family():
    # A hypothetical ELF query must never reach a PE-only upstream. (Phase 1
    # never produces ELF queries, but the gate must hold for Phase 2.)
    elf_q = SymbolQuery(
        family=Family.ELF,
        role=Role.DEBUG,
        ext=None,
        raw_path="/x",
        inbound_layout="symstore",
    )
    pe_up = Upstream.from_config({"name": "microsoft", "base_url": "https://msdl"})
    assert pe_up.candidate_url(elf_q) is None

    elf_up = Upstream.from_config(
        {"name": "arch", "base_url": "https://arch", "binformat": ["elf"]}
    )
    assert elf_up.accepts(elf_q) is True


def test_from_config_parses_axes():
    up = Upstream.from_config(
        {
            "name": "x",
            "base_url": "https://x",
            "binformat": "pe",  # bare string accepted
            "provides": ["image", "debug"],
            "extensions": [".PDB"],  # case-normalized
            "requires_symbolchecksum": True,
            "nocache": True,
        }
    )
    assert up.binformat == frozenset({Family.PE})
    assert up.provides == frozenset({Role.IMAGE, Role.DEBUG})
    assert up.extensions == frozenset({".pdb"})
    assert up.requires_symbolchecksum is True
    assert up.cachable is False


def test_from_config_defaults():
    up = Upstream.from_config({"name": "d", "base_url": "https://d"})
    assert up.layout == "symstore"
    assert up.binformat == frozenset({Family.PE})
    assert up.provides == frozenset({Role.IMAGE, Role.DEBUG})
    assert up.extensions is None
    assert up.cachable is True


def test_from_config_rejects_unknown_layout():
    try:
        Upstream.from_config(
            {"name": "bad", "base_url": "https://b", "layout": "bogus"}
        )
    except ValueError:
        return
    raise AssertionError("expected ValueError for unknown layout")


# ---------------------------------------------------------------------------
# Standalone runner (no pytest required)
# ---------------------------------------------------------------------------


def _run_all() -> int:
    tests = [
        v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"ok   {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {e!r}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_run_all())
