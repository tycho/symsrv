#!/usr/bin/python
# pylint: disable=C0114,C0115,C0116,C0301

"""
Unit tests for the upstream circuit breaker (symsrv/health.py).

Pure/local: uses a temp mmap file and a fake HTTP session, no network. Runs
under pytest or standalone:  python tests/test_health.py
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import tempfile
import types
from pathlib import Path

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


_upstream = _load("upstream")
_health = _load("health")

Upstream = _upstream.Upstream
UpstreamHealth = _health.UpstreamHealth


# --- fakes -----------------------------------------------------------------


class _FakeResp:
    def __init__(self, status=200):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FailCM:
    async def __aenter__(self):
        raise TimeoutError("simulated transport failure")

    async def __aexit__(self, *a):
        return False


class _FakeSession:
    def __init__(self, alive=True):
        self.alive = alive
        self.head_calls = []

    def head(self, url, **kwargs):
        self.head_calls.append(url)
        return _FakeResp() if self.alive else _FailCM()


def _mk_upstreams(names):
    return {
        n: Upstream.from_config({"name": n, "base_url": f"https://{n}.example"})
        for n in names
    }


def _mk_health(session, names, path, **kw):
    h = UpstreamHealth(session, _mk_upstreams(names), path, **kw)
    h._open_state()  # set up the mmap without launching the probe task
    return h


# --- tests -----------------------------------------------------------------


def test_default_all_available():
    with tempfile.TemporaryDirectory() as d:
        h = _mk_health(_FakeSession(), ["a", "b", "c"], os.path.join(d, "h"))
        try:
            assert h.is_available("a")
            assert h.is_available("b")
            # Unknown upstream is never blocked.
            assert h.is_available("nonexistent")
        finally:
            asyncio.run(h.stop())


def test_trip_and_restore():
    with tempfile.TemporaryDirectory() as d:
        h = _mk_health(_FakeSession(), ["a", "b"], os.path.join(d, "h"))
        try:
            h.trip("a")
            assert not h.is_available("a")
            assert h.is_available("b")
            h._restore("a")
            assert h.is_available("a")
        finally:
            asyncio.run(h.stop())


def test_state_persists_across_instances_same_config():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "h")
        names = ["a", "b", "c"]
        h1 = _mk_health(_FakeSession(), names, path)
        h1.trip("b")
        asyncio.run(h1.stop())

        # A fresh instance with the same upstream set reuses the file.
        h2 = _mk_health(_FakeSession(), names, path)
        try:
            assert not h2.is_available("b")
            assert h2.is_available("a")
        finally:
            asyncio.run(h2.stop())


def test_state_discarded_when_config_changes():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "h")
        h1 = _mk_health(_FakeSession(), ["a", "b", "c"], path)
        h1.trip("a")
        asyncio.run(h1.stop())

        # Different upstream set -> config hash mismatch -> file reinitialized.
        h2 = _mk_health(_FakeSession(), ["a", "b"], path)
        try:
            assert h2.is_available("a")
        finally:
            asyncio.run(h2.stop())


def test_probe_restores_only_suspended_when_alive():
    with tempfile.TemporaryDirectory() as d:
        sess = _FakeSession(alive=True)
        h = _mk_health(sess, ["a", "b"], os.path.join(d, "h"))
        try:
            h.trip("a")
            asyncio.run(h.probe_once())
            assert h.is_available("a")  # restored
            # Only the suspended upstream is probed; healthy 'b' is skipped.
            assert sess.head_calls == ["https://a.example"]
        finally:
            asyncio.run(h.stop())


def test_probe_keeps_suspended_when_still_dead():
    with tempfile.TemporaryDirectory() as d:
        sess = _FakeSession(alive=False)
        h = _mk_health(sess, ["a", "b"], os.path.join(d, "h"))
        try:
            h.trip("a")
            asyncio.run(h.probe_once())
            assert not h.is_available("a")  # still suspended
            assert sess.head_calls == ["https://a.example"]
        finally:
            asyncio.run(h.stop())


# --- standalone runner -----------------------------------------------------


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
