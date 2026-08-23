#!/usr/bin/python
# pylint: disable=C0114,C0115,C0116,C0301

"""
Parsed application configuration.

:class:`AppConfig` reads the three TOML config files once at startup and exposes
typed values, replacing the module-global soup that used to live in
``symsrv/__init__.py``. It is stored on ``app.state.config``.
"""

from __future__ import annotations

import logging
import tomllib
from dataclasses import dataclass
from pathlib import Path

from .upstream import Upstream

logger = logging.getLogger("uvicorn.error")


@dataclass
class AppConfig:
    upstreams: dict[str, Upstream]
    substring_blacklist: list[str]
    suffix_blacklist: list[str]
    first_path_component_blacklist: list[str]
    ignore_access: set[tuple[int, str]]
    cache_backend_type: str
    cache_mode: str
    cache_path: str
    cache_ttl: dict[int, int]
    health_probe_interval: float
    health_probe_timeout: float
    # Raw main.toml, for sections consumed verbatim elsewhere (sqlite, uvicorn).
    main: dict

    @classmethod
    def load(cls, config_dir: str = "config") -> "AppConfig":
        d = Path(config_dir)
        with open(d / "main.toml", "rb") as f:
            main_cfg = tomllib.load(f)
        with open(d / "upstreams.toml", "rb") as f:
            ups_cfg = tomllib.load(f)
        with open(d / "blacklist.toml", "rb") as f:
            bl_cfg = tomllib.load(f)

        upstreams: dict[str, Upstream] = {}
        for u in ups_cfg["upstream"]:
            up = Upstream.from_config(u)
            upstreams[up.name] = up

        cache_ttl: dict[int, int] = {}
        for key, value in main_cfg.get("cache_ttl", {}).items():
            try:
                cache_ttl[int(key)] = int(value)
            except ValueError:
                logger.exception("Unparseable cache TTL value %s:%s", key, value)

        cache_cfg = main_cfg.get("cache", {})
        cache_path = cache_cfg.get("path")
        if not cache_path:
            raise ValueError("cache.path must be set in config/main.toml")

        ignore_access: set[tuple[int, str]] = set()
        for spec in main_cfg["log_filters"]["ignore_access"]:
            ignore_access.add((int(spec["status"]), str(spec["path"])))

        health_cfg = main_cfg.get("health", {})

        return cls(
            upstreams=upstreams,
            substring_blacklist=bl_cfg["substring_blacklist"].get("names", []),
            suffix_blacklist=bl_cfg["suffix_blacklist"].get("names", []),
            first_path_component_blacklist=bl_cfg["first_path_component_blacklist"].get(
                "names", []
            ),
            ignore_access=ignore_access,
            cache_backend_type=cache_cfg.get("type", "disk"),
            cache_mode=cache_cfg.get("mode", "standalone"),
            cache_path=cache_path,
            cache_ttl=cache_ttl,
            health_probe_interval=float(health_cfg.get("probe_interval", 30)),
            health_probe_timeout=float(health_cfg.get("probe_timeout", 15)),
            main=main_cfg,
        )
