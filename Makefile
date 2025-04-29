export XDG_CACHE_HOME=$(shell pwd)/.cache
export UV_CACHE_DIR=$(shell pwd)/.cache/uv
export UV_PYTHON_INSTALL_DIR=$(shell pwd)/.local/share/uv/python

all: sync

run:
	uv run --offline --no-sync --no-cache serve

sync: .venv
	uv sync

.venv:
	uv python install
	uv venv

.PHONY: all run sync
