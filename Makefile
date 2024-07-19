PYTHON := /usr/bin/python

export XDG_CACHE_HOME=$(shell pwd)/.cache

all: poetry-install

poetry-install: .venv
	.venv/bin/poetry install --only main

.venv: $(PYTHON)
	$(PYTHON) -m venv .venv
	.venv/bin/python -m pip install poetry
