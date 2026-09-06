.PHONY: setup run test lint demo clean

setup:
	python3 -m venv .venv
	.venv/bin/python -m pip install --upgrade pip
	.venv/bin/pip install -e '.[dev]'

run:
	.venv/bin/crashsafe-stack

test:
	.venv/bin/pytest

lint:
	.venv/bin/ruff check .
	.venv/bin/mypy crashsafe

demo:
	.venv/bin/python scripts/demo_ambiguous_charge.py

clean:
	rm -rf .crashsafe .pytest_cache .mypy_cache .ruff_cache
