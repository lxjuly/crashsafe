.PHONY: setup run test lint demo demo-temporal clean

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

demo-temporal:
	uv run --no-project --python 3.12 --with temporalio==1.18.2 --with-editable . \
		python -m experiments.temporal_compare.demo

clean:
	rm -rf .crashsafe .pytest_cache .mypy_cache .ruff_cache
