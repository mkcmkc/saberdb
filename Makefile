.PHONY: refresh
refresh:
	poetry install --with dev
	poetry update --with dev
	poetry sync --with dev

.PHONY: check
check: typecheck lint

.PHONY: typecheck
typecheck:
	python3 -m mypy saberdb

.PHONY: lint
lint:
	python3 -m ruff check saberdb/

.PHONY: format
format:
	python3 -m ruff format saberdb/
