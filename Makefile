.PHONY: refresh
refresh:
	poetry update --with dev
	poetry sync --with dev

.PHONY: check
check: typecheck

.PHONY: typecheck
typecheck:
	python3 -m mypy saberdb
