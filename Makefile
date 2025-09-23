.PHONY: refresh
refresh:
	rm poetry.lock
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

.PHONY: todos
todos:
	-@find saberdb -type f | xargs -I% egrep --color=always -nHi '[#]\s*todo' '%'

.PHONY: raw_todos
raw_todos:
	-@find saberdb -type f | xargs -I% egrep -nHi '[#]\s*todo' '%' | egrep -v -i '[#] todo\(mkcmkc\)[:]'
