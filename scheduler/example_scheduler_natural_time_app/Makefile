.PHONY: default install bundle release test lint
.DEFAULT_GOAL := default
UV_SYNC = uv sync --frozen --group dev --no-install-project --no-editable
UV_RUN = uv run --frozen --group dev

default: install
install: prepare-venv
	@$(UV_SYNC)
prepare-venv:
	@which uv > /dev/null || (echo "uv not found, please install it first" && exit 1;)
bundle:
	@create-corva-app zip . --bump-version=skip
release:
	@create-corva-app release . --bump-version=skip
test: prepare-venv
	@$(UV_RUN) pytest
lint: prepare-venv
	@$(UV_RUN) ruff check .
