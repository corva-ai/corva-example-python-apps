.PHONY: default install bundle release test
.DEFAULT_GOAL := default

default: install
install: prepare-venv
	@. venv/bin/activate && pip install -r requirements.txt
prepare-venv:
	@which python3 > /dev/null || (echo "python3 not found, please install it first" && exit 1;)
	@test -d venv || python3 -m venv venv
bundle:
	@create-corva-app zip . --bump-version=skip
release:
	@create-corva-app release . --bump-version=skip
test: prepare-venv
	@. venv/bin/activate && pytest
