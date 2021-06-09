srcs_comma_sep = src,tests,lambda_function.py
comma = ,
srcs = $(subst $(comma), ,$(srcs_comma_sep))

.PHONY: test
test:
	@coverage run -m --branch --source=$(srcs_comma_sep) pytest tests

.PHONY: testcov
testcov: test
	@coverage html --precision=2 --skip-covered && x-www-browser htmlcov/index.html

.PHONY: lint
lint:
	@flake8 --max-line-length 88 --extend-ignore=E203,W503 $(srcs)

.PHONY: clean
clean:
	@-python3 -Bc "for p in __import__('pathlib').Path('.').rglob('*.py[co]'): p.unlink()"
	@-python3 -Bc "for p in __import__('pathlib').Path('.').rglob('__pycache__'): p.rmdir()"
	@-rm -rf .pytest_cache
	@-rm -rf htmlcov
	@-rm .coverage
