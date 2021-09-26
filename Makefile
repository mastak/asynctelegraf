.PHONY: install
install-dev:
	@pip install -r ./requirements.dev.txt


.PHONY: test
test:
	pytest --timeout=5 --durations=5 -v --cov-config .coveragerc --cov=asynctelegraf --cov-report term-missing --cov-report=term --cov-report=html --cov-report=xml .

.PHONY: lint-black
lint-black:
	#black --check --diff  --line-length 120 .
	echo 1

.PHONY: lint-flake8
lint-flake8:
	flake8 asynctelegraf tests

.PHONY: lint-isort
lint-isort:
	isort .

.PHONY: lint-mypy
lint-mypy:
	mypy --ignore-missing-imports --follow-imports=silent asynctelegraf tests

.PHONY: lint
lint: lint-black lint-flake8 lint-isort lint-mypy

.PHONY: publish
publish:
	publish --username=$(PYPI_USERNAME) --password=$(PYPI_PASSWORD) --build
