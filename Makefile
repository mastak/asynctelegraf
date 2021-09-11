.PHONY: install
install-dev:
	@pip install -r ./requirements.dev.txt


.PHONY: test
test:
	pytest --timeout=5 --durations=5 -v --cov-config .coveragerc --cov=asynctelegraf --cov-report term-missing .
