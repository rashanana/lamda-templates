.PHONY: build invoke start test lint deploy-dev deploy-staging deploy-prod

build:
	sam build --cached --parallel

invoke: build
	sam local invoke MyFunction -e tests/events/get_item.json --env-vars env.local.json

start: build
	sam local start-api --env-vars env.local.json --port 3000

test:
	pip install pytest pytest-cov --quiet
	pytest tests/ -v --cov=src --cov-report=term-missing

lint:
	sam validate --lint

deploy-dev: build
	sam deploy --config-env default

deploy-staging: build
	sam deploy --config-env staging

deploy-prod: build
	sam deploy --config-env prod
