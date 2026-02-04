.PHONY: help install build deploy test clean local-test

help:
	@echo "Epic to Sprint Planner - Available Commands:"
	@echo ""
	@echo "  make install          - Install development dependencies"
	@echo "  make build            - Build SAM application"
	@echo "  make deploy           - Deploy to AWS (runs build first)"
	@echo "  make deploy-guided    - Deploy with guided setup"
	@echo "  make test             - Run tests"
	@echo "  make local-test       - Run local processing test"
	@echo "  make clean            - Clean build artifacts"
	@echo "  make format           - Format code with black"
	@echo "  make lint             - Lint code with flake8"

install:
	pip install -r requirements-dev.txt
	@echo "Installing Lambda dependencies..."
	cd src/lambdas/chunker && pip install -r requirements.txt -t .
	cd src/lambdas/story_generator && pip install -r requirements.txt -t .
	cd src/lambdas/aggregator && pip install -r requirements.txt -t .

build:
	sam build

deploy: build
	sam deploy

deploy-guided: build
	sam deploy --guided

test:
	pytest tests/ -v --cov=src

local-test:
	python local_test.py process

format:
	black src/ tests/ local_test.py

lint:
	flake8 src/ tests/ local_test.py --max-line-length=120

clean:
	rm -rf .aws-sam
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
