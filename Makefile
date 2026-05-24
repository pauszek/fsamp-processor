.PHONY: help install dev test lint format type-check clean build run docker-build

PYTHON := python3
VENV := .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff
BLACK := $(VENV)/bin/black
MYPY := $(VENV)/bin/mypy

IMAGE_NAME := fsamp-processor
IMAGE_TAG := latest

help:
	@echo "FSAMP Processor - Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "  install       Install production dependencies"
	@echo "  dev           Install development dependencies"
	@echo ""
	@echo "Quality:"
	@echo "  lint          Run linter (ruff)"
	@echo "  format        Format code (black + ruff)"
	@echo "  type-check    Run type checker (mypy)"
	@echo "  check         Run all quality checks"
	@echo ""
	@echo "Testing:"
	@echo "  test          Run all tests"
	@echo "  test-unit     Run unit tests only"
	@echo "  test-int      Run integration tests only"
	@echo "  coverage      Run tests with coverage"
	@echo ""
	@echo "Running:"
	@echo "  run           Run processor locally"
	@echo "  run-debug     Run with debug logging"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build  Build Docker image"
	@echo ""
	@echo "Cleanup:"
	@echo "  clean         Remove build artifacts"
	@echo "  clean-all     Remove all generated files"

$(VENV):
	$(PYTHON) -m venv $(VENV)

install: $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

dev: $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements-dev.txt
	$(PIP) install -e ".[dev]"

lint: $(VENV)
	$(RUFF) check src/ tests/

format: $(VENV)
	$(BLACK) src/ tests/
	$(RUFF) check --fix src/ tests/

type-check: $(VENV)
	$(MYPY) src/

check: lint type-check
	@echo "All checks passed!"

test: $(VENV)
	$(PYTEST) tests/ -v

test-unit: $(VENV)
	$(PYTEST) tests/unit/ -v -m "unit or not integration"

test-int: $(VENV)
	$(PYTEST) tests/integration/ -v -m integration

coverage: $(VENV)
	$(PYTEST) tests/ --cov=processor --cov-report=html --cov-report=term-missing

run: $(VENV)
	$(VENV)/bin/python -m processor.main

run-debug: $(VENV)
	LOG_LEVEL=DEBUG LOG_FORMAT=console $(VENV)/bin/python -m processor.main

docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf src/*.egg-info/
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/
	rm -rf .mypy_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

clean-all: clean
	rm -rf $(VENV)
	rm -rf .env
