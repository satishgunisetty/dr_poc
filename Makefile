.PHONY: build build-all run-dev run-prod test lint format clean help precheck prechecks activate check-precommit check-black check-isort check-flake8 check-pylint check-mypy check-pytest

# Variables
DOCKER_IMAGE = databricks-project
CONTAINER_NAME = databricks-app
PYTHON_VERSION = 3.12

help: ## Display this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@awk '/^[a-zA-Z\-_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "  %-20s %s\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

activate: ## Activate the virtual environment
	@echo "Activating virtual environment..."
	@VENV_PATH=$$(poetry env info --path) && \
	if [ -z "$$VENV_PATH" ]; then \
		echo "Creating new virtual environment..." && \
		poetry install && \
		VENV_PATH=$$(poetry env info --path); \
	fi && \
	echo "Virtual environment path: $$VENV_PATH" && \
	echo "To activate the virtual environment, run:" && \
	echo "poetry env activate $$VENV_PATH" && \
	echo "Or use: poetry run <command>"

check-precommit: ## Run pre-commit hooks
	@echo "Running pre-commit hooks..."
	@poetry run pre-commit run --all-files || { echo "❌ Pre-commit hooks failed"; exit 1; }
	@echo "✅ Pre-commit hooks passed"

check-black: ## Run black formatter
	@echo "Running black formatter..."
	@poetry run black . --check || { echo "❌ Black formatting check failed"; exit 1; }
	@echo "✅ Black formatting check passed"

check-isort: ## Run isort
	@echo "Running isort..."
	@poetry run isort . --check-only || { echo "❌ isort check failed"; exit 1; }
	@echo "✅ isort check passed"

check-flake8: ## Run flake8
	@echo "Running flake8..."
	@poetry run flake8 || { echo "❌ flake8 check failed"; exit 1; }
	@echo "✅ flake8 check passed"

check-pylint: ## Run pylint
	@echo "Running pylint..."
	@poetry run pylint src/ || { echo "❌ pylint check failed"; exit 1; }
	@echo "✅ pylint check passed"

check-mypy: ## Run mypy
	@echo "Running mypy..."
	@poetry run mypy src/ || { echo "❌ mypy check failed"; exit 1; }
	@echo "✅ mypy check passed"

check-pytest: ## Run pytest
	@echo "Running tests..."
	@poetry run pytest || { echo "❌ pytest failed"; exit 1; }
	@echo "✅ pytest passed"

prechecks: check-precommit check-black check-isort check-flake8 check-pylint check-mypy check-pytest ## Run all pre-commit checks and validations
	@echo "✨ All pre-commit checks passed successfully!"

precheck: ## Run all pre-build checks (format, lint, test)
	@echo "Running pre-build checks..."
	@echo "1. Formatting code..."
	@docker run -it --rm \
		--name $(CONTAINER_NAME)-format \
		-v $(PWD):/app \
		$(DOCKER_IMAGE) \
		sh -c "poetry run black . && poetry run isort ."
	@echo "2. Running linters..."
	@docker run -it --rm \
		--name $(CONTAINER_NAME)-lint \
		-v $(PWD):/app \
		$(DOCKER_IMAGE) \
		sh -c "poetry run flake8 && poetry run pylint src/ && poetry run mypy src/"
	@echo "3. Running tests..."
	@docker run -it --rm \
		--name $(CONTAINER_NAME)-test \
		-v $(PWD):/app \
		$(DOCKER_IMAGE) \
		poetry run pytest
	@echo "Pre-build checks completed successfully!"

build: ## Build the Docker image
	docker build -t $(DOCKER_IMAGE) .

build-all: prechecks build ## Run all checks and build the Docker image
	@echo "Build process completed successfully!"

run-dev: ## Run the application in development mode
	docker run -it --rm \
		--name $(CONTAINER_NAME)-dev \
		-v $(PWD):/app \
		-e ENVIRONMENT=development \
		-e PYTHONPATH=/app \
		-e PYTHONUNBUFFERED=1 \
		$(DOCKER_IMAGE) \
		poetry run uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload

run-prod: ## Run the application in production mode
	docker run -it --rm \
		--name $(CONTAINER_NAME)-prod \
		-e ENVIRONMENT=production \
		-e PYTHONPATH=/app \
		-e PYTHONUNBUFFERED=1 \
		$(DOCKER_IMAGE) \
		poetry run uvicorn src.main:app --host 0.0.0.0 --port 8000 --workers 4

run-job: ## Run the example job
	docker run -it --rm \
		--name $(CONTAINER_NAME)-job \
		-e ENVIRONMENT=development \
		-e PYTHONPATH=/app \
		$(DOCKER_IMAGE) \
		poetry run python -m src.jobs.example_job

test: ## Run tests
	docker run -it --rm \
		--name $(CONTAINER_NAME)-test \
		-v $(PWD):/app \
		$(DOCKER_IMAGE) \
		poetry run pytest

lint: ## Run all linters
	docker run -it --rm \
		--name $(CONTAINER_NAME)-lint \
		-v $(PWD):/app \
		$(DOCKER_IMAGE) \
		sh -c "poetry run flake8 && poetry run pylint src/ && poetry run mypy src/"

format: ## Format code using black and isort
	docker run -it --rm \
		--name $(CONTAINER_NAME)-format \
		-v $(PWD):/app \
		$(DOCKER_IMAGE) \
		sh -c "poetry run black . && poetry run isort ."

clean: ## Clean up Docker resources
	docker system prune -f
	docker rmi $(DOCKER_IMAGE) 2>/dev/null || true

setup-local: ## Set up local development environment
	python -m pip install --upgrade pip
	python -m pip install poetry
	poetry install
	poetry run pre-commit install

dev-shell: ## Start a development shell in the container
	docker run -it --rm \
		--name $(CONTAINER_NAME)-shell \
		-v $(PWD):/app \
		$(DOCKER_IMAGE) \
		/bin/bash 