# Databricks Project

A Python project template for Databricks ETL jobs.

## Features

- Modern Python project structure with Poetry for dependency management
- PySpark and Delta Lake integration
- Comprehensive test suite with pytest and coverage reporting
- Code quality tools (black, isort, flake8, mypy, pylint)
- Pre-commit hooks for code quality checks
- Docker support for containerized development and deployment

## Getting Started

1. Clone the repository:
```bash
git clone https://github.com/yourusername/databricks-project.git
cd databricks-project
```

2. Install dependencies using Poetry:
```bash
poetry install
```

3. Run the tests:
```bash
poetry run pytest
```

4. Run the example job:
```bash
poetry run python -m src.jobs.example_job
```

## Project Structure

```
.
├── README.md
├── pyproject.toml
├── .flake8
├── .pylintrc
├── .pre-commit-config.yaml
├── requirements.txt
├── Dockerfile
├── Makefile
├── .dockerignore
├── src/
│   ├── __init__.py
│   ├── config/
│   │   └── __init__.py
│   ├── jobs/
│   │   └── __init__.py
│   └── utils/
│       └── __init__.py
├── tests/
│   └── __init__.py
└── notebooks/
    └── example.ipynb
```

## Development

1. Install pre-commit hooks:
```bash
poetry run pre-commit install
```

2. Run code quality checks:
```bash
make build-all
```

3. Build Docker image:
```bash
docker build -t databricks-project .
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Requirements

- Python 3.12
- Poetry for dependency management
- Docker (optional)
- Make

## Setup

### Local Development

1. Create a new Databricks workspace
2. Import this project into your Databricks workspace
3. Set up local development environment:
   ```bash
   make setup-local
   ```
4. Activate the virtual environment:
   ```bash
   make activate
   ```
   This will show you the virtual environment path and activation command. You can then activate the environment using either:
   ```bash
   # Method 1: Using source
   source <virtual-environment-path>/bin/activate

   # Method 2: Using poetry
   poetry shell
   ```

5. Configure your environment variables in the config directory

### Docker Setup

1. Build the Docker image with pre-build checks:
   ```bash
   make build-all
   ```
   This will:
   - Run all pre-commit checks
   - Format code using black and isort
   - Run all linters (flake8, pylint, mypy)
   - Run tests
   - Build the Docker image

   Or build without checks:
   ```bash
   make build
   ```

2. Run the application:
   ```bash
   # Run in development mode with hot-reload
   make run-dev

   # Run in production mode
   make run-prod

   # Run the example job
   make run-job
   ```

3. Clean up Docker resources:
   ```bash
   make clean
   ```

## Development Commands

The project provides several Make commands for development:

```bash
# Show all available commands
make help

# Show virtual environment activation instructions
make activate

# Run all pre-commit checks and validations
make prechecks

# Run pre-build checks (format, lint, test)
make precheck

# Run tests
make test

# Run linting
make lint

# Format code
make format

# Start a development shell in the container
make dev-shell
```

## Pre-commit Checks

The project includes comprehensive pre-commit checks that can be run using:

```bash
make prechecks
```

This will run:
1. Pre-commit hooks
2. Black formatter (check mode)
3. isort (check mode)
4. flake8
5. pylint
6. mypy
7. pytest

## Code Quality Tools

This project uses several tools to maintain code quality:

- **Poetry**: Modern Python package and dependency management
- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Code linting
- **pylint**: Code analysis
- **mypy**: Static type checking
- **pre-commit**: Git hooks for automated checks

## Usage

1. Navigate to the notebooks directory
2. Open example.ipynb to see a sample notebook
3. Create your own notebooks in the notebooks directory
4. Use the src directory for shared Python modules

## Development Guidelines

- Use `make activate` to get virtual environment activation instructions
- Use `make prechecks` to run all code quality checks
- Use `make build-all` for a complete build with all checks
- Use `make precheck` to run pre-build checks without building
- Use `make format` for code formatting
- Use `make lint` for code linting
- Use `make test` for running tests
- Write tests in the tests directory
- Follow PEP 8 style guide
- Run pre-commit hooks before committing changes
- Use Docker for consistent development and production environments
