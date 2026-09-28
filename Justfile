# Format source and test files.
format:
    uv run --extra dev --frozen ruff format src tests

# Run linting and type checks.
lint:
    uv run --extra dev --frozen ruff check src tests
    uv run --extra dev --frozen mypy src

# Run the test suite with coverage.
test:
    uv run --extra dev --frozen pytest --cov=ocs_submission --cov-report=term-missing

# Format, lint, and test.
validate: format lint test
