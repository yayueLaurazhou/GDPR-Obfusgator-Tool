# Variables
PYTHON = python3
VENV_DIR = .env
PIP = $(VENV_DIR)/bin/pip
BLACK = $(VENV_DIR)/bin/black
FLAKE8 = $(VENV_DIR)/bin/flake8
PYTEST = $(VENV_DIR)/bin/pytest


# Create a virtual environment
venv:
	@echo "Creating virtual environment..."
	$(PYTHON) -m venv $(VENV_DIR)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install black flake8 pytest

# Run PEP8 check using flake8 and auto-format code using black
pep8-check:
	@echo "Running PEP8 auto-format with black..."
	$(BLACK) obfuscate_tool.py
	@echo "Running PEP8 check with flake8..."
	$(FLAKE8) obfuscate_tool.py --max-line-length=100 --exclude=__pycache__

# Run all unit tests using pytest
test:
	@echo "Running unit tests with pytest..."
	$(PYTEST) -v test.py

# Run all tests (PEP8 check + security test + unit tests)
all: venv pep8-check test
	@echo "All checks and tests passed!"
