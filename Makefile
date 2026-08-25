# =============================================================================
# NovaCart Pipeline -- Makefile
#
# Compatible with: macOS, Linux, and Windows (Git Bash / WSL).
# Native Windows cmd/PowerShell: use scripts/run_everything.py instead.
#
# Run `make install` once after cloning, then use the targets below.
#
# Usage:
#   make install          Create .venv and install all dependencies
#   make data             Generate sample landing data
#   make run              Run pipeline for default date (2025-11-10)
#   make run DATE=...     Run for a specific date, e.g. DATE=2025-11-08
#   make backfill         Run with a 3-day backfill window ending at DATE
#   make test             Run the full pytest suite
#   make cov              Run tests with HTML coverage report
#   make all              data -> backfill -> test  (full end-to-end)
#   make clean            Remove all pipeline-generated output
#   make clean-all        Remove generated output AND the virtual environment
#   make help             Print this message
# =============================================================================

# -----------------------------------------------------------------------------
# Platform detection
#
# On Windows (Git Bash / WSL), PYTHON and PIP resolve to the venv Scripts path.
# On macOS / Linux they resolve to the venv bin path.
# The ifeq check on OS is set by Windows to "Windows_NT"; absent on Unix.
# -----------------------------------------------------------------------------

ifeq ($(OS), Windows_NT)
    PYTHON  := .venv/Scripts/python.exe
    PIP     := .venv/Scripts/pip.exe
    # python launcher: prefer py -3 so any Python 3.x install is found
    PY3     := py -3
    RM_VENV := rmdir /s /q .venv
    ACT_MSG := .venv\Scripts\activate
else
    PYTHON  := .venv/bin/python
    PIP     := .venv/bin/pip
    PY3     := python3
    RM_VENV := rm -rf .venv
    ACT_MSG := source .venv/bin/activate
endif

# Default pipeline date and backfill window (override on the command line)
DATE     ?= 2025-11-10
BACKFILL ?= 3

# Coverage output directory
COV_DIR  := htmlcov

# =============================================================================
# Primary targets
# =============================================================================

.PHONY: install
install:  ## Create .venv and install all dependencies from requirements.txt
	$(PY3) -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo ""
	@echo "  Virtual environment ready. Activate with:"
	@echo "    $(ACT_MSG)"

.PHONY: data
data:  ## Generate sample landing data (orders CSVs, customers JSON, products SQLite)
	$(PYTHON) scripts/generate_sample_data.py

.PHONY: run
run:  ## Run the pipeline for DATE (default 2025-11-10). Override: make run DATE=2025-11-08
	$(PYTHON) -m src.pipeline --date $(DATE)

.PHONY: backfill
backfill:  ## Run the pipeline with BACKFILL-day window ending at DATE
	$(PYTHON) -m src.pipeline --date $(DATE) --backfill $(BACKFILL)

.PHONY: test
test:  ## Run the full pytest suite with verbose output
	$(PYTHON) -m pytest tests/ -v

.PHONY: cov
cov:  ## Run tests with coverage; HTML report written to htmlcov/
	$(PYTHON) -m pytest tests/ -v \
		--cov=src \
		--cov-report=term-missing \
		--cov-report=html:$(COV_DIR)
	@echo ""
	@echo "  Coverage report: $(COV_DIR)/index.html"

.PHONY: all
all: data backfill test  ## Generate data, run the pipeline, then run all tests

# =============================================================================
# Utility targets
# =============================================================================

.PHONY: clean
clean:  ## Remove pipeline output (bronze, silver, gold, quarantine, state, logs)
	$(PYTHON) scripts/cleanup.py

.PHONY: clean-all
clean-all: clean  ## Remove generated output AND the virtual environment
	$(RM_VENV)
	@echo "  removed .venv"

.PHONY: help
help:  ## Print available make targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# =============================================================================
# Code quality targets
# =============================================================================

.PHONY: lint
lint:  ## Run ruff linter; report all violations without auto-fixing
	$(PYTHON) -m ruff check src/ tests/

.PHONY: lint-fix
lint-fix:  ## Run ruff linter and auto-fix all safe violations
	$(PYTHON) -m ruff check --fix src/ tests/

.PHONY: format
format:  ## Auto-format all source files with ruff formatter
	$(PYTHON) -m ruff format src/ tests/

.PHONY: format-check
format-check:  ## Check formatting without modifying files (for CI)
	$(PYTHON) -m ruff format --check src/ tests/

.PHONY: typecheck
typecheck:  ## Run mypy static type checker over src/
	$(PYTHON) -m mypy src/

.PHONY: check
check: lint format-check typecheck  ## Run all quality checks (no fixes applied)
