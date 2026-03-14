# DIMQ - Development Guide

## Project Structure

Two packages in one repo:

- **dimq** — Pure Python, built with hatchling. Source in `src/dimq/`, config in `pyproject.toml`.
- **dimq-load-task** — Rust extension (PyO3/maturin). Source in `dimq_load_task/src/lib.rs`, config in `dimq_load_task/pyproject.toml` + `dimq_load_task/Cargo.toml`.

Key source files:
- `src/dimq/orchestrator.py` — ZMQ ROUTER, task dispatch, heartbeat, retry logic
- `src/dimq/worker.py` — ZMQ DEALER, task execution, timeout handling
- `src/dimq/adaptive.py` — Parallelization factor tuning (doubling then linear)
- `src/dimq/models.py` — Pydantic models (TaskRecord, DimqConfig, etc.)
- `src/dimq/task.py` — Task loading and introspection
- `src/dimq/config.py` — YAML config loading
- `src/dimq/cli.py` — CLI entry points

## Common Commands

```bash
# Install dependencies
uv sync

# Build Rust extension (required before running tests)
uv tool run maturin develop --manifest-path dimq_load_task/Cargo.toml --uv

# Run tests (excludes e2e Docker tests)
uv run pytest -v -m "not e2e"

# Run e2e tests (requires Docker)
uv run pytest tests/test_e2e_docker.py -v -s

# Build Python package
uv build
```

## Versioning

- **dimq**: Uses `hatch-vcs` — version derived automatically from git tags. No hardcoded version.
- **dimq-load-task**: Uses `0.0.0` placeholder in `Cargo.toml` and `dimq_load_task/pyproject.toml`. CI injects the real version from the git tag via `sed` before building.
- Tag format: `v{version}` (e.g., `v0.1.2`)

## Releasing

1. Write release notes in `docs/releases/v{version}.md`
2. Commit all changes
3. Tag: `git tag v{version}`
4. Push: `git push && git push --tags`
5. CI (`/.github/workflows/ci.yml`) runs tests, builds wheels for 5 platforms (Linux x86_64, Linux aarch64, macOS x86_64, macOS arm64, Windows x86_64), and publishes both packages to PyPI via OIDC trusted publishing
6. Both `dimq` and `dimq-load-task` must have pending publishers configured on PyPI (environment: `pypi`, workflow: `ci.yml`)

## CI/CD

Workflow: `.github/workflows/ci.yml`
- Triggers: push to main, PRs to main, `v*` tags
- Test job: Python 3.9, Rust stable, `pytest -v -m "not e2e"`
- Build jobs: pure Python wheel + 5 abi3 Rust wheels + Rust sdist
- Publish job: runs only on `v*` tags

## Docker

`Dockerfile` builds a worker image. Note: `SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0` is set as fallback since `.git` is not available in Docker builds.

```bash
docker compose up -d --build    # start 3 workers
docker compose down             # stop
```
