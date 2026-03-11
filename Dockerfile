# Stage 1: Build Rust extension
FROM python:3.9-slim AS builder

# Install Rust toolchain and build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    rm -rf /var/lib/apt/lists/*
ENV PATH="/root/.cargo/bin:${PATH}"

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.6 /uv /usr/local/bin/uv

WORKDIR /app

# Copy project files
COPY pyproject.toml .python-version uv.lock README.md ./
COPY src/ src/
COPY dimq_load_task/ dimq_load_task/

# Fallback version when .git is not available (hatch-vcs needs git history)
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0

# Create venv and install project
RUN uv sync --no-dev

# Build Rust extension into the venv
RUN uv tool run maturin develop --manifest-path dimq_load_task/Cargo.toml --uv


# Stage 2: Runtime image
FROM python:3.9-slim

WORKDIR /app

# Copy the venv from builder (same /app path so shebangs are correct)
COPY --from=builder /app/.venv /app/.venv

# Copy source and config
COPY src/ /app/src/
COPY e2e/config.yaml /app/config.yaml

# Put venv on PATH
ENV PATH="/app/.venv/bin:${PATH}"
ENV VIRTUAL_ENV="/app/.venv"

ENTRYPOINT ["dimq"]
CMD ["worker", "--config", "/app/config.yaml"]
