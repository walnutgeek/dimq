# Stage 1: Build Rust extension
FROM python:3.9-slim AS builder

# Install Rust toolchain and build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    rm -rf /var/lib/apt/lists/*
ENV PATH="/root/.cargo/bin:${PATH}"

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /build

# Copy project files
COPY pyproject.toml .python-version ./
COPY src/ src/
COPY dimq_load_task/ dimq_load_task/

# Create venv and install project
RUN uv sync --no-dev

# Build Rust extension into the venv
RUN uv tool run maturin develop --manifest-path dimq_load_task/Cargo.toml --uv


# Stage 2: Runtime image
FROM python:3.9-slim

WORKDIR /app

# Copy the entire venv from builder
COPY --from=builder /build/.venv /app/.venv

# Copy source and config
COPY src/ /app/src/
COPY e2e/config.yaml /app/config.yaml

# Put venv on PATH
ENV PATH="/app/.venv/bin:${PATH}"
ENV VIRTUAL_ENV="/app/.venv"

ENTRYPOINT ["dimq"]
CMD ["worker", "--config", "/app/config.yaml"]
