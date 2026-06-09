# syntax=docker/dockerfile:1.7
# uv-based image for the interactive-vs-standard benchmark.
# Build context must be the project root so pyproject.toml / uv.lock / iwtest.py are available.
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH=/app/.venv/bin:$PATH

WORKDIR /app

# Install dependencies first for better layer caching.
COPY pyproject.toml uv.lock /app/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Copy benchmark sources.
COPY iwtest.py /app/
COPY spcs/entrypoint.sh /app/spcs/entrypoint.sh
RUN chmod +x /app/spcs/entrypoint.sh

ENTRYPOINT ["/app/spcs/entrypoint.sh"]
