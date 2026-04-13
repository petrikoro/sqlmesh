# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.13

# ---------------------------------------------------------------------------
# Builder: install build tools, compile native extensions, create a venv
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS builder

ARG EXTRAS=""
ARG VERSION=""

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY pyproject.toml README.md ./
COPY sqlmesh/ sqlmesh/
COPY sqlmesh_dbt/ sqlmesh_dbt/
COPY web/ web/

RUN python -m venv /opt/sqlmesh
ENV PATH="/opt/sqlmesh/bin:$PATH"

RUN --mount=type=cache,target=/root/.cache/pip \
    export SETUPTOOLS_SCM_PRETEND_VERSION="${VERSION:-0.0.0dev0}"; \
    if [ -z "$EXTRAS" ]; then \
        pip install .; \
    else \
        pip install ".[$EXTRAS]"; \
    fi

# ---------------------------------------------------------------------------
# Runtime: minimal image with only the installed packages
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -s /bin/bash sqlmesh

COPY --from=builder /opt/sqlmesh /opt/sqlmesh
ENV PATH="/opt/sqlmesh/bin:$PATH"

WORKDIR /app
USER sqlmesh

ENTRYPOINT ["sqlmesh"]
CMD ["--help"]
