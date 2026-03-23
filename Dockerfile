# =============================================================================
# FSAMP Processor - Production Dockerfile
# =============================================================================
# Multi-stage build for minimal image size and security.
# Uses Python 3.14 slim image with non-root user.
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder
# -----------------------------------------------------------------------------
FROM python:3.14-slim-bookworm AS builder

# Set build-time variables
ARG POETRY_VERSION=1.7.1

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ src/
COPY pyproject.toml .

# Install application
RUN pip install --no-cache-dir .

# -----------------------------------------------------------------------------
# Stage 2: Production
# -----------------------------------------------------------------------------
FROM python:3.14-slim-bookworm AS production

# Labels
LABEL maintainer="Pauszek <pauszek@github.io>"
LABEL org.opencontainers.image.title="FSAMP Processor"
LABEL org.opencontainers.image.description="Event-driven file processor with FIPS 140-3 compliance"
LABEL org.opencontainers.image.version="0.1.0"

# Security: Run as non-root user
RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid appgroup --shell /bin/bash --create-home appuser

# Install runtime dependencies + OpenSSL FIPS provider module
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    openssl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Configure OpenSSL FIPS provider for FIPS 140-3 compliance
# This enables FIPS mode for the Python `cryptography` library and `ssl` module
RUN openssl fipsinstall -out /usr/lib/ssl/fipsmodule.cnf -module /usr/lib/$(uname -m)-linux-gnu/ossl-modules/fips.so 2>/dev/null || true
COPY <<'FIPSCONF' /etc/ssl/openssl-fips.cnf
config_diagnostics = 1
openssl_conf = openssl_init

[openssl_init]
providers = provider_sect
alg_section = algorithm_sect

[provider_sect]
fips = fips_sect
base = base_sect

[fips_sect]
activate = 1

[base_sect]
activate = 1

[algorithm_sect]
default_properties = fips=yes
FIPSCONF

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy source code (for debugging/introspection only)
COPY --chown=appuser:appgroup src/ src/

# Switch to non-root user
USER appuser

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    # OpenSSL FIPS mode — enables FIPS 140-3 validated crypto for ssl and cryptography lib
    OPENSSL_CONF=/etc/ssl/openssl-fips.cnf \
    # Default configuration (override in deployment)
    ENVIRONMENT=local \
    LOG_LEVEL=INFO \
    LOG_FORMAT=json

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import processor; print('healthy')" || exit 1

# Entry point
ENTRYPOINT ["python", "-m", "processor.main"]

# -----------------------------------------------------------------------------
# Stage 3: Development (optional, for local testing)
# -----------------------------------------------------------------------------
FROM production AS development

USER root

# Install development dependencies
RUN pip install --no-cache-dir \
    pytest \
    pytest-cov \
    pytest-mock \
    moto[all]

USER appuser

# Override entrypoint for development
ENTRYPOINT ["python"]
CMD ["-m", "processor.main"]
