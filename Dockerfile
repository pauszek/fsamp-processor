FROM python:3.14-slim-bookworm AS builder

ARG POETRY_VERSION=1.7.1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY pyproject.toml .

RUN pip install --no-cache-dir .
FROM python:3.14-slim-bookworm AS production

ARG REQUIRE_FIPS_PROVIDER=true

LABEL maintainer="Pauszek <pauszek@github.io>"
LABEL org.opencontainers.image.title="FSAMP Processor Dev Runtime"
LABEL org.opencontainers.image.description="Local/dev processor image. Production FIPS runtime uses Dockerfile.lambda on AL2023."
LABEL org.opencontainers.image.version="0.1.0"

RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid appgroup --shell /bin/bash --create-home appuser

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    openssl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN FIPS_MODULE="/usr/lib/$(uname -m)-linux-gnu/ossl-modules/fips.so"; \
    if [ -f "$FIPS_MODULE" ]; then \
        openssl fipsinstall -out /usr/lib/ssl/fipsmodule.cnf -module "$FIPS_MODULE"; \
    elif [ "$REQUIRE_FIPS_PROVIDER" = "true" ]; then \
        echo "ERROR: FIPS module not found — refusing to build non-FIPS runtime image" >&2; \
        exit 1; \
    else \
        echo "WARNING: FIPS module not found; allowed only for local builds" >&2; \
    fi
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

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN python - <<'PY'
import urllib3

urllib3_version = tuple(int(part) for part in urllib3.__version__.split(".")[:3])
if urllib3_version < (2, 7, 0):
    raise SystemExit(f"urllib3 {urllib3.__version__} is below the required security baseline 2.7.0")
PY

WORKDIR /app

COPY --chown=appuser:appgroup src/ src/

USER appuser

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    OPENSSL_CONF=/etc/ssl/openssl-fips.cnf \
    ENVIRONMENT=local \
    LOG_LEVEL=INFO \
    LOG_FORMAT=json

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import processor; print('healthy')" || exit 1

ENTRYPOINT ["python", "-m", "processor.main"]
FROM production AS development

USER root

RUN pip install --no-cache-dir \
    pytest \
    pytest-cov \
    pytest-mock \
    moto[all]

USER appuser

ENTRYPOINT ["python"]
CMD ["-m", "processor.main"]
