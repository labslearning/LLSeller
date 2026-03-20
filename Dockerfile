# =========================================================
# [GOD TIER DOCKERFILE] - SOVEREIGN B2B ENGINE V26.0
# Arquitectura: Multi-Stage + Tini Init + Zero-Root + Playwright
# Estatus: Silicon Wadi / Unit 8200 Specification (Anti-Timeout)
# =========================================================

# ---------------------------------------------------------
# ETAPA 1: CONSTRUCTOR (BUILDER) - Compilación en C/C++
# ---------------------------------------------------------
FROM python:3.11-slim-bookworm as builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias críticas para compilar librerías (psycopg2, cffi, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Construcción de "Wheels" (Binarios precompilados) para instalación ultrarrápida
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt


# ---------------------------------------------------------
# ETAPA 2: PRODUCCIÓN (RUNNER) - Máxima Seguridad y Ligereza
# ---------------------------------------------------------
FROM python:3.11-slim-bookworm

LABEL maintainer="Sovereign Architecture <godtier@sovereign.local>"
LABEL version="26.0"
LABEL description="B2B Intelligence Engine - Playwright Headless Ready"

# Variables de entorno inmutables
# [GOD TIER FIX]: PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT a 120000ms (120s) para evitar cortes
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers \
    PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT=120000 \
    DJANGO_SETTINGS_MODULE=core.settings

WORKDIR /app

# 1. Instalación de dependencias Runtime del SO
# Usamos libpq5 (runtime) en vez de libpq-dev (compilador) para reducir peso
RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    libpq5 \
    postgresql-client \
    ca-certificates \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# 2. Creación del Usuario de Seguridad (Tel Aviv Zero-Trust Standard)
# Se crean las carpetas vitales ANTES para asegurar la propiedad
RUN addgroup --system sovereign && \
    adduser --system --ingroup sovereign sovereign && \
    mkdir -p /opt/pw-browsers /app && \
    chown -R sovereign:sovereign /opt/pw-browsers /app

# 3. Instalación de dependencias de Python desde la Etapa 1 (Wheels)
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .
RUN pip install --no-cache-dir /wheels/*

# 4. Aprovisionamiento Aislado de Playwright (Ghost Sniper Engine)
# [GOD TIER FIX]: Triple intento de descarga. Si la red falla, espera 15 segundos y reintenta.
RUN (playwright install --with-deps chromium || \
    (echo "⚠️ Fallo inicial. Reintentando en 15s..." && sleep 15 && playwright install --with-deps chromium) || \
    (echo "⚠️ Segundo fallo. Último intento en 15s..." && sleep 15 && playwright install --with-deps chromium)) && \
    chown -R sovereign:sovereign /opt/pw-browsers && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# 5. Inyección del Código Fuente
# Colocado al final para maximizar el uso de caché en las capas anteriores
COPY . .

# 6. Blindaje de Permisos Final
RUN chown -R sovereign:sovereign /app \
    && chmod +x /app/entrypoint.sh

# 7. Descenso de Privilegios (Modo Seguro)
USER sovereign

# 8. EXPOSICIÓN DE PUERTOS
EXPOSE 8000

# 9. SISTEMA DE ARRANQUE (Shenzhen PID 1 Protection)
# Tini captura las señales del kernel (SIGTERM/SIGINT) y cierra Celery/Django limpiamente
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]