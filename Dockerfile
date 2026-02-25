# =========================================================
# [GOD TIER DOCKERFILE] - SOVEREIGN B2B ENGINE
# Arquitectura: Multi-Stage + Tini Init + Zero-Root
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
LABEL version="1.0"
LABEL description="B2B Intelligence Engine - Production Grade"

# Variables de entorno inmutables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/app/pw-browsers \
    DJANGO_SETTINGS_MODULE=core.settings

WORKDIR /app

# 1. Instalación de dependencias del SO (Tini, PostgreSQL Client y dependencias de Playwright)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    libpq-dev \
    postgresql-client \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# 2. Creación del Usuario de Seguridad (Tel Aviv Standard)
# Se crea ANTES de copiar archivos para asegurar la propiedad correcta
RUN addgroup --system sovereign && adduser --system --ingroup sovereign sovereign

# 3. Instalación de dependencias de Python desde la Etapa 1
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .
RUN pip install --no-cache-dir /wheels/*

# 4. Aprovisionamiento Aislado de Playwright (Ghost Sniper)
# Instalamos solo Chromium y sus dependencias de Linux, luego limpiamos la caché
RUN playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/*

# 5. Inyección del Código Fuente
COPY . .

# 6. Blindaje de Permisos
# El usuario 'sovereign' toma control exclusivo de la app y los navegadores web
RUN chown -R sovereign:sovereign /app \
    && chmod +x /app/entrypoint.sh

# Bajamos privilegios de ROOT al usuario seguro
USER sovereign

# 7. EXPOSICIÓN DE PUERTOS
EXPOSE 8000

# 8. SISTEMA DE ARRANQUE (Shenzhen PID 1 Protection)
# Tini captura las señales del kernel (SIGTERM/SIGINT) y cierra Celery/Django limpiamente
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
