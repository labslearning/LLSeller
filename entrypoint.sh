#!/bin/bash
# =========================================================
# ZERO-DOWNTIME BOOTSTRAP SCRIPT
# =========================================================

# Falla inmediatamente si cualquier comando devuelve un error
set -e

echo "========================================================="
echo "🛡️  [SOVEREIGN INIT] Iniciando Secuencia de Arranque"
echo "========================================================="

# 1. TCP WAIT (Tel Aviv DB Protection)
# Espera activa hasta que el socket de PostgreSQL esté 100% operativo
echo "⏳ Verificando disponibilidad del Vault (PostgreSQL)..."
until pg_isready -h "db" -p "5432" -U "sovereign_db_user"; do
  echo "⚠️  PostgreSQL no está listo. Reintentando en 2 segundos..."
  sleep 2
done
echo "✅ Vault operativo y aceptando conexiones."

# 2. MIGRACIONES TRANSACCIONALES
echo "📦 Aplicando esquemas de Base de Datos..."
python manage.py migrate --noinput

# 3. COMPILACIÓN DE ESTÁTICOS (Dashboard UI)
echo "🧹 Compilando y ofuscando assets estáticos..."
python manage.py collectstatic --noinput --clear

echo "🚀 [SOVEREIGN INIT] Traspasando control al proceso principal..."
echo "========================================================="

# 4. EJECUCIÓN DEL PROCESO MAESTRO
# 'exec' reemplaza el proceso de Bash con Gunicorn/Celery, 
# permitiendo que Tini gestione las señales correctamente.
exec "$@"
