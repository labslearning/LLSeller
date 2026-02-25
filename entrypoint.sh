#!/bin/bash
# =========================================================
# ZERO-DOWNTIME BOOTSTRAP SCRIPT (GOD TIER)
# =========================================================

set -e

BOOTSTRAP_MODE="${BOOTSTRAP_MODE:-full}"
SKIP_BOOTSTRAP="${SKIP_BOOTSTRAP:-0}"

log_header() {
  echo "========================================================="
  echo "🛡️  [SOVEREIGN INIT] Iniciando Secuencia de Arranque"
  echo "========================================================="
  echo "🔧 Modo de bootstrap detectado: ${BOOTSTRAP_MODE}"
}

wait_for_db() {
  local db_host="${POSTGRES_HOST:-db}"
  local db_port="${POSTGRES_PORT:-5432}"
  local db_user="${POSTGRES_USER:-sovereign_db_user}"
  local db_name="${POSTGRES_DB:-sovereign_db}"

  echo "⏳ Verificando disponibilidad del Vault (PostgreSQL en ${db_host}:${db_port})..."
  until pg_isready -h "${db_host}" -p "${db_port}" -U "${db_user}" -d "${db_name}"; do
    echo "⚠️  PostgreSQL no está listo. Reintentando en 2 segundos..."
    sleep 2
  done
  echo "✅ Vault operativo y aceptando conexiones."
}

run_migrations() {
  echo "📦 Aplicando esquemas de Base de Datos..."
  python manage.py migrate --noinput
}

collect_static() {
  echo "🧹 Compilando y ofuscando assets estáticos..."
  python manage.py collectstatic --noinput --clear
}

run_bootstrap() {
  if [ "${SKIP_BOOTSTRAP}" = "1" ]; then
    echo "⏭️  SKIP_BOOTSTRAP=1 detectado: se omite bootstrap."
    return
  fi

  case "${BOOTSTRAP_MODE}" in
    full)
      wait_for_db
      run_migrations
      collect_static
      ;;
    db)
      wait_for_db
      ;;
    none)
      echo "⏭️  Bootstrap desactivado (BOOTSTRAP_MODE=none). Acceso directo."
      ;;
    *)
      echo "❌ BOOTSTRAP_MODE inválido: '${BOOTSTRAP_MODE}'. Usa: full | db | none"
      exit 1
      ;;
  esac
}

log_header
run_bootstrap

echo "🚀 [SOVEREIGN INIT] Traspasando control al proceso principal..."
echo "========================================================="

exec "$@"