import os
import logging
from celery import Celery, Task
from celery.signals import worker_process_init, task_prerun, task_postrun

# ==========================================
# 1. CONFIGURACIÓN DINÁMICA DEL ENTORNO
# ==========================================
# Reemplaza 'core' con el nombre de tu carpeta principal (donde está settings.py)
PROJECT_NAME = 'core' 
os.environ.setdefault('DJANGO_SETTINGS_MODULE', f'{PROJECT_NAME}.settings')

logger = logging.getLogger("CeleryEnterprise")

# ==========================================
# 2. CUSTOM TASK BASE (EL NÚCLEO INMORTAL)
# ==========================================
class SovereignTask(Task):
    """
    Base Task Nivel Dios.
    Todas tus funciones @shared_task heredarán este comportamiento automáticamente.
    Características:
    - Auto-Retries con Retroceso Exponencial (Exponential Backoff).
    - Jitter aleatorio para evitar ataques DDoS accidentales a tu propia DB.
    - Telemetría y captura forense de fallos.
    """
    abstract = True
    
    # Tolerancia a fallos de red por defecto
    autoretry_for = (ConnectionError, TimeoutError) 
    max_retries = 3
    retry_backoff = True       # 1s, 2s, 4s, 8s...
    retry_backoff_max = 600    # Máximo 10 minutos de espera
    retry_jitter = True        # Añade milisegundos aleatorios para desaturar

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Captura absoluta de fallos (Punto de anclaje para Sentry/Datadog)"""
        logger.error(f"❌ [CRITICAL FAILURE] Misión: {self.name} | ID: {task_id}")
        logger.error(f"🔍 Argumentos: {args} | Excepción: {exc}")
        # Aquí se inyectaría: sentry_sdk.capture_exception(exc)
        super().on_failure(exc, task_id, args, kwargs, einfo)

    def on_success(self, retval, task_id, args, kwargs):
        """Telemetría de éxito"""
        logger.info(f"✅ [MISSION ACCOMPLISHED] {self.name} | ID: {task_id}")
        super().on_success(retval, task_id, args, kwargs)

# ==========================================
# 3. INSTANCIACIÓN DE LA MAQUINARIA CELERY
# ==========================================
app = Celery(PROJECT_NAME, task_cls=SovereignTask)

# Extrae la configuración usando el prefijo CELERY_ en settings.py
app.config_from_object('django.conf:settings', namespace='CELERY')

# Autodescubrimiento inteligente de archivos tasks.py
app.autodiscover_tasks()

# ==========================================
# 4. GESTIÓN DE RECURSOS (MEMORY & DB LEAK PREVENTION)
# ==========================================
# ESTO ES LO QUE SEPARA A UN JUNIOR DE UN SENIOR EN SILICON VALLEY.
# Evita que el worker mantenga conexiones TCP abiertas infinitamente hasta colapsar.

@worker_process_init.connect
def fix_multiprocessing(**kwargs):
    """
    Protección de Forks: Cuando Celery crea un hilo nuevo, las conexiones SSL/DB 
    pueden corromperse. Esto purga las conexiones viejas para que el hijo nazca limpio.
    """
    from django.db import connections
    for conn in connections.all():
        conn.close()

@task_prerun.connect
def cleanup_db_connections_before(task_id, task, *args, **kwargs):
    """Limpia conexiones muertas ANTES de empezar la tarea."""
    from django.db import close_old_connections
    close_old_connections()

@task_postrun.connect
def cleanup_db_connections_after(task_id, task, *args, **kwargs):
    """Limpia la basura de memoria y DB DESPUÉS de terminar la tarea (Garbage Collection)."""
    from django.db import close_old_connections
    close_old_connections()
