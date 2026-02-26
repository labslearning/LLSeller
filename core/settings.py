"""
Django settings for core project.
Arquitectura: God-Tier Revenue Engine B2B (Pure Host Mode Edition)
Optimizado para: MLOps, High-Concurrency Scraping, Distributed Mutex Locks.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from celery.schedules import crontab  # <--- [IMPORTANTE] Importación para el Master Clock

# ==========================================
# 🏗️ [NIVEL DIOS 1]: CORE PATHS & ENV INJECTION
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent

# Carga Absoluta de Variables de Entorno
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

# ==========================================
# 🛡️ [NIVEL DIOS 2]: SECURITY & CORE DYNAMICS
# ==========================================
# NUNCA hardcodear el Secret Key en producción.
SECRET_KEY = os.getenv('DJANGO_SECRET_KEY', 'django-insecure-@kz*q(^k3o!#^oy#uym$g9t+1dzwh%-o3!i7x8=dfks$q&pln6')

# Parseo inteligente de DEBUG para evitar fugas de datos en producción
DEBUG = os.getenv('DJANGO_DEBUG', 'False').lower() in ('true', '1', 't')

# Parseo de hosts permitidos
ALLOWED_HOSTS = os.getenv('DJANGO_ALLOWED_HOSTS', '127.0.0.1,localhost').split(',')

# 🔒 Hardening de Seguridad B2B (Enterprise Standard)
if not DEBUG:
    SECURE_BROWSER_XSS_FILTER = True
    SECURE_CONTENT_TYPE_NOSNIFF = True
    X_FRAME_OPTIONS = 'DENY'
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_SSL_REDIRECT = os.getenv('DJANGO_SECURE_SSL_REDIRECT', 'False').lower() == 'true'

# ==========================================
# 📦 [NIVEL DIOS 3]: APPLICATION GEOMETRY
# ==========================================
INSTALLED_APPS = [
    'daphne', # <--- [OBLIGATORIO para Django 5.x + Channels ASGI]
    'unfold', # Admin Theme
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    
    # Custom Apps
    'sales', # Nuestra aplicación B2B Core / MLOps Engine
    'channels',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware', # Optimización estáticos
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'core.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'templates')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'core.wsgi.application'
ASGI_APPLICATION = 'core.asgi.application' # <--- MAPA DE RUTA ASGI GOD TIER

# ==========================================
# 🗄️ [NIVEL DIOS 4]: DATABASE VAULT & CONNECTION POOLING
# ==========================================
# Al usar Podman/Docker en network_mode: "host", el DNS interno desaparece.
# Forzamos IPv4 local (127.0.0.1). Añadimos CONN_MAX_AGE para Connection Pooling (crítico para Celery).
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.getenv('DB_NAME', 'sovereign_db'),
        'USER': os.getenv('DB_USER', 'sovereign_db_user'),
        'PASSWORD': os.getenv('DB_PASSWORD', '9967112fhr'),
        'HOST': os.getenv('DB_HOST', '127.0.0.1'),
        'PORT': os.getenv('DB_PORT', '5454'),
        'CONN_MAX_AGE': 60,  # Reutiliza conexiones por 60s. Reduce CPU de PostgreSQL.
    }
}

# ==========================================
# 🧠 [NIVEL DIOS 5]: DISTRIBUTED MEMORY & LOCKS
# ==========================================
# REDIS_HOST estandarizado para toda la infraestructura
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')

# CRÍTICO: Sin esto, los `cache.add()` de tus Mutex Locks en Celery no funcionarán entre múltiples workers.
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": f"redis://{REDIS_HOST}:{REDIS_PORT}/1", # Base de datos 1 para Cache
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
            "SOCKET_CONNECT_TIMEOUT": 5,
            "SOCKET_TIMEOUT": 5,
        }
    }
}

# CRÍTICO: Reemplazo de InMemoryChannelLayer por RedisChannelLayer para soportar concurrencia masiva.
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [(REDIS_HOST, int(REDIS_PORT))],
        },
    },
}

# ==========================================
# ⚙️ [NIVEL DIOS 6]: CELERY AUTONOMOUS ORCHESTRATOR
# ==========================================
CELERY_BROKER_URL = f'redis://{REDIS_HOST}:{REDIS_PORT}/0' # Base de datos 0 para Broker
CELERY_RESULT_BACKEND = f'redis://{REDIS_HOST}:{REDIS_PORT}/0'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'America/Bogota'
CELERY_ENABLE_UTC = False

# TUNEADO DE RENDIMIENTO B2B SILICON VALLEY
CELERY_WORKER_MAX_TASKS_PER_CHILD = 100 # Previene memory leaks reiniciando el worker tras 100 tareas
CELERY_WORKER_PREFETCH_MULTIPLIER = 1 # Fair Routing: Un worker pesado no monopoliza la cola
CELERY_TASK_ACKS_LATE = True # Solo marca como exitoso si realmente terminó (Idempotencia)
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True

CELERY_TASK_ROUTES = {
    'sales.tasks.task_run_ghost_sniper': {'queue': 'scraping_queue'},
    'sales.tasks.task_run_osm_radar': {'queue': 'discovery_queue'},
    'sales.tasks.task_run_serp_resolver': {'queue': 'default'},
    'sales.tasks.task_retrain_ai_model': {'queue': 'default'},
    'sales.tasks.task_batch_score_leads': {'queue': 'default'},
    'sales.tasks.task_run_inbound_catcher': {'queue': 'default'},
}

# 👇 [AQUÍ ESTÁ EL MASTER CLOCK - CELERY BEAT SCHEDULE] 👇
CELERY_BEAT_SCHEDULE = {
    # 1. Escucha de respuestas entrantes (Cada 5 minutos)
    'poll_inbox_every_5_mins': {
        'task': 'sales.tasks.task_run_inbound_catcher',
        'schedule': 300.0, # 300 segundos = 5 minutos
    },
    
    # 2. Inferencia Diaria de ML (Puntuar Leads todos los días a la 1:00 AM)
    'daily_ml_inference': {
        'task': 'sales.tasks.task_batch_score_leads',
        'schedule': crontab(hour=1, minute=0),
        'kwargs': {'limit': 2000}
    },
    
    # 3. Reentrenamiento de Cerebro IA (Todos los Domingos a las 3:00 AM)
    'weekly_ml_training': {
        'task': 'sales.tasks.task_retrain_ai_model',
        'schedule': crontab(hour=3, minute=0, day_of_week='sunday'),
    },
}

# ==========================================
# 📧 [NIVEL DIOS 7]: OUTBOUND / INBOUND COMMUNICATIONS
# ==========================================
#EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'
EMAIL_HOST = os.getenv('EMAIL_HOST', 'smtp.gmail.com')
EMAIL_PORT = int(os.getenv('EMAIL_PORT', 587))
EMAIL_USE_TLS = os.getenv('EMAIL_USE_TLS', 'True').lower() == 'true'
EMAIL_HOST_USER = os.getenv('EMAIL_HOST_USER')
EMAIL_HOST_PASSWORD = os.getenv('EMAIL_HOST_PASSWORD')

# IMAP INBOUND CONFIGURATION (REPLY CATCHER)
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
IMAP_PORT = int(os.getenv("IMAP_PORT", 993))
IMAP_USERNAME = os.getenv("EMAIL_HOST_USER") 
IMAP_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD")

# ==========================================
# 🤖 [NIVEL DIOS 8]: AI & ML ENGINES
# ==========================================
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

# ==========================================
# 📊 [NIVEL DIOS 9]: ENTERPRISE OBSERVABILITY (LOGGING)
# ==========================================
# Sin esto, estás ciego en producción. Configura salidas estándar y formateo.
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'god_tier': {
            'format': '{asctime}.{msecs:03.0f} [{levelname}] [{name}] {message}',
            'style': '{',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'god_tier',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': os.getenv('DJANGO_LOG_LEVEL', 'INFO'),
        },
        'Sovereign.CeleryWorkers': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
        'Sovereign.MLOps': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}

# ==========================================
# 🎨 THEME UNFOLD & I18N
# ==========================================
LANGUAGE_CODE = 'es-co' # Localizado para B2B LatAm
TIME_ZONE = 'America/Bogota' 
USE_I18N = True
USE_TZ = True

STATIC_URL = 'static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles') # Crítico para WhiteNoise
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

UNFOLD = {
    "SITE_TITLE": "Sovereign Intelligence Engine",
    "SITE_HEADER": "Command Center",
    "COLORS": {
        "primary": {
            "50": "#f0fdfa", "100": "#ccfbf1", "200": "#99f6e4",
            "300": "#5eead4", "400": "#2dd4bf", "500": "#14b8a6", 
            "600": "#0d9488", "700": "#0f766e", "800": "#115e59", "900": "#134e4a",
        },
    },
    "SIDEBAR": {
        "show_search": True,
        "show_all_applications": True,
        "navigation": [
            {
                "title": "Pipeline de Ventas",
                "separator": True,
                "items": [
                    {
                        "title": "Directorio Global",
                        "icon": "domain", 
                        "link": "/admin/sales/institution/",
                    },
                ],
            },
        ],
    },
}