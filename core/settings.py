"""
======================================================================
[GOD TIER ARCHITECTURE: LEVIATHAN CLASS V100 - PROJECT OMNISCIENT]
PROJECT: GHOST SNIPER (SILICON WADI / UNIT 8200 SPEC)
MODULE: CORE SETTINGS (DJANGO 5.x + ASGI)
ENGINEERING: 12-FACTOR APP PARSING, DYNAMIC CONNECTION POOLING,
             FAILSAFE ENV RESOLUTION, MULTIPLEXED CACHE & BROKER
======================================================================
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from celery.schedules import crontab
import dj_database_url # [GOD TIER FIX]: The 12-Factor Standard para bases de datos

# ==========================================
# 🏗️ [NIVEL DIOS 1]: CORE PATHS & ENV INJECTION
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent

# [CRITICAL SECURITY FIX]: `override=False` asegura que las variables inyectadas 
# por Docker en el entorno del OS tengan prioridad máxima sobre el archivo .env.
env_path = BASE_DIR / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=False) 

# ==========================================
# 🛡️ [NIVEL DIOS 2]: SECURITY & CORE DYNAMICS
# ==========================================
SECRET_KEY = os.getenv('DJANGO_SECRET_KEY', 'django-insecure-@kz*q(^k3o!#^oy#uym$g9t+1dzwh%-o3!i7x8=dfks$q&pln6')

# Parseo inteligente
DEBUG = str(os.getenv('DJANGO_DEBUG', 'False')).lower() in ('true', '1', 't')

# Parseo dinámico de hosts
raw_hosts = os.getenv('DJANGO_ALLOWED_HOSTS', '127.0.0.1,localhost,*')
ALLOWED_HOSTS = [host.strip() for host in raw_hosts.split(',') if host.strip()]

# 🔒 Hardening de Seguridad B2B (Enterprise Standard)
if not DEBUG:
    SECURE_BROWSER_XSS_FILTER = True
    SECURE_CONTENT_TYPE_NOSNIFF = True
    X_FRAME_OPTIONS = 'DENY'
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_SSL_REDIRECT = str(os.getenv('DJANGO_SECURE_SSL_REDIRECT', 'False')).lower() == 'true'

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
    'sales', 
    'channels',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware', # Optimización estáticos O(1)
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
ASGI_APPLICATION = 'core.asgi.application'

# ==========================================
# 🗄️ [NIVEL DIOS 4]: DATABASE VAULT & POOLING
# ==========================================
# [GOD TIER FIX]: dj_database_url parsea la cadena completa inyectada por Docker.
# Si falla, cae al SQLite local (evitando el crash `Connection refused`).
# El CONN_MAX_AGE previene que PostgreSQL colapse bajo concurrencia masiva.
default_db_url = 'sqlite:///' + str(BASE_DIR / 'db.sqlite3')
DATABASES = {
    'default': dj_database_url.config(
        default=os.getenv('DATABASE_URL', default_db_url),
        conn_max_age=60,
        conn_health_checks=True,
    )
}

# ==========================================
# 🧠 [NIVEL DIOS 5]: DISTRIBUTED MEMORY & LOCKS
# ==========================================
# [GOD TIER FIX]: Estandarización de URLs de Redis para evitar desconexiones de Celery.
REDIS_URL = os.getenv('CELERY_BROKER_URL', 'redis://127.0.0.1:6379/0')

CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": REDIS_URL.replace('/0', '/1'), # Usa DB 1 para Cache
        "OPTIONS": {
            "socket_connect_timeout": 10,
            "socket_timeout": 10,
            "retry_on_timeout": True,
        }
    }
}

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [REDIS_URL.replace('/0', '')], 
        },
    },
}

# ==========================================
# ⚙️ [NIVEL DIOS 6]: CELERY AUTONOMOUS ORCHESTRATOR
# ==========================================
CELERY_BROKER_URL = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'America/Bogota'
CELERY_ENABLE_UTC = False

# TUNEADO DE RENDIMIENTO B2B SILICON VALLEY
CELERY_WORKER_MAX_TASKS_PER_CHILD = 100 
CELERY_WORKER_PREFETCH_MULTIPLIER = 1 
CELERY_TASK_ACKS_LATE = True 
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_REDIS_MAX_CONNECTIONS = 20 # Previene saturación del pool de Redis

CELERY_TASK_ROUTES = {
    'sales.tasks.task_run_ghost_sniper': {'queue': 'scraping_queue'},
    'sales.tasks.task_run_osm_radar': {'queue': 'discovery_queue'},
    'sales.tasks.task_run_serp_resolver': {'queue': 'default'},
    'sales.tasks.task_retrain_ai_model': {'queue': 'default'},
    'sales.tasks.task_batch_score_leads': {'queue': 'default'},
    'sales.tasks.task_run_inbound_catcher': {'queue': 'default'},
}

# 👇 MASTER CLOCK - CELERY BEAT SCHEDULE 👇
CELERY_BEAT_SCHEDULE = {
    'poll_inbox_every_5_mins': {
        'task': 'sales.tasks.task_run_inbound_catcher',
        'schedule': 300.0, 
    },
    'daily_ml_inference': {
        'task': 'sales.tasks.task_batch_score_leads',
        'schedule': crontab(hour=1, minute=0),
        'kwargs': {'limit': 2000}
    },
    'weekly_ml_training': {
        'task': 'sales.tasks.task_retrain_ai_model',
        'schedule': crontab(hour=3, minute=0, day_of_week='sunday'),
    },
}

# ==========================================
# 📧 [NIVEL DIOS 7]: OUTBOUND / INBOUND COMMUNICATIONS
# ==========================================
EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend' if DEBUG else 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_HOST = os.getenv('EMAIL_HOST', 'smtp.gmail.com')
EMAIL_PORT = int(os.getenv('EMAIL_PORT', 587))
EMAIL_USE_TLS = str(os.getenv('EMAIL_USE_TLS', 'True')).lower() == 'true'
EMAIL_HOST_USER = os.getenv('EMAIL_HOST_USER')
EMAIL_HOST_PASSWORD = os.getenv('EMAIL_HOST_PASSWORD')

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
LANGUAGE_CODE = 'es-co' 
TIME_ZONE = 'America/Bogota' 
USE_I18N = True
USE_TZ = True

STATIC_URL = 'static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles') 
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