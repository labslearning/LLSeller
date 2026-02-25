"""
Django settings for core project.
Arquitectura: God-Tier Revenue Engine B2B
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# [NIVEL DIOS 1]: Carga Absoluta de Variables de Entorno
# Esto garantiza que sin importar desde dónde ejecutes el script (Cron, Celery, Terminal),
# Django siempre encontrará tus contraseñas de forma segura.
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)


# Quick-start development settings - unsuitable for production
# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-@kz*q(^k3o!#^oy#uym$g9t+1dzwh%-o3!i7x8=dfks$q&pln6'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []


# Application definition
INSTALLED_APPS = [
    'daphne', # <--- [OBLIGATORIO para Django 5.x + Channels]
    'unfold',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'sales', # Nuestra aplicación B2B Core
    'channels',
    

]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
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
        'DIRS': [],
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
ASGI_APPLICATION = 'core.asgi.application' # <--- EL MAPA DE RUTA GOD TIER


# ==========================================
# [NIVEL DIOS]: DATABASE VAULT DYNAMICS
# ==========================================


# Si existe la variable DATABASE_URL, asumimos que estamos dentro de Docker
IS_DOCKER = os.getenv('DATABASE_URL') is not None
DB_HOST = 'db' if IS_DOCKER else 'localhost'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'sovereign_db',
        'USER': 'sovereign_db_user',
        'PASSWORD': 'SovereignGodTier2026!',
        'HOST': DB_HOST,
        'PORT': '5432',
    }
}


# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True


# Static files (CSS, JavaScript, Images)
STATIC_URL = 'static/'
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'


# ==========================================
# [NIVEL DIOS 2]: MOTOR DE ENVÍO SMTP (FASE 10)
# ==========================================
# Le decimos a Django que use los servidores de Google (o el proveedor que elijas)
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_HOST = 'smtp.gmail.com'
EMAIL_PORT = 587
EMAIL_USE_TLS = True

# Leemos las credenciales desde el archivo .env de forma segura
EMAIL_HOST_USER = os.getenv('EMAIL_HOST_USER')
EMAIL_HOST_PASSWORD = os.getenv('EMAIL_HOST_PASSWORD')



# Añade esto al FINAL de tu settings.py

UNFOLD = {
    "SITE_TITLE": "B2B Intelligence Engine",
    "SITE_HEADER": "Recon Dashboard",
    #"SITE_ICON": "rocket_launch",  # Icono superior
    "COLORS": {
        "primary": {
            "50": "#f0fdfa", "100": "#ccfbf1", "200": "#99f6e4",
            "300": "#5eead4", "400": "#2dd4bf", "500": "#14b8a6", # Color de acento (Teal)
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
                        "title": "Directorio de Prospectos",
                        "icon": "domain", # Icono de edificio corporativo
                        "link": "/admin/sales/institution/",
                    },
                ],
            },
        ],
    },
}


load_dotenv()
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")


# ==========================================
# ==========================================
# [NIVEL DIOS]: CONFIGURACIÓN CELERY DINÁMICA
# ==========================================
# Detectamos si estamos en Docker/Podman buscando una variable de entorno de la DB
# Si no existe, asumimos que estamos en local y usamos 'localhost'
IS_DOCKER = os.getenv('DATABASE_URL') is not None
REDIS_HOST = 'redis' if IS_DOCKER else 'localhost'

# Configuración Maestra del Broker
CELERY_BROKER_URL = f'redis://{REDIS_HOST}:6379/0'
CELERY_RESULT_BACKEND = f'redis://{REDIS_HOST}:6379/0'

# Protocolos de Serialización y Tiempo
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = TIME_ZONE # Usa la variable TIME_ZONE definida arriba en settings

# --- TUNEADO DE RENDIMIENTO DE SILICON VALLEY ---
# Reinicia el worker tras 100 tareas para prevenir fugas de RAM (Memory Leaks)
CELERY_WORKER_MAX_TASKS_PER_CHILD = 100 

# Optimiza la distribución de tareas: toma 1 a la vez (Evita cuellos de botella)
CELERY_WORKER_PREFETCH_MULTIPLIER = 1 

# Garantía de Ejecución: Solo confirma la tarea después de terminarla
CELERY_TASK_ACKS_LATE = True 

# Enrutamiento de Alta Velocidad
CELERY_TASK_ROUTES = {
    'sales.tasks.task_run_ghost_sniper': {'queue': 'scraping_queue'},
    'sales.tasks.task_run_osm_radar': {'queue': 'discovery_queue'},
    'sales.tasks.task_run_inbound_catcher': {'queue': 'default'},
}


CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    },
}

import os

# ==========================================
# IMAP INBOUND CONFIGURATION (REPLY CATCHER)
# ==========================================
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
IMAP_PORT = int(os.getenv("IMAP_PORT", 993))

# Reutilizamos las mismas credenciales SMTP para no duplicar secretos
IMAP_USERNAME = os.getenv("EMAIL_HOST_USER") 
IMAP_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD")
