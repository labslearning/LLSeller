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
    'unfold',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'sales', # Nuestra aplicación B2B Core
    

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


# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'llseller_db',
        'USER': 'llseller_user',
        'PASSWORD': '9967112',
        'HOST': 'localhost',
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
    "SITE_ICON": "rocket_launch",  # Icono superior
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
# CONFIGURACIÓN CELERY & REDIS (BACKGROUND WORKERS)
# ==========================================
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'America/Bogota'

# ==========================================
# CONFIGURACIÓN CELERY ENTERPRISE (REDIS)
# ==========================================
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

# Serialización segura
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = TIME_ZONE

# --- TUNEADO DE RENDIMIENTO DE SILICON VALLEY ---
# Evita que un worker se trague toda la RAM tras miles de ejecuciones. 
# Reinicia el worker invisiblemente tras procesar 100 colegios.
CELERY_WORKER_MAX_TASKS_PER_CHILD = 100 

# Limita cuántas tareas pre-descarga el worker (Evita cuellos de botella si las tareas son lentas)
CELERY_WORKER_PREFETCH_MULTIPLIER = 1 

# Reconoce la tarea solo DESPUÉS de ejecutarla (Si el servidor se reinicia, no pierdes datos)
CELERY_TASK_ACKS_LATE = True 

# Routing (Pistas de alta velocidad)
CELERY_TASK_ROUTES = {
    'sales.tasks.task_run_ghost_sniper': {'queue': 'scraping_queue'},
    'sales.tasks.task_run_osm_radar': {'queue': 'discovery_queue'},
    'sales.tasks.task_run_serp_resolver': {'queue': 'default'},
}