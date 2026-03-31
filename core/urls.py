"""
======================================================================
[GOD TIER ARCHITECTURE: LEVIATHAN CLASS]
PROJECT: GHOST SWARM V12.0 (SILICON WADI / UNIT 8200 SPEC)
MODULE: MASTER ROUTING NODE (URLS)
======================================================================
Arquitectura de Enrutamiento de Alta Velocidad y Evasión.
- Bypass de HTMX para OSINT.
- Endpoint de Píxel de Rastreo (Zero-Copy).
- Ofuscación de Panel de Administración.
- Servidor estático optimizado para desarrollo.
======================================================================
"""

import os
import logging
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static

# Importación directa de Motores desde sales/views.py
from sales.views import SniperSearchView, tracking_pixel_view

logger = logging.getLogger("Sovereign.Router")

# =========================================================
# [NIVEL DIOS 1]: OFUSCACIÓN DE COMANDO CENTRAL (ADMIN)
# =========================================================
# Exponer "/admin/" a internet atrae ataques de fuerza bruta.
# Con esto, puedes definir "DJANGO_ADMIN_URL=b2b-vault/" en tu archivo .env.
# Si no lo defines, usará 'admin/' por defecto para que no se rompa tu entorno local.
ADMIN_URL = os.getenv('DJANGO_ADMIN_URL', 'admin/')

urlpatterns = [
    # ==========================================
    # 1. TÚNEL DE API (BYPASS DE ADMIN PARA HTMX)
    # ==========================================
    # Ruta directa y pura para que el Frontend envíe el payload OSINT masivo
    # sin que el core de Django Admin interfiera o bloquee el POST.
    path('api/ghost-swarm/search/', SniperSearchView.as_view(), name='api_sniper_search'),

    # ==========================================
    # 2. SISTEMA DE TELEMETRÍA INVISIBLE (TRACKING PIXEL)
    # ==========================================
    # Enlace ultra-rápido para la captura de aperturas de correos (Evita overhead).
    # Formato de camuflaje: https://tudominio.com/t/px/<UUID>.gif
    path('t/px/<str:interaction_id>.gif', tracking_pixel_view, name='tracking_pixel'),

    # ==========================================
    # 3. INTERFAZ DE MANDO CENTRAL (CRM / GRID)
    # ==========================================
    path(ADMIN_URL, admin.site.urls),

    # En core/urls.py, agrega:
    path('sales/', include('sales.urls')),
]

# =========================================================
# [NIVEL DIOS 2]: OPTIMIZACIÓN DE ASSETS PARA DESARROLLO
# =========================================================
# Sirve los archivos estáticos (CSS, JS de Unfold) y multimedia solo si estamos en modo DEBUG.
# En producción (Tel Aviv Tier), Nginx o Caddy deben encargarse de esto.
if settings.DEBUG:
    logger.info("🔧 [ROUTER] Entorno de Desarrollo Detectado: Inyectando URLs de Static/Media.")
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    
    # [TOOLING PROFESIONAL]: Inyección dinámica de Django Debug Toolbar
    # Si la librería está instalada, activa la ruta para analizar cuellos de botella en la DB.
    try:
        import debug_toolbar
        urlpatterns = [
            path('__debug__/', include(debug_toolbar.urls)),
        ] + urlpatterns
        logger.info("🐛 [ROUTER] Django Debug Toolbar montado con éxito.")
    except ImportError:
        pass