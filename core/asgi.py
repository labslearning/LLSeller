import os
import django
from django.core.asgi import get_asgi_application

# 1. INICIALIZACIÓN TEMPRANA DEL KERNEL (Obligatorio antes de importar Channels)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')
django.setup()

# 2. IMPORTACIONES ASGI (Protegidas)
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.security.websocket import AllowedHostsOriginValidator
from channels.auth import AuthMiddlewareStack
from django.urls import path

# Importamos el Consumer God-Tier
from sales.consumers import StatusConsumer 

# =========================================================
# 🛡️ [NIVEL DIOS]: ASGI KERNEL MULTIPLEXER
# =========================================================
# Arquitectura de enrutamiento con protección CSWSH y resolución Capa 7.

django_asgi_app = get_asgi_application()

application = ProtocolTypeRouter({
    # Ruteo HTTP tradicional (Síncrono/Asíncrono nativo de Django)
    "http": django_asgi_app,
    
    # Ruteo WebSocket con Middleware de Seguridad de Triple Capa
    "websocket": AllowedHostsOriginValidator(  # Capa 1: Escudo CSWSH (Verifica el Origin Header)
        AuthMiddlewareStack(                   # Capa 2: Resolución Lazy de Sesión (O(1) Memory)
            URLRouter([                        # Capa 3: Enrutamiento Dinámico
                # Endpoint global unificado (Coincide exactamente con el Frontend)
                path('ws/status/', StatusConsumer.as_asgi()),
            ])
        )
    ),
})