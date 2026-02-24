# core/asgi.py
import os
import django
from django.core.asgi import get_asgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')
django.setup()

from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
from django.urls import path
from sales.consumers import StatusConsumer # Asegúrate de que sales/consumers.py exista

application = ProtocolTypeRouter({
    "http": get_asgi_application(),
    "websocket": AuthMiddlewareStack(
        URLRouter([
            path('ws/status/<str:institution_id>/', StatusConsumer.as_asgi()),
        ])
    ),
})