# Este archivo asegura que la app de Celery se cargue siempre que Django arranque.
# Sin esto, Django usará RabbitMQ por defecto en lugar de Redis.

from .celery import app as celery_app

__all__ = ('celery_app',)