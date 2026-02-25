import json
import logging
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.exceptions import StopConsumer

# Inicializamos el sistema de trazabilidad (Logs SRE)
logger = logging.getLogger(__name__)

class StatusConsumer(AsyncWebsocketConsumer):
    """
    =========================================================
    [NIVEL DIOS]: HIGH-PERFORMANCE WEBSOCKET CONSUMER
    =========================================================
    Arquitectura de telemetría en tiempo real. Gestiona la transmisión 
    asíncrona de eventos desde los Ghost Snipers (Workers Celery) 
    hacia el Command Center (Dashboard) con blindaje de reconexión.
    """

    async def connect(self):
        """
        Intercepta y establece la conexión entrante asegurando el canal en Redis.
        """
        try:
            # 1. Asignación del Nodo de Transmisión Principal
            self.global_group = "radar_updates"
            
            # [UPGRADE FUTURO]: Aquí puedes validar `self.scope["user"].is_authenticated` 
            # para denegar conexiones a usuarios no logueados.

            # 2. Suscripción al bus de mensajes (Redis Pub/Sub)
            await self.channel_layer.group_add(
                self.global_group,
                self.channel_name
            )

            # 3. Aceptación del Handshake Protocol
            await self.accept()
            logger.info(f"[WS: CONNECT] 🟢 Enlace establecido. Canal asignado: {self.channel_name}")

        except Exception as e:
            logger.error(f"[WS: ERROR CRÍTICO] 🔴 Fallo en la secuencia de conexión: {str(e)}")
            await self.close(code=1011) # Código estándar de Internal Server Error
            raise StopConsumer()

    async def disconnect(self, close_code):
        """
        Destruye los canales de forma segura para evitar Memory Leaks en el Broker.
        """
        try:
            await self.channel_layer.group_discard(
                self.global_group,
                self.channel_name
            )
            logger.info(f"[WS: DISCONNECT] 🔌 Enlace terminado (Cód: {close_code}). Canal liberado: {self.channel_name}")
        except Exception as e:
            logger.error(f"[WS: DISCONNECT ERROR] ⚠️ Fuga potencial al desconectar: {str(e)}")
        finally:
            # Obligatorio en arquitecturas pesadas para matar el proceso en RAM
            raise StopConsumer()

    async def receive(self, text_data=None, bytes_data=None):
        """
        [FULL-DUPLEX ENABLED]: Escucha comandos directos desde el navegador del usuario.
        Útil si en el futuro quieres añadir botones para "Pausar Sniper" en tiempo real.
        """
        if text_data:
            try:
                payload = json.loads(text_data)
                logger.debug(f"[WS: INBOUND] 📡 Comando recibido del mando central: {payload}")
                # El ruteo inverso iría aquí
            except json.JSONDecodeError:
                logger.warning("[WS: INBOUND ERROR] ❌ Payload corrupto recibido del cliente.")

    async def send_status(self, event):
        """
        Despachador Asíncrono de Alta Velocidad.
        Recibe el evento crudo de Celery/Redis y lo empaqueta para el Frontend.
        """
        try:
            # Extracción segura de la carga útil con valores por defecto
            message = event.get("message", "SEÑAL VACÍA")
            level = event.get("level", "info") # 'info', 'success', 'warning', 'error'
            task_id = event.get("task_id", None)
            
            # Serialización y envío de telemetría enriquecida
            await self.send(text_data=json.dumps({
                "type": "radar_telemetry",
                "level": level,
                "task_id": task_id,
                "message": message
            }))
            
        except Exception as e:
            logger.error(f"[WS: DISPATCH ERROR] 💥 Fallo al retransmitir telemetría al frontend: {str(e)}")