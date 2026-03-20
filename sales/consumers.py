import ujson as json
import logging
import time
import asyncio
from typing import Dict, Any, Optional

from channels.generic.websocket import AsyncWebsocketConsumer
from channels.exceptions import StopConsumer, DenyConnection

# Telemetría de Grado Militar
logger = logging.getLogger("Sovereign.WebSockets")

class StatusConsumer(AsyncWebsocketConsumer):
    """
    =========================================================
    [NIVEL DIOS]: ULTRA-LOW LATENCY ASGI WEBSOCKET CONSUMER
    =========================================================
    Arquitectura de telemetría Full-Duplex (HFT Standard).
    - Backpressure Tolerance (Buffer Flood Protection).
    - Rate Limiting Algorítmico (Token Bucket O(1)).
    - O(1) Routing (Channel Sharding).
    - C-Level JSON Serialization (ujson).
    - Zero-Trust Admission Control pre-handshake.
    """

    # --- CONSTANTES DE PROTECCIÓN TÉRMICA (CONSTANTS) ---
    MAX_PAYLOAD_SIZE = 51200  # 50 KB estricto
    RATE_LIMIT_TOKENS = 10    # Máximo de comandos por ráfaga
    RATE_LIMIT_REFILL = 2.0   # Tokens recargados por segundo
    SOCKET_TIMEOUT = 5        # Timeout para Redis Groups
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Variables de estado pre-asignadas en memoria para O(1) lookup
        self.shard_id: str = ""
        self.global_group: str = "radar_updates_global"
        self._tokens: float = self.RATE_LIMIT_TOKENS
        self._last_check: float = time.monotonic()
        self._is_active: bool = False

    async def connect(self):
        """[PHASE 1]: Handshake & Strict Admission Control."""
        try:
            user = self.scope.get("user")
            
            # 1. EVALUACIÓN LAZY DE AUTH (O(1) Memory Check, 0 SQL Queries)
            if not user or not user.is_authenticated or not getattr(user, 'is_staff', False):
                client_ip = self.scope.get('client', ['UNKNOWN_IP'])[0]
                logger.warning(f"🛡️ [WAF BLOCK] Intento no autorizado. IP: {client_ip}")
                await self.close(code=4003) 
                raise DenyConnection("Forbidden")

            # 2. CHANNEL SHARDING
            self.shard_id = f"telemetry_shard_{user.id}"

            # 3. CONCURRENCY BINDING (Multiplexación Segura)
            try:
                # Usamos asyncio.wait_for para proteger contra caídas de Redis
                await asyncio.wait_for(
                    asyncio.gather(
                        self.channel_layer.group_add(self.shard_id, self.channel_name),
                        self.channel_layer.group_add(self.global_group, self.channel_name)
                    ),
                    timeout=self.SOCKET_TIMEOUT
                )
            except asyncio.TimeoutError:
                logger.error("🔴 [WS: REDIS DEADLOCK] Tiempo de espera agotado al conectar al Broker.")
                await self.close(code=1011)
                raise DenyConnection("Broker Unavailable")

            # 4. HANDSHAKE 
            await self.accept()
            self._is_active = True
            
            # ACK Inicial
            await self.send_json({
                "type": "system_ack",
                "status": "SECURE_UPLINK_ESTABLISHED",
                "shard": self.shard_id,
                "server_time": time.time()
            })
            
            logger.info(f"🟢 [WS: CONNECT] Command Center: {user.username} | Shard: {self.shard_id}")

        except DenyConnection:
            raise
        except Exception as e:
            logger.error(f"🔴 [WS: CRITICAL] Fallo en Kernel de Conexión: {e}")
            await self.close(code=1011)
            raise StopConsumer()

    async def disconnect(self, close_code):
        """[PHASE 2]: Atomic Garbage Collection (OOM Prevention)."""
        self._is_active = False
        try:
            if self.shard_id:
                # wait_for garantiza que un Redis lento no deje el socket colgado para siempre
                await asyncio.wait_for(
                    asyncio.gather(
                        self.channel_layer.group_discard(self.shard_id, self.channel_name),
                        self.channel_layer.group_discard(self.global_group, self.channel_name)
                    ),
                    timeout=self.SOCKET_TIMEOUT
                )
            logger.info(f"🔌 [WS: DISCONNECT] Uplink Terminado (Code: {close_code}). RAM Liberada.")
        except asyncio.TimeoutError:
            logger.warning("⚠️ [WS: DISCONNECT TIMEOUT] Redis no respondió durante la limpieza.")
        except Exception as e:
            logger.error(f"⚠️ [WS: LEAK WARNING] Fallo al desvincular Redis Groups: {e}")
        finally:
            raise StopConsumer()

    async def receive(self, text_data: Optional[str] = None, bytes_data: Optional[bytes] = None):
        """[PHASE 3]: Full-Duplex Command Router (DDoS & ReDoS Protected)."""
        if not text_data or not self._is_active:
            return

        # 1. RATE LIMITING (Token Bucket O(1))
        # Previene que un script malicioso ahogue la CPU del Worker
        now = time.monotonic()
        time_passed = now - self._last_check
        self._last_check = now
        self._tokens = min(self.RATE_LIMIT_TOKENS, self._tokens + (time_passed * self.RATE_LIMIT_REFILL))

        if self._tokens < 1.0:
            logger.warning(f"⚡ [WS: RATE LIMIT] Estrangulamiento de tráfico (Throttling) aplicado a {self.channel_name}.")
            # Cerramos conexión por abuso de recursos (Policy Violation)
            await self.close(code=1008) 
            return
            
        self._tokens -= 1.0

        # 2. ReDoS PROTECTION
        if len(text_data) > self.MAX_PAYLOAD_SIZE:
            logger.warning(f"❌ [WS: WAF] Payload entrante supera el límite permitido ({self.MAX_PAYLOAD_SIZE} bytes).")
            await self.close(code=1009) 
            return

        # 3. KERNEL ROUTING
        try:
            # ujson libera el GIL, parseo ultra-rápido en C
            payload = json.loads(text_data)
            command = payload.get("command")
            
            match command:
                case "PING":
                    await self.send_json({"type": "PONG", "latency": time.time()})
                case "FORCE_RECONNECT":
                    await self.close(code=1000)
                case _:
                    pass

        except ValueError: 
            logger.warning("❌ [WS: WAF] JSON corrupto detectado.")
            await self.close(code=1003) 
        except Exception as e:
            logger.error(f"💥 [WS: RUNTIME] Falla procesando Inbound: {e}")

    # --- EVENT DISPATCHERS (Invocados vía Redis Pub/Sub) ---

    async def send_status(self, event: Dict[str, Any]):
        """Ruteo de telemetría de tareas (Celery)."""
        if not self._is_active:
            return
            
        payload = {
            "type": "radar_telemetry",
            "level": event.get("level", "info"),
            "task_id": event.get("task_id", "NO_TASK"),
            "message": event.get("message", ""),
            "timestamp": event.get("timestamp", time.time())
        }
        await self._safe_send(payload)

    async def metric_mutation(self, event: Dict[str, Any]):
        """Ruteo de mutaciones de Base de Datos."""
        if not self._is_active:
            return

        if "raw_json" in event:
            await self._safe_send_raw(event["raw_json"])
            return

        payload = {
            "type": "mutation",
            "entity": event.get("entity"),       
            "action": event.get("action"),       
            "payload": event.get("payload", {})
        }
        await self._safe_send(payload)

    # --- LOW-LEVEL NETWORK HELPERS ---

    async def send_json(self, content: Dict[str, Any]):
        """Envío estricto interno para operaciones críticas."""
        if self._is_active:
            await self.send(text_data=json.dumps(content))

    async def _safe_send_raw(self, text_data: str):
        """
        [BACKPRESSURE SHIELD]: Envía datos crudos esquivando cuellos de botella TCP.
        """
        try:
            await asyncio.wait_for(self.send(text_data=text_data), timeout=0.5)
        except asyncio.TimeoutError:
            # Caída Suave: No cerramos el socket, solo perdemos el frame (UDP Style)
            pass 
        except Exception as e:
            logger.error(f"💥 [WS: SEND ERROR] {e}")

    async def _safe_send(self, content: Dict[str, Any]):
        """Empaqueta y despacha con tolerancia al Backpressure."""
        try:
            # Pre-compilación en C (ujson)
            text_data = json.dumps(content)
            await self._safe_send_raw(text_data)
        except Exception as e:
            logger.error(f"💥 [WS: SERIALIZATION ERROR] {e}")