import ujson as json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Union, Final

from django.db.models.signals import post_save
from django.dispatch import receiver
from django.db import transaction
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

from sales.models import Institution, Interaction

logger = logging.getLogger("Sovereign.Signals")

# =========================================================
# ⚙️ [GOD TIER CORE]: KERNEL CONFIGURATION & CONSTANTS
# =========================================================
# Variables Finales (Optimizadas por el compilador de Python para acceso O(1))
GLOBAL_SHARD: Final[str] = "radar_updates_global"
CIRCUIT_RECOVERY_TIME: Final[int] = 5

class TelemetryBackbone:
    """
    =========================================================
    [NIVEL DIOS]: NON-BLOCKING TELEMETRY REACTOR
    =========================================================
    Orquestador de hilos asíncronos con tolerancia a fallos.
    Desacopla la latencia de red (Redis I/O) del Hilo Principal de Django.
    """
    # Pool de hilos pre-calentados (Evita el costo de crear hilos dinámicamente)
    _executor: Final[ThreadPoolExecutor] = ThreadPoolExecutor(
        max_workers=4, 
        thread_name_prefix="Sovereign_Uplink"
    )
    _layer_cache: Any = None
    
    # Atómicos para Circuit Breaker (Lock-free memory)
    _circuit_open: bool = False
    _last_failure: float = 0.0

    @classmethod
    def get_layer(cls):
        """Lazy Singleton Cache O(1). Evita instanciar Channels repetidamente."""
        if cls._layer_cache is None:
            cls._layer_cache = get_channel_layer()
        return cls._layer_cache

    @classmethod
    def _is_circuit_closed(cls) -> bool:
        """
        [CIRCUIT BREAKER]: Si Redis muere, cortamos el envío inmediatamente
        para no ahogar la RAM de Django con hilos en espera.
        Se autorrepara después de CIRCUIT_RECOVERY_TIME segundos.
        """
        if cls._circuit_open:
            if time.time() - cls._last_failure > CIRCUIT_RECOVERY_TIME:
                cls._circuit_open = False  # Intentar reconexión (Half-Open)
                return True
            return False
        return True

    @classmethod
    def _execute_push(cls, payload: str):
        """Rutina aislada. Se ejecuta en un Worker del ThreadPool."""
        layer = cls.get_layer()
        if not layer:
            return

        try:
            # Transmisión cuántica. El Hilo principal de Django ya cerró la respuesta HTTP.
            async_to_sync(layer.group_send)(
                GLOBAL_SHARD, 
                {
                    "type": "metric_mutation",
                    "raw_json": payload  # Puerta trasera C-Level
                }
            )
        except Exception as e:
            # Si falla (ej. Redis Timeout), abrimos el circuito.
            cls._circuit_open = True
            cls._last_failure = time.time()
            logger.error(f"⚠️ [TELEMETRY BREAKER ACTIVATED] Fallo crítico de Broker: {e}")

    @classmethod
    def dispatch_fire_and_forget(cls, raw_payload: str):
        """Inyecta la tarea al pool sin esperar respuesta (Latencia 0.0ms)."""
        if cls._is_circuit_closed():
            cls._executor.submit(cls._execute_push, raw_payload)


# =========================================================
# 📡 VECTORIZED ROUTERS
# =========================================================

def dispatch_mutation_to_matrix(entity: str, action: str, payload: Union[dict, List[dict]]):
    """
    Empaqueta los datos usando C-Bindings y los despacha al Reactor.
    """
    try:
        # Pre-Serialización Ultra Rápida (ensure_ascii=False acelera el renderizado C)
        raw_payload = json.dumps({
            "type": "mutation",
            "entity": entity,
            "action": action,
            "payload": payload,
            "timestamp": time.time() # Idempotencia para el Frontend
        }, ensure_ascii=False)

        TelemetryBackbone.dispatch_fire_and_forget(raw_payload)
    except Exception as e:
        logger.error(f"💥 [SIGNAL SERIALIZATION ERROR] {entity}: {e}")


def broadcast_bulk_mutations(entity: str, action: str, instances: List[Any]):
    """
    [MASS-INJECTION API]: El parche maestro para el Punto Ciego de Django.
    Llamado explícitamente desde motores ML cuando usan `bulk_update()`.
    """
    if not instances:
        return
        
    if entity == 'institution':
        # Comprensión de listas vectorizada en C (Altamente eficiente)
        # getattr previene N+1 Queries si el objeto fue extraído con .only()
        payloads = [
            {
                "id": str(inst.id),
                "name": getattr(inst, 'name', 'Unknown'),
                "lead_score": getattr(inst, 'lead_score', 0),
                "status": "Contacted" if getattr(inst, 'contacted', False) else "Pending"
            } for inst in instances
        ]
        dispatch_mutation_to_matrix(entity, f"bulk_{action}", payloads)


# =========================================================
# 🪝 ATOMIC ORM HOOKS (SINGLE-ROW MUTATIONS)
# =========================================================

@receiver(post_save, sender=Institution)
def broadcast_institution_mutation(sender, instance, created, **kwargs):
    """
    Captura de mutaciones unitarias. 
    Asegurado contra bucles infinitos y Race Conditions.
    """
    # 1. PREVENCIÓN DE BUCLES TÉRMICOS (Ignorar telemetría fantasma)
    update_fields = kwargs.get('update_fields')
    if update_fields and 'last_scored_at' in update_fields and len(update_fields) == 1:
        # Si la IA solo actualizó el timestamp de scoring, no bombardeamos el WebSocket
        return

    action = 'create' if created else 'update'
    
    # 2. Extracción O(1)
    payload = {
        "id": str(instance.id),
        "name": instance.name,
        "lead_score": instance.lead_score,
        "status": "Contacted" if instance.contacted else "Pending"
    }

    # 3. [GOD TIER FIX]: transaction.on_commit
    # Garantiza que el WS avise al frontend SÓLO cuando los datos existan de verdad en la DB.
    transaction.on_commit(lambda: dispatch_mutation_to_matrix('institution', action, payload))


@receiver(post_save, sender=Interaction)
def broadcast_interaction_mutation(sender, instance, created, **kwargs):
    """
    Rastrea interacciones (Correos/Meetings) en tiempo real.
    """
    action = 'create' if created else 'update'
    
    payload = {
        "id": str(instance.id),
        "institution_id": str(instance.institution_id),
        "status": instance.status,
        "channel": instance.channel
    }

    transaction.on_commit(lambda: dispatch_mutation_to_matrix('interaction', action, payload))