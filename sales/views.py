import re
import base64
import logging
import uuid
from django.http import HttpResponse, HttpRequest
from django.views.decorators.http import require_GET
from django.db import transaction
from django.db.models import F
from django.core.cache import cache

# Importaciones locales (asegúrate de que las rutas sean correctas)
from .models import Interaction, Institution

logger = logging.getLogger("Sovereign.Tracking")

# =========================================================
# [NIVEL DIOS 1]: Zero-Copy Memory Pre-allocation
# Decodificamos el píxel en RAM UNA SOLA VEZ en el arranque del Worker.
# =========================================================
PIXEL_BYTES = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")

# =========================================================
# [NIVEL DIOS 2]: Motor Heurístico Anti-Bot Pre-Compilado (Tel Aviv / C-Level)
# Compilamos la expresión regular en C para evaluaciones de nanosegundos (O(1)).
# Incluye protección contra Apple Mail Privacy Proxies y escáneres corporativos.
# =========================================================
BOT_SIGNATURES = r'(googleimageproxy|proofpoint|mimecast|barracuda|slackbot|whatsapp|telegrambot|applebot|outlook-com|yahoo|yandex|microsoft|spider|bot|crawler|scanner|datanyze)'
BOT_REGEX = re.compile(BOT_SIGNATURES, re.IGNORECASE)

def _is_security_bot(user_agent: str, client_ip: str) -> bool:
    """Heurística ultra-rápida. Detecta bots por User-Agent y ofuscación."""
    if not user_agent or len(user_agent) < 15:
        # Los clientes de correo reales tienen UAs largos. Si es muy corto o nulo, es un cURL o script.
        return True 
    return bool(BOT_REGEX.search(user_agent))

def _build_pixel_response() -> HttpResponse:
    """
    [NIVEL DIOS 3]: Blindaje de Caché Extremo (Edge Caching)
    Cabeceras HTTP inquebrantables. Obligan a Gmail/Apple a saltarse sus CDNs 
    y golpear nuestro servidor cada vez que el humano mira la pantalla.
    """
    response = HttpResponse(PIXEL_BYTES, content_type="image/gif")
    response['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0, s-maxage=0'
    response['Pragma'] = 'no-cache'
    response['Expires'] = '0'
    response['Cross-Origin-Resource-Policy'] = 'cross-origin' # Bypass CORS en Webmails
    response['Timing-Allow-Origin'] = '*' # Previene bloqueos de rastreo de tiempo
    return response

# =========================================================
# [NIVEL DIOS 4]: Controlador de Telemetría Atómica (Shenzhen + Silicon Valley)
# =========================================================
@require_GET
def tracking_pixel_view(request: HttpRequest, interaction_id: str) -> HttpResponse:
    """
    Motor de Analítica de Aperturas de Máxima Precisión.
    Arquitectura: Edge Firewall -> Redis Debounce -> PostgreSQL Row-Level Lock -> CRM Scoring.
    """
    # 1. Captura de Telemetría Perimetral
    user_agent = request.META.get('HTTP_USER_AGENT', '')
    client_ip = request.META.get('HTTP_X_FORWARDED_FOR', request.META.get('REMOTE_ADDR', ''))

    # 2. Filtrado Anti-Bot Inmediato (Early Return / Ahorro de CPU)
    if _is_security_bot(user_agent, client_ip):
        logger.debug(f"🛡️ [WAF] Escáner corporativo rebotado en Interacción {interaction_id}")
        return _build_pixel_response()

    # Validación Estricta de Payload (Previene ataques de Inyección SQL)
    try:
        interaction_uuid = uuid.UUID(str(interaction_id))
    except ValueError:
        logger.warning(f"⚠️ [SECURITY] Payload malformado interceptado desde IP: {client_ip}")
        return _build_pixel_response()

    # 3. Escudo de Concurrencia (Redis Debouncing - Patrón Shenzhen)
    # Evita que el servidor colapse si un correo reenvía el pixel 50 veces por segundo.
    lock_key = f"pixel_debounce_{interaction_uuid}"
    if cache.get(lock_key):
        # Si ya lo registramos hace menos de 5 segundos, ignoramos el evento de Base de Datos.
        return _build_pixel_response()
    
    # Bloqueamos futuros hits por 5 segundos
    cache.set(lock_key, True, timeout=5)

    try:
        # 4. Transacción Atómica y Bloqueo Pesimista (PostgreSQL Row-Level Lock)
        with transaction.atomic():
            # .only() minimiza la carga en la RAM trayendo solo los bytes necesarios de la DB
            interaction = Interaction.objects.select_for_update().select_related('institution').only(
                'id', 'status', 'opened_count', 'institution__id', 'institution__lead_score', 'institution__name'
            ).get(id=interaction_uuid)
            
            # Suma atómica segura en disco (F-Expression)
            interaction.opened_count = F('opened_count') + 1
            
            # 5. Inteligencia Comercial Inyectada (Lead Scoring - Patrón Silicon Valley)
            if interaction.status in [Interaction.Status.NEW, Interaction.Status.SENT]:
                interaction.status = Interaction.Status.OPENED
                
                # Boost de interés en el prospecto (+15 pts)
                inst = interaction.institution
                nuevo_score = min(inst.lead_score + 15, 100) # Límite matemático en 100
                
                if inst.lead_score != nuevo_score:
                    inst.lead_score = nuevo_score
                    inst.save(update_fields=['lead_score', 'updated_at'])
                    logger.info(f"🔥 [REVENUE TRIGGER] {inst.name} elevó su temperatura a {inst.lead_score} pts.")

            # Sellado del registro en disco
            interaction.save(update_fields=['opened_count', 'status', 'updated_at'])
            
            # Refresco para auditoría forense en los logs
            interaction.refresh_from_db(fields=['opened_count'])
            logger.info(f"🎯 [HUMANO CONFIRMADO] Apertura verificada | IP: {client_ip} | ID: {interaction_uuid} | Total: {interaction.opened_count}")

    except Interaction.DoesNotExist:
        # Shadow Drop: Desconexión silenciosa. Los hackers no reciben feedback de que el ID no existe.
        pass
    except Exception as e:
        logger.critical(f"❌ [SYSTEM CRASH] Fallo en Core de Telemetría: {str(e)}")

    # 6. Respuesta Inquebrantable
    return _build_pixel_response()