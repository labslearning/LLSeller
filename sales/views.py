import base64
import logging
from django.http import HttpResponse
from django.views.decorators.http import require_GET
from django.db.models import F
from .models import Interaction

logger = logging.getLogger(__name__)

# [NIVEL DIOS 1]: Decodificación en Memoria (Zero-Cost Abstraction)
# Decodificamos el pixel UNA SOLA VEZ cuando arranca el servidor, no por cada petición.
PIXEL_BYTES = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")

# [NIVEL DIOS 2]: Motor Heurístico Anti-Bot
# Bloquea los escáneres de seguridad corporativos para que no inflen tus métricas de aperturas.
SECURITY_BOTS = {
    'googleimageproxy', 'proofpoint', 'mimecast', 'barracuda', 
    'slackbot', 'whatsapp', 'telegrambot', 'applebot'
}

def _is_security_bot(user_agent: str) -> bool:
    """Evalúa si el User-Agent pertenece a un escáner de seguridad o a un humano real."""
    if not user_agent:
        return True # Si no tiene User-Agent, es un script malicioso o un bot muy básico
    
    ua_lower = user_agent.lower()
    return any(bot in ua_lower for bot in SECURITY_BOTS)

@require_GET
def tracking_pixel(request, interaction_id):
    """
    Motor de Analítica de Aperturas de Máxima Precisión.
    """
    # 1. Capturar Telemetría Avanzada (IP y Dispositivo)
    user_agent = request.META.get('HTTP_USER_AGENT', '')
    client_ip = request.META.get('HTTP_X_FORWARDED_FOR', request.META.get('REMOTE_ADDR', ''))

    # 2. Filtrado Anti-Bot Inmediato
    if _is_security_bot(user_agent):
        logger.info(f"🛡️ [BOT BLOQUEADO] Escaneo de seguridad interceptado en Interacción {interaction_id}")
        return _build_pixel_response()

    try:
        # 3. Traemos la interacción de la DB
        interaction = Interaction.objects.get(id=interaction_id)
        
        # [NIVEL DIOS 3]: Prevención de Race Conditions (Updates Atómicos)
        # F('opened_count') le dice a PostgreSQL: "Bloquea la fila y suma 1 directamente en el disco duro"
        interaction.opened_count = F('opened_count') + 1
        
        # Solo actualizamos el estado si no ha respondido o no hemos cerrado trato
        # No queremos degradar un lead "REPLIED" a "OPENED" solo porque volvió a leer el correo
        if interaction.status in [Interaction.Status.NEW, Interaction.Status.SENT]:
            interaction.status = Interaction.Status.OPENED
            
        interaction.save(update_fields=['opened_count', 'status', 'updated_at'])
        
        # Refrescamos el valor desde la DB para el log (ya que F() es una expresión SQL, no un número Python)
        interaction.refresh_from_db(fields=['opened_count'])
        logger.info(f"🎯 [HUMANO REAL] ¡Apertura detectada! IP: {client_ip} | Interacción: {interaction_id} | Total: {interaction.opened_count}")

    except Interaction.DoesNotExist:
        # Falla silenciosamente. No damos pistas a atacantes.
        pass
    except Exception as e:
        logger.error(f"[X] [CRÍTICO] Fallo en procesamiento de Pixel: {str(e)}")

    return _build_pixel_response()

def _build_pixel_response() -> HttpResponse:
    """
    [NIVEL DIOS 4]: Blindaje de Caché
    Cabeceras HTTP estrictas para obligar a Gmail/Outlook a descargar la imagen CADA VEZ que el usuario abre el correo.
    """
    response = HttpResponse(PIXEL_BYTES, content_type="image/gif")
    response['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
    response['Pragma'] = 'no-cache'
    response['Expires'] = '0'
    return response