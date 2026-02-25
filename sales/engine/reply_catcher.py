import socket
import imaplib
import email
import logging
import re
import uuid
from email.header import decode_header
from typing import Optional, List
from django.conf import settings
from django.db import transaction
from django.utils import timezone

# Importaciones locales
from sales.models import Interaction, Contact, Institution

# =========================================================
# CONFIGURACIÓN TIER GOD: TELEMETRÍA Y OBSERVABILIDAD
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] [InboundEngine] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.Inbound")

# =========================================================
# PRE-COMPILACIÓN EN MEMORIA (O(1) CPU TIME) - Patrón Shenzhen
# =========================================================
# Extrae el UUID inyectado en la cabecera del correo
THREAD_ID_REGEX = re.compile(r'<([a-f0-9\-]{36})@sovereign\.local>', re.IGNORECASE)
# Extracción de correo electrónico blindada contra RFCs malformados
EMAIL_CLEAN_REGEX = re.compile(r'([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)')


class OmniReplyCatcher:
    """
    [GOD TIER INBOUND CATCHER] 
    Interceptor de respuestas con lógica de 'Kill-Switch' automático.
    Diseñado bajo estándares de resiliencia y ciberseguridad de Tel Aviv.
    """
    def __init__(self):
        self.server = getattr(settings, 'IMAP_SERVER', 'imap.gmail.com')
        self.port = getattr(settings, 'IMAP_PORT', 993)
        self.username = getattr(settings, 'IMAP_USERNAME', None)
        self.password = getattr(settings, 'IMAP_PASSWORD', None)
        self.mail = None
        
        # Ajustamos el timeout global del Socket para evitar conexiones Zombie (Bloqueos Infinitos)
        socket.setdefaulttimeout(15.0)

    def connect(self) -> bool:
        """Establece conexión SSL/TLS segura con manejo de fallas a bajo nivel."""
        if not self.username or not self.password:
            logger.critical("❌ Credenciales IMAP no detectadas en el entorno.")
            return False

        try:
            self.mail = imaplib.IMAP4_SSL(self.server, self.port)
            self.mail.login(self.username, self.password)
            logger.info("🔐 Enlace criptográfico IMAP establecido.")
            return True
        except imaplib.IMAP4.error as e:
            logger.critical(f"⛔ Fallo de Autenticación IMAP (Revisa Google App Passwords): {e}")
            return False
        except socket.timeout:
            logger.critical("⛔ Timeout de conexión. El servidor IMAP no responde.")
            return False
        except Exception as e:
            logger.critical(f"⛔ Falla de infraestructura de Red: {e}")
            return False

    def _decode_header_value(self, value: str) -> str:
        """Decodifica cadenas ofuscadas (Base64/Quoted-Printable) sin corromper el Payload."""
        if not value: return ""
        try:
            decoded = decode_header(value)
            parts = []
            for text, charset in decoded:
                if isinstance(text, bytes):
                    parts.append(text.decode(charset or 'utf-8', errors='replace'))
                else: 
                    parts.append(str(text))
            return "".join(parts).strip()
        except Exception:
            return str(value)

    def process_unread_emails(self):
        """
        [MODO STEALTH] Escaneo de alta velocidad en la bandeja de entrada usando PEEK.
        Analiza las cabeceras sin disparar alarmas ni marcar los correos como leídos.
        """
        if not self.mail:
            return

        try:
            self.mail.select('inbox', readonly=False)
            status, messages = self.mail.search(None, 'UNSEEN')
            
            if status != 'OK' or not messages[0]:
                logger.info("📭 Silencio en la red. Sin respuestas nuevas.")
                return

            email_ids = messages[0].split()
            logger.info(f"📬 Interceptados {len(email_ids)} paquetes no leídos. Analizando firmas...")

            for num in email_ids:
                # [NIVEL DIOS]: Usamos BODY.PEEK[] en lugar de RFC822. 
                # Esto descarga el correo PERO le dice a Gmail que NO marque el correo como visto.
                # Tú seguirás recibiendo la notificación en tu iPhone.
                res, data = self.mail.fetch(num, '(BODY.PEEK[])')
                if res != 'OK': continue
                
                raw_email = data[0][1]
                msg = email.message_from_bytes(raw_email)
                
                # Extracción Forense de Cabeceras
                from_raw = self._decode_header_value(msg.get("From", ""))
                
                # Extracción segura del correo del remitente (Falla silenciosamente si es Spam malformado)
                sender_match = EMAIL_CLEAN_REGEX.search(from_raw)
                if not sender_match: continue
                sender_email = sender_match.group(1).lower()
                
                # Buscamos nuestro UUID oculto en In-Reply-To o References (Búsqueda determinística)
                in_reply_to = msg.get("In-Reply-To", "")
                references = msg.get("References", "")
                
                match = THREAD_ID_REGEX.search(in_reply_to) or THREAD_ID_REGEX.search(references)
                interaction_id = match.group(1) if match else None
                
                self._route_reply(interaction_id, sender_email)

        except Exception as e:
            logger.error(f"❌ Colapso en bucle de procesamiento IMAP: {str(e)}")
        finally:
            # [TEL AVIV SECURITY]: Siempre cerramos la conexión para liberar puertos TCP
            try:
                self.mail.logout()
            except:
                pass

    def _route_reply(self, interaction_id: Optional[str], sender_email: str):
        """
        [DATA WAREHOUSE ADAPTER]
        Ejecuta el Kill-Switch transaccional. Frenar la campaña y activar alerta de Revenue.
        """
        try:
            with transaction.atomic():
                interaction = None
                
                # A. Búsqueda Criptográfica Exacta (Prioridad 1)
                if interaction_id:
                    # skip_locked=True evita que el CRM se congele si otra tarea está leyendo este lead
                    interaction = Interaction.objects.select_for_update(skip_locked=True).select_related('institution').filter(id=interaction_id).first()
                
                # B. Búsqueda Difusa por Remitente (Fallback)
                if not interaction:
                    interaction = Interaction.objects.select_for_update(skip_locked=True).select_related('institution').filter(
                        institution__email__iexact=sender_email,
                        status__in=['SENT', 'OPENED']
                    ).order_by('-created_at').first()

                if interaction:
                    # 1. Kill-Switch: Marcar como respondido para aislarlo del asedio omnicanal
                    interaction.status = Interaction.Status.REPLIED if hasattr(Interaction.Status, 'REPLIED') else 'REPLIED'
                    interaction.replied = True
                    interaction.save(update_fields=['status', 'replied', 'updated_at'])
                    
                    # 2. Revenue Boost: Convertir la Institución en Lead Caliente
                    inst = interaction.institution
                    inst.lead_score = 100
                    inst.contacted = True
                    inst.save(update_fields=['lead_score', 'contacted', 'updated_at'])
                    
                    logger.info(f"🔥🔥 [HOT LEAD OBTENIDO] {inst.name} ha respondido. Secuencias de acoso ABORTADAS. Score -> 100.")
                else:
                    logger.debug(f"⚪ Paquete descartado. {sender_email} no pertenece a una cadencia activa.")
                    
        except Exception as e:
            logger.error(f"⚠️ Error de concurrencia al rutear respuesta de {sender_email}: {e}")

# =========================================================
# PUNTO DE ENTRADA PÚBLICO (WRAPPER)
# =========================================================
def run_inbound_catcher():
    """Lanzador seguro. Instancia el Demonio, procesa y destruye la instancia."""
    logger.info("==================================================")
    logger.info("🎧 ZERO-LEAK IMAP LISTENER INICIANDO PROTOCOLO 🎧")
    logger.info("==================================================")
    
    catcher = OmniReplyCatcher()
    if catcher.connect():
        catcher.process_unread_emails()
        
    logger.info("🏁 Escucha perimetral finalizada. Sistema en espera.")
