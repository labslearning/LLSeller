import socket
import imaplib
import email
import logging
import re
from email.header import decode_header
from typing import Optional, Dict

from django.conf import settings
from django.db import transaction
from django.core.cache import cache
from django.utils import timezone

from openai import OpenAI

# Importaciones locales
from sales.models import Interaction, Contact, Institution

# =========================================================
# ⚙️ CONFIGURACIÓN TIER GOD: TELEMETRÍA Y OBSERVABILIDAD
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] [InboundEngine] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.Inbound")

# Expresiones regulares pre-compiladas para máxima velocidad de CPU
THREAD_ID_REGEX = re.compile(r'<([a-f0-9\-]{36})@sovereign\.local>', re.IGNORECASE)
EMAIL_CLEAN_REGEX = re.compile(r'([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)')


class OmniReplyCatcher:
    """
    [GOD TIER INBOUND CATCHER] 
    Interceptor asíncrono con Deduplicación en Memoria, Análisis de Sentimiento (IA) 
    y Kill-Switch transaccional. Estándar de Tel Aviv / Silicon Wadi.
    """
    def __init__(self):
        self.server = getattr(settings, 'IMAP_SERVER', 'imap.gmail.com')
        self.port = getattr(settings, 'IMAP_PORT', 993)
        self.username = getattr(settings, 'IMAP_USERNAME', None)
        self.password = getattr(settings, 'IMAP_PASSWORD', None)
        self.mail = None
        
        # IA Setup: DeepSeek o GPT-4o-mini
        api_key = getattr(settings, 'DEEPSEEK_API_KEY', None)
        self.ai_enabled = bool(api_key)
        if self.ai_enabled:
            base_url = "https://api.deepseek.com" if "deepseek" in (api_key or "").lower() else None
            self.ai_client = OpenAI(api_key=api_key, base_url=base_url)

        socket.setdefaulttimeout(15.0) # Previene conexiones Zombie

    # =========================================================
    # 🛡️ CONTEXT MANAGER (Gestión Absoluta de Sockets TCP)
    # =========================================================
    def __enter__(self):
        """Abre la conexión de forma segura al iniciar el bloque `with`."""
        if not self.username or not self.password:
            logger.critical("❌ [FATAL] Credenciales IMAP no detectadas en las variables de entorno.")
            raise ValueError("Missing IMAP Credentials")

        try:
            self.mail = imaplib.IMAP4_SSL(self.server, self.port)
            self.mail.login(self.username, self.password)
            logger.info("🔐 Enlace criptográfico IMAP establecido.")
            return self
        except Exception as e:
            logger.critical(f"⛔ Falla de infraestructura de Red IMAP: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Garantiza la liberación del puerto TCP sin importar qué error ocurra."""
        if self.mail:
            try:
                self.mail.logout()
                logger.debug("🔒 Conexión IMAP cerrada y puerto liberado.")
            except Exception:
                pass

    # =========================================================
    # 🧠 INTELIGENCIA ARTIFICIAL (NPL SENTIMENT ANALYSIS)
    # =========================================================
    def _classify_intent_with_ai(self, email_body: str) -> str:
        """Clasifica el correo usando Modelos de Lenguaje para evitar Falsos Positivos."""
        if not self.ai_enabled or not email_body.strip():
            return "INTERESTED" # Fallback conservador si no hay IA
            
        prompt = f"""
        Act as an elite B2B Sales SDR. Read the following reply from a prospect.
        Classify their intent into exactly ONE of these four categories:
        - INTERESTED (They want to meet, ask for info, positive tone, or forwarded to someone else)
        - NOT_INTERESTED (They said no, stop emailing, unsubscribe, or negative tone)
        - OUT_OF_OFFICE (Automated vacation response, maternity leave, etc)
        - BOUNCE (Delivery failed, email not found, postmaster error)

        Email Text:
        "{email_body[:1000]}"

        Respond with ONLY the exact category name.
        """
        try:
            response = self.ai_client.chat.completions.create(
                model="deepseek-chat", # Ajustar a gpt-4o-mini si usas OpenAI
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=10
            )
            intent = response.choices[0].message.content.strip().upper()
            if intent not in ["INTERESTED", "NOT_INTERESTED", "OUT_OF_OFFICE", "BOUNCE"]:
                return "INTERESTED"
            return intent
        except Exception as e:
            logger.error(f"⚠️ Falla en Motor IA, aplicando heurística básica: {e}")
            return "INTERESTED"

    def _extract_plain_text(self, msg) -> str:
        """Extrae únicamente el texto plano, ignorando HTML y adjuntos pesados."""
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    try:
                        body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
                        break
                    except:
                        pass
        else:
            try:
                body = msg.get_payload(decode=True).decode(msg.get_content_charset() or 'utf-8', errors='ignore')
            except:
                pass
        return body.strip()

    def _decode_header_value(self, value: str) -> str:
        """Decodifica cadenas ofuscadas (Base64/Quoted-Printable)."""
        if not value: return ""
        try:
            decoded = decode_header(value)
            parts = [text.decode(charset or 'utf-8', errors='replace') if isinstance(text, bytes) else str(text) for text, charset in decoded]
            return "".join(parts).strip()
        except Exception:
            return str(value)

    # =========================================================
    # ⚡ MOTOR DE PROCESAMIENTO PRINCIPAL
    # =========================================================
    def process_unread_emails(self):
        """
        [MODO STEALTH + REDIS DEDUPLICATION] 
        Analiza las cabeceras usando PEEK. Mantiene el correo No Leído en la bandeja, 
        pero usa Memoria Caché para no reprocesar el mismo correo en el siguiente ciclo.
        """
        try:
            self.mail.select('inbox', readonly=False)
            status, messages = self.mail.search(None, 'UNSEEN')
            
            if status != 'OK' or not messages[0]:
                logger.info("📭 Silencio en la red. Sin respuestas nuevas.")
                return

            email_ids = messages[0].split()
            logger.info(f"📬 Interceptados {len(email_ids)} paquetes no leídos. Analizando firmas...")

            for num in email_ids:
                # BODY.PEEK[] asegura que el correo siga "No Leído" visualmente en el cliente de correo
                res, data = self.mail.fetch(num, '(BODY.PEEK[])')
                if res != 'OK': continue
                
                raw_email = data[0][1]
                msg = email.message_from_bytes(raw_email)
                
                # 1. Deduplicación por Message-ID (Evita Infinite Loops de PEEK)
                message_id = msg.get("Message-ID", "").strip()
                if not message_id: message_id = str(num) # Fallback
                
                cache_key = f"processed_email_{message_id}"
                if cache.get(cache_key):
                    continue # El sistema ya leyó y procesó este correo. Ignorar.
                
                # Marcar en Redis como procesado (Retención de 30 días)
                cache.set(cache_key, True, timeout=2592000)

                # 2. Extracción Forense
                from_raw = self._decode_header_value(msg.get("From", ""))
                sender_match = EMAIL_CLEAN_REGEX.search(from_raw)
                if not sender_match: continue
                sender_email = sender_match.group(1).lower()
                
                # Excluir correos propios o del sistema
                if settings.EMAIL_HOST_USER and sender_email == settings.EMAIL_HOST_USER.lower():
                    continue

                # 3. Localización de UUID (In-Reply-To)
                in_reply_to = msg.get("In-Reply-To", "")
                references = msg.get("References", "")
                match = THREAD_ID_REGEX.search(in_reply_to) or THREAD_ID_REGEX.search(references)
                interaction_id = match.group(1) if match else None
                
                # 4. Inferencia Textual
                email_text = self._extract_plain_text(msg)
                intent = self._classify_intent_with_ai(email_text)
                
                logger.info(f"🔎 Analizando {sender_email} | IA Sentimiento: {intent}")
                
                # 5. Ruteo Transaccional
                self._route_reply(interaction_id, sender_email, intent)

        except Exception as e:
            logger.error(f"❌ Colapso en bucle de procesamiento IMAP: {str(e)}")

    def _route_reply(self, interaction_id: Optional[str], sender_email: str, intent: str):
        """
        [DATA WAREHOUSE ADAPTER]
        Ejecuta el Kill-Switch transaccional. Asigna Lead Score dinámicamente según la IA.
        """
        try:
            with transaction.atomic():
                interaction = None
                
                # A. Búsqueda Criptográfica Exacta
                if interaction_id:
                    interaction = Interaction.objects.select_for_update(skip_locked=True).select_related('institution').filter(id=interaction_id).first()
                
                # B. Búsqueda Difusa por Remitente
                if not interaction:
                    interaction = Interaction.objects.select_for_update(skip_locked=True).select_related('institution').filter(
                        institution__email__iexact=sender_email,
                        status__in=['SENT', 'OPENED']
                    ).order_by('-created_at').first()

                if interaction:
                    inst = interaction.institution
                    inst.contacted = True # Frena automáticamente la fase 2 de la cadencia
                    
                    # Kill-Switch Inteligente basado en AI Intent
                    if intent == "INTERESTED":
                        interaction.status = Interaction.Status.REPLIED if hasattr(Interaction.Status, 'REPLIED') else 'REPLIED'
                        interaction.replied = True
                        inst.lead_score = 100
                        logger.info(f"🔥🔥 [HOT LEAD] {inst.name} respondió positivamente. Score -> 100.")
                        
                    elif intent == "NOT_INTERESTED":
                        interaction.status = "CLOSED"
                        inst.lead_score = 0
                        logger.info(f"🧊 [COLD LEAD] {inst.name} declinó. Cadencia abortada. Score -> 0.")
                        
                    elif intent == "BOUNCE":
                        interaction.status = "FAILED"
                        inst.lead_score = -10
                        logger.warning(f"⚠️ [BOUNCE] Correo de {inst.name} rebotó. Penalizando Lead Score.")
                        
                    elif intent == "OUT_OF_OFFICE":
                        # No cerramos el lead, lo dejamos en pausa
                        logger.info(f"🌴 [OOO] {inst.name} está fuera de la oficina. Se pausará la cadencia temporalmente.")

                    interaction.save(update_fields=['status', 'replied', 'updated_at'])
                    inst.save(update_fields=['lead_score', 'contacted', 'updated_at'])
                else:
                    logger.debug(f"⚪ Paquete descartado. {sender_email} no pertenece a una cadencia activa.")
                    
        except Exception as e:
            logger.error(f"⚠️ Error de concurrencia al rutear respuesta de {sender_email}: {e}")

# =========================================================
# PUNTO DE ENTRADA PÚBLICO (WRAPPER PARA CELERY)
# =========================================================
def run_inbound_catcher():
    """Lanzador robusto usando Context Managers."""
    logger.info("==================================================")
    logger.info("🎧 ZERO-LEAK IMAP LISTENER INICIANDO PROTOCOLO 🎧")
    logger.info("==================================================")
    
    try:
        with OmniReplyCatcher() as catcher:
            catcher.process_unread_emails()
    except Exception as e:
        logger.error(f"❌ Fallo al inicializar el Inbound Catcher: {e}")
        
    logger.info("🏁 Escucha perimetral finalizada. Sistema en espera.")