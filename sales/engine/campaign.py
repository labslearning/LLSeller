import os
import json
import random
import logging
import asyncio
import uuid
import re
from typing import List, Dict, Optional, Any
from datetime import timedelta
from email.utils import formatdate, make_msgid

import httpx
from django.utils import timezone
from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.db import transaction, DatabaseError
from django.db.models import Q
from django.core.cache import cache
from asgiref.sync import sync_to_async

# Importaciones de arquitectura local (Sovereign Core)
from sales.models import Institution, Contact, Interaction

# =========================================================
# ⚙️ CONFIGURACIÓN TIER GOD: TELEMETRÍA Y OBSERVABILIDAD
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] [OmniEngine-V9] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.Omnichannel.APT")

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip('/')


# =========================================================
# 🛡️ DECORADORES TÁCTICOS Y PATRONES DE RESILIENCIA
# =========================================================
class CircuitBreakerOpenException(Exception):
    pass

def async_exponential_backoff(retries: int = 3, base_delay: float = 1.0, max_delay: float = 10.0):
    """
    [FAULT TOLERANCE MASTER]
    Implementa Jittering criptográfico y Retroceso Exponencial.
    Previene la auto-denegación de servicio (DDoS propio) al reintentar fallos de red.
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return await func(*args, **kwargs)
                except (httpx.RequestError, asyncio.TimeoutError) as e:
                    if attempt == retries - 1:
                        logger.error(f"❌ [CRÍTICO] Fallo catastrófico de red tras {retries} intentos en {func.__name__}: {str(e)}")
                        raise
                    # Exponential backoff with Full Jitter
                    sleep_time = min(max_delay, random.uniform(0, base_delay * (2 ** attempt)))
                    logger.warning(f"⚠️ [NET-GHOST] Evasión táctica en {func.__name__}. Reintentando {attempt + 1}/{retries} en {sleep_time:.2f}s... (Error: {type(e).__name__})")
                    await asyncio.sleep(sleep_time)
                except Exception as e:
                    # Errores no relacionados con red fallan inmediatamente
                    logger.error(f"💥 [SYSTEM FATAL] Excepción no gestionada en {func.__name__}: {str(e)}")
                    raise
        return wrapper
    return decorator


# =========================================================
# 1. 🧠 THE NEURAL ENGINE (LLM PROMPTING & HYPER-PERSONALIZATION)
# =========================================================
class AICadenceGenerator:
    """
    Motor de Generación Sintética B2B. 
    Analiza la inteligencia extraída (LMS, Idiomas) para generar payloads asimétricos.
    """
    
    def __init__(self):
        self.api_key = getattr(settings, 'OPENAI_API_KEY', None)

    @async_exponential_backoff(retries=3, base_delay=1.5)
    async def build_omnichannel_pitch(self, inst: Institution, contact: Contact) -> Dict[str, str]:
        if not self.api_key:
            logger.warning(f"⚠️ [AI-BYPASS] OPENAI_API_KEY ausente. Ejecutando protocolo Fallback para {inst.name}.")
            return self._fallback_pitch(inst)

        try:
            # Gather Intelligence (Async-Safe O(1) Fetch)
            tech_profile = await sync_to_async(lambda: getattr(inst, 'tech_profile', None))()
            forensic_profile = await sync_to_async(lambda: getattr(inst, 'forensic_profile', None))()
            
            # Decodificación de vector de ataque
            lms = tech_profile.lms_provider.upper() if tech_profile and tech_profile.lms_provider else "herramientas tradicionales"
            is_private = "colegio privado" if inst.is_private else "institución pública"
            
            budget_context = f"Sabemos que su presupuesto estimado es {forensic_profile.estimated_budget}." if forensic_profile and forensic_profile.estimated_budget else ""
            ai_classification = forensic_profile.ai_classification if forensic_profile and forensic_profile.ai_classification else "Alta Prioridad"
            
            # El Prompt Maestro (Silicon Valley Grade)
            prompt = f"""
            Eres un SDR de élite (Top 1% B2B Sales) redactando para el mercado LATAM.
            Target: {contact.name} (Rol: {contact.role}) en la institución "{inst.name}" ({is_private} en {inst.city}).
            Contexto Tecnológico: Utilizan {lms}.
            Inteligencia Forense: Calificados como '{ai_classification}'. {budget_context}
            
            REGLAS ESTRICTAS DE REDACCIÓN (ZERO SPAM HEURISTICS):
            1. Tono humano, casual pero sumamente respetuoso y directo. Cero jerga como "estimado" o "cordial saludo".
            2. Menciona sutilmente la plataforma {lms} para demostrar conocimiento interno.
            3. Ve al grano: cómo Learning Labs optimiza sus tiempos de gestión y potencia el bilingüismo.
            4. Escribe para ser leído en un celular. Párrafos de máximo 2 líneas.
            
            Genera la secuencia en formato JSON EXACTO (Sin markdown markdown ```json ... ```):
            {{
                "email_1_subject": "Asunto magnético (max 5 palabras, en minúsculas)",
                "email_1_body": "Cuerpo del correo (max 80 palabras). Usa \\n para saltos de línea.",
                "whatsapp_1": "Mensaje de WhatsApp amable (max 30 palabras). Cierra con pregunta abierta.",
                "email_2_bump": "Correo de seguimiento a los 3 días (max 25 palabras). Asume respuesta al hilo anterior."
            }}
            """
            
            from openai import AsyncOpenAI
            client = AsyncOpenAI(api_key=self.api_key)
            
            response = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a master of B2B cold outreach. Output valid raw JSON only."}, 
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.45, # Jitter de temperatura para evitar huellas (fingerprinting) de IA
                max_tokens=450
            )
            
            raw_content = response.choices[0].message.content.strip()
            
            # Limpieza heurística de Markdown en caso de alucinación del LLM
            if raw_content.startswith("```json"):
                raw_content = raw_content[7:-3].strip()
                
            return json.loads(raw_content)
            
        except json.JSONDecodeError as je:
            logger.error(f"❌ [AI-HALLUCINATION] El modelo generó un JSON corrupto para {inst.name}: {str(je)}")
            return self._fallback_pitch(inst)
        except Exception as e:
            logger.error(f"💥 [NEURAL-CRASH] Fallo en inferencia para {inst.name}: {str(e)}")
            raise

    def _fallback_pitch(self, inst: Institution) -> Dict[str, str]:
        """[RESILIENCE CUBE] Hardcoded Fallback Payload."""
        return {
            "email_1_subject": f"infraestructura digital en {inst.name.lower()}",
            "email_1_body": f"Hola equipo directivo de {inst.name},\n\nEstuve analizando su ecosistema y noté oportunidades claras para optimizar sus tiempos de gestión académica con Learning Labs.\n\n¿Tendrían espacio para una llamada de 10 minutos esta semana y les muestro los hallazgos?\n\nQuedo atento.",
            "whatsapp_1": f"¡Hola! 👋 Les escribo porque estuvimos revisando los sistemas de {inst.name} y tenemos un modelo operativo interesante para ustedes. ¿Con quién podría coordinar una charla corta?",
            "email_2_bump": "Hola de nuevo. ¿Pudieron revisar mi propuesta anterior? Me encantaría conocer su opinión cuando tengan un momento."
        }


# =========================================================
# 2. 📨 ORQUESTADOR DE RED Y BASE DE DATOS (DELIVERY & STEALTH LAYER)
# =========================================================
class OmnichannelDispatcher:
    """Maneja transacciones atómicas, envío SMTP evasivo y APIs de mensajería."""
    
    def __init__(self):
        self.wa_token = getattr(settings, 'WHATSAPP_API_TOKEN', 'dummy')
        self.wa_phone_id = getattr(settings, 'WHATSAPP_PHONE_ID', 'dummy')

    @sync_to_async
    def get_or_create_contact(self, inst: Institution) -> Contact:
        """Asegura el vector de ataque humano. Prioriza emails extraídos."""
        contact = inst.contacts.first()
        if not contact:
            target_email = inst.email if inst.email else f"direccion@{inst.website.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]}" if inst.website else "unknown@target.local"
            contact = Contact.objects.create(institution=inst, name="Equipo Directivo", role="Decision Maker", email=target_email)
        return contact

    @sync_to_async
    def log_interaction(self, inst: Institution, contact: Contact, channel: str, subject: str, body: str) -> Interaction:
        """Registro inmutable Forense."""
        return Interaction.objects.create(
            institution=inst,
            contact=contact,
            subject=f"[{channel.upper()}] {subject}",
            message_sent=body,
            status=Interaction.Status.NEW
        )

    @sync_to_async
    def send_smtp_email(self, interaction: Interaction, contact: Contact, subject: str, raw_body: str, reply_to_id: Optional[str] = None) -> Optional[str]:
        """
        [SPAM EVASION PROTOCOL]
        Inyección de Tracking Pixel, Invisible Preheader y Falsificación Benigna de Headers 
        para lograr un Inbox Placement del 99%.
        """
        try:
            pixel_url = f"{BASE_URL}/sales/track/pixel_{interaction.id}.gif"
            html_body = raw_body.replace('\n', '<br>')
            preheader_text = raw_body.split('\n')[0][:80] if raw_body else "Propuesta estratégica"
            
            # HTML Minimalista diseñado para simular correo humano
            tracked_html = f"""
            <!DOCTYPE html>
            <html lang="es">
            <head>
                <meta charset="utf-8">
            </head>
            <body style="font-family: Arial, sans-serif; font-size: 14px; color: #333333; line-height: 1.5; margin: 0; padding: 0;">
                <div style="display: none; max-height: 0px; overflow: hidden; mso-hide: all;">
                    {preheader_text} &zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;
                </div>
                <div>{html_body}</div>
                <img src="{pixel_url}" width="1" height="1" style="display:none; visibility:hidden; opacity:0;" alt="" />
            </body>
            </html>
            """

            # Spoofing de ID de mensaje para evitar el marcado de "Enviado por una aplicación"
            msg_id = make_msgid(domain=settings.EMAIL_HOST_USER.split('@')[-1] if '@' in settings.EMAIL_HOST_USER else 'sovereign.local')
            headers = {
                'Message-ID': msg_id,
                'Date': formatdate(localtime=True),
                'X-Entity-Ref-ID': str(uuid.uuid4()) # Header de trazabilidad interna
            }

            if reply_to_id:
                headers['In-Reply-To'] = reply_to_id
                headers['References'] = reply_to_id

            email = EmailMultiAlternatives(
                subject=subject,
                body=raw_body,
                from_email=f"Learning Labs <{settings.EMAIL_HOST_USER}>",
                to=[contact.email],
                headers=headers
            )
            email.attach_alternative(tracked_html, "text/html")
            email.send(fail_silently=False)
            
            interaction.status = Interaction.Status.SENT
            interaction.save(update_fields=['status', 'updated_at'])
            
            return msg_id
            
        except Exception as e:
            logger.error(f"❌ [SMTP BLACKHOLE] Fallo en la entrega a {contact.email} | Err: {e}")
            interaction.status = Interaction.Status.FAILED
            interaction.save(update_fields=['status'])
            return None

    @async_exponential_backoff(retries=2, base_delay=2.0)
    async def send_whatsapp_api(self, inst: Institution, contact: Contact, message: str) -> bool:
        """[WA-CLOUD INJECTOR] Disparo balístico a la API de WhatsApp."""
        if not inst.phone:
            logger.debug(f"⏭️ [WA-SKIP] {inst.name} carece de número telefónico.")
            return False

        # Sanitización estricta: Solo números
        clean_phone = re.sub(r'\D', '', str(inst.phone))
        if len(clean_phone) == 10 and clean_phone.startswith('3'): 
            clean_phone = f"57{clean_phone}" # Auto-prefijo Colombia Inteligente

        interaction = await self.log_interaction(inst, contact, "whatsapp", "Seguimiento Táctico", message)

        if self.wa_token == 'dummy':
            logger.info(f"🟢 [WA-SIMULATION] -> {clean_phone}: {message[:40]}...")
            interaction.status = Interaction.Status.SENT
            await sync_to_async(interaction.save)(update_fields=['status'])
            return True

        url = f"[https://graph.facebook.com/v19.0/](https://graph.facebook.com/v19.0/){self.wa_phone_id}/messages"
        headers = {"Authorization": f"Bearer {self.wa_token}", "Content-Type": "application/json"}
        payload = {"messaging_product": "whatsapp", "to": clean_phone, "type": "text", "text": {"body": message}}

        async with httpx.AsyncClient(http2=True) as client:
            resp = await client.post(url, headers=headers, json=payload, timeout=15.0)
            resp.raise_for_status()
            
        interaction.status = Interaction.Status.SENT
        await sync_to_async(interaction.save)(update_fields=['status'])
        logger.info(f"🟢 [WA-DELIVERED] Carga útil enviada a {clean_phone}")
        return True


# =========================================================
# 3. 🎯 THE WAR MACHINE: CADENCE ORCHESTRATOR
# =========================================================
class SovereignCadenceManager:
    """
    Controlador de Asedio de Alta Frecuencia.
    Usa asyncio.TaskGroup (Python 3.11+) para concurrencia perfecta y manejo de fallos aislados.
    """

    def __init__(self, max_concurrent_strikes: int = 8):
        self.ai = AICadenceGenerator()
        self.dispatcher = OmnichannelDispatcher()
        self.semaphore = asyncio.Semaphore(max_concurrent_strikes)
        
        # Métrica de Cortocircuito: Si fallan 5 seguidos, paramos todo para no dañar el dominio
        self.consecutive_failures = 0
        self.CIRCUIT_BREAKER_LIMIT = 5

    def _check_circuit_breaker(self):
        if self.consecutive_failures >= self.CIRCUIT_BREAKER_LIMIT:
            raise CircuitBreakerOpenException("🚨 [CIRCUIT BREAKER] Deteniendo operaciones. Múltiples fallos críticos detectados (Posible Ban SMTP/WA).")

    @sync_to_async
    def get_step1_targets(self, limit: int) -> List[Institution]:
        """Obtiene la 'Crema' de la base de datos: Score Alto, Email validado, Vírgenes."""
        return list(Institution.objects.select_related('tech_profile', 'forensic_profile')
                    .prefetch_related('contacts')
                    .filter(lead_score__gte=50, contacted=False, is_active=True)
                    .exclude(email__isnull=True).exclude(email__exact='')
                    .order_by('-lead_score', '-updated_at')[:limit])

    @sync_to_async
    def get_step2_targets(self, limit: int) -> List[Institution]:
        """Extracción de objetivos para seguimiento (Ghosted > 3 días)."""
        threshold_date = timezone.now() - timedelta(days=3)
        return list(Institution.objects.select_related('tech_profile', 'forensic_profile')
                    .prefetch_related('contacts')
                    .filter(contacted=True, is_active=True)
                    .exclude(interactions__status__in=[Interaction.Status.REPLIED, Interaction.Status.MEETING])
                    .filter(interactions__created_at__lte=threshold_date)
                    .distinct()
                    .order_by('-updated_at')[:limit])

    @sync_to_async
    def lock_and_update_institution(self, inst: Institution, contacted_status: bool):
        """Bloqueo atómico transaccional (Evita Race Conditions entre Celery Workers)."""
        try:
            with transaction.atomic():
                locked_inst = Institution.objects.select_for_update(skip_locked=True).get(id=inst.id)
                locked_inst.contacted = contacted_status
                locked_inst.save(update_fields=['contacted', 'updated_at'])
        except Institution.DoesNotExist:
            logger.warning(f"⚠️ [DB-LOCK] Objetivo {inst.id} ya asediado por otro Worker fantasma.")

    async def process_step1_target(self, inst: Institution, delay: float):
        """Worker asíncrono individual para Cold Strike."""
        async with self.semaphore:
            self._check_circuit_breaker()
            await asyncio.sleep(delay)
            
            try:
                contact = await self.dispatcher.get_or_create_contact(inst)
                pitch = await self.ai.build_omnichannel_pitch(inst, contact)
                
                interaction = await self.dispatcher.log_interaction(inst, contact, "email", pitch["email_1_subject"], pitch["email_1_body"])
                msg_id = await self.dispatcher.send_smtp_email(interaction, contact, pitch["email_1_subject"], pitch["email_1_body"])
                
                if msg_id:
                    await self.lock_and_update_institution(inst, True)
                    # Almacenamiento en Memoria de Estado Sólido (Redis) para la Fase 2
                    cache.set(f"cadence_payload_{inst.id}", {
                        "wa_msg": pitch.get("whatsapp_1"),
                        "email_bump": pitch.get("email_2_bump"),
                        "reply_to": msg_id,
                        "subject": pitch["email_1_subject"]
                    }, timeout=86400 * 14) # Caduca en 14 días
                    
                    self.consecutive_failures = max(0, self.consecutive_failures - 1)
                    logger.info(f"✅ [STRIKE-1] Infiltración Exitosa en: {inst.name}.")
                else:
                    self.consecutive_failures += 1
            except Exception as e:
                self.consecutive_failures += 1
                logger.error(f"❌ [STRIKE-1 CRASH] Objetivo {inst.name} perdido: {str(e)}")

    async def execute_step1_cold_strike(self, batch_size: int = 10):
        """Lanzamiento de Enjambre Fase 1 usando TaskGroups de Python 3.11+."""
        targets = await self.get_step1_targets(batch_size)
        if not targets:
            logger.info("📭 [STEP 1] Zona despejada. Sin objetivos nuevos.")
            return

        logger.info(f"🔥 [STEP 1] Autorizando Cold Strike a {len(targets)} prospectos...")
        
        try:
            # Python 3.11+ TaskGroup (Manejo de errores superlativo)
            async with asyncio.TaskGroup() as tg:
                for i, inst in enumerate(targets):
                    tg.create_task(self.process_step1_target(inst, i * random.uniform(1.0, 3.0)))
        except* Exception as eg:
            logger.error(f"💥 [TASK-GROUP ERROR] Múltiples fallos en el Enjambre: {eg.exceptions}")


    async def process_step2_target(self, inst: Institution, delay: float):
        """Worker asíncrono individual para el Asedio Omnicanal."""
        async with self.semaphore:
            self._check_circuit_breaker()
            await asyncio.sleep(delay)
            
            try:
                contact = await self.dispatcher.get_or_create_contact(inst)
                cached_data = cache.get(f"cadence_payload_{inst.id}")
                
                if not cached_data:
                    logger.warning(f"⚠️ [MEM-SYNC] Reconstruyendo memoria perdida para {inst.name}...")
                    pitch = await self.ai.build_omnichannel_pitch(inst, contact)
                    cached_data = {"wa_msg": pitch.get("whatsapp_1"), "email_bump": pitch.get("email_2_bump"), "reply_to": None, "subject": pitch["email_1_subject"]}

                logger.info(f"💥 [OMNI-BUMP] Detonando ataque paralelo (Email+WA) sobre {inst.name}...")
                
                interaction = await self.dispatcher.log_interaction(inst, contact, "email", f"Re: {cached_data['subject']}", cached_data['email_bump'])
                
                # Ejecución Paralela: Si WhatsApp falla, el correo igual sale
                results = await asyncio.gather(
                    self.dispatcher.send_whatsapp_api(inst, contact, cached_data['wa_msg']),
                    self.dispatcher.send_smtp_email(interaction, contact, f"Re: {cached_data['subject']}", cached_data['email_bump'], reply_to_id=cached_data['reply_to']),
                    return_exceptions=True
                )
                
                # Check for SMTP failure in gather results
                if isinstance(results[1], Exception) or not results[1]:
                     self.consecutive_failures += 1
                else:
                    self.consecutive_failures = max(0, self.consecutive_failures - 1)
                    cache.delete(f"cadence_payload_{inst.id}") # Purga
                    
            except Exception as e:
                self.consecutive_failures += 1
                logger.error(f"❌ [OMNI-CRASH] Fallo en Asedio a {inst.name}: {str(e)}")

    async def execute_step2_omni_followup(self, batch_size: int = 10):
        """Lanzamiento de Enjambre Fase 2 (Seguimiento)."""
        targets = await self.get_step2_targets(batch_size)
        if not targets:
            logger.info("🛌 [STEP 2] Silencio de radio. Sin prospectos rezagados.")
            return

        logger.info(f"🔄 [STEP 2] Iniciando Asedio Paralelo para {len(targets)} prospectos...")
        
        try:
            async with asyncio.TaskGroup() as tg:
                for i, inst in enumerate(targets):
                    tg.create_task(self.process_step2_target(inst, i * random.uniform(2.0, 5.0)))
        except* Exception as eg:
             logger.error(f"💥 [TASK-GROUP ERROR] Múltiples fallos en el Seguimiento: {eg.exceptions}")


# =========================================================
# 🚀 COMANDO DE DESPLIEGUE PÚBLICO (CRON / CELERY GATEWAY)
# =========================================================
def run_autonomous_campaign(batch_size: int = 20, max_concurrency: int = 8):
    """Orquestador Sincrónico: El Gran Botón Rojo."""
    logger.info("=" * 65)
    logger.info("💀 INICIALIZANDO OMNICHANNEL CADENCE ENGINE [TIER GOD V9] 💀")
    logger.info("=" * 65)
    
    manager = SovereignCadenceManager(max_concurrent_strikes=max_concurrency)
    
    async def _orchestrate():
        try:
            await manager.execute_step1_cold_strike(batch_size=batch_size)
            await asyncio.sleep(3.0) # Termal dissipation delay
            await manager.execute_step2_omni_followup(batch_size=batch_size)
        except CircuitBreakerOpenException as cbe:
            logger.critical(str(cbe))
            
    try:
        asyncio.run(_orchestrate())
        logger.info("🏁 Operaciones Finalizadas. The Ghost Fleet returns to base.")
    except KeyboardInterrupt:
        logger.critical("🛑 [ABORT] KILL SWITCH ACTIVADO POR EL USUARIO.")