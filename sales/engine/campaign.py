import os
import json
import random
import logging
import asyncio
from typing import List, Dict, Optional, Any
from datetime import timedelta

import httpx
from django.utils import timezone
from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.db import transaction, DatabaseError
from django.db.models import Q
from django.core.cache import cache
from asgiref.sync import sync_to_async

# Importaciones de arquitectura local
from sales.models import Institution, Contact, Interaction

# =========================================================
# ⚙️ CONFIGURACIÓN TIER GOD: TELEMETRÍA Y OBSERVABILIDAD
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] [OmniEngine] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.Omnichannel")

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip('/')


# =========================================================
# 🛡️ DECORADORES DE INGENIERÍA DE RESILIENCIA
# =========================================================
def async_exponential_backoff(retries: int = 3, base_delay: float = 1.0):
    """
    [FAULT TOLERANCE]
    Reintenta llamadas asíncronas de red usando retroceso exponencial y Jitter 
    para evadir Rate Limits y micro-cortes de red.
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries - 1:
                        logger.error(f"❌ [CRÍTICO] Fallo definitivo tras {retries} intentos en {func.__name__}: {str(e)}")
                        raise
                    sleep_time = (base_delay * (2 ** attempt)) + random.uniform(0.1, 0.5)
                    logger.warning(f"⚠️ Fallo en {func.__name__}. Reintentando {attempt + 1}/{retries} en {sleep_time:.2f}s... Error: {str(e)}")
                    await asyncio.sleep(sleep_time)
        return wrapper
    return decorator


# =========================================================
# 1. 🧠 MOTOR DE HIPER-PERSONALIZACIÓN CON IA (SILICON VALLEY)
# =========================================================
class AICadenceGenerator:
    """
    Generador de Secuencias B2B Neural. 
    Analiza el Tier 1 (Tech) y Tier 2 (Forensic) para orquestar un pitch indetectable como bot.
    """
    
    def __init__(self):
        self.api_key = getattr(settings, 'OPENAI_API_KEY', None)

    @async_exponential_backoff(retries=3, base_delay=2.0)
    async def build_omnichannel_pitch(self, inst: Institution, contact: Contact) -> Dict[str, str]:
        if not self.api_key:
            return self._fallback_pitch(inst)

        try:
            # Extracción profunda de datos relacionales (Cross-Tier Data Gathering)
            tech_profile = await sync_to_async(lambda: getattr(inst, 'tech_profile', None))()
            forensic_profile = await sync_to_async(lambda: getattr(inst, 'forensic_profile', None))()
            
            lms = tech_profile.lms_provider.upper() if tech_profile and tech_profile.lms_provider else "herramientas tradicionales"
            is_private = "colegio privado" if inst.is_private else "institución pública"
            
            # Integración de Inteligencia Forense (Si existe)
            budget_context = f"Sabemos que su presupuesto estimado es {forensic_profile.estimated_budget}." if forensic_profile and forensic_profile.estimated_budget else ""
            ai_classification = forensic_profile.ai_classification if forensic_profile and forensic_profile.ai_classification else "Alta Prioridad"
            
            prompt = f"""
            Eres un SDR de élite (Top 1% B2B Sales) redactando para el mercado LATAM.
            Target: {contact.name} (Rol: {contact.role}) en la institución "{inst.name}" ({is_private} en {inst.city}).
            Contexto Tecnológico: Utilizan {lms}.
            Inteligencia Forense: Calificados como '{ai_classification}'. {budget_context}
            
            REGLAS ESTRICTAS DE REDACCIÓN:
            1. Tono humano, casual pero sumamente respetuoso y directo. Cero jerga de "estimado" o "cordial saludo".
            2. Menciona de forma sutil que notaste que usan {lms}.
            3. Ve al grano sobre ahorrarles tiempo/dinero en su gestión operativa.
            
            Genera la secuencia en formato JSON EXACTO:
            {{
                "email_1_subject": "Asunto magnético y corto (max 5 palabras, en minúsculas)",
                "email_1_body": "Cuerpo del correo (max 80 palabras). Usa saltos de línea \\n.",
                "whatsapp_1": "Mensaje de WhatsApp amable (max 30 palabras). Cierra con una pregunta abierta.",
                "email_2_bump": "Correo de seguimiento a los 3 días (max 25 palabras). Asume que responde al primer hilo."
            }}
            """
            
            from openai import AsyncOpenAI
            client = AsyncOpenAI(api_key=self.api_key)
            
            response = await client.chat.completions.create(
                model="gpt-4o-mini", # Motor ultrarrápido y económico para despliegue masivo
                messages=[
                    {"role": "system", "content": "You are a master of B2B cold outreach. Output valid JSON only."}, 
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.4, # Ligera variabilidad para evitar que todos los correos suenen idénticos
                max_tokens=400
            )
            
            return json.loads(response.choices[0].message.content)
            
        except Exception as e:
            logger.error(f"Fallo en Inferencia de Red Neuronal para {inst.name}: {str(e)}")
            return self._fallback_pitch(inst)

    def _fallback_pitch(self, inst: Institution) -> Dict[str, str]:
        """Resiliencia estructural (Circuit Breaker) en caso de caída global de la API de IA."""
        return {
            "email_1_subject": f"infraestructura en {inst.name.lower()}",
            "email_1_body": f"Hola equipo de {inst.name},\n\nEstuve revisando su ecosistema digital y noté oportunidades claras para optimizar sus tiempos de gestión administrativa.\n\n¿Tendrían espacio para una llamada de 10 minutos esta semana y les muestro los hallazgos?\n\nSaludos.",
            "whatsapp_1": f"¡Hola! 👋 Les escribo porque estuvimos revisando los sistemas de {inst.name} y tenemos un modelo operativo muy interesante. ¿Con quién puedo coordinar una charla corta?",
            "email_2_bump": "Hola de nuevo. ¿Tuvieron oportunidad de revisar mi propuesta anterior? Me encantaría conocer su opinión."
        }


# =========================================================
# 2. 📨 ORQUESTADOR DE RED Y BASE DE DATOS (DELIVERY LAYER)
# =========================================================
class OmnichannelDispatcher:
    """Maneja transacciones seguras, envío SMTP evasivo (Spam Bypass) y APIs."""
    
    def __init__(self):
        self.wa_token = getattr(settings, 'WHATSAPP_API_TOKEN', 'dummy')
        self.wa_phone_id = getattr(settings, 'WHATSAPP_PHONE_ID', 'dummy')

    @sync_to_async
    def get_or_create_contact(self, inst: Institution) -> Contact:
        """Asegura la existencia de un vector de ataque humano."""
        contact = inst.contacts.first()
        if not contact:
            contact = Contact.objects.create(institution=inst, name="Equipo Directivo", role="Dirección", email=inst.email)
        return contact

    @sync_to_async
    def log_interaction(self, inst: Institution, contact: Contact, channel: str, subject: str, body: str) -> Interaction:
        """Registro inmutable de la acción en el Data Warehouse."""
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
        [SPAM BYPASS ENGINE]
        Disparo SMTP con inyección de Tracking Pixel, Invisible Preheader 
        y Threading Seguro (Falsificación benigna de Message-ID).
        """
        try:
            pixel_url = f"{BASE_URL}/sales/track/{interaction.id}/"
            
            # Sanitización de saltos de línea antes de inyectar en el F-String
            html_body = raw_body.replace('\n', '<br>')
            
            # Extracción de la primera línea para el "Invisible Preheader" (Mejora radicalmente el Open Rate)
            preheader_text = raw_body.split('\n')[0][:100] if raw_body else "Propuesta estratégica"
            
            # Arquitectura HTML diseñada para evadir filtros bayesianos de SPAM
            tracked_html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
            </head>
            <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; font-size: 15px; color: #111827; line-height: 1.6; max-width: 600px; margin: 0; padding: 10px;">
                <div style="display: none; max-height: 0px; overflow: hidden;">
                    {preheader_text} &zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;
                </div>
                
                <div>
                    {html_body}
                </div>
                
                <img src="{pixel_url}" width="1" height="1" style="display:none; visibility:hidden; opacity:0;" alt="" />
            </body>
            </html>
            """

            email = EmailMultiAlternatives(
                subject=subject,
                body=raw_body, # Plain text fallback obligatorio para deliverability
                from_email=settings.EMAIL_HOST_USER,
                to=[contact.email],
            )
            email.attach_alternative(tracked_html, "text/html")
            
            # [THREADING HACK] Engaña a Gmail/Outlook para agrupar correos en un solo hilo
            if reply_to_id:
                email.extra_headers = {'In-Reply-To': reply_to_id, 'References': reply_to_id}

            email.send(fail_silently=False)
            
            interaction.status = Interaction.Status.SENT
            interaction.save(update_fields=['status', 'updated_at'])
            
            # Generación de la Firma Criptográfica para futuros Threadings
            return f"<{interaction.id}@sovereign.local>"
            
        except Exception as e:
            logger.error(f"❌ [SMTP FAILURE] {contact.email} | Err: {e}")
            interaction.status = "FAILED"
            interaction.save(update_fields=['status'])
            return None

    @async_exponential_backoff(retries=2, base_delay=1.5)
    async def send_whatsapp_api(self, inst: Institution, contact: Contact, message: str) -> bool:
        """Disparo a WhatsApp Cloud API con sanitización balística de números."""
        if not inst.phone:
            return False

        clean_phone = ''.join(filter(str.isdigit, inst.phone))
        if len(clean_phone) == 10: clean_phone = f"57{clean_phone}" # Auto-prefijo Colombia (Modificable)

        interaction = await self.log_interaction(inst, contact, "whatsapp", "Follow-Up Directo", message)

        if self.wa_token == 'dummy':
            logger.info(f"🟢 [WA DEV SIMULATION] -> {clean_phone}: {message[:35]}...")
            interaction.status = Interaction.Status.SENT
            await sync_to_async(interaction.save)(update_fields=['status'])
            return True

        url = f"https://graph.facebook.com/v19.0/{self.wa_phone_id}/messages"
        headers = {"Authorization": f"Bearer {self.wa_token}", "Content-Type": "application/json"}
        payload = {"messaging_product": "whatsapp", "to": clean_phone, "type": "text", "text": {"body": message}}

        async with httpx.AsyncClient() as client:
            resp = await client.post(url, headers=headers, json=payload, timeout=12.0)
            resp.raise_for_status()
            
        interaction.status = Interaction.Status.SENT
        await sync_to_async(interaction.save)(update_fields=['status'])
        logger.info(f"🟢 [WA DELIVERED] -> {clean_phone}")
        return True


# =========================================================
# 3. 🎯 CORE ENGINE: CADENCE ORCHESTRATOR (LA MÁQUINA DE GUERRA)
# =========================================================
class SovereignCadenceManager:
    """
    Controlador Estratégico de Asedio. 
    Implementa Semáforos de Concurrencia para evitar baneos de IPs y saturación de Memoria.
    """

    def __init__(self, max_concurrent_strikes: int = 10):
        self.ai = AICadenceGenerator()
        self.dispatcher = OmnichannelDispatcher()
        # [GOD TIER]: Limitador de embudo. Impide que se procesen más de N prospectos simultáneamente.
        self.semaphore = asyncio.Semaphore(max_concurrent_strikes)

    @sync_to_async
    def get_step1_targets(self, limit: int) -> List[Institution]:
        """Prospectos Calientes (Score > 60), jamás contactados y con email válido."""
        return list(Institution.objects.select_related('tech_profile', 'forensic_profile')
                    .prefetch_related('contacts')
                    .filter(lead_score__gte=60, contacted=False, is_active=True)
                    .exclude(email__isnull=True).exclude(email__exact='')[:limit])

    @sync_to_async
    def get_step2_targets(self, limit: int) -> List[Institution]:
        """Prospectos contactados hace > 3 días que no han respondido ni agendado."""
        threshold_date = timezone.now() - timedelta(days=3)
        return list(Institution.objects.select_related('tech_profile', 'forensic_profile')
                    .prefetch_related('contacts')
                    .filter(contacted=True, is_active=True)
                    .exclude(interactions__status__in=[Interaction.Status.REPLIED, Interaction.Status.MEETING])
                    .filter(interactions__created_at__lte=threshold_date)
                    .distinct()[:limit])

    @sync_to_async
    def lock_and_update_institution(self, inst: Institution, contacted_status: bool):
        """Bloqueo atómico a nivel de Base de Datos para evitar condiciones de carrera entre Celery Workers."""
        try:
            with transaction.atomic():
                # lock row para asegurar sincronía de workers
                locked_inst = Institution.objects.select_for_update(skip_locked=True).get(id=inst.id)
                locked_inst.contacted = contacted_status
                locked_inst.save(update_fields=['contacted', 'updated_at'])
        except Institution.DoesNotExist:
            logger.warning(f"⚠️ [DB LOCK] Institución {inst.id} ya está siendo procesada por otro Worker.")

    async def execute_step1_cold_strike(self, batch_size: int = 10):
        """Fase 1: Apertura y Primer Contacto (Cold Strike)."""
        targets = await self.get_step1_targets(batch_size)
        if not targets:
            logger.info("📭 [STEP 1] Scanner limpio. Sin objetivos vírgenes en cola.")
            return

        logger.info(f"🔥 [STEP 1] Autorizando Invasión Inicial a {len(targets)} prospectos...")
        
        async def process_target(inst: Institution, delay: float):
            async with self.semaphore: # Protección contra desbordamiento de I/O
                await asyncio.sleep(delay) # Human Jitter para distribución en el tiempo
                
                contact = await self.dispatcher.get_or_create_contact(inst)
                
                # Generación IA de toda la campaña
                pitch = await self.ai.build_omnichannel_pitch(inst, contact)
                
                # Registro DB y Envío SMTP
                interaction = await self.dispatcher.log_interaction(inst, contact, "email", pitch["email_1_subject"], pitch["email_1_body"])
                msg_id = await self.dispatcher.send_smtp_email(interaction, contact, pitch["email_1_subject"], pitch["email_1_body"])
                
                if msg_id:
                    await self.lock_and_update_institution(inst, True)
                    # Guardamos la artillería pesada del Step 2 en Memoria Rápida (Redis)
                    cache.set(f"cadence_{inst.id}", {
                        "wa_msg": pitch.get("whatsapp_1"),
                        "email_bump": pitch.get("email_2_bump"),
                        "reply_to": msg_id,
                        "subject": pitch["email_1_subject"]
                    }, timeout=86400 * 15) # Retención de 15 días
                    logger.info(f"✅ [STRIKE 1] Impacto confirmado en objetivo: {inst.name}.")

        # Ejecución Concurrente con Dispersión de Jittering
        tasks = [process_target(inst, i * random.uniform(1.0, 3.5)) for i, inst in enumerate(targets)]
        await asyncio.gather(*tasks)

    async def execute_step2_omni_followup(self, batch_size: int = 10):
        """Fase 2: Asedio Omnicanal (Despliegue simultáneo de WhatsApp + Email Bump)."""
        targets = await self.get_step2_targets(batch_size)
        if not targets:
            logger.info("🛌 [STEP 2] Silencio de radio. Sin seguimientos pendientes.")
            return

        logger.info(f"🔄 [STEP 2] Iniciando Asedio Omnicanal Táctico para {len(targets)} prospectos...")

        async def process_followup(inst: Institution, delay: float):
            async with self.semaphore:
                await asyncio.sleep(delay)
                contact = await self.dispatcher.get_or_create_contact(inst)
                
                cached_data = cache.get(f"cadence_{inst.id}")
                if not cached_data:
                    # Self-Healing: Si Redis perdió los datos, la IA los re-sintetiza en vivo.
                    logger.warning(f"⚠️ [RE-SYNC] Pérdida de caché para {inst.name}. Re-invocando Neural Engine...")
                    pitch = await self.ai.build_omnichannel_pitch(inst, contact)
                    cached_data = {"wa_msg": pitch.get("whatsapp_1"), "email_bump": pitch.get("email_2_bump"), "reply_to": None, "subject": pitch["email_1_subject"]}

                logger.info(f"💥 [OMNI-STRIKE] Detonando ataque sincronizado sobre {inst.name}...")
                
                # Lanzamos Email y WhatsApp de forma 100% paralela
                interaction = await self.dispatcher.log_interaction(inst, contact, "email", f"Re: {cached_data['subject']}", cached_data['email_bump'])
                
                await asyncio.gather(
                    self.dispatcher.send_whatsapp_api(inst, contact, cached_data['wa_msg']),
                    self.dispatcher.send_smtp_email(interaction, contact, f"Re: {cached_data['subject']}", cached_data['email_bump'], reply_to_id=cached_data['reply_to']),
                    return_exceptions=True # Evita que un fallo en WA detenga el Email
                )
                
                # Purga de memoria post-asedio
                cache.delete(f"cadence_{inst.id}")

        tasks = [process_followup(inst, i * random.uniform(2.0, 5.0)) for i, inst in enumerate(targets)]
        await asyncio.gather(*tasks)


# =========================================================
# 🚀 PUNTOS DE ENTRADA PÚBLICOS (CLI / CELERY ORCHESTRATOR)
# =========================================================
def run_autonomous_campaign(batch_size: int = 20, max_concurrency: int = 8):
    """
    Gateway sincrónico para cronjobs y workers de Celery.
    Orquesta toda la línea de tiempo asíncrona.
    """
    logger.info("=" * 60)
    logger.info("🔥 INICIALIZANDO OMNICHANNEL CADENCE ENGINE V4.1 🔥")
    logger.info("=" * 60)
    
    manager = SovereignCadenceManager(max_concurrent_strikes=max_concurrency)
    
    async def _orchestrate():
        # Despliegue secuencial de Fases
        await manager.execute_step1_cold_strike(batch_size=batch_size)
        await asyncio.sleep(2.0) # Buffer térmico
        await manager.execute_step2_omni_followup(batch_size=batch_size)
        
    try:
        asyncio.run(_orchestrate())
        logger.info("🏁 Operaciones tácticas finalizadas. Pasando a Standby.")
    except KeyboardInterrupt:
        logger.warning("🛑 [ABORT] Interrupción manual detectada. Frenando ataques de inmediato.")