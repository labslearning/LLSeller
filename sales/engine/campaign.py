import os
import json
import random
import logging
import asyncio
from typing import List, Dict, Optional
from datetime import timedelta

import httpx
from django.utils import timezone
from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.db import transaction
from django.db.models import Q
from django.core.cache import cache
from asgiref.sync import sync_to_async

# Importaciones de arquitectura local
from sales.models import Institution, Contact, Interaction

# =========================================================
# CONFIGURACIÓN TIER GOD: TELEMETRÍA Y OBSERVABILIDAD
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] [OmniEngine] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.Omnichannel")

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip('/')


# =========================================================
# 1. MOTOR DE HIPER-PERSONALIZACIÓN CON IA (SILICON VALLEY)
# =========================================================
class AICadenceGenerator:
    """Generador de Secuencias B2B. Crea todo el embudo de un prospecto en una sola inferencia."""
    
    def __init__(self):
        self.api_key = getattr(settings, 'OPENAI_API_KEY', None)

    async def build_omnichannel_pitch(self, inst: Institution, contact: Contact) -> Dict[str, str]:
        if not self.api_key:
            return self._fallback_pitch(inst)

        try:
            # Extracción segura de datos relacionales
            tech_profile = await sync_to_async(lambda: getattr(inst, 'tech_profile', None))()
            lms = tech_profile.lms_provider.upper() if tech_profile and tech_profile.lms_provider else "métodos tradicionales"
            is_private = "colegio privado" if inst.is_private else "institución pública"
            
            prompt = f"""
            Eres un SDR de élite en Silicon Valley cerrando negocios B2B.
            Objetivo: {contact.name} (Rol: {contact.role}) en {inst.name} ({is_private} en {inst.city}).
            Contexto Técnico: Sabemos que usan {lms} como plataforma.
            
            Genera una secuencia de ventas en formato JSON estricto:
            {{
                "email_1_subject": "Asunto magnético (max 5 palabras, en minúsculas)",
                "email_1_body": "Correo inicial corto (max 80 palabras). Menciona que notaste que usan {lms} y tienes una propuesta para optimizarlo. Tono casual, directo y sin formalismos arcaicos.",
                "whatsapp_1": "Mensaje de WhatsApp (max 30 palabras) amable y directo, pidiendo 5 min. Usa emojis discretos.",
                "email_2_bump": "Correo de seguimiento (max 25 palabras). Asume que este correo es una respuesta al Email 1. Ej: 'Hola {contact.name}, ¿pudiste ver el correo anterior? Me encantaría conectar.'"
            }}
            """
            
            from openai import AsyncOpenAI
            client = AsyncOpenAI(api_key=self.api_key)
            response = await client.chat.completions.create(
                model="gpt-4o-mini", # Optimizado para velocidad y costo
                messages=[{"role": "system", "content": "Devuelve solo JSON válido."}, {"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            return json.loads(response.choices[0].message.content)
            
        except Exception as e:
            logger.error(f"Fallo en Generación IA para {inst.name}: {str(e)}")
            return self._fallback_pitch(inst)

    def _fallback_pitch(self, inst: Institution) -> Dict[str, str]:
        """Resiliencia estructural en caso de caída de OpenAI."""
        return {
            "email_1_subject": f"iniciativa tecnológica - {inst.name}",
            "email_1_body": f"Hola equipo de {inst.name},<br><br>Estuve analizando su ecosistema digital y noté que podríamos optimizar significativamente su flujo académico y administrativo.<br><br>¿Tienen 10 minutos esta semana para compartirles un reporte sin costo?<br><br>Saludos.",
            "whatsapp_1": f"¡Hola! 👋 Les escribo porque estuvimos revisando la infraestructura de {inst.name} y tenemos un modelo que les ahorraría horas de trabajo. ¿Con quién puedo coordinar una llamada corta?",
            "email_2_bump": "Hola de nuevo. ¿Tuvieron oportunidad de revisar mi propuesta anterior? Me encantaría conectar."
        }


# =========================================================
# 2. ORQUESTADOR DE RED Y BASE DE DATOS (TEL AVIV & SHENZHEN)
# =========================================================
class OmnichannelDispatcher:
    """Maneja transacciones atómicas, envío SMTP, APIs HTTP y rastreo Forense."""
    
    def __init__(self):
        self.wa_token = getattr(settings, 'WHATSAPP_API_TOKEN', 'dummy')
        self.wa_phone_id = getattr(settings, 'WHATSAPP_PHONE_ID', 'dummy')

    @sync_to_async
    def get_or_create_contact(self, inst: Institution) -> Contact:
        """Asegura la existencia de un tomador de decisiones."""
        contact = inst.contacts.first()
        if not contact:
            contact = Contact.objects.create(institution=inst, name="Equipo Directivo", role="Dirección", email=inst.email)
        return contact

    @sync_to_async
    def log_interaction(self, inst: Institution, contact: Contact, channel: str, subject: str, body: str) -> Interaction:
        """Registro inmutable de la acción de ventas."""
        return Interaction.objects.create(
            institution=inst,
            contact=contact,
            subject=f"[{channel.upper()}] {subject}",
            message_sent=body,
            status=Interaction.Status.NEW
        )

    @sync_to_async
    def send_smtp_email(self, interaction: Interaction, contact: Contact, subject: str, html_body: str, reply_to_id: str = None) -> Optional[str]:
        """Disparo SMTP con inyección de Tracking Pixel y Threading."""
        try:
            pixel_url = f"{BASE_URL}/s/track/px/{interaction.id}.gif"
            tracked_html = f"""
            <div style="font-family: Arial, sans-serif; font-size: 14px; color: #1f2937;">
                {html_body.replace('\n', '<br>')}
            </div>
            <img src="{pixel_url}" width="1" height="1" style="display:none;" alt="" />
            """

            email = EmailMultiAlternatives(
                subject=subject,
                body=html_body, 
                from_email=settings.EMAIL_HOST_USER,
                to=[contact.email],
            )
            email.attach_alternative(tracked_html, "text/html")
            
            # [THREADING HACK] Engaña a Gmail para agrupar correos
            if reply_to_id:
                email.extra_headers = {'In-Reply-To': reply_to_id, 'References': reply_to_id}

            email.send(fail_silently=False)
            
            interaction.status = Interaction.Status.SENT
            interaction.save(update_fields=['status', 'updated_at'])
            
            # Simulamos un Message-ID para usar en futuros seguimientos
            return f"<{interaction.id}@sovereign.local>"
            
        except Exception as e:
            logger.error(f"❌ [SMTP ERROR] {contact.email}: {e}")
            interaction.status = "FAILED"
            interaction.save(update_fields=['status'])
            return None

    async def send_whatsapp_api(self, inst: Institution, contact: Contact, message: str) -> bool:
        """Disparo a WhatsApp Cloud API con sanitización de números."""
        if not inst.phone:
            return False

        clean_phone = ''.join(filter(str.isdigit, inst.phone))
        if len(clean_phone) == 10: clean_phone = f"57{clean_phone}" # Auto-prefijo Colombia

        interaction = await self.log_interaction(inst, contact, "whatsapp", "Follow-Up Directo", message)

        if self.wa_token == 'dummy':
            logger.info(f"🟢 [WA DEV SIMULADO] -> {clean_phone}: {message[:30]}...")
            interaction.status = Interaction.Status.SENT
            await sync_to_async(interaction.save)(update_fields=['status'])
            return True

        url = f"https://graph.facebook.com/v19.0/{self.wa_phone_id}/messages"
        headers = {"Authorization": f"Bearer {self.wa_token}", "Content-Type": "application/json"}
        payload = {"messaging_product": "whatsapp", "to": clean_phone, "type": "text", "text": {"body": message}}

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(url, headers=headers, json=payload, timeout=10.0)
                resp.raise_for_status()
                
            interaction.status = Interaction.Status.SENT
            await sync_to_async(interaction.save)(update_fields=['status'])
            logger.info(f"🟢 [WA ENVIADO] -> {clean_phone}")
            return True
        except Exception as e:
            logger.error(f"❌ [WA ERROR] {clean_phone}: {e}")
            interaction.status = "FAILED"
            await sync_to_async(interaction.save)(update_fields=['status'])
            return False


# =========================================================
# 3. CORE ENGINE: CADENCE ORCHESTRATOR (LA MÁQUINA DE GUERRA)
# =========================================================
class SovereignCadenceManager:
    """Controla los Tiers de ataque. Día 1 (Email) -> Día 4 (WhatsApp + Email Bump)."""

    def __init__(self):
        self.ai = AICadenceGenerator()
        self.dispatcher = OmnichannelDispatcher()

    @sync_to_async
    def get_step1_targets(self, limit: int) -> List[Institution]:
        """Prospectos Calientes (Score > 60), jamás contactados y con email válido."""
        return list(Institution.objects.prefetch_related('contacts').filter(
            lead_score__gte=60, contacted=False, is_active=True
        ).exclude(email__isnull=True).exclude(email__exact='')[:limit])

    @sync_to_async
    def get_step2_targets(self, limit: int) -> List[Institution]:
        """Prospectos contactados hace > 3 días que no han respondido ni agendado."""
        threshold_date = timezone.now() - timedelta(days=3)
        return list(Institution.objects.prefetch_related('contacts').filter(
            contacted=True, is_active=True
        ).exclude(
            interactions__status__in=[Interaction.Status.REPLIED, Interaction.Status.MEETING]
        ).filter(
            interactions__created_at__lte=threshold_date
        ).distinct()[:limit])

    @sync_to_async
    def lock_and_update_institution(self, inst: Institution, contacted_status: bool):
        """Bloqueo atómico a nivel de Base de Datos para evitar condiciones de carrera."""
        with transaction.atomic():
            locked_inst = Institution.objects.select_for_update().get(id=inst.id)
            locked_inst.contacted = contacted_status
            locked_inst.save(update_fields=['contacted', 'updated_at'])

    async def execute_step1_cold_strike(self, batch_size: int = 10):
        """Fase 1: Apertura."""
        targets = await self.get_step1_targets(batch_size)
        if not targets:
            logger.info("📭 [STEP 1] Sin objetivos vírgenes.")
            return

        logger.info(f"🔥 [STEP 1] Lanzando ataque inicial a {len(targets)} prospectos...")
        
        async def process_target(inst: Institution, delay: float):
            await asyncio.sleep(delay) # Human Jitter
            contact = await self.dispatcher.get_or_create_contact(inst)
            
            # Generación IA de toda la campaña
            pitch = await self.ai.build_omnichannel_pitch(inst, contact)
            
            # Registro DB y Envío SMTP
            interaction = await self.dispatcher.log_interaction(inst, contact, "email", pitch["email_1_subject"], pitch["email_1_body"])
            msg_id = await self.dispatcher.send_smtp_email(interaction, contact, pitch["email_1_subject"], pitch["email_1_body"])
            
            if msg_id:
                await self.lock_and_update_institution(inst, True)
                # Guardamos los textos del Step 2 en Redis para no repagar OpenAI
                cache.set(f"cadence_{inst.id}", {
                    "wa_msg": pitch.get("whatsapp_1"),
                    "email_bump": pitch.get("email_2_bump"),
                    "reply_to": msg_id,
                    "subject": pitch["email_1_subject"]
                }, timeout=86400 * 15) # 15 días de vida
                logger.info(f"✅ [STRIKE 1] Impacto confirmado en {inst.name}.")

        # Ejecución Concurrente con Jitter (Escalonamiento)
        tasks = [process_target(inst, i * random.uniform(2.0, 5.0)) for i, inst in enumerate(targets)]
        await asyncio.gather(*tasks)

    async def execute_step2_omni_followup(self, batch_size: int = 10):
        """Fase 2: Asedio Omnicanal (WhatsApp + Correo sobre el mismo Hilo)."""
        targets = await self.get_step2_targets(batch_size)
        if not targets:
            logger.info("🛌 [STEP 2] Sin seguimientos pendientes.")
            return

        logger.info(f"🔄 [STEP 2] Iniciando Asedio Omnicanal para {len(targets)} prospectos...")

        async def process_followup(inst: Institution, delay: float):
            await asyncio.sleep(delay)
            contact = await self.dispatcher.get_or_create_contact(inst)
            
            cached_data = cache.get(f"cadence_{inst.id}")
            if not cached_data:
                # Si Redis limpió caché, forzamos generación IA de nuevo
                pitch = await self.ai.build_omnichannel_pitch(inst, contact)
                cached_data = {"wa_msg": pitch.get("whatsapp_1"), "email_bump": pitch.get("email_2_bump"), "reply_to": None, "subject": pitch["email_1_subject"]}

            logger.info(f"💥 [OMNI-STRIKE] Detonando ataque múltiple sobre {inst.name}...")
            
            # Lanzamos WhatsApp y Email de forma SIMULTÁNEA
            interaction = await self.dispatcher.log_interaction(inst, contact, "email", f"Re: {cached_data['subject']}", cached_data['email_bump'])
            
            await asyncio.gather(
                self.dispatcher.send_whatsapp_api(inst, contact, cached_data['wa_msg']),
                self.dispatcher.send_smtp_email(interaction, contact, f"Re: {cached_data['subject']}", cached_data['email_bump'], reply_to_id=cached_data['reply_to'])
            )
            
            # Borramos el caché porque ya agotamos la cadencia principal
            cache.delete(f"cadence_{inst.id}")

        tasks = [process_followup(inst, i * random.uniform(3.0, 7.0)) for i, inst in enumerate(targets)]
        await asyncio.gather(*tasks)


# =========================================================
# PUNTOS DE ENTRADA PÚBLICOS (CLI / CELERY BEAT)
# =========================================================
def run_autonomous_campaign(batch_size=10):
    """
    Controlador maestro síncrono.
    Llamar este método desde un cronjob diario o desde Django Shell.
    """
    logger.info("==================================================")
    logger.info("🔥 INICIALIZANDO OMNICHANNEL CADENCE ENGINE 🔥")
    logger.info("==================================================")
    
    manager = SovereignCadenceManager()
    
    async def _orchestrate():
        await manager.execute_step1_cold_strike(batch_size=batch_size)
        await manager.execute_step2_omni_followup(batch_size=batch_size)
        
    asyncio.run(_orchestrate())
    logger.info("🏁 Operaciones tácticas finalizadas. Pasando a Standby.")