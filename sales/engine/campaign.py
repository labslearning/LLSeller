import os
import random
import logging
import asyncio
from typing import List, Tuple
from django.core.mail import EmailMultiAlternatives
from django.conf import settings
from django.db import transaction
from asgiref.sync import sync_to_async
from sales.models import Institution, Contact, Interaction
from sales.engine.ai import generate_b2b_email

# Configuración de Logging Estructurado
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

# URL Base Dinámica (Soporta entornos locales y de producción)
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip('/')

# [NIVEL DIOS 1]: Interfaz Segura con el ORM de Django
@sync_to_async
def get_target_institutions(batch_size: int) -> List[Institution]:
    """Obtiene instituciones vírgenes con correo, usando evaluación forzada (list)."""
    # Usamos select_related para evitar el problema N+1 al buscar contactos luego
    qs = Institution.objects.prefetch_related('contacts').filter(
        contacted=False
    ).exclude(email__isnull=True).exclude(email__exact='')[:batch_size]
    return list(qs)

@sync_to_async
def get_or_create_contact(inst: Institution) -> Contact:
    """Asegura la integridad referencial creando un contacto genérico si no existe."""
    contact = inst.contacts.first()
    if not contact:
        contact = Contact.objects.create(
            institution=inst,
            name="Equipo Directivo",
            role="Dirección",
            email=inst.email
        )
        logger.debug(f"🛠️ Contacto maestro creado para {inst.name}")
    return contact

@sync_to_async
def save_interaction_and_update_inst(inst: Institution, contact: Contact, email_body: str) -> Interaction:
    """
    [NIVEL DIOS 2]: Transacciones Atómicas.
    Asegura que la interacción se cree y la institución se actualice todo de una vez o nada.
    """
    with transaction.atomic():
        interaction = Interaction.objects.create(
            institution=inst,
            contact=contact,
            subject=f"Propuesta tecnológica para {inst.name}",
            message_sent=email_body,
            status=Interaction.Status.NEW
        )
        inst.contacted = True
        inst.save(update_fields=['contacted', 'updated_at'])
    return interaction

@sync_to_async
def rollback_interaction(interaction: Interaction, inst: Institution):
    """Si el envío SMTP falla, revertimos los cambios para no perder el lead."""
    with transaction.atomic():
        interaction.delete()
        inst.contacted = False
        inst.save(update_fields=['contacted', 'updated_at'])

@sync_to_async
def send_email_sync(interaction: Interaction, contact: Contact, html_content: str, email_body: str):
    """Envío SMTP síncrono aislado."""
    msg = EmailMultiAlternatives(
        subject=interaction.subject,
        body=email_body, 
        from_email=settings.EMAIL_HOST_USER,
        to=[contact.email],
    )
    msg.attach_alternative(html_content, "text/html")
    msg.send(fail_silently=False)
    
    # Sellado de estado
    interaction.status = Interaction.Status.SENT
    interaction.save(update_fields=['status', 'updated_at'])

# [NIVEL DIOS 3]: El Pipeline de Procesamiento Asíncrono
async def process_single_lead(inst: Institution) -> bool:
    """Procesa el ciclo de vida completo de un prospecto de principio a fin."""
    try:
        contact = await get_or_create_contact(inst)
        
        logger.info(f"⚙️ [GEN] Solicitando pitch neural para {inst.name}...")
        # Generación IA (No bloqueante)
        email_body = await generate_b2b_email(contact.name, contact.role, inst.name)
        
        if not email_body:
            logger.error(f"[X] Fallo generación copy para {inst.name}.")
            return False

        # Preparación en BD
        interaction = await save_interaction_and_update_inst(inst, contact, email_body)
        tracking_url = f"{BASE_URL}/s/track/px/{interaction.id}.gif"
        
        # Ensamblaje Stealth HTML
        html_content = f"""
        <div style="font-family: Arial, sans-serif; font-size: 14px; color: #222;">
            <p style="white-space: pre-wrap;">{email_body}</p>
        </div>
        <img src="{tracking_url}" width="1" height="1" style="display:none;" alt="" />
        """

        # Disparo (Hacemos que el envío síncrono corra en un thread pool de asgiref)
        logger.info(f"🚀 [SMTP] Disparando a {contact.email} ({inst.name})...")
        await send_email_sync(interaction, contact, html_content, email_body)
        
        logger.info(f"✅ [IMPACTO] Entregado a {inst.name} | Tracking ID: {interaction.id}")
        return True

    except Exception as e:
        logger.error(f"[X] [FUEGO AMIGO] Fallo procesando {inst.name}: {e}")
        # Rollback seguro
        if 'interaction' in locals():
            await rollback_interaction(interaction, inst)
        return False

async def async_campaign_runner(batch_size: int):
    """Orquestador maestro que lanza las tareas."""
    logger.info(f"🔥 [CORE ENGINE] Iniciando Campaña B2B Asíncrona (Batch: {batch_size})...")
    
    institutions = await get_target_institutions(batch_size)
    
    if not institutions:
        logger.info("📭 No hay prospectos válidos y vírgenes en la base de datos.")
        return

    # [NIVEL DIOS 4]: Ejecución Concurrente con Jitter
    # En lugar de procesar uno por uno, lanzamos todos los leads como tareas asíncronas
    # con un pequeño retraso (jitter) entre cada inicio para no sobrecargar la API de OpenAI
    # ni activar filtros anti-spam de golpe.
    
    tasks = []
    for i, inst in enumerate(institutions):
        # Human Jitter: Escalonamos el inicio de las tareas
        delay = i * random.uniform(1.5, 3.5)
        
        async def delayed_process(institution=inst, wait_time=delay):
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            return await process_single_lead(institution)
            
        tasks.append(asyncio.create_task(delayed_process()))

    # Esperamos a que todas las tareas (correos) terminen
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    success_count = sum(1 for r in results if r is True)
    logger.info(f"🏁 [REPORTE] Campaña finalizada. Impactos exitosos: {success_count}/{len(institutions)}")

def run_outreach_campaign(batch_size=5):
    """Punto de entrada síncrono para ejecutar desde la terminal o Celery."""
    asyncio.run(async_campaign_runner(batch_size))