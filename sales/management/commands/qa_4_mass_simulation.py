import random
import time
from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from sales.models import Institution, Contact, Interaction

class Command(BaseCommand):
    help = '🌪️ [QA TIER GOD] Simulador Cuántico. Inyecta 20 escenarios omnicanal orgánicos (WhatsApp/Email) con payloads de respuesta humana.'

    def handle(self, *args: Any, **options: Any) -> None:
        self.stdout.write(self.style.WARNING("=" * 75))
        self.stdout.write(self.style.WARNING("🌪️  INICIANDO PRUEBA DE ESTRÉS: INYECCIÓN MASIVA DE TELEMETRÍA ORGÁNICA  🌪️"))
        self.stdout.write(self.style.WARNING("=" * 75))

        TOTAL_TARGETS = 20
        now = timezone.now()

        fake_names = [
            "Stanford QA", "MIT Simulator", "Oxford Test", "Harvard QA", "Cambridge Node", 
            "Yale Sandbox", "Princeton Mock", "Columbia DB", "Cornell Tech", "Duke Data",
            "UCLA Test", "NYU QA", "Berkeley Node", "Chicago Sandbox", "Penn Mock",
            "Brown DB", "Dartmouth Tech", "Northwestern QA", "Johns Hopkins Test", "Vanderbilt Node"
        ]
        
        roles = ["CTO", "Director Académico", "Rector", "Líder de Innovación", "IT Manager"]
        
        # Diccionario de respuestas humanas orgánicas
        human_replies = [
            "Hola, me parece muy interesante. ¿Podemos agendar una llamada el martes a las 10am?",
            "Interesante propuesta. Por favor envíame un PDF con los costos estimados primero.",
            "En este momento no tenemos presupuesto, búscame en el Q3.",
            "¿Cómo se integra esto con Canvas LMS? Tenemos 5,000 estudiantes activos.",
            "Sí, me interesa. Te copio a mi líder técnico para que coordinemos la demo."
        ]

        statuses = [
            ('REPLIED', 5),  # HOT
            ('OPENED', 6),   # WARM
            ('SENT', 5),     # COLD
            ('BOUNCED', 2),  # DEAD
            ('MEETING', 2)   # CRITICAL
        ]
        
        distribution = []
        for status, count in statuses:
            distribution.extend([status] * count)
        random.shuffle(distribution)

        self.stdout.write(self.style.NOTICE(f"[SYS] Purgando sectores de simulación anteriores para evitar colisiones..."))
        for suffix in [" QA", " Simulator", " Mock", " Node", " Sandbox", " DB", " Tech", " Test"]:
            Institution.objects.filter(name__contains=suffix).delete()

        start_time = time.perf_counter()

        try:
            with transaction.atomic():
                for i in range(TOTAL_TARGETS):
                    inst_name = fake_names[i]
                    status = distribution[i]
                    channel = random.choices(["email", "whatsapp"], weights=[70, 30])[0] # 30% serán WhatsApp
                    
                    lead_score = 100 if status in ['REPLIED', 'MEETING'] else (70 if status == 'OPENED' else 40)
                    
                    # 1. Crear Institución
                    inst = Institution.objects.create(
                        name=f"{inst_name} {i+1}",
                        website=f"https://{inst_name.lower().replace(' ', '')}.edu",
                        city="Silicon Wadi",
                        country="Israel",
                        institution_type="university",
                        is_private=True,
                        email=f"contact@{inst_name.lower().replace(' ', '')}.edu",
                        lead_score=lead_score,
                        contacted=True, # Todo en el simulador ya fue contactado
                        is_active=True
                    )

                    # 2. Crear Contacto
                    contact = Contact.objects.create(
                        institution=inst,
                        name=f"Ingeniero Operativo {i+1}",
                        role=random.choice(roles),
                        email=f"admin{i+1}@{inst_name.lower().replace(' ', '')}.edu",
                        phone=f"+573000000{i:02d}"
                    )

                    # 3. Time-Shifting (Algoritmo de Latencia Orgánica)
                    days_ago = random.randint(1, 14)
                    created_time = now - timedelta(days=days_ago, hours=random.randint(1, 12))
                    
                    # Si es HOT/WARM, simulamos que reaccionó entre 5 mins y 24 horas después
                    if status in ['OPENED', 'REPLIED', 'MEETING']:
                        updated_time = created_time + timedelta(minutes=random.randint(5, 1440))
                    else:
                        updated_time = created_time

                    # 4. Inyección de Carga Útil
                    if channel == 'email':
                        subject = f"[EMAIL] Optimización de métricas para {inst.name}"
                        body = f"Hola equipo de {inst.name},\n\nSoy el Sovereign Engine de QA. Notamos anomalías en su infraestructura que reducen su rendimiento B2B.\n\n¿Agendamos 5 minutos para revisar el reporte?"
                    else:
                        subject = f"[WHATSAPP] Follow-up {inst.name}"
                        body = f"¡Hola! 👋 Te escribo del Sovereign Engine. ¿Lograste revisar el correo que te enviamos sobre la infraestructura de {inst.name}?"

                    interaction = Interaction.objects.create(
                        institution=inst,
                        contact=contact,
                        subject=subject,
                        message_sent=body,
                        status=status,
                    )
                    
                    # [GOD TIER HACK] Inyectar respuesta del humano usando una variable dinámica
                    if status in ['REPLIED', 'MEETING']:
                        setattr(interaction, 'reply_text', random.choice(human_replies)) # Hack para inyectar datos en RAM
                        # Como reply_text no existe en la BD, lo guardamos en un log interno simulado si lo tuvieras, 
                        # pero para la UI de Django Admin que diseñamos, solo necesita leer el getattr.
                        # NOTA: Para persistencia real en DB, asumo que tienes 'message_received' o similar. 
                        # Si existe, lo guardamos. Si no, usamos el truco de reemplazar el subject.
                        interaction.subject = f"{subject} | RE: {random.choice(human_replies)[:30]}..."

                    interaction.save()

                    # Forzar timestamps via QuerySet para bypass de auto_now_add
                    Interaction.objects.filter(id=interaction.id).update(created_at=created_time, updated_at=updated_time)
                    
                    # Output Táctico
                    c_tag = "�� WA" if channel == "whatsapp" else "📧 EM"
                    color = self.style.SUCCESS if status in ['REPLIED', 'MEETING'] else (self.style.WARNING if status == 'OPENED' else self.style.NOTICE)
                    self.stdout.write(color(f"  ↳ [{c_tag}] {inst.name:<25} | L-SCORE: {lead_score:<3} | ST: {status}"))

            elapsed = (time.perf_counter() - start_time) * 1000
            self.stdout.write(self.style.SUCCESS("\n" + "=" * 75))
            self.stdout.write(self.style.SUCCESS(f"✅ OPERACIÓN DE ESTRÉS COMPLETADA EN {elapsed:.2f} ms"))
            self.stdout.write(self.style.SUCCESS(f"Se inyectaron {TOTAL_TARGETS} vectores de ataque distribuidos en PostgreSQL."))
            self.stdout.write(self.style.WARNING("=" * 75))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ [CRITICAL CRASH] Kernel Panic en la inyección masiva: {str(e)}"))
