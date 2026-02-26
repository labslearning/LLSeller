import sys
import uuid
import random
import time
from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand
from django.db import transaction, DatabaseError
from django.utils import timezone
from django.db.models import Q

from sales.models import Institution, Contact, Interaction

class Command(BaseCommand):
    help = '🚀 [QA TIER GOD] Motor Cuántico de Inyección B2B. Telemetría Orgánica, Tolerancia a Fallos y Diseño Orientado al Dominio.'

    # --- CONSTANTES DE CONFIGURACIÓN ---
    TOTAL_TARGETS = 20
    
    FAKE_NAMES = [
        "Stanford QA", "MIT Simulator", "Oxford Test", "Harvard QA", "Cambridge Node", 
        "Yale Sandbox", "Princeton Mock", "Columbia DB", "Cornell Tech", "Duke Data",
        "UCLA Test", "NYU QA", "Berkeley Node", "Chicago Sandbox", "Penn Mock",
        "Brown DB", "Dartmouth Tech", "Northwestern QA", "Johns Hopkins Test", "Vanderbilt Node"
    ]
    
    ROLES = ["CTO", "Director Académico", "Rector", "Líder de Innovación", "IT Manager", "VP of Engineering"]
    
    HUMAN_REPLIES = [
        "Hola, me parece muy interesante. ¿Podemos agendar una llamada el martes a las 10am?",
        "Interesante propuesta. Por favor envíame un PDF con los costos estimados primero.",
        "En este momento no tenemos presupuesto, búscame en el Q3.",
        "¿Cómo se integra esto con Canvas LMS? Tenemos 5,000 estudiantes activos.",
        "Sí, me interesa. Te copio a mi líder técnico para que coordinemos la demo.",
        "No gracias, ya usamos una solución in-house.",
        "¿Tienen integración nativa con Blackboard? Si es así, hablemos mañana."
    ]

    USER_AGENTS = [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0",
        "WhatsApp/2.23.25.76 A"
    ]

    def handle(self, *args: Any, **options: Any) -> None:
        self.stdout.write(self.style.WARNING("╔" + "═" * 85 + "╗"))
        self.stdout.write(self.style.WARNING("║ ") + self.style.SUCCESS("🚀 INICIANDO MOTOR CUÁNTICO DE ESTRÉS: INYECCIÓN MASIVA DE TELEMETRÍA ORGÁNICA") + self.style.WARNING("  ║"))
        self.stdout.write(self.style.WARNING("╚" + "═" * 85 + "╝"))

        now = timezone.now()
        success_count = 0
        failed_count = 0

        # Distribución de probabilidad de estados (Curva de embudo de ventas)
        statuses = [('REPLIED', 4), ('OPENED', 6), ('SENT', 6), ('BOUNCED', 2), ('MEETING', 2)]
        distribution = [status for status, count in statuses for _ in range(count)]
        random.shuffle(distribution)

        # ==========================================
        # 1. PURGA QUIRÚRGICA (CLEANUP)
        # ==========================================
        self.stdout.write(self.style.NOTICE("\n[SYS] Ejecutando algoritmo de limpieza para evitar colisiones espectrales..."))
        
        cleanup_query = Q()
        suffixes = ["QA", "Simulator", "Mock", "Node", "Sandbox", "DB", "Tech", "Test", "Data"]
        for suffix in suffixes:
            cleanup_query |= Q(name__icontains=suffix)
        
        deleted_count, _ = Institution.objects.filter(cleanup_query).delete()
        self.stdout.write(self.style.SUCCESS(f"🧹 Sector purgado exitosamente: {deleted_count} registros fantasmas eliminados.\n"))

        start_time = time.perf_counter()

        # ==========================================
        # 2. INYECCIÓN ATÓMICA CON SAVEPOINTS
        # ==========================================
        self.stdout.write(self.style.WARNING("┌─[ PIPELINE DE INYECCIÓN EN TIEMPO REAL ]" + "─" * 46 + "┐"))

        for i in range(self.TOTAL_TARGETS):
            inst_name = self.FAKE_NAMES[i]
            target_status = distribution[i]
            channel_choice = Interaction.Channel.WHATSAPP if random.random() < 0.3 else Interaction.Channel.EMAIL
            lead_score = 100 if target_status in ['REPLIED', 'MEETING'] else (70 if target_status == 'OPENED' else 40)
            
            # Hash único para garantizar 0% colisiones en unique_constraints (God Tier Fix)
            crypto_hash = uuid.uuid4().hex[:6]
            base_domain = f"{inst_name.lower().replace(' ', '')}-{crypto_hash}"
            
            # --- PROTECCIÓN POR SAVEPOINT ---
            # Si un registro falla, no rompe toda la simulación. Aisla el error.
            try:
                with transaction.atomic():
                    # 1. Instanciación B2B (Master Node)
                    inst = Institution.objects.create(
                        name=f"{inst_name} {crypto_hash.upper()}",
                        website=f"https://{base_domain}.edu",
                        city=random.choice(["Silicon Wadi", "Silicon Valley", "London", "Bangalore"]),
                        country=random.choice(["Israel", "USA", "UK", "India"]),
                        institution_type="university",
                        is_private=True,
                        email=f"ceo@{base_domain}.edu",
                        lead_score=lead_score,
                        contacted=True,
                        is_active=True
                    )

                    # 2. Creación del Tomador de Decisiones (Contact Node)
                    contact = Contact.objects.create(
                        institution=inst,
                        name=f"Ingeniero Operativo {crypto_hash.upper()}",
                        role=random.choice(self.ROLES),
                        email=f"admin-{crypto_hash}@{base_domain}.edu",
                        phone=f"+{random.randint(10000000000, 99999999999)}"
                    )

                    # 3. Time-Shifting Estocástico (Latencia Humana)
                    days_ago = random.randint(1, 14)
                    created_time = now - timedelta(days=days_ago, hours=random.randint(1, 12))
                    updated_time = created_time + timedelta(minutes=random.randint(2, 2880)) if target_status != 'SENT' else created_time

                    # 4. Origen de la Interacción (Nace en estado legal SENT)
                    interaction = Interaction.objects.create(
                        institution=inst,
                        contact=contact,
                        channel=channel_choice,
                        subject=f"Propuesta Estratégica para {inst.name}",
                        message_sent=f"Hola equipo de {inst_name},\n\nSoy el Sovereign Engine. Adjunto propuesta B2B.",
                        status=Interaction.Status.SENT, 
                    )
                    
                    # 5. Domain-Driven State Machine (Evolución Orgánica)
                    if target_status in ['OPENED', 'REPLIED', 'MEETING']:
                        ip_fake = f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
                        interaction.register_open(
                            ip_address=ip_fake, 
                            user_agent=random.choice(self.USER_AGENTS)
                        )

                    if target_status in ['REPLIED', 'MEETING']:
                        interaction.register_inbound_reply(
                            raw_payload=random.choice(self.HUMAN_REPLIES),
                            intent="POSITIVE" if target_status == 'MEETING' else random.choice(["NEUTRAL", "POSITIVE", "NEGATIVE"]),
                            sentiment_score=round(random.uniform(0.10, 0.99), 2)
                        )
                        # Mutación cosmética del asunto para la UI
                        interaction.subject = f"RE: {interaction.subject}"
                        interaction.save(update_fields=['subject'])
                        
                    if target_status == 'MEETING':
                        interaction.status = Interaction.Status.MEETING
                        interaction.meeting_date = updated_time + timedelta(days=random.randint(1, 10))
                        interaction.save(update_fields=['status', 'meeting_date'])
                        
                    if target_status == 'BOUNCED':
                        interaction.status = Interaction.Status.BOUNCED
                        interaction.save(update_fields=['status'])

                    # 6. Sobreescritura de Cuarta Dimensión (Timestamps)
                    Interaction.objects.filter(id=interaction.id).update(created_at=created_time, updated_at=updated_time)
                    
                    # Log Táctico Formateado
                    c_tag = "🟢 WA" if channel_choice == Interaction.Channel.WHATSAPP else "📧 EM"
                    status_colored = self.style.SUCCESS(f"{target_status:<7}") if target_status in ['REPLIED', 'MEETING'] else (self.style.WARNING(f"{target_status:<7}") if target_status == 'OPENED' else self.style.NOTICE(f"{target_status:<7}"))
                    
                    self.stdout.write(f"│  ↳ [{c_tag}] {inst.name:<30} │ SCORE: {str(lead_score).zfill(3)} │ ST: {status_colored} │")
                    success_count += 1

            except DatabaseError as e:
                failed_count += 1
                self.stdout.write(self.style.ERROR(f"│  ❌ [DB FAULT] Falla aislada en {inst_name}: {str(e)[:50]}... │"))
            except Exception as e:
                failed_count += 1
                self.stdout.write(self.style.ERROR(f"│  ⚠️ [RUNTIME] Falla de ejecución en {inst_name}: {str(e)[:50]}... │"))

        self.stdout.write(self.style.WARNING("└" + "─" * 85 + "┘\n"))

        # ==========================================
        # 3. REPORTE EJECUTIVO
        # ==========================================
        elapsed = (time.perf_counter() - start_time) * 1000
        
        self.stdout.write(self.style.SUCCESS("╔" + "═" * 85 + "╗"))
        self.stdout.write(self.style.SUCCESS(f"║ 🏁 [MISSION ACCOMPLISHED] OPERACIÓN DE ESTRÉS COMPLETADA EN {elapsed:.2f} ms{' '*19}║"))
        self.stdout.write(self.style.SUCCESS("╠" + "═" * 85 + "╣"))
        self.stdout.write(self.style.SUCCESS(f"║  ✅ Nodos Sincronizados : {success_count}/{self.TOTAL_TARGETS} (Ready for ML Ingestion){' '*30}║"))
        
        if failed_count > 0:
            self.stdout.write(self.style.ERROR(f"║  ❌ Nodos Rechazados    : {failed_count}/{self.TOTAL_TARGETS} (Revisar Constraints DB){' '*28}║"))
        else:
            self.stdout.write(self.style.SUCCESS(f"║  🛡️ Nodos Rechazados    : 0 (Cero colisiones, Integridad Estructural del 100%){' '*8}║"))
            
        self.stdout.write(self.style.SUCCESS("╚" + "═" * 85 + "╝"))