import sys
import uuid
import random
import time
from datetime import timedelta
from typing import Any, List, Dict

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction, DatabaseError
from django.utils import timezone
from django.db.models import Q

from sales.models import Institution, Contact, Interaction

class Command(BaseCommand):
    help = '🚀 [QA TIER GOD] Motor Cuántico de Inyección B2B. Operaciones Vectoriales (Bulk), Complejidad O(1) en latencia de red.'

    # --- CONSTANTES DE VANGUARDIA ---
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

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            '--targets', 
            type=int, 
            default=200, 
            help='Número de nodos sintéticos a inyectar masivamente. Default: 200.'
        )
        parser.add_argument(
            '--batch-size', 
            type=int, 
            default=1000, 
            help='Tamaño de fragmentación (Chunking) para evitar bloqueos OOM en PostgreSQL.'
        )

    def handle(self, *args: Any, **options: Any) -> None:
        total_targets = options['targets']
        batch_size = options['batch_size']

        self.stdout.write(self.style.WARNING("╔" + "═" * 85 + "╗"))
        self.stdout.write(self.style.WARNING("║ ") + self.style.SUCCESS(f"🚀 INICIANDO MOTOR MASIVO: INYECTANDO {total_targets} NODOS (BULK MODE O(1) LATENCY)") + self.style.WARNING("   ║"))
        self.stdout.write(self.style.WARNING("╚" + "═" * 85 + "╝"))

        now = timezone.now()

        # Generación de distribución probabilística O(N) en memoria
        statuses = [('REPLIED', 15), ('OPENED', 25), ('SENT', 40), ('BOUNCED', 10), ('MEETING', 10)]
        distribution = []
        for status, weight in statuses:
            distribution.extend([status] * weight)
        
        # ==========================================
        # 1. PURGA QUIRÚRGICA (VECTORIAL CLEANUP)
        # ==========================================
        self.stdout.write(self.style.NOTICE("\n[SYS] Ejecutando purga vectorial de colisiones espectrales..."))
        
        cleanup_query = Q()
        suffixes = ["QA", "Simulator", "Mock", "Node", "Sandbox", "DB", "Tech", "Test", "Data"]
        for suffix in suffixes:
            cleanup_query |= Q(name__icontains=suffix)
        
        # Eliminación masiva delegada enteramente al motor de Base de Datos (O(1) llamadas)
        deleted_count, _ = Institution.objects.filter(cleanup_query).delete()
        self.stdout.write(self.style.SUCCESS(f"🧹 Sector purgado: {deleted_count} registros fantasmas vaporizados.\n"))

        start_time = time.perf_counter()

        # ==========================================
        # 2. EVALUACIÓN DIFERIDA (RAM FABRICATION)
        # ==========================================
        self.stdout.write(self.style.WARNING("┌─[ CONSTRUYENDO MATRIZ DE DATOS SINTÉTICA EN RAM ]" + "─" * 33 + "┐"))

        institutions_to_create = []
        
        # Diccionarios temporales para mantener la relación relacional (Foreign Keys) en memoria
        institution_meta = {} # Mapea uuid -> (status, created, updated)

        for i in range(total_targets):
            inst_name = random.choice(self.FAKE_NAMES)
            target_status = random.choice(distribution)
            lead_score = 100 if target_status in ['REPLIED', 'MEETING'] else (70 if target_status == 'OPENED' else 40)
            
            crypto_hash = uuid.uuid4().hex[:8]
            base_domain = f"{inst_name.lower().replace(' ', '')}-{crypto_hash}"
            inst_uuid = uuid.uuid4()
            
            days_ago = random.randint(1, 14)
            created_time = now - timedelta(days=days_ago, hours=random.randint(1, 12))
            updated_time = created_time + timedelta(minutes=random.randint(2, 2880)) if target_status != 'SENT' else created_time

            institutions_to_create.append(
                Institution(
                    id=inst_uuid,
                    name=f"{inst_name} {crypto_hash.upper()}",
                    website=f"https://{base_domain}.edu",
                    city=random.choice(["Silicon Wadi", "Silicon Valley", "London", "Bangalore"]),
                    country=random.choice(["Israel", "USA", "UK", "India"]),
                    institution_type="university",
                    is_private=True,
                    email=f"ceo@{base_domain}.edu",
                    lead_score=lead_score,
                    contacted=True,
                    is_active=True,
                    # Bypass auto_now_add forzando el valor
                    created_at=created_time,
                    updated_at=updated_time
                )
            )
            
            institution_meta[inst_uuid] = {
                'status': target_status,
                'created': created_time,
                'updated': updated_time,
                'hash': crypto_hash,
                'domain': base_domain
            }

        # ==========================================
        # 3. INYECCIÓN VECTORIAL (BULK INSERTS O(1) LATENCY)
        # ==========================================
        try:
            with transaction.atomic():
                self.stdout.write(self.style.NOTICE("💾 [NET] Despachando Instituciones hacia PostgreSQL (Bulk Insert)..."))
                # Ignoramos conflictos (por si hay nombres repetidos por pura probabilidad extrema)
                Institution.objects.bulk_create(institutions_to_create, batch_size=batch_size, ignore_conflicts=True)
                
                # Rescatamos los objetos reales creados para mapear los IDs correctamente a los Contactos
                # (bulk_create en Postgres no siempre retorna los PKs si hay ignore_conflicts, así que hacemos un select optimizado)
                created_insts = Institution.objects.filter(id__in=[inst.id for inst in institutions_to_create]).only('id', 'name')
                
                contacts_to_create = []
                interactions_to_create = []

                self.stdout.write(self.style.NOTICE("⚙️  [RAM] Ensamblando grafos de Contactos e Interacciones..."))
                
                for inst in created_insts:
                    meta = institution_meta[inst.id]
                    c_hash = meta['hash']
                    b_domain = meta['domain']
                    target_status = meta['status']
                    created_time = meta['created']
                    updated_time = meta['updated']
                    
                    contact_id = uuid.uuid4()
                    channel_choice = Interaction.Channel.WHATSAPP if random.random() < 0.3 else Interaction.Channel.EMAIL
                    
                    contacts_to_create.append(
                        Contact(
                            id=contact_id,
                            institution=inst,
                            name=f"Ingeniero Operativo {c_hash.upper()}",
                            role=random.choice(self.ROLES),
                            email=f"admin-{c_hash}@{b_domain}.edu",
                            phone=f"+{random.randint(10000000000, 99999999999)}",
                            created_at=created_time,
                            updated_at=created_time
                        )
                    )
                    
                    # Generación determinista de telemetría sin guardar aún
                    telemetry = {}
                    interaction_status = Interaction.Status.SENT
                    subject = f"Propuesta Estratégica para {inst.name}"
                    message_received = None
                    replied = False
                    intent = ""
                    meeting_date = None
                    
                    if target_status in ['OPENED', 'REPLIED', 'MEETING']:
                        interaction_status = Interaction.Status.OPENED
                        telemetry['opens'] = [{
                            'timestamp': updated_time.isoformat(), 
                            'ip': f"{random.randint(1, 255)}.X.X.X", 
                            'user_agent': random.choice(self.USER_AGENTS)
                        }]
                        
                    if target_status in ['REPLIED', 'MEETING']:
                        interaction_status = Interaction.Status.MEETING if target_status == 'MEETING' else Interaction.Status.REPLIED
                        message_received = random.choice(self.HUMAN_REPLIES)
                        replied = True
                        intent = "POSITIVE" if target_status == 'MEETING' else random.choice(["NEUTRAL", "POSITIVE", "NEGATIVE"])
                        subject = f"RE: {subject}"
                        telemetry['nlp_engine'] = {
                            'intent': intent, 'sentiment_score': round(random.uniform(0.10, 0.99), 2), 'processed_at': updated_time.isoformat()
                        }
                        if target_status == 'MEETING':
                            meeting_date = updated_time + timedelta(days=random.randint(1, 10))

                    if target_status == 'BOUNCED':
                        interaction_status = Interaction.Status.BOUNCED

                    interactions_to_create.append(
                        Interaction(
                            id=uuid.uuid4(),
                            institution=inst,
                            contact_id=contact_id, # Linkeamos directamente la llave foránea
                            channel=channel_choice,
                            subject=subject,
                            message_sent=f"Hola equipo,\n\nSoy el Sovereign Engine. Adjunto propuesta B2B.",
                            message_received=message_received,
                            telemetry_data=telemetry,
                            opened_count=1 if target_status in ['OPENED', 'REPLIED', 'MEETING'] else 0,
                            replied=replied,
                            status=interaction_status,
                            ai_sentiment=intent,
                            meeting_date=meeting_date,
                            created_at=created_time,
                            updated_at=updated_time
                        )
                    )

                self.stdout.write(self.style.NOTICE("💾 [NET] Despachando Contactos e Interacciones hacia PostgreSQL..."))
                
                # Inyección masiva final
                Contact.objects.bulk_create(contacts_to_create, batch_size=batch_size)
                Interaction.objects.bulk_create(interactions_to_create, batch_size=batch_size)

                success_count = len(created_insts)
                self.stdout.write(self.style.SUCCESS(f"│  ↳ Matriz Vectorial Completada: {success_count} grafos institucionales generados. │"))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"│  ❌ [CRITICAL CRASH] Falla de Transacción Atómica: {str(e)} │"))
            logger.exception("Mass Simulation Crash")
            return

        self.stdout.write(self.style.WARNING("└" + "─" * 85 + "┘\n"))

        # ==========================================
        # 3. REPORTE EJECUTIVO Y AUDITORÍA
        # ==========================================
        elapsed = (time.perf_counter() - start_time)
        velocity = success_count / elapsed if elapsed > 0 else 0
        
        self.stdout.write(self.style.SUCCESS("╔" + "═" * 85 + "╗"))
        self.stdout.write(self.style.SUCCESS(f"║ 🏁 [MISSION ACCOMPLISHED] ESTRÉS CUÁNTICO COMPLETADO EN {elapsed:.3f} s{' '*19}║"))
        self.stdout.write(self.style.SUCCESS("╠" + "═" * 85 + "╣"))
        self.stdout.write(self.style.SUCCESS(f"║  ✅ Nodos Sincronizados : {success_count}/{total_targets} (Ready for ML Ingestion){' '*30}║"))
        self.stdout.write(self.style.SUCCESS(f"║  ⚡ Velocidad de Red    : {velocity:.0f} nodos / segundo{' '*43}║"))
        self.stdout.write(self.style.SUCCESS("╚" + "═" * 85 + "╝"))

        # Desplegar distribución real
        self.stdout.write(self.style.NOTICE("\n📊 DISTRIBUCIÓN REAL GENERADA EN LA BASE DE DATOS (EMBUDO B2B):"))
        for stat in ['MEETING', 'REPLIED', 'OPENED', 'SENT', 'BOUNCED']:
            count = Interaction.objects.filter(status=stat).count()
            self.stdout.write(f"   - {stat:<10}: {count} nodos.")