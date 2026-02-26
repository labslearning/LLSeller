import time
import logging
from typing import Any

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction, DatabaseError
from django.utils import timezone

# Importaciones de todos los Tiers de Inteligencia
from sales.models import Institution, TechProfile, DeepForensicProfile, Contact, Interaction

# Logger de auditoría estricto (Nivel Enterprise)
logger = logging.getLogger("Sovereign.QA")

class Command(BaseCommand):
    help = '🚀 [QA TIER GOD] Forja e inyecta un objetivo de pruebas aislado (Caballo de Troya) con estado 100% Idempotente.'

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            '--email', 
            type=str, 
            required=True, 
            help='Tu correo personal real para interceptar la carga útil de la prueba (Email Payload).'
        )
        parser.add_argument(
            '--hard-reset',
            action='store_true',
            help='Aniquila cualquier rastro del objetivo en la base de datos antes de reconstruirlo.'
        )

    def handle(self, *args: Any, **options: Any) -> None:
        start_time = time.perf_counter()
        test_email = options['email'].strip().lower()
        hard_reset = options['hard_reset']
        
        # Constante de anclaje (CRÍTICO: No cambiar, qa_2_fire_outreach depende de esto)
        TARGET_NAME = 'Hydra Tech Academy (QA Target)'

        self.stdout.write(self.style.WARNING("=" * 65))
        self.stdout.write(self.style.WARNING("🛡️  INICIANDO PROTOCOLO DE INYECCIÓN QA SANDBOX (GOD TIER) 🛡️"))
        self.stdout.write(self.style.WARNING("=" * 65))
        self.stdout.write(self.style.NOTICE(f"▶ Target Interception Email: {test_email}"))

        try:
            # Transacción Atómica Estricta: O se inyecta todo el cluster, o se hace rollback automático.
            with transaction.atomic():
                
                # ---------------------------------------------------------
                # 0. PROTOCOLO DE PURGA (MEMORY WIPE)
                # ---------------------------------------------------------
                if hard_reset:
                    self.stdout.write(self.style.ERROR("🧨 [HARD RESET] Ejecutando purga de aniquilación de datos previos..."))
                    Institution.objects.filter(name=TARGET_NAME).delete()

                # Buscamos la institución para limpiar sus interacciones y asegurar que la Cadencia dispare como "Primer Contacto"
                existing_inst = Institution.objects.filter(name=TARGET_NAME).first()
                if existing_inst:
                    deleted_interactions, _ = Interaction.objects.filter(institution=existing_inst).delete()
                    if deleted_interactions > 0:
                        self.stdout.write(self.style.WARNING(f"🧹 Historial limpiado: Se eliminaron {deleted_interactions} interacciones previas."))

                # ---------------------------------------------------------
                # 1. INYECCIÓN DEL NODO MAESTRO (INSTITUTION TIER 0)
                # ---------------------------------------------------------
                self.stdout.write(self.style.NOTICE("⚙️  Forjando Identidad de la Institución (Master Node)..."))
                inst, inst_created = Institution.objects.update_or_create(
                    name=TARGET_NAME, # Llave primaria lógica y determinista
                    defaults={
                        'website': 'https://qa-hydra-academy.edu.co',
                        'city': 'Bogotá',
                        'country': 'Colombia',
                        'institution_type': 'university',
                        'is_private': True,
                        'student_count': 4500, # Variable inyectada para dar contexto financiero a la IA
                        'email': 'contacto@qa-hydra-academy.edu.co',
                        'lead_score': 99, # Prioridad Máxima garantizada para el motor de Cadencia
                        'last_scored_at': timezone.now(), # Simula ejecución reciente del motor ML
                        'contacted': False, # [CRÍTICO] Debe ser False para que la IA lo ataque
                        'is_active': True,
                        'discovery_source': 'manual'
                    }
                )

                # ---------------------------------------------------------
                # 2. INYECCIÓN DE PERFIL TECNOLÓGICO (TECH STACK TIER 1)
                # ---------------------------------------------------------
                self.stdout.write(self.style.NOTICE("⚙️  Sintetizando Huella Tecnológica (LMS/Analytics)..."))
                TechProfile.objects.update_or_create(
                    institution=inst,
                    defaults={
                        'has_lms': True,
                        'lms_provider': 'Canvas LMS', # Cebo algorítmico específico para el Prompt de la IA
                        'has_analytics': True,
                        'is_wordpress': False
                    }
                )

                # ---------------------------------------------------------
                # 3. INYECCIÓN FORENSE PROFUNDA (AI DATA TIER 2)
                # ---------------------------------------------------------
                self.stdout.write(self.style.NOTICE("⚙️  Simulando Datos Forenses de Nivel 2..."))
                DeepForensicProfile.objects.update_or_create(
                    institution=inst,
                    defaults={
                        'ai_classification': 'A+ High Ticket',
                        'estimated_budget': '$50k - $100k USD / Anual'
                    }
                )

                # ---------------------------------------------------------
                # 4. INYECCIÓN DEL CONTACTO SEÑUELO (DECISION MAKER)
                # ---------------------------------------------------------
                self.stdout.write(self.style.NOTICE("⚙️  Alineando Vector de Ataque (Decision Maker)..."))
                # Limpiamos anomalías: Borramos cualquier CTO anterior que no sea el correo actual
                Contact.objects.filter(institution=inst).exclude(email=test_email).delete()
                
                Contact.objects.update_or_create(
                    institution=inst, # Amarre estructural
                    defaults={
                        'email': test_email, # El email se actualiza dinámicamente si el usuario lo cambia
                        'name': 'Señor Arquitecto',
                        'role': 'Director de Tecnología e Innovación (CTO)',
                        'phone': '+573000000000'
                    }
                )

            # ---------------------------------------------------------
            # 5. REPORTE DE TELEMETRÍA FINAL
            # ---------------------------------------------------------
            elapsed_time = (time.perf_counter() - start_time) * 1000
            
            self.stdout.write(self.style.SUCCESS("\n" + "=" * 65))
            if inst_created or hard_reset:
                self.stdout.write(self.style.SUCCESS(f"✅ [ÉXITO] Caballo de Troya inyectado desde cero. ID: {inst.id}"))
            else:
                self.stdout.write(self.style.SUCCESS(f"🔄 [RECALIBRADO] Sandbox restaurado y listo. ID: {inst.id}"))
                
            self.stdout.write(self.style.SUCCESS(f"🎯 VECTOR DE ATAQUE APUNTADO A: {test_email}"))
            self.stdout.write(self.style.SUCCESS(f"⏱️  Tiempo de reconstrucción: {elapsed_time:.2f} ms"))
            self.stdout.write(self.style.WARNING("=" * 65))
            
            self.stdout.write(self.style.NOTICE("\n💡 PRÓXIMO PASO - DETONAR CADENCIA:"))
            self.stdout.write(self.style.NOTICE("   Ejecuta: python manage.py qa_2_fire_outreach"))

        except DatabaseError as db_err:
            logger.critical(f"Database constraint or connection failure: {db_err}")
            self.stdout.write(self.style.ERROR(f"\n❌ [CRÍTICO] Falla en el Kernel de PostgreSQL: {db_err}"))
        except Exception as e:
            logger.error(f"QA Target Setup failed: {e}")
            self.stdout.write(self.style.ERROR(f"\n❌ [ERROR] Fallo estructural en la inyección QA: {e}"))