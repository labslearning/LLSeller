import sys
import time
import asyncio
import logging
from typing import Any, Optional

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction
from asgiref.sync import sync_to_async  # [GOD TIER] El puente de élite entre Async I/O y Django ORM Sync

# Importaciones Core
from sales.models import Interaction, Institution
from sales.engine.reply_catcher import OmniReplyCatcher

# Telemetría de Grado Militar
logger = logging.getLogger("Sovereign.QA")

class Command(BaseCommand):
    help = '🎣 [QA TIER GOD] Neural Inbound Interceptor. Simula la captura de un correo, clasifica el Intent con IA y ejecuta el Kill-Switch.'

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            '--interaction_id', 
            type=str, 
            required=True, 
            help='UUID de la interacción original inyectada por el Dispatcher.'
        )
        parser.add_argument(
            '--reply_text', 
            type=str, 
            required=True, 
            help='Texto crudo (Payload) de respuesta del prospecto.'
        )

    async def _async_spinner(self, message: str, delay: float = 0.1) -> None:
        """
        [UI CONCURRENTE]
        Mantiene un spinner táctico en la terminal sin bloquear el hilo de inferencia.
        Usa Non-blocking I/O para asegurar fluidez.
        """
        spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        i = 0
        try:
            while True:
                sys.stdout.write(f'\r{self.style.WARNING(spinner_chars[i % len(spinner_chars)])} {self.style.NOTICE(message)}')
                sys.stdout.flush()
                await asyncio.sleep(delay)
                i += 1
        except asyncio.CancelledError:
            # Limpieza limpia y determinista al finalizar el proceso principal
            sys.stdout.write('\r' + ' ' * (len(message) + 10) + '\r')
            sys.stdout.flush()

    def handle(self, *args: Any, **options: Any) -> None:
        self.stdout.write(self.style.WARNING("=" * 75))
        self.stdout.write(self.style.WARNING("🕵️‍♂️  INICIANDO SIMULACIÓN DE CAPTURA INBOUND (NEURAL ANALYSIS)  🕵️‍♂️"))
        self.stdout.write(self.style.WARNING("=" * 75))

        # 1. LOCALIZACIÓN ESTRICTA DE LA CARGA ÚTIL (INTERACCIÓN)
        interaction_id = options['interaction_id'].strip()
        reply_text = options['reply_text'].strip()

        # Operación síncrona segura: Aún no estamos en el Event Loop asíncrono
        # [OPTIMIZACIÓN]: select_related previene el problema N+1 Query en las evaluaciones posteriores.
        interaction: Optional[Interaction] = Interaction.objects.select_related('institution', 'contact').filter(id=interaction_id).first()
        
        if not interaction:
            self.stdout.write(self.style.ERROR(f"\n❌ [FATAL ERROR] Interacción UUID '{interaction_id}' no encontrada en el Warehouse."))
            self.stdout.write(self.style.NOTICE("👉 Verifica haber copiado el ID exacto del comando qa_2."))
            return

        if interaction.status == Interaction.Status.REPLIED:
            self.stdout.write(self.style.ERROR(f"\n⚠️ [WARNING] La interacción ya fue procesada previamente y marcada como REPLIED."))
            return

        # Precarga de datos síncronos vitales antes de entrar al agujero negro asíncrono
        sender_email = interaction.contact.email if interaction.contact else "unknown@target.com"
        inst_before_score = interaction.institution.lead_score

        # 2. ORQUESTACIÓN ASÍNCRONA BLINDADA (ASYNC SANDBOX)
        async def execute_inbound_interception() -> None:
            spinner_task = None
            try:
                # [ARCH] Inicializamos la clase sincronamente porque la configuración subyacente de Django settings es síncrona
                catcher = OmniReplyCatcher()
                
                # --- FASE A: INFERENCIA DE SENTIMIENTO (NLP) ---
                self.stdout.write(self.style.NOTICE(f"[NET] Interceptando Payload: '{reply_text[:60]}...'"))
                spinner_task = asyncio.create_task(self._async_spinner("Neural Engine procesando NLP Sentimental Analysis..."))
                
                # [GOD TIER PROTECTION]: Delega a un thread seguro para I/O de red
                start_ai = time.perf_counter()
                intent = await asyncio.to_thread(catcher._classify_intent_with_ai, reply_text)
                ai_duration = (time.perf_counter() - start_ai)
                
                if spinner_task:
                    spinner_task.cancel()
                    await asyncio.gather(spinner_task, return_exceptions=True)

                self.stdout.write(self.style.SUCCESS(f"🎯 [IA] VERDICTO OBTENIDO: {intent} (Latencia: {ai_duration:.3f}s)"))

                # --- FASE B: EJECUCIÓN TRANSACCIONAL DEL KILL-SWITCH ---
                self.stdout.write(self.style.NOTICE("\n[SYS] Inyectando vector de enrutamiento y bloqueando Cadencia..."))
                
                # [GOD TIER PROTECTION]: sync_to_async envuelve el acceso a DB de _route_reply
                # para asegurar Thread-Safety y evitar el SynchronousOnlyOperation error.
                start_db = time.perf_counter()
                route_reply_async = sync_to_async(catcher._route_reply, thread_sensitive=True)
                await route_reply_async(interaction_id, sender_email, intent)
                db_duration = (time.perf_counter() - start_db)

                # --- 3. AUDITORÍA FORENSE POST-MORTEM ---
                # [GOD TIER PROTECTION]: Refrescar datos del ORM requiere puente síncrono.
                @sync_to_async(thread_sensitive=True)
                def get_refreshed_data():
                    interaction.refresh_from_db()
                    inst = interaction.institution
                    return interaction.status, inst.lead_score, inst.name

                final_status, final_score, inst_name = await get_refreshed_data()
                
                status_color = self.style.SUCCESS if final_status == 'REPLIED' else self.style.ERROR
                score_shift = f"{inst_before_score} ➔ {final_score}"
                cadence_status = "KILLED (Bloqueo Exitoso)" if final_score >= 100 else "ACTIVA (Requiere Atención)"

                self.stdout.write(self.style.WARNING("\n" + "┌" + "─"*73 + "┐"))
                self.stdout.write(self.style.WARNING("│ ") + self.style.SUCCESS("📊 [INBOUND FORENSICS] DB STATE MUTATION REPORT                         ") + self.style.WARNING("│"))
                self.stdout.write(self.style.WARNING("├" + "─"*73 + "┤"))
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("TARGET INSTITUTION : ") + f"{inst_name[:40]}")
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("INTERACTION STATUS : ") + status_color(f"{final_status}"))
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("INTENT CLASSIFIED  : ") + self.style.SUCCESS(f"{intent}"))
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("LEAD SCORE SHIFT   : ") + f"{score_shift} / 100")
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("CADENCE ENGINE     : ") + self.style.SUCCESS(cadence_status))
                self.stdout.write(self.style.WARNING("├" + "─"*73 + "┤"))
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("⏱️ IA INFERENCE LATENCY : ") + f"{ai_duration:.3f}s")
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("⏱️ DB ROUTING LATENCY   : ") + f"{db_duration:.3f}s")
                self.stdout.write(self.style.WARNING("└" + "─"*73 + "┘\n"))

                # 4. VEREDICTO ARQUITECTÓNICO
                if final_score >= 100 and final_status == 'REPLIED':
                    self.stdout.write(self.style.SUCCESS("🏆 [SYSTEM PERFECT] QA EXITOSO: EL CEREBRO HA CERRADO EL BUCLE DE VENTA. 🏆"))
                    self.stdout.write(self.style.SUCCESS("La máquina es plenamente autónoma y segura para producción global."))
                else:
                    self.stdout.write(self.style.ERROR("⚠️ [ALERTA DE INTEGRIDAD]: Los datos no mutaron como se esperaba. Revisa los logs transaccionales."))

            except Exception as e:
                if spinner_task and not spinner_task.done():
                    spinner_task.cancel()
                self.stdout.write(self.style.ERROR(f"\n❌ [CRITICAL CRASH] Colapso en la Red Neuronal Inbound: {str(e)}"))
                logger.exception("Inbound QA Pipeline Crash Detected")

        # Inyectar corrutina en el Event Loop
        try:
            asyncio.run(execute_inbound_interception())
        except KeyboardInterrupt:
            self.stdout.write(self.style.ERROR("\n⚠️ [ABORT] Simulación interceptada por el usuario (SIGINT)."))
            sys.exit(1)