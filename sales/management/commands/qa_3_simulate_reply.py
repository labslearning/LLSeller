import sys
import time
import asyncio
import logging
from typing import Any, Optional

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction

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
            sys.stdout.write('\r' + ' ' * (len(message) + 10) + '\r')
            sys.stdout.flush()

    def handle(self, *args: Any, **options: Any) -> None:
        self.stdout.write(self.style.WARNING("=" * 70))
        self.stdout.write(self.style.WARNING("🕵️‍♂️  INICIANDO SIMULACIÓN DE CAPTURA INBOUND (NEURAL ANALYSIS)  🕵️‍♂️"))
        self.stdout.write(self.style.WARNING("=" * 70))

        # 1. LOCALIZACIÓN ESTRICTA DE LA CARGA ÚTIL (INTERACCIÓN)
        interaction_id = options['interaction_id'].strip()
        reply_text = options['reply_text'].strip()

        interaction: Optional[Interaction] = Interaction.objects.select_related('institution', 'contact').filter(id=interaction_id).first()
        
        if not interaction:
            self.stdout.write(self.style.ERROR(f"\n❌ [FATAL ERROR] Interacción UUID '{interaction_id}' no encontrada en el Warehouse."))
            self.stdout.write(self.style.NOTICE("👉 Verifica haber copiado el ID exacto del comando qa_2."))
            return

        if interaction.status == Interaction.Status.REPLIED:
            self.stdout.write(self.style.ERROR(f"\n⚠️ [WARNING] La interacción ya fue procesada previamente y marcada como REPLIED."))
            return

        sender_email = interaction.contact.email if interaction.contact else "unknown@target.com"
        inst_before_score = interaction.institution.lead_score

        # 2. ORQUESTACIÓN ASÍNCRONA
        async def execute_inbound_interception() -> None:
            spinner_task = None
            try:
                catcher = OmniReplyCatcher()
                
                # FASE A: Inferencia de Sentimiento (Delegada a Thread para no bloquear Event Loop)
                self.stdout.write(self.style.NOTICE(f"[NET] Interceptando Payload: '{reply_text[:60]}...'"))
                spinner_task = asyncio.create_task(self._async_spinner("Neural Engine procesando NLP Sentimental Analysis..."))
                
                start_ai = time.perf_counter()
                intent = await asyncio.to_thread(catcher._classify_intent_with_ai, reply_text)
                ai_duration = (time.perf_counter() - start_ai)
                
                if spinner_task:
                    spinner_task.cancel()
                    await asyncio.gather(spinner_task, return_exceptions=True)

                self.stdout.write(self.style.SUCCESS(f"🎯 [IA] VERDICTO OBTENIDO: {intent} (Latencia: {ai_duration:.3f}s)"))

                # FASE B: Ejecución Transaccional del Kill-Switch
                self.stdout.write(self.style.NOTICE("\n[SYS] Inyectando vector de enrutamiento y bloqueando Cadencia..."))
                
                start_db = time.perf_counter()
                await asyncio.to_thread(catcher._route_reply, interaction_id, sender_email, intent)
                db_duration = (time.perf_counter() - start_db)

                # 3. AUDITORÍA FORENSE POST-MORTEM (VERIFICACIÓN DE MUTACIÓN DE ESTADO)
                # Refrescamos los modelos directo desde la DB maestra
                await asyncio.to_thread(interaction.refresh_from_db)
                inst = interaction.institution
                
                status_color = self.style.SUCCESS if interaction.status == 'REPLIED' else self.style.ERROR
                score_shift = f"{inst_before_score} ➔ {inst.lead_score}"
                cadence_status = "KILLED (Bloqueo Exitoso)" if inst.lead_score == 100 else "ACTIVA (Requiere Atención)"

                self.stdout.write(self.style.WARNING("\n" + "┌" + "─"*68 + "┐"))
                self.stdout.write(self.style.WARNING("│ ") + self.style.SUCCESS("📊 [INBOUND FORENSICS] DB STATE MUTATION REPORT                   ") + self.style.WARNING("│"))
                self.stdout.write(self.style.WARNING("├" + "─"*68 + "┤"))
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("INTERACTION STATUS : ") + status_color(f"{interaction.status}"))
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("INTENT CLASSIFIED  : ") + self.style.SUCCESS(f"{intent}"))
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("LEAD SCORE SHIFT   : ") + f"{score_shift} / 100")
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("CADENCE ENGINE     : ") + self.style.SUCCESS(cadence_status))
                self.stdout.write(self.style.WARNING("├" + "─"*68 + "┤"))
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("⏱️ IA INFERENCE LATENCY : ") + f"{ai_duration:.3f}s")
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("⏱️ DB ROUTING LATENCY   : ") + f"{db_duration:.3f}s")
                self.stdout.write(self.style.WARNING("└" + "─"*68 + "┘\n"))

                # 4. VEREDICTO ARQUITECTÓNICO
                if inst.lead_score == 100 and interaction.status == 'REPLIED':
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