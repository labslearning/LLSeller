import sys
import asyncio
import logging
import time
from typing import Any, Optional

from django.core.management.base import BaseCommand
from django.utils import timezone
from django.conf import settings

# Importaciones de Motores Core
from sales.models import Institution, Interaction, Contact
from sales.engine.campaign import AICadenceGenerator, OmnichannelDispatcher

# Logger de telemetría de alto rendimiento
logger = logging.getLogger("Sovereign.QA")

class Command(BaseCommand):
    help = '🔫 [QA TIER GOD] Detonador de Cadencia IA. Orquesta inferencia asíncrona, genera el payload y dispara el vector de ataque.'

    async def _async_spinner(self, message: str, delay: float = 0.1) -> None:
        """
        [UI CONCURRENTE]
        Mantiene un spinner táctico en la terminal sin bloquear el Event Loop.
        Demuestra el dominio absoluto de I/O asíncrono.
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
            # Limpia la línea cuando la tarea principal finaliza o la cancela
            sys.stdout.write('\r' + ' ' * (len(message) + 10) + '\r')
            sys.stdout.flush()

    def handle(self, *args: Any, **options: Any) -> None:
        self.stdout.write(self.style.WARNING("=" * 65))
        self.stdout.write(self.style.WARNING("🧠  INICIANDO MOTOR DE INFERENCIA IA & OMNICHANNEL DISPATCHER  🧠"))
        self.stdout.write(self.style.WARNING("=" * 65))

        # 1. PRE-FLIGHT CHECK: Localización sincrónica del Objetivo (Caballo de Troya)
        self.stdout.write(self.style.NOTICE("[SYS] Ejecutando escaneo de perímetro en la base de datos..."))
        inst: Optional[Institution] = Institution.objects.filter(name='Hydra Tech Academy (QA Target)').first()
        
        if not inst:
            self.stdout.write(self.style.ERROR("\n❌ [FATAL ERROR] Objetivo no detectado en el Data Warehouse."))
            self.stdout.write(self.style.NOTICE("👉  Protocolo requerido: Ejecuta primero 'python manage.py qa_1_setup_target --email tu@email.com'"))
            return

        if inst.contacted:
            self.stdout.write(self.style.ERROR("\n⚠️ [WARNING] El objetivo ya figura como 'Contactado'."))
            self.stdout.write(self.style.NOTICE("👉  Para una prueba limpia, ejecuta 'qa_1_setup_target' con el flag '--hard-reset'."))
            return

        # 2. INICIALIZACIÓN DE MOTORES DE COMBATE
        ai_engine = AICadenceGenerator()
        dispatcher = OmnichannelDispatcher()

        # 3. NÚCLEO ASÍNCRONO (ASYNC EVENT LOOP)
        async def execute_outreach_test() -> None:
            spinner_task = None
            try:
                # Identidad del Decision Maker
                contact = await dispatcher.get_or_create_contact(inst)
                self.stdout.write(self.style.SUCCESS(f"[DB] Target Acquired: {contact.name} ({contact.role})"))
                self.stdout.write(self.style.SUCCESS(f"[DB] Vector Destination: {contact.email}\n"))
                
                # Iniciar Hilo Concurrente de UI (Spinner)
                spinner_task = asyncio.create_task(self._async_spinner("Conectando con Neural Engine (IA) y sintetizando Pitch..."))
                
                # Inferencia IA (Generación del Pitch) midiendo latencia de microsegundos
                start_ai = time.perf_counter()
                
                # Llamada bloqueante a nivel de red (DeepSeek/OpenAI API) pero liberada en el Event Loop
                pitch = await ai_engine.build_omnichannel_pitch(inst, contact)
                
                ai_duration = (time.perf_counter() - start_ai)
                
                # Detener el spinner
                if spinner_task:
                    spinner_task.cancel()
                    await asyncio.gather(spinner_task, return_exceptions=True)

                self.stdout.write(self.style.SUCCESS(f"✅ [IA] Inferencia completada y decodificada en {ai_duration:.3f} segundos."))

                # 4. AUDITORÍA FORENSE DE LA CARGA ÚTIL (PAYLOAD PRE-VIEW)
                self.stdout.write(self.style.WARNING("\n" + "┌" + "─"*63 + "┐"))
                self.stdout.write(self.style.WARNING("│") + self.style.SUCCESS(" 🚀 [PAYLOAD PRE-VIEW TIER GOD]                                ") + self.style.WARNING("│"))
                self.stdout.write(self.style.WARNING("├" + "─"*63 + "┤"))
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("SUBJECT: ") + f"{pitch.get('email_1_subject')[:50]}...")
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("BODY: "))
                
                # Imprimir el cuerpo limitando el ancho para que la terminal se vea profesional
                for line in pitch.get('email_1_body', '').split('\n'):
                    if line.strip():
                        self.stdout.write(self.style.WARNING("│   ") + line[:58] + ("..." if len(line) > 58 else ""))
                        
                self.stdout.write(self.style.WARNING("│ ") + self.style.NOTICE("WHATSAPP: ") + f"{pitch.get('whatsapp_1', '')[:50]}...")
                self.stdout.write(self.style.WARNING("└" + "─"*63 + "┘\n"))

                # 5. TRANSACCIÓN ATÓMICA DE DATA WAREHOUSE Y DESPACHO SMTP
                self.stdout.write(self.style.NOTICE("💾 [DB] Commiteando interacción en el Data Warehouse..."))
                
                interaction = await dispatcher.log_interaction(
                    inst, 
                    contact, 
                    "email", 
                    pitch["email_1_subject"], 
                    pitch["email_1_body"]
                )

                self.stdout.write(self.style.NOTICE("📨 [NET] Ruteando payload a través del Email Service Layer..."))
                
                dispatch_start = time.perf_counter()
                msg_id = await dispatcher.send_smtp_email(
                    interaction, 
                    contact, 
                    pitch["email_1_subject"], 
                    pitch["email_1_body"]
                )
                dispatch_duration = (time.perf_counter() - dispatch_start)

                if msg_id:
                    # 6. CIERRE DEL CICLO (UPDATE ASÍNCRONO)
                    inst.contacted = True
                    await inst.asave(update_fields=['contacted', 'updated_at'])
                    
                    self.stdout.write(self.style.SUCCESS("\n" + "=" * 65))
                    self.stdout.write(self.style.SUCCESS("🏆  MISIÓN DE OUTREACH EXITOSA (STATUS: 200 OK)  🏆"))
                    self.stdout.write(self.style.SUCCESS("=" * 65))
                    self.stdout.write(self.style.NOTICE(f"📍 ID DE INTERACCIÓN : {interaction.id}"))
                    self.stdout.write(self.style.NOTICE(f"⏱️  LATENCIA DESPACHO: {dispatch_duration:.3f}s"))
                    
                    self.stdout.write(self.style.WARNING("\n👉 PASO FINAL: CÓPIATE EL ID DE INTERACCIÓN DE ARRIBA."))
                    self.stdout.write(self.style.SUCCESS("Ejecuta el Kill-Switch de simulación de respuesta con este comando:"))
                    self.stdout.write(self.style.NOTICE(f"python manage.py qa_3_simulate_reply --interaction_id {interaction.id} --reply_text 'Me interesa la propuesta, ¿agendamos?'"))
                else:
                    self.stdout.write(self.style.ERROR("\n❌ [FALLO DE RED] El Dispatcher no pudo entregar el mensaje al SMTP Backend."))

            except Exception as e:
                # Si algo falla, asegurarnos de apagar el spinner visual
                if spinner_task and not spinner_task.done():
                    spinner_task.cancel()
                    
                self.stdout.write(self.style.ERROR(f"\n❌ [SYSTEM CRASH] Colapso en la tubería de Outreach: {str(e)}"))
                logger.exception("Outreach QA Pipeline Crash Detected")

        # Inyectar la corrutina en el Event Loop de Python
        try:
            asyncio.run(execute_outreach_test())
        except KeyboardInterrupt:
            self.stdout.write(self.style.ERROR("\n⚠️ [ABORT] Misión abortada por el usuario (SIGINT)."))
            sys.exit(1)