import sys
import time
import signal
import asyncio
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.db import reset_queries
from django.conf import settings

from sales.models import Institution
# Importamos el orquestador asíncrono directamente (Bypass de alto rendimiento)
from sales.engine.recon_engine import _orchestrate

class Command(BaseCommand):
    help = 'Enterprise B2B Enrichment Daemon (The Ghost Sniper Worker)'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Bandera de estado para el Graceful Shutdown (Apagado Seguro)
        self.stop_requested = False

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size', 
            type=int, 
            default=30, 
            help='Número de prospectos a procesar por lote (Default: 30)'
        )
        parser.add_argument(
            '--continuous', 
            action='store_true', 
            help='Modo Daemon: Se ejecuta en un bucle infinito procesando la cola.'
        )
        parser.add_argument(
            '--cooldown', 
            type=int, 
            default=15, 
            help='Tiempo de enfriamiento (segundos) entre lotes para evadir WAFs.'
        )

    def _signal_handler(self, sig, frame):
        """
        Intercepta señales del OS (SIGINT/SIGTERM) enviadas por Docker, Kubernetes o el usuario.
        Evita que la base de datos se corrompa cerrando transacciones limpiamente.
        """
        if not self.stop_requested:
            self.stdout.write(self.style.WARNING("\n⏳ [SYSTEM] Señal de apagado recibida (Graceful Shutdown)..."))
            self.stdout.write(self.style.WARNING("Terminando el escaneo del lote actual antes de apagar el motor. Por favor espera..."))
            self.stop_requested = True
        else:
            self.stdout.write(self.style.ERROR("💀 [SYSTEM] Apagado forzado de emergencia."))
            sys.exit(1)

    def handle(self, *args, **options):
        # 1. Registrar Listeners de Sistema Operativo
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        batch_size = options['batch_size']
        continuous = options['continuous']
        cooldown = options['cooldown']

        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("🚀 INICIANDO GHOST SNIPER WORKER DAEMON"))
        self.stdout.write(self.style.SUCCESS(f"⚙️  Lote: {batch_size} | Continuo: {continuous} | Cooldown: {cooldown}s"))
        self.stdout.write(self.style.SUCCESS("=" * 70))

        total_processed_global = 0
        daemon_start_time = time.time()

        try:
            while not self.stop_requested:
                # 2. Query de Alta Prioridad
                # Buscamos colegios que tengan URL pero NO tengan fecha de escaneo
                qs = Institution.objects.filter(
                    website__isnull=False,
                    last_scored_at__isnull=True,
                    is_active=True
                ).exclude(website='')[:batch_size]

                pending_count = qs.count()

                if pending_count == 0:
                    if continuous:
                        self.stdout.write(self.style.NOTICE("📭 Bandeja vacía. Esperando nuevos leads... (Polling en 60s)"))
                        # Sleep interrumpible
                        for _ in range(60):
                            if self.stop_requested: break
                            time.sleep(1)
                        continue
                    else:
                        self.stdout.write(self.style.SUCCESS("\n🏆 INBOX ZERO: No hay prospectos pendientes por escanear."))
                        break

                self.stdout.write(self.style.WARNING(f"\n📥 Extrayendo Lote de {pending_count} objetivos..."))
                
                # 3. Transformación de Datos para el Motor Asíncrono
                # Esto es clave: extraemos los datos a memoria para no bloquear el DB connection en el loop async
                targets = []
                for inst in qs:
                    targets.append({
                        'id': inst.id,
                        'name': inst.name,
                        'url': inst.website,
                        'city': inst.city or "Unknown"
                    })

                # 4. Inyección Directa (Browser Reuse)
                # Aquí enviamos la lista completa. El motor abrirá un solo navegador y procesará todos.
                batch_start_time = time.time()
                try:
                    asyncio.run(_orchestrate(targets))
                except Exception as batch_error:
                    self.stdout.write(self.style.ERROR(f"❌ Fallo crítico en el lote: {str(batch_error)}"))
                
                batch_elapsed = time.time() - batch_start_time
                total_processed_global += len(targets)

                self.stdout.write(self.style.SUCCESS(f"✅ Lote de {len(targets)} completado en {batch_elapsed:.2f}s"))

                # 5. Evitar Memory Leaks de Django (Crítico en Daemons 24/7)
                if settings.DEBUG:
                    reset_queries()

                if not continuous or self.stop_requested:
                    break

                # 6. Evasión de Radar (Cooldown)
                self.stdout.write(self.style.NOTICE(f"⏱️ Enfriando IP del Servidor por {cooldown} segundos..."))
                for _ in range(cooldown):
                    if self.stop_requested: break
                    time.sleep(1)

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ FATAL DAEMON EXCEPTION: {str(e)}"))
        finally:
            # 7. Telemetría de Cierre
            total_elapsed = time.time() - daemon_start_time
            self.stdout.write(self.style.SUCCESS("\n" + "=" * 70))
            self.stdout.write(self.style.SUCCESS("🛑 MOTOR DETENIDO Y MEMORIA LIBERADA"))
            self.stdout.write(self.style.SUCCESS(f"📊 Colegios Enriquecidos: {total_processed_global}"))
            self.stdout.write(self.style.SUCCESS(f"⏱️ Tiempo de Operación: {total_elapsed / 60:.2f} minutos"))
            self.stdout.write(self.style.SUCCESS("=" * 70))
