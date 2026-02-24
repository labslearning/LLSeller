import time
import signal
import sys
from django.core.management.base import BaseCommand
from sales.engine.serp_resolver import SERPResolverEngine
from sales.models import Institution

class Command(BaseCommand):
    help = 'Enterprise SERP Resolver: Descubre URLs con alta concurrencia, daemon mode y graceful shutdown.'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Bandera de control para apagar el motor sin corromper la base de datos
        self.stop_requested = False

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit', 
            type=int, 
            default=50, 
            help='Límite de prospectos por lote (Default: 50)'
        )
        parser.add_argument(
            '--concurrency', 
            type=int, 
            default=3, 
            help='Hilos asíncronos concurrentes (Hard-capped a 5 para evitar ban de IP)'
        )
        parser.add_argument(
            '--continuous', 
            action='store_true', 
            help='Modo Daemon: Ejecuta en bucle continuo hasta vaciar la base de datos completa.'
        )
        parser.add_argument(
            '--batch-delay', 
            type=int, 
            default=15, 
            help='En modo continuo, segundos de pausa entre cada lote para evadir anti-bots.'
        )

    def _signal_handler(self, sig, frame):
        """
        Intercepción de Señales del Sistema Operativo (SIGINT / SIGTERM).
        Si el usuario presiona Ctrl+C o el servidor (Docker/K8s) pide apagado,
        esto asegura que el lote actual termine de guardarse en la DB antes de morir.
        """
        if not self.stop_requested:
            self.stdout.write(self.style.WARNING("\n⏳ Señal de apagado detectada (Graceful Shutdown)..."))
            self.stdout.write(self.style.WARNING("Por favor espera, terminando el lote actual para no corromper la base de datos..."))
            self.stop_requested = True
        else:
            self.stdout.write(self.style.ERROR("💀 Apagado forzado. Posible pérdida de datos."))
            sys.exit(1)

    def handle(self, *args, **options):
        # 1. Registrar escuchadores de señales del OS
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        limit = options['limit']
        # Hard-cap de seguridad: No importa si el usuario pide 100 hilos, lo topamos a 5
        concurrency = min(options['concurrency'], 5) 
        continuous = options['continuous']
        batch_delay = options['batch_delay']

        self.stdout.write(self.style.SUCCESS("=" * 65))
        self.stdout.write(self.style.SUCCESS(f"🚀 SILICON VALLEY SERP RESOLVER INICIADO"))
        self.stdout.write(self.style.SUCCESS(f"⚙️ Lote: {limit} | Concurrencia: {concurrency} | Modo Continuo: {continuous}"))
        self.stdout.write(self.style.SUCCESS("=" * 65))

        engine = SERPResolverEngine(concurrency_limit=concurrency)
        total_processed = 0
        start_time = time.time()

        try:
            while not self.stop_requested:
                # Consultar la cola de trabajo en vivo
                pending_count = Institution.objects.filter(website__isnull=True, is_active=True).count()
                
                if pending_count == 0:
                    self.stdout.write(self.style.SUCCESS("\n🏆 INBOX ZERO: No hay más colegios sin URL en la base de datos."))
                    break

                self.stdout.write(self.style.WARNING(f"\n📥 Cola actual: {pending_count} prospectos ciegos. Procesando lote de {limit}..."))
                
                # 2. Disparar el Motor
                engine.resolve_missing_urls(limit=limit)
                total_processed += limit

                # Si no estamos en modo continuo, o si nos pidieron parar, rompemos el bucle
                if not continuous or self.stop_requested:
                    break

                # 3. Enfriamiento (Cool-down) inteligente entre lotes
                self.stdout.write(self.style.NOTICE(f"⏱️ Lote terminado. Enfriando radar por {batch_delay}s para evadir firewalls..."))
                
                # Sleep interrumpible (revisa la señal de apagado cada segundo)
                for _ in range(batch_delay):
                    if self.stop_requested: break
                    time.sleep(1)

        except Exception as e:
            # En un entorno real, aquí se envía la traza a Sentry o Datadog
            self.stdout.write(self.style.ERROR(f"\n❌ FATAL EXCEPTION: {str(e)}"))
        finally:
            # 4. Telemetría de Salida (Siempre se ejecuta, haya error o no)
            elapsed = time.time() - start_time
            self.stdout.write(self.style.SUCCESS("\n" + "=" * 65))
            self.stdout.write(self.style.SUCCESS(f"🛑 EJECUCIÓN DEL MOTOR FINALIZADA"))
            self.stdout.write(self.style.SUCCESS(f"📊 Colegios Enviados a Resolución: {total_processed}"))
            self.stdout.write(self.style.SUCCESS(f"⏱️ Tiempo Total de Operación: {elapsed:.2f} segundos"))
            self.stdout.write(self.style.SUCCESS("=" * 65))
