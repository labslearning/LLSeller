import time
from django.core.management.base import BaseCommand
from django.db import connection
from sales.models import Institution

class Command(BaseCommand):
    help = "🔥 [GOD TIER] Protocolo Tierra Arrasada: Purga O(1) de la Matriz de Ventas."

    def handle(self, *args, **kwargs):
        self.stdout.write(self.style.ERROR(
            "\n╔══════════════════════════════════════════════════════════════════════════════╗"
            "\n║ 🔥 INICIANDO PROTOCOLO TIERRA ARRASADA (O(1) CASCADING TRUNCATE)               ║"
            "\n╚══════════════════════════════════════════════════════════════════════════════╝\n"
        ))

        table_name = Institution._meta.db_table

        self.stdout.write(self.style.WARNING("⚠️ ADVERTENCIA: Esta acción vaporizará todas las Instituciones, Contactos e Interacciones."))
        confirm = input("¿Confirmar código de autorización nuclear? (Escribe 'HYDRA' para proceder): ")

        if confirm != 'HYDRA':
            self.stdout.write(self.style.SUCCESS("🛡️ Secuencia abortada. Los datos están a salvo."))
            return

        start_time = time.time()
        self.stdout.write(self.style.WARNING(f"⚡ Ejecutando purga en el Kernel PostgreSQL sobre la tabla '{table_name}'..."))

        try:
            # [GOD TIER DELETION]: O(1) Truncate.
            # RESTART IDENTITY: Reinicia los UUIDs/IDs a cero.
            # CASCADE: Destruye automáticamente los Contactos e Interacciones dependientes sin usar Python.
            with connection.cursor() as cursor:
                cursor.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE;')
                
            elapsed = round(time.time() - start_time, 4)
            self.stdout.write(self.style.SUCCESS(
                f"\n✅ PURGA CUÁNTICA COMPLETADA EN {elapsed}s."
            ))
            self.stdout.write(self.style.SUCCESS("Sector estéril. El sistema está listo para recibir inteligencia real."))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ [FALLO CRÍTICO] Error en la base de datos: {e}"))