import time
from django.core.management.base import BaseCommand
from django.db import connection
from sales.models import Institution

class Command(BaseCommand):
    help = "🔥 [GOD TIER] Protocolo Tierra Arrasada: Purga Cuántica O(1) de la Matriz de Ventas."

    def handle(self, *args, **kwargs):
        self.stdout.write(self.style.ERROR(
            "\n" + "═" * 80 +
            "\n║ ☢️  INICIANDO PROTOCOLO TIERRA ARRASADA (O(1) CASCADING TRUNCATE)          ║" +
            "\n" + "═" * 80 + "\n"
        ))

        table_name = Institution._meta.db_table

        self.stdout.write(self.style.WARNING("⚠️  ADVERTENCIA DE SEGURIDAD CRÍTICA ⚠️"))
        self.stdout.write(self.style.WARNING("Esta acción vaporizará TODAS las Instituciones, Contactos, Perfiles Tech e Interacciones."))
        self.stdout.write(self.style.WARNING("Los contadores de base de datos se reiniciarán a CERO.\n"))
        
        confirm = input("¿Confirmar código de autorización nuclear? (Escribe 'HYDRA' para proceder): ")

        if confirm != 'HYDRA':
            self.stdout.write(self.style.SUCCESS("\n🛡️ Secuencia de detonación abortada. Los datos están a salvo."))
            return

        start_time = time.time()
        self.stdout.write(self.style.WARNING(f"\n⚡ Inyectando orden de aniquilación en el Kernel PostgreSQL sobre '{table_name}'..."))

        try:
            # [GOD TIER DELETION]: O(1) Truncate.
            # RESTART IDENTITY: Reinicia las secuencias de IDs automáticos.
            # CASCADE: Aniquila todas las tablas hijas sin cargar objetos en memoria.
            with connection.cursor() as cursor:
                cursor.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE;')
                
            elapsed = round(time.time() - start_time, 4)
            self.stdout.write(self.style.SUCCESS(
                f"✅ PURGA CUÁNTICA COMPLETADA EN {elapsed} SEGUNDOS."
            ))
            self.stdout.write(self.style.SUCCESS("Sector estéril. La base de datos está en blanco y lista para nueva inteligencia."))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ [FALLO CRÍTICO] La base de datos resistió el impacto: {e}"))