import time
from django.core.management.base import BaseCommand
from django.db import connection, transaction
from django.apps import apps
from django.core.cache import cache
from django.contrib.sessions.models import Session

class Command(BaseCommand):
    help = "🔥 [GOD TIER] Protocolo Obliviate V3: Aniquilación forzada (Docker Bypass)."

    def add_arguments(self, parser):
        parser.add_argument('--force', action='store_true', help='Bypass de seguridad.')

    def handle(self, *args, **options):
        if not options['force']:
            self.stdout.write(self.style.ERROR("❌ EJECUCIÓN DENEGADA: Usa el flag --force en Docker."))
            return

        self.stdout.write(self.style.ERROR("\n☢️ INICIANDO ANIQUILACIÓN FORZADA DE LA BASE DE DATOS (V3) ☢️\n"))
        start_time = time.time()

        try:
            cache.clear()
            Session.objects.all().delete()
            self.stdout.write("[-] Caché y Sesiones eliminadas.")
        except: pass

        try:
            sales_app = apps.get_app_config('sales')
            models = sales_app.get_models()
            table_names = [model._meta.db_table for model in models]

            if not table_names:
                self.stdout.write(self.style.SUCCESS("Sector ya estéril. No hay tablas que borrar."))
                return

            tables_formatted = ", ".join([f'"{table}"' for table in table_names])

            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {tables_formatted} RESTART IDENTITY CASCADE;")

            elapsed = round(time.time() - start_time, 4)
            self.stdout.write(self.style.SUCCESS(f"\n✅ PURGA CUÁNTICA (SQL) COMPLETADA EN {elapsed}s."))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ El Truncate falló por bloqueos de PostgreSQL: {e}"))
            self.stdout.write(self.style.WARNING("⚠️ Aplicando Plan B: Eliminación ORM en Cascada..."))
            try:
                models_list = list(models)
                models_list.reverse() 
                with transaction.atomic():
                    for model in models_list:
                        model.objects.all().delete()
                self.stdout.write(self.style.SUCCESS("\n✅ PURGA DE EMERGENCIA (ORM) COMPLETADA."))
            except Exception as e2:
                self.stdout.write(self.style.ERROR(f"❌ FALLO ABSOLUTO: {e2}"))
