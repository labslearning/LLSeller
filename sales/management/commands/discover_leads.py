
from django.core.management.base import BaseCommand
from sales.engine.discovery_engine import OSMDiscoveryEngine

class Command(BaseCommand):
    help = 'Motor de Descubrimiento (Tier God): Extrae colegios del mundo usando OpenStreetMap y los inyecta en la BD.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--city', 
            type=str, 
            required=True, 
            help='Nombre de la ciudad exacta (Ej: "Bogotá", "Madrid", "Lima", "Chía")'
        )
        parser.add_argument(
            '--country', 
            type=str, 
            required=True, 
            help='País de búsqueda (Ej: "Colombia", "España", "Perú")'
        )
        parser.add_argument(
            '--state', 
            type=str, 
            required=False, 
            help='(Opcional) Región, Estado o Departamento para mayor precisión (Ej: "Cundinamarca").'
        )

    def handle(self, *args, **options):
        city = options['city']
        country = options['country']
        state = options.get('state')

        # Feedback visual en la consola
        self.stdout.write(self.style.WARNING(f"🚀 Iniciando Discovery Engine..."))
        self.stdout.write(self.style.WARNING(f"📍 Objetivo: {city}, {state if state else ''} {country}"))
        self.stdout.write(self.style.WARNING(f"📡 Conectando con satélites de OpenStreetMap..."))
        self.stdout.write("-" * 50)
        
        try:
            # Instanciar el motor y disparar
            engine = OSMDiscoveryEngine()
            engine.discover_and_inject(city=city, country=country, state=state)
            
            self.stdout.write("-" * 50)
            self.stdout.write(self.style.SUCCESS("✅ Misión de Descubrimiento completada exitosamente. Revisa tu panel de Django."))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Fallo crítico en la misión: {str(e)}"))
