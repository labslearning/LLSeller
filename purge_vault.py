import os
import django

# Inicialización del Kernel de Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')
django.setup()

from sales.models import Institution, Interaction, Contact, TechProfile, DeepForensicProfile

def nuclear_purge():
    print("☢️ INICIANDO PROTOCOLO DE PURGA NUCLEAR...")
    
    try:
        # Borramos en orden inverso para evitar conflictos de Foreign Key
        counts = {
            "Interacciones": Interaction.objects.all().delete()[0],
            "Contactos": Contact.objects.all().delete()[0],
            "Perfiles Tech": TechProfile.objects.all().delete()[0],
            "Perfiles Forenses": DeepForensicProfile.objects.all().delete()[0],
            "Instituciones": Institution.objects.all().delete()[0],
        }
        
        for key, value in counts.items():
            print(f"[-] {key} eliminados: {value}")
            
        print("\n✅ OPERACIÓN COMPLETADA: La base de datos está limpia.")
        
    except Exception as e:
        print(f"❌ FALLO CRÍTICO DURANTE LA PURGA: {e}")

if __name__ == "__main__":
    nuclear_purge()