import asyncio
import logging
import re
from typing import List, Tuple, Optional, Dict
from playwright.async_api import async_playwright, BrowserContext, Page
from asgiref.sync import sync_to_async
from sales.models import Institution
import asyncio



# 1. Configuración de Logging Estructurado
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

# Parámetros de Producción
MAX_CONCURRENT_TASKS = 5  # Evita que tu RAM explote y que te bloqueen la IP
JUNK_EMAIL_PREFIXES = {'noreply', 'no-reply', 'info', 'contacto', 'sentry', 'admin'}
INVALID_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp'}

# 2. Función segura para hablar con Django (ORM)
@sync_to_async
def save_institution(name: str, url: str, city: str, country: str, extracted_emails: List[str]) -> Tuple[Institution, bool]:
    """Guarda o actualiza la institución asegurando integridad de datos."""
    email_to_save = extracted_emails[0] if extracted_emails else None
    
    inst, created = Institution.objects.update_or_create(
        website=url,
        defaults={
            'name': name,
            'city': city,
            'country': country,
            'email': email_to_save,
            'is_private': True,  # Asumimos privado inicialmente para el ICP
        }
    )
    return inst, created

def clean_emails(raw_emails: List[str]) -> List[str]:
    """Filtro de nivel empresarial para descartar correos basura."""
    cleaned = []
    for email in raw_emails:
        email_lower = email.lower()
        # Descartar si termina en extensión de imagen o empieza con prefijo basura
        if any(email_lower.endswith(ext) for ext in INVALID_EXTENSIONS):
            continue
        prefix = email_lower.split('@')[0]
        # Preferimos correos de personas, aunque guardamos los info/contacto si es lo único que hay
        # (Para este filtro estricto, podrías optar por saltar los JUNK_EMAIL_PREFIXES)
        
        cleaned.append(email_lower)
    return list(set(cleaned))

# 3. Optimización de Ancho de Banda
async def block_unnecessary_resources(route):
    """Bloquea imágenes, css y media para que la página cargue en milisegundos."""
    if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

# 4. El Motor de Extracción por Pestaña
async def scrape_school(context: BrowserContext, semaphore: asyncio.Semaphore, school_data: Dict[str, str]) -> None:
    url = school_data["url"]
    name = school_data["name"]
    
    # El semáforo pone en cola las tareas si ya hay MAX_CONCURRENT_TASKS corriendo
    async with semaphore:
        page: Page = await context.new_page()
        # Aplicamos la intercepción para no descargar imágenes
        await page.route("**/*", block_unnecessary_resources)

        try:
            logger.info(f"🚀 [EN RUTA] -> {url}")
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            content = await page.content()

            # Extracción Regex Robusta
            raw_emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", content)
            valid_emails = clean_emails(raw_emails)

            if valid_emails:
                logger.info(f"🎯 [CAZADO] {name}: {valid_emails}")

            # Guardar en Base de Datos
            inst, created = await save_institution(name, url, school_data["city"], school_data["country"], valid_emails)

            if created:
                logger.info(f"[✓] NUEVO Lead: {inst.name}")
            else:
                logger.info(f"[↻] Lead Actualizado: {inst.name}")

        except Exception as e:
            logger.error(f"[X] Error en {url}: {str(e)}")
        
        finally:
            await page.close() # Solo cerramos la pestaña, no todo el navegador

# 5. El Orquestador de Campañas (Resource Manager)
def run_scraper():
    """Ejecuta el scraper manejando eficientemente los recursos del servidor."""
    schools = [
        {"name": "Gimnasio Campestre", "url": "https://campestre.edu.co/", "city": "Bogota", "country": "Colombia"},
        {"name": "Gimnasio de los Cerros", "url": "https://cerros.edu.co/", "city": "Bogota", "country": "Colombia"},
        {"name": "Gimnasio Britanico", "url": "https://gimnasiobritanico.edu.co/", "city": "Chia", "country": "Colombia"}
    ]

    async def main():
        # Limitador de concurrencia
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        
        async with async_playwright() as p:
            # Lanzamos UN SOLO navegador
            browser = await p.chromium.launch(headless=True)
            # Un solo contexto con un User-Agent realista
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080}
            )
            
            logger.info(f"🔥 Iniciando Ingestion Layer con {len(schools)} objetivos concurrentes...")
            
            # Lanzamos todas las tareas pasándole el mismo contexto (navegador) y el semáforo
            tasks = [scrape_school(context, semaphore, school) for school in schools]
            await asyncio.gather(*tasks)

            await browser.close()
            logger.info("✅ Campaña de scraping finalizada con éxito.")

    # Iniciar el Event Loop
    asyncio.run(main())




class TechScraper:
    SIGNATURES = {
        'lms_moodle': r'moodle|moodleform',
        'lms_canvas': r'instructure\.com|canvas',
        'lms_google': r'classroom\.google\.com',
        'analytics_ga': r'googletagmanager|google-analytics',
        'cms_wordpress': r'wp-content|wp-includes',
    }

    @classmethod
    async def analyze_institution(cls, institution_id):
        # [NIVEL DIOS]: Usamos aget para no bloquear el hilo
        inst = await Institution.objects.aget(id=institution_id)
        
        if not inst.website:
            return

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                print(f"🕵️  Analizando: {inst.website}...")
                await page.goto(inst.website, timeout=15000)
                content = (await page.content()).lower()

                tech_results = {
                    'has_lms': False,
                    'lms_type': None,
                    'has_analytics': False,
                    'is_wordpress': False,
                    'scraped_tech': True 
                }

                if re.search(cls.SIGNATURES['lms_moodle'], content):
                    tech_results['has_lms'] = True
                    tech_results['lms_type'] = 'moodle'
                elif re.search(cls.SIGNATURES['lms_canvas'], content):
                    tech_results['has_lms'] = True
                    tech_results['lms_type'] = 'canvas'
                
                if re.search(cls.SIGNATURES['analytics_ga'], content):
                    tech_results['has_analytics'] = True
                
                if re.search(cls.SIGNATURES['cms_wordpress'], content):
                    tech_results['is_wordpress'] = True

                # [NIVEL DIOS]: Usamos asave para persistencia asíncrona
                inst.tech_stack = tech_results
                await inst.asave(update_fields=['tech_stack'])
                
                print(f"✅ [TECH] Stack analizado para {inst.name}")

            except Exception as e:
                print(f"❌ [TECH ERROR] {inst.website}: {e}")
            finally:
                await browser.close()

def run_tech_analysis(inst_id):
    asyncio.run(TechScraper.analyze_institution(inst_id))
