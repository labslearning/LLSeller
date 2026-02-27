import os
import asyncio
import logging
import re
import random
import socket
import json
import uuid
import dns.asyncresolver
from typing import List, Optional, Dict, Any, Set, Tuple, Pattern, Union
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse
from datetime import datetime

from django.utils import timezone
from playwright.async_api import (
    async_playwright,
    BrowserContext,
    Page,
    Error as PlaywrightError,
    TimeoutError as PlaywrightTimeoutError,
    Route,
    Request
)
from asgiref.sync import sync_to_async
import whois
import tldextract

# Importamos el modelo desde su lugar correcto en la arquitectura de Django
from sales.models import Institution

# [APT INTEGRATION]: Importación del controlador maestro de evasión
from sales.engine.tor_controller import async_force_new_tor_identity 

# ==========================================
# CONFIGURACIÓN DE LOGGING FORENSE Y TELEMETRÍA
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('recon_engine_enterprise.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
# Cambiamos el nombre para reflejar su nueva capacidad
logger = logging.getLogger("Sovereign.OmniSniper")
logger.setLevel(logging.DEBUG)

# ==========================================
# CONFIGURACIÓN EMPRESARIAL Y OBSERVABILIDAD
# ==========================================

@dataclass
class ReconConfig:
    """
    Configuración inmutable para operaciones de alta disponibilidad.
    Diseñada para evadir WAFs modernos (Cloudflare, Akamai, AWS Shield, Datadome).
    """
    MAX_CONCURRENT: int = 1  # MODO FRANCOTIRADOR: 1 a la vez para máximo sigilo y cero baneos de IP
    GLOBAL_TIMEOUT_MS: int = 90000  # 90 segundos máximo por ciclo completo
    PAGE_LOAD_TIMEOUT_MS: int = 45000  # 45 segundos de paciencia para sitios lentos de LATAM
    MAX_RETRIES: int = 3
    DEEP_SCAN_LIMIT: int = 12  # Aumentado para analizar más secciones (portal, admisiones, staff)
    REQUEST_DELAY_MS: Tuple[int, int] = (4000, 12000)  # Jitter: Pausa pseudo-aleatoria

    # User Agents rotativos (Tier 1 Desktop & Mobile - Actualizados)
    USER_AGENTS: List[str] = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPad; CPU OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1"
    ])

    # Viewports realistas (Resoluciones de pantalla del mercado actual)
    VIEWPORTS: List[Dict[str, int]] = field(default_factory=lambda: [
        {'width': 1920, 'height': 1080},  # Desktop HD
        {'width': 1366, 'height': 768},   # Laptops estándar
        {'width': 1536, 'height': 864},   # Laptops modernas Windows
        {'width': 1440, 'height': 900},   # MacBook Air/Pro 13"
        {'width': 2560, 'height': 1440},  # Monitores 2K
        {'width': 390, 'height': 844},    # iPhone 12/13/14
        {'width': 414, 'height': 896}     # iPhone 11 Pro Max
    ])

    # Cabeceras (Headers) diseñadas para pasar análisis heurístico de Cloudflare
    CUSTOM_HEADERS: Dict[str, str] = field(default_factory=lambda: {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "es-CO,es-419;q=0.9,es;q=0.8,en-US;q=0.7,en;q=0.6",
        "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0"
    })

# ==========================================
# FIRMAS DE INTELIGENCIA (FINGERPRINTING)
# ==========================================

class ReconSignatures:
    """
    Repositorio masivo de firmas tecnológicas y semánticas.
    Actualizado para capturar el 99% de las EdTech y herramientas SaaS.
    """
    TECH: Dict[str, Pattern] = {
        # 🔥 TIER 1: LMS Premium (Objetivos de alto valor) 🔥
        'lms_schoolnet': re.compile(r'schoolnet|sieweb|redcol\.co|portal\.schoolnet|login\.sieweb|carvajal\.com', re.I),
        'lms_cibercolegios': re.compile(r'cibercolegios\.com|v3\.cibercolegios|login\.cibercolegios', re.I),
        'lms_phidias': re.compile(r'phidias\.co|phidias\.cloud|phidias-static|app\.phidias|phidias\.js|\.phidias\.co', re.I),
        'lms_educamos': re.compile(r'educamos\.com|sm-educamos|plataformaeducamos|edelvives', re.I),
        
        # 🟢 TIER 2: LMS Open Source / Masivos 🟢
        'lms_moodle': re.compile(r'moodle|moodleform|pluginfile\.php|theme/moodle|/login/index\.php|moodlesession', re.I),
        'lms_canvas': re.compile(r'instructure\.com|canvas-lms|canvas\.js', re.I),
        'lms_google': re.compile(r'classroom\.google\.com|google-workspace|google\.com/edu', re.I),
        'lms_microsoft': re.compile(r'teams\.microsoft\.com|education\.microsoft', re.I),
        
        # 🟡 TIER 3: Otros LMS y Plataformas Regionales 🟡
        'lms_sapred': re.compile(r'sapred\.com|plataformadecolegios|sapred\.net', re.I),
        'lms_gnosoft': re.compile(r'gnosoft\.com\.co|gnosoft\.com|gnosoft-portal', re.I),
        'lms_schoology': re.compile(r'schoology\.com|schoology-app', re.I),
        'lms_blackboard': re.compile(r'blackboard\.com|bbhosted\.com', re.I),
        'lms_edmodo': re.compile(r'edmodo\.com', re.I),
        'lms_sakai': re.compile(r'sakai-project|portal/site', re.I),
        'lms_chamilo': re.compile(r'chamilo\.org|main/css/chamilo', re.I),

        # 🌐 ECOSISTEMA CMS Y CONSTRUCTORES
        'cms_wordpress': re.compile(r'wp-content|wp-includes|wp-json|/wp-|yoast|elementor', re.I),
        'cms_drupal': re.compile(r'drupal|sites/default/files', re.I),
        'cms_joomla': re.compile(r'joomla|/media/system/js', re.I),
        'cms_wix': re.compile(r'wix\.com|wixsite\.com|_wix', re.I),
        'cms_squarespace': re.compile(r'squarespace\.com|static\d+\.squarespace', re.I),

        # 💼 CRM & MARKETING
        'crm_hubspot': re.compile(r'hs-scripts|hs-static|hubspot\.com', re.I),
        'crm_salesforce': re.compile(r'salesforce\.com|sfdc\.net|pardot', re.I),
        'crm_rdstation': re.compile(r'rdstation|rd-station', re.I),
        'analytics_ga': re.compile(r'googletagmanager\.com|google-analytics\.com/ga\.js', re.I),
        'analytics_matomo': re.compile(r'matomo\.js|piwik\.js', re.I),
        'analytics_fb_pixel': re.compile(r'connect\.facebook\.net/en_US/fbevents\.js|fbq\(', re.I),

        # 🛡️ INFRAESTRUCTURA Y SEGURIDAD
        'security_cloudflare': re.compile(r'__cf_bm|cloudflare-static|cdn-cgi|cf-Ray', re.I),
        'security_akamai': re.compile(r'akamai\.net|akamaitechnologies|akamaized\.net', re.I),
        'security_aws_shield': re.compile(r'awsglobalaccelerator', re.I),
        'cdn_cloudfront': re.compile(r'cloudfront\.net|d[0-9A-Za-z]+\.cloudfront', re.I),
        'cdn_fastly': re.compile(r'fastly\.net|fastly-insights', re.I),

        # 💰 PASARELAS DE PAGO (Latam Focus)
        'payment_payu': re.compile(r'payu\.com|payulatam\.com', re.I),
        'payment_epayco': re.compile(r'epayco\.co|epayco\.com', re.I),
        'payment_mercadopago': re.compile(r'mercadopago\.com|mp-merchant', re.I),
        'payment_wompi': re.compile(r'wompi\.co|wompi\.com', re.I),
        'payment_stripe': re.compile(r'stripe\.com', re.I),
    }

    BUSINESS: Dict[str, Pattern] = {
        'cert_ib': re.compile(r'bachillerato internacional|international baccalaureate|ib world school|ib\.org', re.I),
        'cert_cambridge': re.compile(r'cambridge english|cambridge assessment|cambridge international|cambridge\.org', re.I),
        'cert_efqm': re.compile(r'efqm|iso 9001|great place to study|excelencia educativa|calidad educativa', re.I),
        'is_bilingual': re.compile(r'bilingüe|bilingual school|dual language|inglés-español|formación bilingüe', re.I),
        'is_campestre': re.compile(r'campestre|country school|finca educativa|entorno natural|amplias zonas verdes', re.I),
        'is_international': re.compile(r'internacional|global school|colegio internacional|ciudadanos del mundo', re.I),
        'has_robotics': re.compile(r'robótica|stem|first lego league|olimpiadas de robótica|mecatrónica', re.I),
        'has_steam': re.compile(r'steam|ciencia tecnología|taller de programación|maker space', re.I),
        'has_inclusion': re.compile(r'educación inclusiva|necesidades educativas especiales|apoyo pedagógico', re.I),
    }

    SOCIAL: Dict[str, Pattern] = {
        'linkedin': re.compile(r'linkedin\.com/(company|school)/[a-zA-Z0-9_-]+', re.I),
        'instagram': re.compile(r'instagram\.com/[a-zA-Z0-9_.]+', re.I),
        'facebook': re.compile(r'facebook\.com/[a-zA-Z0-9.]+|fb\.me/[a-zA-Z0-9]+', re.I),
        'youtube': re.compile(r'youtube\.com/(c/|channel/|user/)?[a-zA-Z0-9_-]+', re.I),
        'twitter': re.compile(r'twitter\.com/[a-zA-Z0-9_]+|x\.com/[a-zA-Z0-9_]+', re.I),
        'tiktok': re.compile(r'tiktok\.com/@[a-zA-Z0-9_.]+', re.I),
    }

    EDU_LEVELS: Dict[str, Pattern] = {
        'maternal': re.compile(r'maternal|sala cuna|caminadores', re.I),
        'preescolar': re.compile(r'preescolar|kinder|párvulos|inicial|jardín infantil|transición', re.I),
        'primaria': re.compile(r'primaria|básica primaria|elementary|primero a quinto', re.I),
        'bachillerato': re.compile(r'bachillerato|secundaria|media|básica secundaria|media académica|high school|middle school', re.I),
        'universitario': re.compile(r'universidad|pregrado|grados|facultad|licenciatura', re.I),
        'posgrado': re.compile(r'posgrado|maestría|doctorado|especialización', re.I),
    }

    EMAIL_REGEX: Pattern = re.compile(r"(?<!\S)[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?!\S)", re.I)
    PHONE_REGEX: Pattern = re.compile(
        r"(?:\+?57\s*)?(?:3\d{2}[\s-]?\d{3}[\s-]?\d{4}|\(?60[1-9]\)?[\s-]?\d{3}[\s-]?\d{4}|[1-9]\d{2}[\s-]?\d{3}[\s-]?\d{4})",
        re.I
    )
    ADDRESS_REGEX: Pattern = re.compile(
        r"(?:Calle|Cra|Carrera|Av|Avenida|Dg|Diagonal|Tv|Transversal|Km|Kilómetro|Vía|Carrera|Avenida)\s+"
        r"[A-Za-z0-9\s.-]+(?:#|No\.?|Nro\.?|N°)\s*\d+[A-Za-z]?(?:\s*[-–]\s*\d+)?",
        re.I
    )
    MAPS_REGEX: Pattern = re.compile(
        r"(?:https?://)?(?:www\.)?(?:google\.com/maps|maps\.app\.goo\.gl|g\.page|goo\.gl/maps)/[^\s'\"<>]+",
        re.I
    )
    COORDINATES_REGEX: Pattern = re.compile(r"@(-?\d+\.\d+),(-?\d+\.\d+)", re.I)
    PLACE_ID_REGEX: Pattern = re.compile(r"!1s([a-zA-Z0-9_-]+)", re.I)
    GOOGLE_API_KEY_REGEX: Pattern = re.compile(r"AIza[0-9A-Za-z-_]{35}", re.I)

    SEO_TAGS: Dict[str, Pattern] = {
        'og_title': re.compile(r'<meta property="og:title" content="([^"]+)">', re.I),
        'og_description': re.compile(r'<meta property="og:description" content="([^"]+)">', re.I),
        'og_image': re.compile(r'<meta property="og:image" content="([^"]+)">', re.I),
        'og_url': re.compile(r'<meta property="og:url" content="([^"]+)">', re.I),
        'og_type': re.compile(r'<meta property="og:type" content="([^"]+)">', re.I),
        'twitter_card': re.compile(r'<meta name="twitter:card" content="([^"]+)">', re.I),
        'canonical': re.compile(r'<link rel="canonical" href="([^"]+)">', re.I),
    }

    SCHEMA_ORG_REGEX: Pattern = re.compile(r'<script type="application/ld\+json">([^<]+)</script>', re.I)

# ==========================================
# MÓDULOS DE UTILIDAD (HELPERS DE RED)
# ==========================================

class ReconUtils:
    """Clase estática para manipulación de redes y strings."""
    
    @staticmethod
    def extract_domain_info(url: str) -> Dict[str, Any]:
        """Extrae información del dominio garantizando la disponibilidad del dominio raíz."""
        extracted = tldextract.extract(url)
        # Construimos el dominio raíz manualmente para máxima compatibilidad y evitar fallos
        root_domain = f"{extracted.domain}.{extracted.suffix}" if extracted.suffix else extracted.domain
        return {
            'domain': extracted.domain,
            'subdomain': extracted.subdomain,
            'suffix': extracted.suffix,
            'full_domain': root_domain,
            'registrable_domain': root_domain
        }

    @staticmethod
    async def get_whois_info(domain: str) -> Dict[str, Any]:
        """Obtiene información WHOIS del dominio (Enviado a Thread para no bloquear el Event Loop de Asyncio)."""
        try:
            domain_info = await asyncio.to_thread(whois.whois, domain)
            return {
                'registrar': getattr(domain_info, 'registrar', 'N/A'),
                'creation_date': str(getattr(domain_info, 'creation_date', 'N/A')),
                'expiration_date': str(getattr(domain_info, 'expiration_date', 'N/A')),
                'name_servers': getattr(domain_info, 'name_servers', []),
                'emails': list(set([
                    str(contact.email) for contact in getattr(domain_info, 'contacts', [])
                    if hasattr(contact, 'email') and contact.email
                ])),
                'org': getattr(domain_info, 'org', 'N/A')
            }
        except Exception as e:
            return {'error': str(e)}

    @staticmethod
    async def get_dns_records(domain: str) -> Dict[str, Any]:
        """Obtiene registros DNS críticos (SPF, DKIM, DMARC, MX) usando asyncresolver puro."""
        records = {}
        try:
            resolver = dns.asyncresolver.Resolver()
            resolver.lifetime = 5.0
            
            # Registros MX (servidores de correo)
            mx_records = []
            try:
                answers = await resolver.resolve(domain, 'MX')
                mx_records = [str(r.exchange) for r in answers]
            except Exception: pass

            # Registros TXT (Seguridad de Email)
            txt_records = []
            try:
                answers = await resolver.resolve(domain, 'TXT')
                txt_records = [str(r) for r in answers]
            except Exception: pass

            # Registros CNAME (Útiles para detectar servicios de terceros)
            cname_records = []
            try:
                answers = await resolver.resolve(f"www.{domain}", 'CNAME')
                cname_records = [str(r.target) for r in answers]
            except Exception: pass

            records = {
                'mx': mx_records,
                'txt': txt_records,
                'cname': cname_records,
                'spf': [r for r in txt_records if 'v=spf1' in r],
                'dkim': [r for r in txt_records if 'dkim=' in r],
                'dmarc': [r for r in txt_records if 'dmarc=' in r],
                'google_site_verification': [r for r in txt_records if 'google-site-verification' in r]
            }
        except Exception as e:
            records['error'] = str(e)

        return records

    @staticmethod
    def validate_json(json_str: str) -> bool:
        """Valida si un string es JSON válido de forma segura."""
        try:
            json.loads(json_str)
            return True
        except ValueError:
            return False

    @staticmethod
    def clean_text(text: str) -> str:
        """Limpia texto extraído (elimina espacios múltiples, saltos de línea y ruido)."""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'[^\x00-\x7F]+', ' ', text)  # Eliminar caracteres no ASCII si es necesario
        return text

# ==========================================
# MÓDULO DE SÍNTESIS CON INTELIGENCIA ARTIFICIAL
# ==========================================

class AIInsightsGenerator:
    """
    Generador de insights y correos de venta usando Modelos de Lenguaje Avanzados (LLMs).
    Obliga al motor de IA a responder en JSON estructurado.
    """

    def __init__(self, api_key: str = None, model: str = "gpt-4o"):
        self.api_key = api_key
        self.model = model
        self.client = None
        if api_key:
            try:
                from openai import OpenAI
                self.client = OpenAI(api_key=api_key)
            except ImportError:
                logger.warning("❌ OpenAI SDK no está instalado. Usa `pip install openai` para activar la IA.")

    def generate_prompt(self, institution_data: Dict[str, Any]) -> str:
        """Construye un prompt de sistema inyectando los datos extraídos por el scraper."""
        tech_stack = institution_data.get('tech_stack', {}).get('technologies', {})
        bi_data = institution_data.get('tech_stack', {}).get('business_intel', {})

        prompt = f"""
        Eres un VP de Ventas Senior especializado en soluciones EdTech (LMS y CRM) para colegios en Latinoamérica.
        Analiza el perfil técnico y comercial de la siguiente institución educativa y genera el resultado ÚNICAMENTE en un formato JSON válido con las siguientes claves estrictas:
        
        {{
            "executive_summary": "Un string de máximo 3 líneas destacando lo más relevante.",
            "sales_recommendations": ["Táctica 1", "Táctica 2", "Táctica 3"],
            "prospect_classification": "Alto, Medio o Bajo (con breve justificación)",
            "sales_email_draft": "String con el borrador de un cold email (max 150 palabras) atacando sus dolores actuales"
        }}

        ---
        **Datos Crudos de Inteligencia**:
        - Nombre Institución: {institution_data.get('name', 'Desconocido')}
        - LMS Actual Detectado: {tech_stack.get('lms_type', 'Ninguno / In-House')}
        - CMS Web: {'Wordpress' if tech_stack.get('wordpress') else 'Otro'}
        - Niveles Educativos: {', '.join(bi_data.get('education_levels', [])) or 'Desconocidos'}
        - Señales de Prestigio (VIP): {', '.join(bi_data.get('premium_flags', [])) or 'Ninguna'}
        - Triggers Técnicos Detectados: {', '.join(bi_data.get('sales_triggers', []))}
        """
        return prompt.strip()

    def generate_insights(self, institution_data: Dict[str, Any]) -> Dict[str, Any]:
        """Envía el prompt a la API y asegura una respuesta JSON parseable."""
        if not self.client:
            return {"error": "Cliente de IA no configurado. Proporciona una API key válida."}

        prompt = self.generate_prompt(institution_data)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Eres una máquina experta en B2B que responde EXCLUSIVAMENTE en formato JSON nativo sin Markdown adicional (sin bloques de código ```json)."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1500,
                response_format={"type": "json_object"}  # 🔥 Característica clave para pipelines automáticos
            )

            raw_content = response.choices[0].message.content
            insights = json.loads(raw_content)
            insights['model_used'] = self.model
            insights['generated_at'] = datetime.now().isoformat()

            return insights

        except Exception as e:
            return {"error": f"Error crítico al generar insights de IA: {str(e)}"}


# ==========================================
# NÚCLEO DE EXTRACCIÓN (THE GHOST SNIPER - OMNI SNIPER)
# ==========================================

class B2BReconEngine:
    """
    [GOD TIER - APT LEVEL]
    Motor de Inteligencia de Mercado (OSINT) Asíncrono.
    Incluye evasión extrema de WAFs, gestión de memoria y heurísticas DOM.
    Ahora con Resiliencia de Red Distribuida vía Tor.
    """

    def __init__(self, config: ReconConfig = ReconConfig()):
        self.config = config
        self.semaphore = asyncio.Semaphore(config.MAX_CONCURRENT)

    async def _check_dns_resolution(self, hostname: str) -> bool:
        """Verifica si el dominio existe antes de lanzar el navegador pesado."""
        loop = asyncio.get_running_loop()
        try:
            await loop.getaddrinfo(hostname, None)
            return True
        except socket.gaierror:
            return False

    async def _apply_stealth(self, page: Page):
        """
        🔥 INYECCIÓN DE JS ANTICUERPOS (STEALTH MODE TIER GOD) 🔥
        Engaña a Cloudflare, Akamai, Datadome y reCAPTCHA falsificando APIs del navegador.
        """
        await page.add_init_script("""
            // 1. Ocultar bandera de automatización (Puppeteer/Playwright marker)
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            
            // 2. Falsificar Plugins (Los bots headless no tienen plugins, los humanos sí)
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            
            // 3. Falsificar lenguajes
            Object.defineProperty(navigator, 'languages', {get: () => ['es-CO', 'es', 'en-US', 'en']});

            // 4. WebGL Spoofing (Crítico para evadir análisis de huellas dactilares gráficas)
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) return 'Intel Inc.'; // vendor
                if (parameter === 37446) return 'Intel Iris OpenGL Engine'; // renderer
                return getParameter.call(this, parameter);
            };

            // 5. Override de Chrome Runtime (Solo existe en navegadores reales)
            window.chrome = {
                runtime: {},
                app: {isInstalled: false},
                webstore: {}
            };

            // 6. Simular permisos de notificaciones interactivos
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                Promise.resolve({state: Notification.permission}) :
                originalQuery(parameters)
            );

            // 7. Simular conexión de red de un usuario real (4G)
            Object.defineProperty(navigator, 'connection', {
                value: {
                    downlink: 10,
                    effectiveType: '4g',
                    rtt: 50,
                    saveData: false
                }
            });
            
            // 8. Falsificar Hardware Concurrency (Cores de CPU ficticios)
            Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
            
            // 9. Falsificar memoria de dispositivo
            Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
        """)

    async def _intercept_resources(self, route: Route, request: Request):
        """
        Optimización extrema: Bloquea recursos pesados e inútiles en memoria RAM.
        Acelera el escaneo un 400% y previene crashes de OutOfMemory.
        """
        blocked_types = {"image", "media", "font", "stylesheet", "websocket", "other", "eventsource"}
        blocked_domains = {
            "google-analytics.com", "analytics.twitter.com", "doubleclick.net",
            "facebook.com", "tiktok.com", "googletagmanager.com",
            "adservice.google.com", "cdn.instagram.com", "platform.twitter.com",
            "[youtube.com/embed](https://youtube.com/embed)", "vimeo.com"
        }

        req_url = request.url.lower()
        resource_type = request.resource_type

        if resource_type in blocked_types or any(domain in req_url for domain in blocked_domains):
            await route.abort()
        else:
            await route.continue_()

    async def _simulate_human_behavior(self, page: Page):
        """Inyecta eventos de movimiento de ratón y scroll suavizados usando Curvas de Bézier."""
        await page.evaluate("""() => {
            // Mouse Movement Simulation
            const moveMouse = (x, y) => {
                const event = new MouseEvent('mousemove', {
                    clientX: x, clientY: y, bubbles: true, cancelable: true, view: window
                });
                document.dispatchEvent(event);
            };

            const humanLikeMove = () => {
                const startX = Math.random() * window.innerWidth;
                const startY = Math.random() * window.innerHeight;
                const endX = Math.random() * window.innerWidth;
                const endY = Math.random() * window.innerHeight;

                for (let t = 0; t <= 1; t += 0.1) {
                    const x = startX + (endX - startX) * t;
                    const y = startY + (endY - startY) * t + Math.sin(t * Math.PI) * 20;
                    moveMouse(x, y);
                }
            };

            // Smooth Inercial Scrolling
            const humanLikeScroll = () => {
                const start = window.scrollY;
                const target = Math.random() * (document.body.scrollHeight || window.innerHeight * 2);
                const duration = 1000 + Math.random() * 2000; 

                let startTime = null;
                const scroll = (timestamp) => {
                    if (!startTime) startTime = timestamp;
                    const progress = timestamp - startTime;
                    const percentage = Math.min(progress / duration, 1);

                    window.scrollTo(0, start + (target - start) *
                        (percentage < 0.5 ? 2 * Math.pow(percentage, 2) : -1 + (4 - 2 * percentage) * percentage));

                    if (percentage < 1) requestAnimationFrame(scroll);
                };
                requestAnimationFrame(scroll);
            };

            humanLikeMove();
            humanLikeScroll();
        }""")
        # Pausa aleatoria para engañar a los WAF basados temporalmente
        await asyncio.sleep(random.uniform(1.2, 3.5))

    # ==========================================
    # [APT TACTIC]: NAVEGACIÓN RESILIENTE
    # ==========================================
    async def _navigate_with_stealth(self, page: Page, url: str) -> bool:
        """
        Navegación quirúrgica. Si detecta WAF, muta el clúster entero.
        """
        for attempt in range(self.config.MAX_RETRIES):
            try:
                logger.info(f"🎯 [TARGET] {url} | Intento {attempt + 1}")
                # Ajustamos la estrategia de carga según el intento
                strategy = "networkidle" if attempt == self.config.MAX_RETRIES - 1 else "domcontentloaded"
                response = await page.goto(url, wait_until=strategy, timeout=self.config.PAGE_LOAD_TIMEOUT_MS)
                
                content = await page.content()
                # Detección de muros de fuego (WAF)
                is_blocked = any(term in content.lower() for term in [
                    "access denied", "cloudflare", "captcha", "checking your browser",
                    "403 forbidden", "ip has been blocked"
                ])
                
                if (response and response.status in [403, 429]) or is_blocked:
                    logger.warning(f"🚫 [BLOCKED] {url}. Rotando Identidad Global...")
                    # LLAMADA AL CONTROLADOR APT
                    await async_force_new_tor_identity(strict_verification=True)
                    continue 
                
                return True
            except PlaywrightTimeoutError:
                 logger.debug(f"⏳ [{url}] Timeout (Att: {attempt+1}). Analizando DOM parcial.")
                 # Si hay timeout pero tenemos página, intentamos extraer lo que haya
                 return True 
            except Exception as e:
                logger.error(f"⚠️ [NET ERROR] {url}: {str(e)}")
                # Si falla por red cruda, intentamos rotar sin verificación estricta para ser más rápidos
                await async_force_new_tor_identity(strict_verification=False)
                await asyncio.sleep(random.uniform(2.0, 5.0))
                
        return False


    async def _extract_deep_links(self, page: Page, base_url: str) -> List[str]:
        """Estrategia 'Spelunking': Busca páginas internas ricas en datos (Contacto, Staff, Admisión)."""
        keywords = {
            'contacto', 'contact', 'nosotros', 'staff', 'directorio', 'equipo',
            'portal', 'ingreso', 'admision', 'admissions', 'about', 'quienes-somos',
            'trabaja-con-nosotros', 'empleos', 'vacantes', 'transparencia',
            'gobernanza', 'acreditaciones', 'certificaciones', 'matriculas',
            'campus', 'instalaciones'
        }

        domain = urlparse(base_url).netloc
        discovery_pool = set()

        try:
            links = await page.query_selector_all("a[href]")
            for link in links:
                href = await link.get_attribute("href")
                if not href or href.startswith(('javascript:', 'mailto:', 'tel:', '#')):
                    continue

                full_url = urljoin(base_url, href)
                if urlparse(full_url).netloc == domain and any(k in full_url.lower() for k in keywords):
                    discovery_pool.add(full_url)

            # Escaneo de menús ocultos
            dropdowns = await page.query_selector_all(".dropdown-menu a[href], .nav-menu a[href]")
            for menu in dropdowns:
                href = await menu.get_attribute("href")
                if href:
                    full_url = urljoin(base_url, href)
                    if urlparse(full_url).netloc == domain:
                        discovery_pool.add(full_url)

        except Exception as e:
            logger.debug(f"Aviso en extracción de Deep Links: {e}")

        return list(discovery_pool)[:self.config.DEEP_SCAN_LIMIT]

    async def _extract_google_maps_data(self, page: Page) -> Dict[str, Any]:
        """Extrae coordenadas de mapas incrustados para geo-localización pasiva."""
        maps_data = {'coordinates': None, 'place_id': None, 'query': None, 'embedded_urls': set(), 'api_keys': set()}

        try:
            iframes = await page.query_selector_all('iframe[src*="[google.com/maps](https://google.com/maps)"]')
            for iframe in iframes:
                src = await iframe.get_attribute("src")
                if not src: continue
                
                maps_data['embedded_urls'].add(src)
                
                coord_match = ReconSignatures.COORDINATES_REGEX.search(src)
                if coord_match:
                    maps_data['coordinates'] = {'lat': coord_match.group(1), 'lng': coord_match.group(2)}

                place_match = ReconSignatures.PLACE_ID_REGEX.search(src)
                if place_match: maps_data['place_id'] = place_match.group(1)

            # Buscar keys de API filtradas en el código
            scripts = await page.query_selector_all('script')
            for script in scripts:
                content = await script.inner_text()
                keys = ReconSignatures.GOOGLE_API_KEY_REGEX.findall(content)
                for key in keys: maps_data['api_keys'].add(key)

        except Exception: pass

        maps_data['embedded_urls'] = list(maps_data['embedded_urls'])
        maps_data['api_keys'] = list(maps_data['api_keys'])
        return maps_data

    async def _extract_seo_metadata(self, page: Page) -> Dict[str, Any]:
        """Levanta datos semánticos de las etiquetas HEAD."""
        seo_data = {}
        try:
            content = await page.content()
            for key, pattern in ReconSignatures.SEO_TAGS.items():
                match = pattern.search(content)
                if match: seo_data[key] = match.group(1).strip()

            schemas = ReconSignatures.SCHEMA_ORG_REGEX.findall(content)
            if schemas:
                valid_schemas = [json.loads(m) for m in schemas if ReconUtils.validate_json(m)]
                if valid_schemas: seo_data['schema_org'] = valid_schemas
        except Exception: pass
        return seo_data

    async def _extract_education_levels(self, page: Page) -> List[str]:
        """Identifica de qué tipo de colegio se trata (Preescolar vs Bachillerato)."""
        levels = set()
        try:
            content = await page.content()
            for level, pattern in ReconSignatures.EDU_LEVELS.items():
                if pattern.search(content): levels.add(level)
        except Exception: pass
        return list(levels)

    async def _extract_business_signals(self, page: Page) -> Dict[str, Any]:
        """Detector de Dinero: Busca señales de presupuestos altos (IB, Bilingüismo)."""
        signals = {}
        try:
            content = await page.content()
            for signal, pattern in ReconSignatures.BUSINESS.items():
                if pattern.search(content): signals[signal] = True

            year_match = re.search(r'(?:fundado en|desde|año)\s+(\d{4})', content, re.I)
            if year_match: signals['foundation_year'] = int(year_match.group(1))
        except Exception: pass
        return signals

    async def _extract_social_media(self, page: Page) -> Dict[str, str]:
        social_media = {}
        try:
            content = await page.content()
            for network, pattern in ReconSignatures.SOCIAL.items():
                match = pattern.search(content)
                if match: social_media[network] = match.group(0)
        except Exception: pass
        return social_media

    async def _extract_contact_info(self, page: Page) -> Dict[str, Set[str]]:
        """El Rastreador de Leads: Combina JS del cliente con Regex de Python para no perder nada."""
        contacts = {'phones': set(), 'whatsapp': set(), 'emails': set(), 'addresses': set()}

        try:
            # 1. Extracción Estructurada desde el DOM (Evita ofuscaciones simples)
            payload = await page.evaluate("""() => {
                const getAttr = (sel, attr) => Array.from(document.querySelectorAll(sel)).map(el => el.getAttribute(attr)).filter(Boolean);
                return {
                    tel: getAttr('a[href^="tel:"]', 'href').map(h => h.replace('tel:', '').trim()),
                    wa: getAttr('a[href*="wa.me"], a[href*="api.whatsapp.com"]', 'href'),
                    eml: getAttr('a[href^="mailto:"]', 'href').map(h => h.replace('mailto:', '').trim()),
                    addr: Array.from(document.querySelectorAll('address')).map(el => el.innerText.trim()),
                    body: document.body ? document.body.innerText.substring(0, 15000) : ''
                };
            }""")

            contacts['phones'].update(payload['tel'])
            contacts['whatsapp'].update(payload['wa'])
            contacts['emails'].update(payload['eml'])
            contacts['addresses'].update(payload['addr'])

            # 2. Extracción Regex de Fuerza Bruta sobre el Texto Visible
            search_text = payload['body']
            contacts['phones'].update(ReconSignatures.PHONE_REGEX.findall(search_text))
            contacts['emails'].update(ReconSignatures.EMAIL_REGEX.findall(search_text))
            
            for addr in ReconSignatures.ADDRESS_REGEX.findall(search_text):
                val = addr[0] if isinstance(addr, tuple) else addr
                contacts['addresses'].add(val.replace('\n', ', '))

            for wa_link in payload['wa']:
                match = re.search(r'wa\.me/(\d+)', wa_link)
                if match: contacts['whatsapp'].add(f"+{match.group(1)}")

        except Exception as e:
            logger.debug(f"Fallo menor en recolección de contactos: {e}")

        return contacts

    async def _detect_technologies(self, page: Page, domain: str) -> Dict[str, Any]:
        """
        🔥 El Analizador de Huellas Digitales 🔥
        Descarga el HTML profundo (incluyendo iframes y tokens locales) para detectar el LMS y CMS.
        """
        tech_stack = {}
        try:
            # Carga profunda del contexto de la página (No truncada a 50k para no perder el footer)
            payload = await page.evaluate("""() => {
                return {
                    scripts: Array.from(document.scripts).map(s => s.src).join(' | '),
                    iframes: Array.from(document.querySelectorAll('iframe')).map(i => i.src).join(' | '),
                    metas: Array.from(document.querySelectorAll('meta')).map(m => m.content).join(' | '),
                    links: Array.from(document.querySelectorAll('link[href]')).map(l => l.href).join(' | '),
                    html: document.documentElement.outerHTML, 
                    storage: JSON.stringify(Object.keys(localStorage || {})),
                    cookies: document.cookie
                };
            }""")

            # Generar un super-string unificado en memoria baja
            context_string = f"{payload['scripts']} {payload['iframes']} {payload['html']} {payload['metas']} {payload['links']} {payload['storage']} {payload['cookies']} {domain}".lower()

            # Barrido contra el diccionario de Firmas Tech (O(N) Complexity)
            for tech, pattern in ReconSignatures.TECH.items():
                if pattern.search(context_string):
                    tech_stack[tech] = True

            # Lógica Empresarial de Priorización de LMS
            lms_techs = [k.replace('lms_', '') for k in ReconSignatures.TECH if k.startswith('lms_') and tech_stack.get(k)]
            
            if lms_techs:
                tech_stack['has_lms'] = True
                # Priorizar plataformas de pago (Phidias, Schoolnet, Cibercolegios) frente a Open Source
                premium_lms = [l for l in lms_techs if l in ['schoolnet', 'phidias', 'cibercolegios', 'educamos']]
                tech_stack['lms_type'] = premium_lms[0] if premium_lms else lms_techs[0]
            else:
                tech_stack['has_lms'] = False

            # Detección moderna adicional
            if any(fw in context_string for fw in ['react', 'angular', 'vue', 'nextjs']):
                tech_stack['modern_frontend'] = True

        except Exception as e:
            logger.debug(f"Aviso en detección tecnológica: {e}")

        return tech_stack

    async def _check_security_headers(self, page: Page) -> Dict[str, Any]:
        """Auditoría rápida de seguridad de transporte de red."""
        headers_info = {}
        try:
            response = await page.goto(page.url, wait_until="domcontentloaded")
            headers = response.headers

            security_headers = {
                'X-Frame-Options': headers.get('x-frame-options', 'Missing'),
                'Content-Security-Policy': headers.get('content-security-policy', 'Missing'),
                'Strict-Transport-Security': headers.get('strict-transport-security', 'Missing')
            }

            headers_info['security_headers'] = security_headers
            headers_info['uses_https'] = page.url.startswith('https://')
        except Exception: pass
        return headers_info

    def _clean_emails(self, raw_emails: List[str]) -> str:
        """Heurística para encontrar el correo 'Rector/Principal' y descartar Spam Traps."""
        if not raw_emails: return ""

        bad_ext = ('.png', '.jpg', '.jpeg', '.pdf', '.js', '.css', 'sentry.io', 'wixpress.com')
        junk_prefixes = {'info@', 'contacto@', 'webmaster@', 'noreply@', 'admin@', 'hello@'}

        cleaned = {e.lower().strip() for e in raw_emails if not e.lower().strip().endswith(bad_ext) and '@' in e and len(e) > 5}
        if not cleaned: return ""

        # Aislar prioritarios (no junk)
        priority = [e for e in cleaned if not any(e.startswith(p) for p in junk_prefixes)]
        
        # Correos con nombres personales tienen prioridad absoluta (ej: carlos.gomez@colegio.edu.co)
        named_emails = [e for e in priority if '.' in e.split('@')[0]]
        
        if named_emails: return named_emails[0]
        if priority: return priority[0]
        return list(cleaned)[0]

    def _generate_sales_triggers(self, tech_data: Dict[str, Any], bi_data: Dict[str, Any]) -> List[str]:
        """Motor de Reglas de Negocio: Genera consejos tácticos para el vendedor."""
        triggers = []

        if 'cert_ib' in bi_data.get('premium_flags', []):
            triggers.append("🔥 IB World School: Presupuesto extremadamente alto. Vender calidad suprema y soporte VIP.")
        if 'cert_cambridge' in bi_data.get('premium_flags', []):
            triggers.append("📚 Certificación Cambridge: Integración internacional es su prioridad.")

        if tech_data.get('has_lms'):
            lms_actual = str(tech_data.get('lms_type', 'desconocido')).upper()
            triggers.append(f"⚙️ Usa {lms_actual}: Atacar con una campaña de migración sencilla, mejor UI/UX y soporte local.")
        else:
            triggers.append("🟢 Blue Ocean: No se detectó LMS comercial. Gran oportunidad para digitalización total.")

        if 'linkedin' in bi_data.get('social_media', {}):
            triggers.append("💼 LinkedIn Activo: Busca al Rector o Director de TI directamente por InMail.")

        return triggers

    @sync_to_async
    def _save_intelligence_to_db(self, inst_id: str, master_contacts: dict, tech_data: dict, bi_data: dict):
        """
        [DATA WAREHOUSE ADAPTER]
        Operación atómica síncrona envuelta en asincronismo.
        Mapea el JSON crudo extraído por Playwright hacia los modelos relacionales de Django.
        """
        from sales.models import Institution, TechProfile
        from django.db import transaction
        from django.utils import timezone

        with transaction.atomic():
            # 1. Bloqueo de fila exclusivo para evitar colisiones
            inst = Institution.objects.select_for_update().get(id=inst_id)
            
            # 2. Extracción de los mejores datos de contacto
            best_email = self._clean_emails(list(master_contacts.get('emails', [])))
            best_phone = list(master_contacts['phones'])[0] if master_contacts.get('phones') else None
            
            update_fields = ['last_scored_at']
            inst.last_scored_at = timezone.now()

            if best_email and not inst.email:
                inst.email = best_email
                update_fields.append('email')
                
            if best_phone and not inst.phone:
                inst.phone = best_phone
                update_fields.append('phone')

            # 3. Dynamic Lead Scoring (Cálculo de calidad del prospecto en tiempo real)
            score = 10  # Base
            if tech_data.get('has_lms'): score += 40
            if best_email: score += 25
            if best_phone: score += 15
            if bi_data.get('premium_flags'): score += 10
            
            inst.lead_score = min(score, 100) # Tope en 100
            update_fields.append('lead_score')

            inst.save(update_fields=update_fields)

            # 4. Actualización del Perfil Tecnológico (TechProfile)
            tech_profile, created = TechProfile.objects.get_or_create(institution=inst)
            tech_profile.has_lms = tech_data.get('has_lms', False)
            tech_profile.lms_provider = str(tech_data.get('lms_type', '')).lower()
            tech_profile.is_wordpress = tech_data.get('cms_wordpress', False)
            tech_profile.has_analytics = tech_data.get('analytics_ga', False)
            tech_profile.save()

            return inst.name, tech_profile.lms_provider

    async def scan_target(self, browser: BrowserContext, target: Dict[str, Any]):
        """
        Ejecución Atómica Principal.
        Orquesta todas las subrutinas sobre un solo dominio con máxima protección de fallos.
        """
        async with self.semaphore:
            target_url = target['url'].rstrip('/')
            if not target_url.startswith('http'):
                target_url = f"https://{target_url}"
            domain = urlparse(target_url).netloc

            # 1. DNS Fail-Fast
            if not await self._check_dns_resolution(domain):
                logger.warning(f"🚫 [{domain}] Dominio inaccesible a nivel DNS. Skip.")
                return

            if 'id' not in target:
                logger.error(f"⚠️ ID no provisto en el target: {domain}")
                return

            page = await browser.new_page()
            await self._apply_stealth(page)
            await page.route("**/*", lambda route, request: self._intercept_resources(route, request))

            # Estructuras maestras de recolección
            master_contacts = {'phones': set(), 'whatsapp': set(), 'emails': set(), 'addresses': set(), 'google_maps_links': set()}
            tech_data = {}
            bi_data = {
                'premium_flags': [], 'education_levels': [], 'social_media': {},
                'sales_triggers': [], 'seo_profile': {}, 'google_maps_intel': {},
                'domain_intel': {}, 'ai_ready': False
            }

            try:
                # --- NAVEGACIÓN RESILIENTE CON TOR ---
                if await self._navigate_with_stealth(page, target_url):
                    await self._simulate_human_behavior(page)

                    # --- EJECUCIÓN DEL BARRIDO FORENSE ---
                    tech_data = await self._detect_technologies(page, domain)
                    bi_data['seo_profile'] = await self._extract_seo_metadata(page)
                    bi_data['education_levels'] = await self._extract_education_levels(page)

                    business_signals = await self._extract_business_signals(page)
                    bi_data['premium_flags'] = [k for k, v in business_signals.items() if v and k != 'foundation_year']
                    if 'foundation_year' in business_signals: bi_data['foundation_year'] = business_signals['foundation_year']

                    contacts = await self._extract_contact_info(page)
                    for k in master_contacts: master_contacts[k].update(contacts.get(k, set()))

                    bi_data['google_maps_intel'] = await self._extract_google_maps_data(page)
                    bi_data['social_media'] = await self._extract_social_media(page)

                    # --- SPELUNKING (ESCANEO DE SUBSITIOS) ---
                    deep_links = await self._extract_deep_links(page, target_url)
                    for link in deep_links:
                        try:
                            # Reutilizamos la navegación resiliente para los enlaces profundos
                            if await self._navigate_with_stealth(page, link):
                                await self._simulate_human_behavior(page)
                                sub_contacts = await self._extract_contact_info(page)
                                for k in master_contacts: master_contacts[k].update(sub_contacts.get(k, set()))
                        except Exception:
                            pass # Silenciar fallos de sub-páginas rotas

                    # --- ENRIQUECIMIENTO BACKEND Y TRIGGERS ---
                    bi_data['sales_triggers'] = self._generate_sales_triggers(tech_data, bi_data)

                    # --- GUARDADO EN DB A TRAVÉS DE ADAPTADOR SEGURO ---
                    inst_name, found_lms = await self._save_intelligence_to_db(
                        inst_id=target['id'],
                        master_contacts=master_contacts,
                        tech_data=tech_data,
                        bi_data=bi_data
                    )

                    logger.info(f"✅ [{domain}] | LMS: {str(found_lms).upper() or 'NINGUNO'} | E-mails Hallados: {len(master_contacts['emails'])}")
                else:
                    raise Exception("Fallo de conexión persistente tras reintentos.")

            except Exception as e:
                logger.error(f"❌ [{domain}] Colapso en Scraper: {str(e)}")
            finally:
                await page.close()


# ==========================================
# ORQUESTADOR MAESTRO Y PUNTO DE ENTRADA
# ==========================================

async def _orchestrate(targets: Optional[List[Dict]] = None):
    """
    Inicializa el Motor Playwright y distribuye la carga de trabajo secuencialmente.
    Controla el apagado correcto para evitar Zombies en Memoria (Memory Leaks).
    """
    config = ReconConfig()
    engine = B2BReconEngine(config)
    
    # [APT TACTIC]: Configuración del proxy para el navegador
    # Asumimos que Tor expone su proxy SOCKS5 en el host
    tor_proxy = {"server": f"socks5://{os.getenv('TOR_PROXY_HOST', '127.0.0.1')}:{os.getenv('TOR_PROXY_PORT', 9050)}"}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            proxy=tor_proxy, # Todo el tráfico del orquestador pasa por Tor
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-infobars",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--disable-gpu",
                "--window-size=1920,1080"
            ]
        )

        context = await browser.new_context(
            user_agent=random.choice(config.USER_AGENTS),
            viewport=random.choice(config.VIEWPORTS),
            locale="es-CO",
            timezone_id="America/Bogota",
            ignore_https_errors=True,
            bypass_csp=True,
            java_script_enabled=True,
            geolocation={'latitude': 4.7110, 'longitude': -74.0721},  
            permissions=['geolocation'],
            proxy=tor_proxy
        )

        await context.set_extra_http_headers(config.CUSTOM_HEADERS)

        # Sembrar cookies falsas para engañar a trackers simples
        await context.add_init_script("""
            window.localStorage.setItem('session', JSON.stringify({lastVisit: new Date().toISOString()}));
            document.cookie = 'session=old_session_123; path=/; max-age=31536000';
        """)

        try:
            if not targets:
                logger.info("📡 Escaneando Toda la Base de Datos (Límite: 500)...")
                count = 0
                async for inst in Institution.objects.filter(is_active=True).order_by('-id'):
                    if count >= 500: break
                    await engine.scan_target(context, {'id': inst.id, 'name': inst.name, 'url': inst.website, 'city': inst.city})
                    await asyncio.sleep(random.uniform(config.REQUEST_DELAY_MS[0] / 1000, config.REQUEST_DELAY_MS[1] / 1000))
                    count += 1
            else:
                logger.info(f"📡 Desplegando ataque focalizado sobre {len(targets)} dominios...")
                for t in targets:
                    await engine.scan_target(context, t)
                    await asyncio.sleep(random.uniform(config.REQUEST_DELAY_MS[0] / 1000, config.REQUEST_DELAY_MS[1] / 1000))

        except Exception as e:
            logger.error(f"❌ Error crítico en Orquestador: {e}")
        finally:
            logger.info("🧹 Purgando contextos y liberando Memoria RAM...")
            await context.close()
            await browser.close()


def execute_recon(inst_id: Union[int, str, uuid.UUID, None] = None):
    """
    Punto de Entrada Universal (Síncrono) para el Admin de Django o Celery.
    Nota: Acepta UUIDs como strings, fundamental para arquitecturas modernas.
    """
    targets = None
    if inst_id:
        try:
            # Soporta tanto IDs numéricos antiguos como UUIDs modernos
            inst = Institution.objects.get(id=inst_id)
            if not inst.website:
                logger.error(f"⚠️ Operación abortada: {inst.name} carece de URL configurada.")
                return
            targets = [{'id': inst.id, 'name': inst.name, 'url': inst.website, 'city': inst.city}]
            logger.info(f"🎯 Modo Quirúrgico: Analizando {inst.name}")
        except Institution.DoesNotExist:
            logger.error(f"⚠️ Fallo: Institución {inst_id} purgada o inexistente.")
            return
        except Exception as e:
            logger.error(f"❌ Fallo de Query: {e}")
            return

    try:
        logger.info("🚀 Encendiendo el The Ghost Sniper Engine...")
        asyncio.run(_orchestrate(targets))
        logger.info("🏁 Operación concluida exitosamente.")
    except KeyboardInterrupt:
        logger.warning("⏹️ Sistema abortado manualmente por el administrador.")
    except Exception as e:
        logger.error(f"❌ Crash global en el hilo de ejecución principal: {e}")

run_recon = execute_recon

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # Permite ejecutar desde terminal: python recon_engine.py 1234
        execute_recon(inst_id=sys.argv[1])
    else:
        execute_recon()