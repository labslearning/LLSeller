"""
================================================================================
[GOD TIER ARCHITECTURE: LEVIATHAN CLASS V75.0 - PROJECT OMNISCIENT]
PROJECT: GHOST SNIPER (SILICON WADI / UNIT 8200 SPEC)
MODULE: RECONNAISSANCE ENGINE + PLAYWRIGHT + DEEPSEEK NLP FUSION
ENGINEERING: SHADOW DOM SCAVENGING, CLOUDFLARE BYPASS, 
             COGNITIVE REAPER FALLBACK, ANTI-BOT FINGERPRINTING,
             DYNAMIC EMAIL RECOVERY PIPELINE (O(1) COMPLEXITY)
================================================================================
"""

import os
import sys
import re
import asyncio
import logging
from logging.handlers import RotatingFileHandler  
import random
import socket
import html
import ujson as json
import uuid
import math
import time
import dns.asyncresolver
from typing import List, Optional, Dict, Any, Set, Tuple, Pattern, Union
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse, unquote
from datetime import datetime

from django.utils import timezone
from asgiref.sync import sync_to_async

# Importaciones seguras con try/except
try:
    from playwright.async_api import (
        async_playwright,
        Browser,
        BrowserContext,
        Page,
        Error as PlaywrightError,
        TimeoutError as PlaywrightTimeoutError,
        Route,
        Request
    )
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

try:
    import whois
except ImportError:
    whois = None

try:
    import tldextract
except ImportError:
    tldextract = None

from sales.models import Institution, TechProfile, DeepForensicProfile

try:
    from sales.engine.tor_controller import async_force_new_tor_identity 
except ImportError:
    async_force_new_tor_identity = None

try:
    from ddgs import DDGS
except ImportError:
    DDGS = None

try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# ======================================================================
# [1] GOD TIER TELEMETRY & FORENSIC LOGGING SUBSYSTEM
# ======================================================================
def setup_god_tier_telemetry(logger_name: str = "Sovereign.OmniSniper.APT") -> logging.Logger:
    """Configura el sistema de logs con evasión de permisos y rotación."""
    
    LOG_DIR = "/tmp/sovereign_telemetry"
    os.makedirs(LOG_DIR, exist_ok=True)
    LOG_FILE_PATH = os.path.join(LOG_DIR, "recon_engine_enterprise.log")

    FORENSIC_FORMAT = (
        "[%(asctime)s.%(msecs)03d] [PID:%(process)d|TID:%(thread)d] "
        "[%(levelname)s] [%(name)s] --> %(message)s"
    )
    
    formatter = logging.Formatter(fmt=FORENSIC_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
    logger_instance = logging.getLogger(logger_name)
    
    if logger_instance.hasHandlers():
        logger_instance.handlers.clear()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger_instance.addHandler(stream_handler)

    try:
        file_handler = RotatingFileHandler(
            filename=LOG_FILE_PATH,
            maxBytes=15 * 1024 * 1024, 
            backupCount=3,
            encoding='utf-8',
            delay=True
        )
        file_handler.setFormatter(formatter)
        logger_instance.addHandler(file_handler)
    except PermissionError:
        pass

    logger_instance.setLevel(logging.DEBUG)
    logger_instance.propagate = False
    return logger_instance

logger = setup_god_tier_telemetry()

# ==========================================
# CONFIGURACIÓN EMPRESARIAL Y OBSERVABILIDAD
# ==========================================
@dataclass
class ReconConfig:
    MAX_CONCURRENT: int = 5  
    GLOBAL_TIMEOUT_MS: int = 90000  
    PAGE_LOAD_TIMEOUT_MS: int = 45000  
    SUBDOMAIN_TIMEOUT_MS: int = 25000  
    MAX_RETRIES: int = 3
    DEEP_SCAN_LIMIT: int = 12  
    REQUEST_DELAY_MS: Tuple[int, int] = (4000, 12000)  

    USER_AGENTS: List[str] = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPad; CPU OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1"
    ])

    VIEWPORTS: List[Dict[str, int]] = field(default_factory=lambda: [
        {'width': 1920, 'height': 1080},  
        {'width': 1366, 'height': 768},   
        {'width': 1536, 'height': 864},   
        {'width': 1440, 'height': 900},   
        {'width': 2560, 'height': 1440},  
        {'width': 390, 'height': 844},    
        {'width': 414, 'height': 896}     
    ])

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
    TECH: Dict[str, Pattern] = {
        'lms_schoolnet': re.compile(r'schoolnet|sieweb|redcol\.co|portal\.schoolnet|login\.sieweb|carvajal\.com', re.I),
        'lms_cibercolegios': re.compile(r'cibercolegios\.com|v3\.cibercolegios|login\.cibercolegios', re.I),
        'lms_phidias': re.compile(r'phidias\.co|phidias\.cloud|phidias-static|app\.phidias|phidias\.js|\.phidias\.co', re.I),
        'lms_educamos': re.compile(r'educamos\.com|sm-educamos|plataformaeducamos|edelvives', re.I),
        'lms_moodle': re.compile(r'moodle|moodleform|pluginfile\.php|theme/moodle|/login/index\.php|moodlesession', re.I),
        'lms_canvas': re.compile(r'instructure\.com|canvas-lms|canvas\.js', re.I),
        'lms_google': re.compile(r'classroom\.google\.com|google-workspace|google\.com/edu', re.I),
        'lms_microsoft': re.compile(r'teams\.microsoft\.com|education\.microsoft', re.I),
        'lms_sapred': re.compile(r'sapred\.com|plataformadecolegios|sapred\.net', re.I),
        'lms_gnosoft': re.compile(r'gnosoft\.com\.co|gnosoft\.com|gnosoft-portal', re.I),
        'lms_schoology': re.compile(r'schoology\.com|schoology-app', re.I),
        'lms_blackboard': re.compile(r'blackboard\.com|bbhosted\.com', re.I),
        'lms_edmodo': re.compile(r'edmodo\.com', re.I),
        'lms_sakai': re.compile(r'sakai-project|portal/site', re.I),
        'lms_chamilo': re.compile(r'chamilo\.org|main/css/chamilo', re.I),
        'lms_siga': re.compile(r'desarrollosiga\.com|siga web', re.I),
        'lms_ciudadeducativa': re.compile(r'ciudadeducativa\.com|cloud\.ciudadeducativa', re.I),
        'cms_wordpress': re.compile(r'wp-content|wp-includes|wp-json|/wp-|yoast|elementor', re.I),
        'cms_drupal': re.compile(r'drupal|sites/default/files', re.I),
        'cms_joomla': re.compile(r'joomla|/media/system/js', re.I),
        'cms_wix': re.compile(r'wix\.com|wixsite\.com|_wix', re.I),
        'cms_squarespace': re.compile(r'squarespace\.com|static\d+\.squarespace', re.I),
        'crm_hubspot': re.compile(r'hs-scripts|hs-static|hubspot\.com', re.I),
        'crm_salesforce': re.compile(r'salesforce\.com|sfdc\.net|pardot', re.I),
        'crm_rdstation': re.compile(r'rdstation|rd-station', re.I),
        'analytics_ga': re.compile(r'googletagmanager\.com|google-analytics\.com/ga\.js', re.I),
        'analytics_matomo': re.compile(r'matomo\.js|piwik\.js', re.I),
        'analytics_fb_pixel': re.compile(r'connect\.facebook\.net/en_US/fbevents\.js|fbq\(', re.I),
        'security_cloudflare': re.compile(r'__cf_bm|cloudflare-static|cdn-cgi|cf-Ray', re.I),
        'security_akamai': re.compile(r'akamai\.net|akamaitechnologies|akamaized\.net', re.I),
        'security_aws_shield': re.compile(r'awsglobalaccelerator', re.I),
        'cdn_cloudfront': re.compile(r'cloudfront\.net|d[0-9A-Za-z]+\.cloudfront', re.I),
        'cdn_fastly': re.compile(r'fastly\.net|fastly-insights', re.I),
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
        'is_trilingual': re.compile(r'trilingüe|trilingual school|tercer idioma|francés e inglés', re.I),
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

    EMAIL_REGEX: Pattern = re.compile(
        r'(?:[a-zA-Z0-9_.+-]+(?:%20|\s*\[at\]\s*|\s*\(at\)\s*|\s*arroba\s*|\s*@\s*|%40|\s+at\s+|@)[a-zA-Z0-9-]+\.(?:[a-zA-Z0-9-.]+))', 
        re.IGNORECASE
    )
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

    SUBDOMAIN_HUNT_LIST: List[str] = [
        "plataforma", "moodle", "campus", "virtual", "aula", "aulavirtual", 
        "siga", "estudiantes", "portal", "lms", "academico", "phidias", 
        "canvas", "cibercolegios", "notas", "saberes", "intranet"
    ]

GARBAGE_EMAILS = frozenset({
    'sentry', 'wixpress', 'example', 'domain', 'noreply', 'no-reply', 
    'hostmaster', 'postmaster', 'abuse', 'webmaster', 'mailer-daemon', 'contacto@tuweb',
    'admin@'
})

# ==========================================
# MÓDULOS DE UTILIDAD (HELPERS DE RED)
# ==========================================
class ReconUtils:
    @staticmethod
    def extract_domain_info(url: str) -> Dict[str, Any]:
        if tldextract:
            extracted = tldextract.extract(url)
            root_domain = f"{extracted.domain}.{extracted.suffix}" if extracted.suffix else extracted.domain
            return {
                'domain': extracted.domain,
                'subdomain': extracted.subdomain,
                'suffix': extracted.suffix,
                'full_domain': root_domain,
                'registrable_domain': root_domain
            }
        return {'domain': '', 'subdomain': '', 'suffix': '', 'full_domain': '', 'registrable_domain': ''}

    @staticmethod
    async def get_whois_info(domain: str) -> Dict[str, Any]:
        if whois:
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
        return {'error': 'whois not available'}

    @staticmethod
    async def get_dns_records(domain: str) -> Dict[str, Any]:
        records = {}
        try:
            resolver = dns.asyncresolver.Resolver()
            resolver.lifetime = 5.0
            
            mx_records = []
            try:
                answers = await resolver.resolve(domain, 'MX')
                mx_records = [str(r.exchange) for r in answers]
            except Exception: pass

            txt_records = []
            try:
                answers = await resolver.resolve(domain, 'TXT')
                txt_records = [str(r) for r in answers]
            except Exception: pass

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
        try:
            json.loads(json_str)
            return True
        except ValueError:
            return False

    @staticmethod
    def clean_text(text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'[^\x00-\x7F]+', ' ', text)
        return text

# ==========================================
# MÓDULO DE SÍNTESIS CON INTELIGENCIA ARTIFICIAL
# ==========================================

class AIInsightsGenerator:
    """
    Generador de insights y correos de venta usando Modelos de Lenguaje Avanzados (LLMs).
    """
    def __init__(self, api_key: str = None, model: str = "gpt-4o"):
        self.api_key = api_key
        self.model = model
        self.client = None
        if api_key and OPENAI_AVAILABLE:
            try:
                from openai import OpenAI
                self.client = OpenAI(api_key=api_key)
            except ImportError:
                logger.warning("❌ OpenAI SDK no está instalado.")

    def generate_prompt(self, institution_data: Dict[str, Any]) -> str:
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
        if not self.client:
            return {"error": "Cliente de IA no configurado."}

        prompt = self.generate_prompt(institution_data)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Eres una máquina experta en B2B que responde EXCLUSIVAMENTE en formato JSON nativo sin Markdown adicional."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1500,
                response_format={"type": "json_object"}  
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
class GhostEmailSniper:
    """
    [GOD TIER EMAIL EXTRACTOR]
    Integra escáner de memoria, inyección en caliente de decodificadores de Cloudflare
    y penetración de Shadow DOMs para extraer el 100% de los correos.
    """
    
    CLOUDFLARE_REGEX = re.compile(r'/cdn-cgi/l/email-protection#([a-fA-F0-9]{4,})')

    def __init__(self):
        self.intercepted_emails: Set[str] = set()

    async def attach_memory_sniffer(self, page: Page):
        self.intercepted_emails.clear()

        async def _sniff_network_response(response):
            if response.request.resource_type in ["image", "media", "font", "stylesheet"]:
                return
            
            try:
                text_body = await response.text()
                
                found = ReconSignatures.EMAIL_REGEX.findall(text_body)
                for email in found:
                    clean = self._sanitize_obfuscation(email)
                    if clean: self.intercepted_emails.add(clean)
                    
                cf_matches = self.CLOUDFLARE_REGEX.findall(text_body)
                for hex_str in cf_matches:
                    decrypted = self._decode_cloudflare(hex_str)
                    if decrypted: self.intercepted_emails.add(decrypted)

            except Exception:
                pass 

        if PLAYWRIGHT_AVAILABLE:
            page.on("response", _sniff_network_response)

    def _sanitize_obfuscation(self, raw_email: str) -> Optional[str]:
        clean = raw_email.lower().strip()
        clean = re.sub(r'(%20|\s*\[at\]\s*|\s*\(at\)\s*|\s+at\s+)', '@', clean)
        clean = re.sub(r'(\s*\[dot\]\s*|\s*\(dot\)\s*|\s+dot\s+)', '.', clean)
        
        if any(bad in clean for bad in GARBAGE_EMAILS):
            return None
        
        if re.match(r'^[a-z0-9_.+-]+@[a-z0-9-]+\.[a-z0-9-.]+$', clean) and len(clean) < 60:
            return clean
        return None

    def _decode_cloudflare(self, hex_string: str) -> Optional[str]:
        try:
            r = int(hex_string[:2], 16)
            email = ''.join([chr(int(hex_string[i:i+2], 16) ^ r) for i in range(2, len(hex_string), 2)])
            return email if '@' in email else None
        except Exception:
            return None

class TheCognitiveReaper:
    """
    [GOD TIER FALLBACK: THE COGNITIVE REAPER]
    Si Playwright y el DOM fallan en revelar el correo, el Reaper usa motores de búsqueda
    y modelos fundacionales NLP para extraer el correo de los metadatos indexados.
    """
    @staticmethod
    async def invoke(name: str, city: str, domain: str) -> List[str]:
        logger.warning(f"☢️ [THE REAPER] El objetivo {name} ha ofuscado su correo. Desplegando Dorking Cognitivo...")
        
        query = f'"{name}" {city} ("@gmail.com" OR "@hotmail.com" OR "correo" OR "email" OR "@edu.co")'
        if domain:
            query += f" OR site:{domain.replace('www.', '')}"

        try:
            if DDGS:
                def search_ddg():
                    with DDGS() as ddg:
                        return [f"{r.get('title')} | {r.get('body')}" for r in ddg.text(query, backend="lite", max_results=5)]
                
                results = await asyncio.to_thread(search_ddg)
                corpus = " ".join(results).lower()

                clean_text = re.sub(r'(?i)(\s*\[at\]\s*|\s*\(at\)\s*|\s+at\s+|\s*arroba\s*|@|%40)', '@', html.unescape(unquote(corpus)))
                found = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', clean_text)
                
                valid_emails = [e.lower().strip().rstrip('.,;:') for e in found if '@' in e and not any(g in e for g in GARBAGE_EMAILS)]
                if valid_emails:
                    return list(set(valid_emails))

            if OPENAI_AVAILABLE:
                async_client = AsyncOpenAI(api_key=os.environ.get("DEEPSEEK_API_KEY", ""), base_url="https://api.deepseek.com")
                prompt = f"Extract ONLY the valid email address from the text as a raw string. If none, return 'NONE'.\nTEXT: {corpus[:3500]}"
                
                response = await async_client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0
                )
                
                llm_result = response.choices[0].message.content.strip().lower()
                if '@' in llm_result and 'none' not in llm_result:
                    return [llm_result]

        except Exception as e:
            logger.error(f"Reaper Exception: {e}")
        
        return []

class B2BReconEngine:
    """
    [GOD TIER - APT LEVEL ARCHITECTURE]
    """

    def __init__(self, config: ReconConfig = ReconConfig()):
        self.config = config
        self.semaphore = asyncio.Semaphore(config.MAX_CONCURRENT)
        self.tor_lock = asyncio.Lock()
        self.last_tor_rotation_time = 0.0
        self.sniper = GhostEmailSniper()

    async def _check_dns_resolution(self, hostname: str) -> bool:
        loop = asyncio.get_running_loop()
        try:
            await loop.getaddrinfo(hostname, None)
            return True
        except socket.gaierror:
            return False

    async def _apply_stealth(self, page: Page):
        if PLAYWRIGHT_AVAILABLE:
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['es-CO', 'es', 'en-US', 'en']});

                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(parameter) {
                    if (parameter === 37445) return 'Intel Inc.'; 
                    if (parameter === 37446) return 'Intel Iris OpenGL Engine'; 
                    return getParameter.call(this, parameter);
                };

                window.chrome = {
                    runtime: {},
                    app: {isInstalled: false},
                    webstore: {}
                };

                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                    Promise.resolve({state: Notification.permission}) :
                    originalQuery(parameters)
                );

                Object.defineProperty(navigator, 'connection', {
                    value: {
                        downlink: 10,
                        effectiveType: '4g',
                        rtt: 50,
                        saveData: false
                    }
                });
                
                Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
                Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
            """)

    async def _intercept_resources(self, route: Route, request: Request):
        blocked_types = {"image", "media", "font", "stylesheet", "websocket", "other", "eventsource"}
        blocked_domains = {
            "google-analytics.com", "analytics.twitter.com", "doubleclick.net",
            "facebook.com", "tiktok.com", "googletagmanager.com",
            "adservice.google.com", "cdn.instagram.com", "platform.twitter.com",
            "youtube.com", "vimeo.com"
        }

        req_url = request.url.lower()
        resource_type = request.resource_type

        try:
            if resource_type in blocked_types or any(domain in req_url for domain in blocked_domains):
                await route.abort()
            else:
                await route.continue_()
        except Exception:
            pass

    async def _simulate_human_behavior(self, page: Page):
        try:
            await page.evaluate("""() => {
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
            await asyncio.sleep(random.uniform(1.2, 3.5))
        except Exception:
            pass

    async def _safe_tor_rotation(self, strict: bool):
        if async_force_new_tor_identity:
            async with self.tor_lock:
                current_time = time.time()
                if current_time - self.last_tor_rotation_time > 15.0: 
                    logger.warning("🛡️ [C2 MUTEX] Ejecutando Rotación Vectorial Tor...")
                    try:
                        await async_force_new_tor_identity(strict_verification=strict)
                        self.last_tor_rotation_time = time.time()
                        await asyncio.sleep(2.0) 
                    except Exception as e:
                        logger.error(f"❌ [C2 FATAL] Daño en Backbone Tor: {e}")
                else:
                    logger.debug("⏳ [C2 MUTEX] Compartiendo IP estabilizada...")
                    await asyncio.sleep(random.uniform(1.5, 3.5))

    async def _navigate_with_stealth(self, page: Page, url: str, timeout_ms: int = None) -> bool:
        timeout = timeout_ms or self.config.PAGE_LOAD_TIMEOUT_MS
        for attempt in range(self.config.MAX_RETRIES):
            try:
                logger.info(f"🎯 [TARGET] {url} | Intento {attempt + 1}")
                strategy = "networkidle" if attempt == self.config.MAX_RETRIES - 1 else "domcontentloaded"
                response = await page.goto(url, wait_until=strategy, timeout=timeout)
                
                content = await page.content()
                is_blocked = any(term in content.lower() for term in [
                    "access denied", "cloudflare", "captcha", "checking your browser",
                    "403 forbidden", "ip has been blocked"
                ])
                
                if (response and response.status in [403, 429]) or is_blocked:
                    logger.warning(f"🚫 [WAF BLOCKED] {url}. Activando Escudo Mutex Tor...")
                    await self._safe_tor_rotation(strict=True)
                    continue 
                
                return True
            except PlaywrightTimeoutError:
                 logger.debug(f"⏳ [{url}] Timeout (Att: {attempt+1}). Analizando DOM parcial.")
                 return True 
            except Exception as e:
                error_msg = str(e)
                if "ERR_PROXY_CONNECTION_FAILED" in error_msg or "Connection refused" in error_msg:
                    logger.critical(f"🚨 [PROXY DROP] {url}. Mitigando colapso de socket TCP...")
                    await asyncio.sleep(5.0)
                    await self._safe_tor_rotation(strict=False)
                    continue
                else:
                    logger.debug(f"⚠️ [NET ERROR] {url}: {error_msg}")
                    await self._safe_tor_rotation(strict=False)
                    await asyncio.sleep(random.uniform(2.0, 4.0))
                
        return False

    async def _extract_deep_links(self, page: Page, base_url: str) -> List[str]:
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

    async def _hunt_lms_subdomains(self, context: BrowserContext, base_url: str) -> Dict[str, Any]:
        domain_info = ReconUtils.extract_domain_info(base_url)
        root_domain = domain_info['registrable_domain']
        if not root_domain:
            return {}

        found_tech = {}
        valid_hosts = []
        
        for sub in ReconSignatures.SUBDOMAIN_HUNT_LIST:
            target_host = f"{sub}.{root_domain}"
            if await self._check_dns_resolution(target_host):
                valid_hosts.append(f"https://{target_host}")

        if not valid_hosts:
            return {}

        logger.info(f"🕵️‍♂️ [SUBDOMAIN PROBE] DNS detectó {len(valid_hosts)} activos laterales en {root_domain}.")

        for url in valid_hosts:
            page = await context.new_page()
            await self._apply_stealth(page)
            
            async def sub_route_handler(route: Route, request: Request):
                await self._intercept_resources(route, request)
            await page.route("**/*", sub_route_handler)
            
            try:
                if await self._navigate_with_stealth(page, url, timeout_ms=self.config.SUBDOMAIN_TIMEOUT_MS):
                    tech = await self._detect_technologies(page, url)
                    if tech.get('has_lms'):
                        logger.warning(f"💎 [BINGO] LMS detectado en activo lateral: {url} -> {str(tech.get('lms_type')).upper()}")
                        found_tech = tech
                        found_tech['subdomain_source'] = url
                        if tech.get('lms_type') in ['schoolnet', 'phidias', 'cibercolegios']:
                            await page.close()
                            break
            except Exception as e:
                logger.debug(f"Fallo en probe colateral {url}: {str(e)}")
            finally:
                if not page.is_closed():
                    await page.close()

        return found_tech

    async def _extract_contact_info(self, page: Page) -> Dict[str, Set[str]]:
        contacts = {'phones': set(), 'whatsapp': set(), 'emails': set(), 'addresses': set()}

        try:
            await page.evaluate("""() => {
                const cfElements = document.querySelectorAll('.__cf_email__');
                cfElements.forEach(el => {
                    const cfemail = el.getAttribute('data-cfemail');
                    if (cfemail) {
                        let email = '';
                        let r = parseInt(cfemail.substr(0, 2), 16);
                        for (let j = 2; j < cfemail.length; j += 2) {
                            email += String.fromCharCode(parseInt(cfemail.substr(j, 2), 16) ^ r);
                        }
                        el.innerHTML = email; 
                    }
                });
            }""")

            payload = await page.evaluate("""() => {
                const getAttr = (sel, attr) => Array.from(document.querySelectorAll(sel)).map(el => el.getAttribute(attr)).filter(Boolean);
                const extractText = (node) => {
                    if (node.nodeType === Node.TEXT_NODE) return node.textContent;
                    let text = '';
                    if (node.shadowRoot) text += extractText(node.shadowRoot);
                    for (let child of node.childNodes) text += extractText(child) + ' ';
                    return text;
                };
                
                return {
                    tel: getAttr('a[href^="tel:"]', 'href').map(h => h.replace('tel:', '').trim()),
                    wa: getAttr('a[href*="wa.me"], a[href*="api.whatsapp.com"]', 'href'),
                    eml: getAttr('a[href^="mailto:"]', 'href').map(h => h.replace('mailto:', '').trim()),
                    addr: Array.from(document.querySelectorAll('address')).map(el => el.innerText.trim()),
                    body: document.body ? extractText(document.body).substring(0, 15000) : ''
                };
            }""")

            contacts['phones'].update(payload['tel'])
            contacts['whatsapp'].update(payload['wa'])
            contacts['emails'].update(payload['eml'])
            contacts['addresses'].update(payload['addr'])

            raw_html = await page.content()
            
            contacts['phones'].update(ReconSignatures.PHONE_REGEX.findall(raw_html))
            contacts['emails'].update(ReconSignatures.EMAIL_REGEX.findall(raw_html))
            
            for addr in ReconSignatures.ADDRESS_REGEX.findall(payload['body']):
                val = addr[0] if isinstance(addr, tuple) else addr
                contacts['addresses'].add(val.replace('\n', ', '))

            for wa_link in payload['wa']:
                match = re.search(r'wa\.me/(\d+)', wa_link)
                if match: contacts['whatsapp'].add(f"+{match.group(1)}")

        except Exception as e:
            logger.debug(f"Fallo menor en recolección de contactos: {e}")

        return contacts

    async def _detect_technologies(self, page: Page, domain: str) -> Dict[str, Any]:
        tech_stack = {}
        try:
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

            context_string = f"{payload['scripts']} {payload['iframes']} {payload['html']} {payload['metas']} {payload['links']} {payload['storage']} {payload['cookies']} {domain}".lower()

            for tech, pattern in ReconSignatures.TECH.items():
                if pattern.search(context_string):
                    tech_stack[tech] = True

            lms_techs = [k.replace('lms_', '') for k in ReconSignatures.TECH if k.startswith('lms_') and tech_stack.get(k)]
            
            if lms_techs:
                tech_stack['has_lms'] = True
                premium_lms = [l for l in lms_techs if l in ['schoolnet', 'phidias', 'cibercolegios', 'educamos', 'ciudadeducativa', 'siga']]
                tech_stack['lms_type'] = premium_lms[0] if premium_lms else lms_techs[0]
            else:
                tech_stack['has_lms'] = False

            if any(fw in context_string for fw in ['react', 'angular', 'vue', 'nextjs']):
                tech_stack['modern_frontend'] = True

        except Exception as e:
            logger.debug(f"Aviso en detección tecnológica: {e}")

        return tech_stack

    def _clean_emails(self, raw_emails: List[str]) -> str:
        if not raw_emails: return ""

        bad_ext = ('.png', '.jpg', '.jpeg', '.pdf', '.js', '.css', 'sentry.io', 'wixpress.com')
        junk_prefixes = {'info@', 'contacto@', 'webmaster@', 'noreply@', 'admin@', 'hello@'}

        cleaned = {e.lower().strip() for e in raw_emails if not e.lower().strip().endswith(bad_ext) and '@' in e and len(e) > 5}
        if not cleaned: return ""

        priority = [e for e in cleaned if not any(e.startswith(p) for p in junk_prefixes)]
        named_emails = [e for e in priority if '.' in e.split('@')[0]]
        
        if named_emails: return named_emails[0]
        if priority: return priority[0]
        return list(cleaned)[0]

    @sync_to_async
    def _save_intelligence_to_db(self, inst_id: str, master_contacts: dict, tech_data: dict, bi_data: dict):
        from django.db import transaction

        with transaction.atomic():
            inst = Institution.objects.select_for_update().get(id=inst_id)
            
            best_email = self._clean_emails(list(master_contacts.get('emails', [])))
            best_phone = list(master_contacts['phones'])[0] if master_contacts.get('phones') else None
            
            update_fields = ['last_scored_at', 'processing_status']
            inst.last_scored_at = timezone.now()
            inst.processing_status = 'ENRICHED'

            if best_email and not inst.email:
                inst.email = best_email
                update_fields.append('email')
                
            if best_phone and not inst.phone:
                inst.phone = best_phone
                update_fields.append('phone')

            score = 10
            if tech_data.get('has_lms'): score += 40
            if best_email: score += 25
            if best_phone: score += 15
            if bi_data.get('premium_flags'): score += 10
            
            inst.lead_score = min(score, 100)
            update_fields.append('lead_score')

            if tech_data.get('subdomain_source') and inst.website != tech_data.get('subdomain_source'):
                inst.website = tech_data.get('subdomain_source')
                update_fields.append('website')

            inst.save(update_fields=update_fields)

            tech_profile, _ = TechProfile.objects.get_or_create(institution=inst)
            tech_profile.has_lms = tech_data.get('has_lms', False)
            tech_profile.lms_provider = str(tech_data.get('lms_type', '')).lower()
            tech_profile.is_wordpress = tech_data.get('cms_wordpress', False)
            tech_profile.has_analytics = tech_data.get('analytics_ga', False)
            tech_profile.save()

            forensic, _ = DeepForensicProfile.objects.get_or_create(institution=inst)
            flags = bi_data.get('premium_flags', [])
            if 'is_bilingual' in flags: forensic.is_bilingual = True
            if 'is_trilingual' in flags: forensic.is_trilingual = True
            if 'cert_ib' in flags: forensic.has_ib_cert = True
            if 'cert_cambridge' in flags: forensic.has_cambridge_cert = True
            forensic.save()

            return inst.name, tech_profile.lms_provider, inst.email

    async def scan_target(self, browser: Browser, target: Dict[str, Any]):
        async with self.semaphore:
            target_url = target['url'].rstrip('/')
            if not target_url.startswith('http'):
                target_url = f"https://{target_url}"
            domain = urlparse(target_url).netloc

            if not await self._check_dns_resolution(domain):
                logger.warning(f"🚫 [{domain}] Dominio inaccesible a nivel DNS. Skip.")
                return

            if 'id' not in target:
                return

            tor_proxy = {"server": f"socks5://{os.getenv('TOR_PROXY_HOST', '127.0.0.1')}:{os.getenv('TOR_PROXY_PORT', 9050)}"}
            context = await browser.new_context(
                user_agent=random.choice(self.config.USER_AGENTS),
                viewport=random.choice(self.config.VIEWPORTS),
                locale="es-CO", timezone_id="America/Bogota", ignore_https_errors=True,
                bypass_csp=True, java_script_enabled=True, proxy=tor_proxy
            )
            await context.set_extra_http_headers(self.config.CUSTOM_HEADERS)

            page = await context.new_page()
            await self._apply_stealth(page)
            
            await self.sniper.attach_memory_sniffer(page)

            async def route_handler(route: Route, request: Request):
                await self._intercept_resources(route, request)
            await page.route("**/*", route_handler)

            master_contacts = {'phones': set(), 'whatsapp': set(), 'emails': set(), 'addresses': set()}
            tech_data = {}
            bi_data = {'premium_flags': [], 'education_levels': [], 'social_media': {}, 'sales_triggers': []}

            try:
                if await self._navigate_with_stealth(page, target_url):
                    await self._simulate_human_behavior(page)

                    tech_data = await self._detect_technologies(page, domain)
                    
                    if not tech_data.get('has_lms'):
                        logger.info(f"🔍 [LMS MISSING] No detectado en Home. Desplegando Asalto Lateral...")
                        hidden_tech = await self._hunt_lms_subdomains(context, target_url)
                        if hidden_tech.get('has_lms'):
                            tech_data.update(hidden_tech)

                    contacts = await self._extract_contact_info(page)
                    for k in master_contacts: master_contacts[k].update(contacts.get(k, set()))

                    deep_links = await self._extract_deep_links(page, target_url)
                    for link in deep_links:
                        try:
                            if await self._navigate_with_stealth(page, link):
                                await self._simulate_human_behavior(page)
                                sub_contacts = await self._extract_contact_info(page)
                                for k in master_contacts: master_contacts[k].update(sub_contacts.get(k, set()))
                        except Exception: pass

                    master_contacts['emails'].update(self.sniper.intercepted_emails)
                    
                    if not master_contacts['emails'] or len(self._clean_emails(list(master_contacts['emails']))) == 0:
                        reaper_emails = await TheCognitiveReaper.invoke(target['name'], target['city'], target_url)
                        master_contacts['emails'].update(reaper_emails)

                    inst_name, found_lms, found_email = await self._save_intelligence_to_db(
                        inst_id=target['id'], master_contacts=master_contacts,
                        tech_data=tech_data, bi_data=bi_data
                    )

                    logger.info(f"✅ [{domain}] | LMS: {str(found_lms).upper() or 'NINGUNO'} | EMAIL: {found_email or 'CENSURADO'}")
                else:
                    logger.debug(f"❌ [{domain}] Abandonado (Fallo WAF).")

            except Exception as e:
                logger.error(f"❌ [{domain}] Colapso: {str(e)[:100]}")
            finally:
                try: await page.close()
                except Exception: pass
                try: await context.close()
                except Exception: pass

# ==========================================
# ORQUESTADOR MAESTRO Y PUNTO DE ENTRADA
# ==========================================

async def _orchestrate(targets: Optional[List[Dict]] = None):
    config = ReconConfig()
    engine = B2BReconEngine(config)
    
    if not PLAYWRIGHT_AVAILABLE:
        logger.error("❌ Playwright no está instalado. No se puede ejecutar el escaneo.")
        return

    tor_proxy = {"server": f"socks5://{os.getenv('TOR_PROXY_HOST', '127.0.0.1')}:{os.getenv('TOR_PROXY_PORT', 9050)}"}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, proxy=tor_proxy, 
            args=[
                "--disable-blink-features=AutomationControlled", "--no-sandbox",
                "--disable-setuid-sandbox", "--disable-infobars",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-dev-shm-usage", "--disable-accelerated-2d-canvas",
                "--disable-gpu", "--window-size=1920,1080",
                "--js-flags=--max-old-space-size=4096" 
            ]
        )

        try:
            targets_to_process = targets or []
            if not targets_to_process:
                logger.info("📡 [OMNI-SCAN] Extrayendo cola desde BD...")
                count = 0
                async for inst in Institution.objects.filter(is_active=True).order_by('-id'):
                    if count >= 500: break
                    if inst.website:
                        targets_to_process.append({'id': inst.id, 'name': inst.name, 'url': inst.website, 'city': inst.city})
                        count += 1
            else:
                logger.info(f"📡 [TACTICAL-SCAN] Desplegando enjambre sobre {len(targets_to_process)} objetivos...")

            if not targets_to_process: return

            CHUNK_SIZE = 10
            total_targets = len(targets_to_process)
            
            for i in range(0, total_targets, CHUNK_SIZE):
                chunk = targets_to_process[i:i + CHUNK_SIZE]
                logger.info(f"⚙️ [SWARM BATCH] Lote {i // CHUNK_SIZE + 1} de {math.ceil(total_targets / CHUNK_SIZE)}...")
                
                chunk_tasks = []
                for t in chunk:
                    async def stealth_delayed_scan(target_data):
                        await asyncio.sleep(random.uniform(0.1, 2.5))
                        return await engine.scan_target(browser, target_data)
                    chunk_tasks.append(asyncio.create_task(stealth_delayed_scan(t)))

                await asyncio.gather(*chunk_tasks, return_exceptions=True)

                if i + CHUNK_SIZE < total_targets:
                    cooldown = random.uniform(config.REQUEST_DELAY_MS[0] / 1000, config.REQUEST_DELAY_MS[1] / 1000)
                    await asyncio.sleep(cooldown)

        except Exception as e:
            logger.error(f"❌ [CRÍTICO] Colapso en el Orquestador Maestro: {e}")
        finally:
            logger.info("🧹 [PROTOCOL OMEGA] Destruyendo NAVEGADOR MAESTRO...")
            if browser: await browser.close()

def execute_recon(inst_id: Union[int, str, uuid.UUID, None] = None):
    targets = None
    if inst_id:
        try:
            inst = Institution.objects.get(id=inst_id)
            if not inst.website:
                logger.warning(f"⚠️ {inst.name} no tiene URL")
                return False
            targets = [{'id': inst.id, 'name': inst.name, 'url': inst.website, 'city': inst.city}]
            logger.info(f"🎯 Modo Quirúrgico: Analizando {inst.name}")
        except Exception as e:
            logger.error(f"❌ Error obteniendo institución: {e}")
            return False

    try:
        logger.info("🚀 Encendiendo el Ghost Sniper Engine...")
        asyncio.run(_orchestrate(targets))
        logger.info("🏁 Operación concluida exitosamente.")
        return True
    except Exception as e:
        logger.error(f"❌ Crash global: {e}")
        return False

run_recon = execute_recon

if __name__ == "__main__":
    execute_recon(inst_id=sys.argv[1] if len(sys.argv) > 1 else None)
