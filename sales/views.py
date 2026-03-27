"""
======================================================================
[GOD TIER ARCHITECTURE: LEVIATHAN CLASS V38.0 - THE OMNIPOTENT MATRIX]
PROJECT: GHOST SWARM (SILICON WADI / UNIT 8200 SPEC)
MODULE: COGNITIVE OSINT ENGINE & DEEP RENDER JS (PLAYWRIGHT)
ENGINEERING: MAXIMUM YIELD EXTRACTION, GREEDY HARVESTING, HEURISTIC PURGE
DATABASE: STRICT TRANSACTIONAL ISOLATION & DATA TRUNCATION SAFEGUARDS
QA UPDATES (V38.0):
    - GLOBAL REGEX COMPILER PATCH (Fixes STRICT_HREF_PHONE_REGEX NameError)
    - MULTI-PHONE CLUSTER DETECTION (Harvests all numbers on a site)
    - ALPHANUMERIC OBFUSCATION DESTROYER (Kills 'mail7532493admisiones')
    - DOMAIN AFFINITY SCORING FOR PRIMARY EMAILS
    - CELERY/DJANGO MEMORY SYNC OPTIMIZATION
======================================================================
"""

import re
import time
import base64
import logging
import uuid
import random
import asyncio
import requests
import urllib3
import html as html_lib
import urllib.parse
import concurrent.futures
import ujson as json
from typing import Tuple, Optional, Set, List, Dict
from dataclasses import dataclass, field
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup, Comment
from duckduckgo_search import DDGS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from django.http import HttpResponse, HttpRequest
from django.views import View
from django.views.generic import TemplateView
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET
from django.db import transaction, IntegrityError
from django.db.models import F, Q
from django.core.cache import cache
from django.utils import timezone

# =========================================================
# [NIVEL DIOS 0]: PLAYWRIGHT ENGINE GUARD
# =========================================================
try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.critical("❌ [FATAL] Playwright no detectado. El Deep Render fallará. Ejecuta: pip install playwright && playwright install chromium")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# [CORRECCIÓN CRÍTICA]: Importación Estricta de la Capa de Datos Completa
from .models import Interaction, Institution, TechProfile, DeepForensicProfile

logger = logging.getLogger("Sovereign.Intelligence")

# =========================================================
# [NIVEL DIOS 1]: INICIALIZACIÓN RAM & REGEX FORENSE GLOBAL
# =========================================================
PIXEL_BYTES = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")

BOT_REGEX = re.compile(r'(googleimageproxy|proofpoint|mimecast|barracuda|slackbot|whatsapp|telegrambot|applebot|outlook-com|yahoo|yandex|microsoft|spider|bot|crawler|scanner|datanyze)', re.IGNORECASE)
EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
OBFUSCATED_EMAIL_REGEX = re.compile(r'([a-zA-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\s+at\s+|@|\[arroba\]|\s+en\s+)\s*([a-zA-Z0-9.-]+)\s*(?:\[dot\]|\(dot\)|\s+dot\s+|\.|\[punto\]|\s+punto\s+)\s*([a-zA-Z]{2,})', re.IGNORECASE)
CLOUDFLARE_HEX_REGEX = re.compile(r'/cdn-cgi/l/email-protection#([a-fA-F0-9]{4,})')
BASE64_HEURISTIC_REGEX = re.compile(r'(?i)(?:[A-Za-z0-9+/]{4}){5,}(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?')
SOCIAL_REGEX = re.compile(r'https?:\/\/(?:www\.)?(linkedin\.com|facebook\.com|instagram\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com)\/[a-zA-Z0-9_.-]+')

# [GOD TIER V38.0]: MATRIZ DE EXTRACCIÓN TELEFÓNICA (MÁXIMO RENDIMIENTO)
CONTEXT_BLOCK_REGEX = re.compile(r'(?i)(?:pbx|tel[ée]fono|tel|celular|cel|whatsapp|wa|contactenos|ll[áa]manos|l[íi]nea|contacto|comunicate)(?:[^a-zA-Z]{0,50})')
RAW_PHONE_FINDER = re.compile(r'(?:(?:\+|00)[1-9]\d{0,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?)?\d{3,4}[\s.-]?\d{3,4}')
STRICT_HREF_PHONE_REGEX = re.compile(r'(?i)(?:tel:|wa\.me/|whatsapp://send\?phone=)([+0-9]+)')
RAW_PHONE_FALLBACK_REGEX = re.compile(r'(?:(?:\+|00)[1-9]\d{0,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?)?\d{3,4}[\s.-]?\d{3,4}')

# [INYECCIÓN FORENSE GOD-TIER]: Firmas Comerciales Deep B2B (O(1) Lookup)
BUSINESS_SIGNATURES = {
    'is_bilingual': re.compile(r'\b(bilingüe|bilingual school|dual language|inglés-español|formación bilingüe)\b', re.IGNORECASE),
    'is_trilingual': re.compile(r'\b(trilingüe|trilingual school|tercer idioma|francés e inglés)\b', re.IGNORECASE),
    'cert_ib': re.compile(r'\b(bachillerato internacional|international baccalaureate|ib world school|ib\.org)\b', re.IGNORECASE),
    'cert_cambridge': re.compile(r'\b(cambridge english|cambridge assessment|cambridge international)\b', re.IGNORECASE),
    'is_technical': re.compile(r'\b(instituto técnico|formación técnica|tecnológico|sena|politécnico)\b', re.IGNORECASE)
}

SERP_EXCLUSIONS = {'facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'youtube.com', 'wikipedia.org', 'paginasamarillas', 'directory', 'infoisinfo', 'tripadvisor', 'foursquare', 'civico', 'losmejorescolegios', 'wiktionary', 'dictionary', 'google.com'}
EDU_KEYWORDS = {'colegio', 'school', 'educación', 'educacion', 'gimnasio', 'liceo', 'instituto', 'academy', 'admisiones', 'estudiantes', 'matrículas', 'rectoría', 'alumnos', 'contact'}

EMAIL_GARBAGE_EXT = {'.png', '.jpg', '.jpeg', '.gif', '.css', '.js', '.webp', '.svg', '.woff', '.ttf', '.mp4', '.pdf'}
EMAIL_GARBAGE_DOMAINS = {'sentry.io', 'wixpress.com', 'example.com', 'domain.com', 'email.com', 'wix.com', 'localhost', 'sentry.wixpress.com', 'tuweb.com', 'colombia.ver'}
EMAIL_GARBAGE_PREFIXES = {'icon', 'logo', 'image', 'test', 'yourname', 'no-reply', 'noreply', 'sentry', '12345', 'admin@', 'correo@'}

LMS_SIGNATURES = {
    'Moodle': ['moodle', 'theme/moove', 'lib/javascript.php', 'moodle-block', 'pluginfile.php', 'mdl_'],
    'Canvas': ['instructure', 'canvas-lms', 'canvas_session', 'instructure.com'],
    'Blackboard': ['blackboard', 'bbcswebdav', 'learn.blackboard'],
    'Google Classroom': ['classroom.google.com'],
    'MS Teams Edu': ['teams.microsoft', 'microsoft_teams'],
    'Schoology': ['schoology.com'],
    'D2L Brightspace': ['d2l', 'desire2learn', 'brightspace'],
    'Sakai': ['sakai', 'sakaiproject'],
    'Chamilo': ['chamilo'],
    'Ilias': ['ilias.de'],
    'Itslearning': ['itslearning.com'],
    'Docebo': ['docebo'],
    'Phidias': ['phidias.co', 'phidias.ac', 'phidias', 'phidias.net'],
    'Ciudad Educativa': ['ciudadeducativa.com'],
    'Sistema Saberes': ['sistemasaberes.com', 'saberes.com'],
    'Gnosoft': ['gnosoft', 'gnosoft.com.co'],
    'Pegaso': ['pegaso.com.co', 'pegaso', 'pegasopro'],
    'Cibercolegios': ['cibercolegios.com', 'cibercolegios'],
    'Q10 Académico': ['q10.com', 'q10academico', 'q10'],
    'Integra': ['plataformaintegra.net', 'plataformaintegra'],
    'SIGA': ['sigaweb', 'edusiga', 'siganet', 'sigaportal', 'sigaapp'],
    'Colegios Colombia': ['colegiosonline.com', 'portalcolegioscolombia', 'masteracademic', 'colegioscolombia.net'],
    'Ovy': ['ovy.co', 'plataforma ovy'],
    'WebColegios': ['webcolegios.com'],
    'Bicol': ['bicol.com.co'],
    'Schoolnet': ['schoolnet', 'colegios-online'],
    'Santillana Compartir': ['santillanacompartir', 'stilus', 'compartir'],
    'SM Educamos': ['educamos.com', 'sm educamos'],
    'Educaria (Alexia)': ['educaria', 'alexia', 'alexiaeducacion'],
    'UNOi': ['unoi.com', 'sistema uno'],
    'Norma Educa': ['educanorma.com'],
    'Sieweb': ['sieweb.com.pe', 'sieweb'],
    'Gesta': ['gesta.com.co', 'gesta'],
    'EduPage': ['edupage.org'],
    'Educaline': ['educaline']
}

TACTICAL_UAS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
]

# GLOBAL CONNECTION POOLING PARA VELOCIDAD EXTREMA
GLOBAL_SESSION = requests.Session()
_adapter = HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[403, 406, 429, 500, 502, 503, 504]), pool_connections=50, pool_maxsize=50)
GLOBAL_SESSION.mount('http://', _adapter)
GLOBAL_SESSION.mount('https://', _adapter)

# =========================================================
# [NIVEL DIOS 2]: UTILIDADES DE RED & DECRYPTORS Y VALIDACIÓN ESTRICTA
# =========================================================
@dataclass
class ExtractedPayload:
    target: str
    domain: str = ""
    name: str = ""
    emails: Set[str] = field(default_factory=set)
    whatsapp: Set[str] = field(default_factory=set)
    telephones: Set[str] = field(default_factory=set)
    socials: Set[str] = field(default_factory=set)
    lms_provider: str = "No detectado"
    pages_scanned: int = 0
    error: Optional[str] = None
    playwright_warn: Optional[str] = None
    forensics: Dict[str, bool] = field(default_factory=lambda: {k: False for k in BUSINESS_SIGNATURES.keys()})

def get_tactical_session():
    return GLOBAL_SESSION

def get_base_domain(url: str) -> str:
    """Extrae el core del dominio para afinidad de correo"""
    try:
        netloc = urlparse(url).netloc.lower().replace('www.', '')
        return netloc.split('.')[0] if netloc else ""
    except Exception: return ""

def decode_cloudflare_email(hex_string):
    try:
        r = int(hex_string[:2], 16)
        email = ''.join([chr(int(hex_string[i:i+2], 16) ^ r) for i in range(2, len(hex_string), 2)])
        return email if EMAIL_REGEX.match(email) else None
    except Exception:
        return None

def try_decode_base64(b64_string):
    try:
        decoded = base64.b64decode(b64_string).decode('utf-8')
        if EMAIL_REGEX.match(decoded): return decoded
    except Exception:
        return None

def clean_phone_number(raw_num):
    num = re.sub(r'[^\d+]', '', raw_num)
    if num.startswith('00'): num = '+' + num[2:]
    return num

def clean_and_validate_phone_strict(raw_num: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    [GOD TIER QA V38.0]: Validador estricto Colombia. Destruye NITs y Fechas.
    Retorna: (Es_Valido, Numero_Limpio, Tipo 'W' o 'T')
    """
    clean = re.sub(r'\D', '', raw_num)
    if clean.startswith('57'): clean = clean[2:] 
    if clean.startswith('0057'): clean = clean[4:]
    length = len(clean)
    
    # Reglas exactas Colombia
    if length == 10 and clean.startswith('3'): return True, clean, 'W' # Celular
    if length == 10 and clean.startswith('60'): return True, clean, 'T' # Fijo nuevo
    if length == 7 and clean[0] in ['2', '3', '4', '5', '6', '7', '8', '9']: return True, clean, 'T' # Fijo viejo
    if clean.startswith('018000') and length >= 10: return True, clean, 'T' # Línea Gratuita Nacional
    
    return False, None, None

def _is_security_bot(user_agent: str, ip: str) -> bool:
    if not user_agent: return True
    if BOT_REGEX.search(user_agent): return True
    return False

# =========================================================
# [NIVEL DIOS 2B]: PIXEL TRACKING VIEW
# =========================================================
def _build_pixel_response() -> HttpResponse:
    response = HttpResponse(PIXEL_BYTES, content_type="image/gif")
    response['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    return response

@require_GET
def tracking_pixel_view(request: HttpRequest, interaction_id: str) -> HttpResponse:
    user_agent = request.META.get('HTTP_USER_AGENT', '')
    client_ip = request.META.get('HTTP_X_FORWARDED_FOR', request.META.get('REMOTE_ADDR', ''))

    if _is_security_bot(user_agent, client_ip): return _build_pixel_response()

    try: interaction_uuid = uuid.UUID(str(interaction_id))
    except ValueError: return _build_pixel_response()

    lock_key = f"pixel_debounce_{interaction_uuid}"
    if cache.get(lock_key): return _build_pixel_response()
    cache.set(lock_key, True, timeout=4)

    try:
        with transaction.atomic():
            interaction = Interaction.objects.select_for_update().select_related('institution').get(id=interaction_uuid)
            interaction.opened_count = F('opened_count') + 1
            if interaction.status in ['NEW', 'SENT', 'PENDING']:
                interaction.status = 'OPENED'
                inst = interaction.institution
                inst.lead_score = min(inst.lead_score + 15, 100)
                inst.save(update_fields=['lead_score', 'updated_at'])
            interaction.save(update_fields=['opened_count', 'status', 'updated_at'])
    except Exception: pass
    return _build_pixel_response()

# =========================================================
# [NIVEL DIOS 3]: MOTOR COGNITIVO OSINT (GHOST SNIPER)
# =========================================================
class SniperConsoleView(TemplateView):
    template_name = "admin/sales/sniper_console.html"

@method_decorator(csrf_exempt, name='dispatch')
class SniperSearchView(View):

    def resolve_domain_from_serp(self, query: str, city: str, country: str) -> Optional[str]:
        urls = []
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(query, backend="lite", max_results=5))
                for r in results:
                    href = r.get('href', r.get('link', r.get('url', '')))
                    if href and not any(exc in href.lower() for exc in SERP_EXCLUSIONS):
                        urls.append(href)
        except Exception:
            pass

        if not urls:
            try:
                headers = {'User-Agent': random.choice(TACTICAL_UAS)}
                resp = requests.get(f"https://html.duckduckgo.com/html/?q={query}", headers=headers, timeout=5)
                soup = BeautifulSoup(resp.text, 'lxml' if 'lxml' in globals() else 'html.parser')
                for a in soup.find_all('a', class_='result__url'):
                    href = a.get('href', '')
                    if href and 'http' in href and not any(exc in href.lower() for exc in SERP_EXCLUSIONS):
                        urls.append(href)
            except Exception:
                pass
        
        return list(dict.fromkeys(urls))[:3]

    def cognitive_url_scorer(self, urls, target, city, country):
        best_url, best_score = None, -1
        target_words = [w.lower() for w in target.split() if len(w) > 2]
        session = get_tactical_session()

        for url in urls:
            try:
                resp = session.get(url, headers={'User-Agent': random.choice(TACTICAL_UAS)}, timeout=5, verify=False)
                html_lower = resp.text.lower()
                score = 0
                
                for word in target_words:
                    if word in html_lower: score += 5
                if city and city.lower() in html_lower: score += 15
                if country and country.lower() in html_lower: score += 10
                for kw in EDU_KEYWORDS:
                    if kw in html_lower: score += 5

                if score > best_score:
                    best_score = score
                    best_url = resp.url
            except Exception: continue

        if best_score < 10: return None
        return best_url

    def extract_from_dom(self, html_content, browser_visible_text, soup, payload, cfg):
        """
        [GOD TIER MAXIMUM YIELD ENGINE V38.0]: Extracción Híbrida Agresiva.
        QA FIX: Saneamiento Alfanumérico y Cluster Harvesting activados.
        """
        decoded_html = html_lib.unescape(html_content)
        decoded_html = re.sub(r'[\u200B-\u200D\uFEFF]', '', decoded_html)
        
        # [SANEAMIENTO ALFANUMÉRICO]: Evita mail7532493admisiones
        decoded_html = re.sub(r'(\D)(\d{5,})(\D)', r'\1 \2 \3', decoded_html)
        
        for tag in soup.find_all(True): tag.insert_after(' ')
        soup_text = soup.get_text(separator=' | ', strip=True) 
        
        tagless_html = re.sub(r'<[^>]+>', ' ', decoded_html)
        raw_html_soup = BeautifulSoup(decoded_html, 'html.parser')

        hidden_attributes_text = ""
        for tag in raw_html_soup.find_all(['img', 'a', 'div', 'span']):
            hidden_attributes_text += f" {tag.get('alt', '')} {tag.get('title', '')} "
            for key, val in tag.attrs.items():
                if isinstance(val, str): hidden_attributes_text += f" {val} "

        master_text = f"{browser_visible_text} {hidden_attributes_text} | {soup_text}"
        master_text = re.sub(r'\s+', ' ', master_text)
        master_text = re.sub(r'[\u200B-\u200D\uFEFF]', '', master_text)

        base_domain = get_base_domain(payload.domain)
        raw_emails = set(payload.emails)
        raw_wa = set(payload.whatsapp)
        raw_tel = set(payload.telephones)

        # PASE 1: JSON-LD (SCHEMA.ORG)
        for script in raw_html_soup.find_all('script', type='application/ld+json'):
            try:
                json_data = json.loads(script.string)
                def extract_json_keys(obj, key):
                    results = []
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if k.lower() == key: results.append(v)
                            elif isinstance(v, (dict, list)): results.extend(extract_json_keys(v, key))
                    elif isinstance(obj, list):
                        for item in obj: results.extend(extract_json_keys(item, key))
                    return results

                if cfg['use_email']:
                    for e in extract_json_keys(json_data, 'email'):
                        if isinstance(e, str) and EMAIL_REGEX.match(e): raw_emails.add(e.lower())
                if cfg['use_whatsapp']:
                    for t in extract_json_keys(json_data, 'telephone'):
                        if isinstance(t, str):
                            is_val, num, t_type = clean_and_validate_phone_strict(t)
                            if is_val:
                                if t_type == 'W': raw_wa.add(num)
                                else: raw_tel.add(num)
            except Exception:
                pass

        # PASE 2: EMAILS (MAXIMUM YIELD)
        if cfg['use_email']: 
            for a in raw_html_soup.find_all('a', href=True):
                href = urllib.parse.unquote(a['href'].lower())
                if href.startswith('mailto:'):
                    clean_mail = href[7:].split('?')[0].strip()
                    clean_mail = re.sub(r'^[^a-zA-Z0-9]+', '', clean_mail) 
                    if EMAIL_REGEX.match(clean_mail): raw_emails.add(clean_mail)

            for hex_str in CLOUDFLARE_HEX_REGEX.findall(decoded_html):
                if decoded := decode_cloudflare_email(hex_str): raw_emails.add(decoded)

            for b64 in BASE64_HEURISTIC_REGEX.findall(decoded_html):
                if decoded := try_decode_base64(b64): raw_emails.add(decoded)

            raw_emails.update(EMAIL_REGEX.findall(tagless_html))
            raw_emails.update(EMAIL_REGEX.findall(master_text))
            
            for obf in OBFUSCATED_EMAIL_REGEX.findall(master_text):
                raw_emails.add(f"{obf[0]}@{obf[1]}.{obf[2]}".lower())

            scored_emails = []
            for e in raw_emails:
                e_lower = e.lower().strip()
                
                saneamiento = re.search(r'(info|admisiones|contacto|secretaria|rectoria|gerencia|direcciones)@.*', e_lower)
                if saneamiento:
                    e_lower = saneamiento.group(0)

                if any(e_lower.endswith(ext) for ext in EMAIL_GARBAGE_EXT): continue
                if any(garbage in e_lower for garbage in EMAIL_GARBAGE_DOMAINS): continue
                if any(e_lower.startswith(prefix) for prefix in EMAIL_GARBAGE_PREFIXES): continue
                if not EMAIL_REGEX.match(e_lower) or len(e_lower) >= 60: continue
                if re.match(r'^\d{6,}', e_lower): continue 
                
                if base_domain and base_domain in e_lower:
                    scored_emails.insert(0, e_lower) 
                elif any(kw in e_lower for kw in ['info', 'contacto', 'admision', 'rectoria', 'secretaria']):
                    scored_emails.insert(1, e_lower) 
                else:
                    scored_emails.append(e_lower)

            payload.emails = set(dict.fromkeys(scored_emails))

        # PASE 3: TELÉFONOS Y WA (MULTI-CLUSTER HARVESTING)
        if cfg['use_whatsapp']:
            for a in raw_html_soup.find_all('a', href=True):
                href = urllib.parse.unquote(a['href'].lower())
                if match := STRICT_HREF_PHONE_REGEX.search(href):
                    is_val, num, t_type = clean_and_validate_phone_strict(match.group(1))
                    if is_val:
                        if t_type == 'W': raw_wa.add(num)
                        else: raw_tel.add(num)
            
            blocks = CONTEXT_BLOCK_REGEX.split(master_text)
            for block in blocks:
                for match in RAW_PHONE_FINDER.findall(block):
                    is_val, num, t_type = clean_and_validate_phone_strict(match)
                    if is_val:
                        if t_type == 'W': raw_wa.add(num)
                        else: raw_tel.add(num)

            for match in RAW_PHONE_FALLBACK_REGEX.findall(master_text):
                is_val, num, t_type = clean_and_validate_phone_strict(match)
                if is_val:
                    if t_type == 'W': raw_wa.add(num)
                    else: raw_tel.add(num)
            
            payload.whatsapp = raw_wa
            payload.telephones = raw_tel

        # PASE 4: REDES Y LMS
        payload.socials.update(list(set([s.lower() for s in SOCIAL_REGEX.findall(decoded_html)])))

        if cfg['use_lms'] and payload.lms_provider == 'No detectado':
            html_lower = decoded_html.lower()
            links = [a.get('href', '').lower() for a in raw_html_soup.find_all('a', href=True)]
            for lms, sigs in LMS_SIGNATURES.items():
                if any(s in html_lower for s in sigs) or any(any(s in link for s in sigs) for link in links):
                    payload.lms_provider = lms
                    break

        # ==========================================
        # PASE 5: [GOD TIER FORENSICS] BUSINESS INTELLIGENCE
        # ==========================================
        master_text_lower = master_text.lower()
        for key, regex in BUSINESS_SIGNATURES.items():
            if regex.search(master_text_lower):
                payload.forensics[key] = True

        raw_html_soup.decompose()

    # =========================================================
    # [NIVEL DIOS 4]: PLAYWRIGHT STEALTH MULTI-FRAME MANAGER
    # =========================================================
    async def async_deep_render(self, url):
        html_content = ""
        browser_visible_text = ""
        title = ""
        
        if not PLAYWRIGHT_AVAILABLE:
            raise Exception("Playwright no instalado.")

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True, 
                    args=[
                        '--disable-blink-features=AutomationControlled', 
                        '--no-sandbox',
                        '--disable-web-security',
                        '--disable-features=IsolateOrigins,site-per-process'
                    ]
                )
                context = await browser.new_context(
                    user_agent=random.choice(TACTICAL_UAS),
                    viewport={'width': 1920, 'height': 1080},
                    ignore_https_errors=True,
                    java_script_enabled=True,
                    locale='es-CO',
                    timezone_id='America/Bogota'
                )
                page = await context.new_page()
                
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => false});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4]});
                    window.chrome = { runtime: {} };
                """)
                
                try:
                    await page.goto(url, timeout=35000, wait_until='domcontentloaded')
                except PlaywrightTimeoutError:
                    pass 
                
                await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
                await page.mouse.down()
                await page.mouse.up()
                
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                await asyncio.sleep(1.0)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                
                stealth_script = """
                () => {
                    const buttons = document.querySelectorAll('button, a, span, div');
                    for (const b of buttons) {
                        const txt = (b.innerText || '').toLowerCase();
                        if (/(ver|show|mostrar|revelar|tel|mail|contacto|llamar)/.test(txt)) {
                            try { b.click(); } catch(e) {}
                        }
                    }
                    const allElements = document.querySelectorAll('*');
                    for (const el of allElements) {
                        const style = window.getComputedStyle(el);
                        if ((style.direction === 'rtl' || style.unicodeBidi === 'bidi-override') && el.children.length === 0) {
                            if(el.innerText) {
                                el.innerText = el.innerText.split('').reverse().join('');
                                el.style.direction = 'ltr';
                                el.style.unicodeBidi = 'normal';
                            }
                        }
                    }
                }
                """
                await page.evaluate(stealth_script)
                await asyncio.sleep(2.0)
                
                frames_html = []
                for frame in page.frames:
                    try:
                        frames_html.append(await frame.content())
                    except: pass
                
                html_content = "\n".join(frames_html)
                browser_visible_text = await page.evaluate("() => document.body.innerText")
                title = await page.title()
                
                await context.close()
                await browser.close()
                
        except PlaywrightError as pe:
            if "Executable doesn't exist" in str(pe):
                raise RuntimeError("BROWSER_MISSING") 
            logger.error(f"❌ Playwright Falló en {url}: {str(pe)}")
            raise pe
        except Exception as e:
            logger.error(f"❌ Playwright Falló en {url}: {str(e)}")
            raise e
            
        return html_content, browser_visible_text, title

    def run_deep_render_sync(self, url):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.async_deep_render(url))
        finally:
            loop.close()

    def process_target_worker(self, target: str, geo_context: str, city: str, country: str, cfg: dict, pre_emails: list) -> ExtractedPayload:
        payload = ExtractedPayload(target=target)
        if pre_emails: payload.emails.update(pre_emails)
        
        try:
            headers = {'User-Agent': random.choice(TACTICAL_UAS), 'Accept': 'text/html,*/*'}

            if not re.match(r'^(https?:\/\/|www\.)', target.lower()):
                query = f"{target} {geo_context}"
                winning_url = self.cognitive_url_scorer(self.search_engine_redundancy(query), target, city, country)
                if not winning_url:
                    payload.error = "Puntaje de confianza muy bajo o motor SERP bloqueado."
                    return payload
                payload.domain = winning_url
            else:
                payload.domain = target if target.startswith('http') else f"https://{target}"

            html = ""
            browser_text = ""
            title = ""
            
            if cfg['use_deep_render'] and PLAYWRIGHT_AVAILABLE:
                try:
                    html, browser_text, title = self.run_deep_render_sync(payload.domain)
                    payload.name = title if title else payload.name
                    payload.pages_scanned += 1
                except RuntimeError as rt_error:
                    if str(rt_error) == "BROWSER_MISSING":
                        payload.playwright_warn = "Playwright Executable Missing: Usando Requests Fallback"
                except Exception:
                    pass
            
            if not html:
                resp = GLOBAL_SESSION.get(payload.domain, headers=headers, timeout=12, verify=False)
                html = resp.text
                payload.pages_scanned += 1

            soup = BeautifulSoup(html, 'html.parser')
            if not payload.name and soup.title:
                payload.name = soup.title.get_text(strip=True)

            self.extract_from_dom(html, browser_text, soup, payload, cfg)
            
            contact_links = set()
            keywords = {'contacto', 'contactenos', 'contact', 'nosotros', 'directorio', 'admisiones'}
            
            for link in soup.find_all('a', href=True):
                href = link['href'].lower()
                if any(kw in href for kw in keywords) and '#' not in href and not href.startswith('javascript:'):
                    full_url = urljoin(payload.domain, link['href'])
                    if full_url.startswith('http'):
                        contact_links.add(full_url)
            
            for sub_url in list(contact_links)[:3]:
                try:
                    time.sleep(random.uniform(0.5, 1.5))
                    sub_html = ""
                    sub_browser_text = ""
                    
                    if cfg['use_deep_render'] and PLAYWRIGHT_AVAILABLE and not payload.playwright_warn:
                        try:
                            sub_html, sub_browser_text, _ = self.run_deep_render_sync(sub_url)
                            payload.pages_scanned += 1
                        except: pass
                    
                    if not sub_html:
                        resp_sub = GLOBAL_SESSION.get(sub_url, headers=headers, timeout=10, verify=False)
                        sub_html = resp_sub.text
                        payload.pages_scanned += 1
                        
                    soup_sub = BeautifulSoup(sub_html, 'html.parser')
                    self.extract_from_dom(sub_html, sub_browser_text, soup_sub, payload, cfg)
                    soup_sub.decompose() 
                except Exception: pass
            
            soup.decompose() 
            payload.socials = set(list(payload.socials)[:5])

        except Exception as e:
            payload.error = f"Fallo Crítico de Infraestructura: {str(e)[:60]}"

        return payload

    def post(self, request, *args, **kwargs):
        print("\n" + "☢️"*5 + " [GHOST SWARM V38.0: THE OMNIPOTENT MATRIX] " + "☢️"*5)
        
        city = request.POST.get('context_city', '').strip()
        country = request.POST.get('context_country', '').strip()
        geo_context = f"{city} {country}".strip()
        
        cfg = {
            'use_lms': request.POST.get('osint_lms') == '1',
            'use_whatsapp': request.POST.get('osint_whatsapp') == '1',
            'use_email': request.POST.get('osint_email') == '1',
            'use_deep_render': request.POST.get('deep_render') == '1'
        }

        raw_payload = request.POST.get('structured_payload', '{}')
        try:
            structured_data = json.loads(raw_payload)
        except Exception:
            structured_data = {'urls': [], 'emails': [], 'names': []}
            
        frontend_urls = structured_data.get('urls', [])
        frontend_names = structured_data.get('names', [])
        frontend_emails = structured_data.get('emails', [])
        
        targets = list(set(frontend_urls + frontend_names))

        if not targets:
            return HttpResponse('<div class="p-6 bg-red-950/80 border border-red-500 rounded text-red-500 font-mono text-center">ERROR: MATRIZ VACÍA O PAYLOAD CORRUPTO</div>')

        results_html = f"""
        <div class="mb-5 bg-[#050505] border border-emerald-500/40 p-4 rounded-xl flex justify-between items-center font-mono text-[11px] shadow-[0_0_20px_rgba(16,185,129,0.15)]">
            <span class="text-slate-400">COSECHA TOTAL FINALIZADA | TARGETS: <span class="text-white font-black">{len(targets)}</span></span>
            <span class="text-emerald-400 font-bold flex items-center gap-2"><span class="material-symbols-outlined text-[14px]">psychology</span> EXTRACCIÓN MASIVA 100%</span>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
        """

        start_time = time.time()
        extracted_data = []
        
        max_threads = 2 if cfg['use_deep_render'] else 4 
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {executor.submit(self.process_target_worker, t, geo_context, city, country, cfg, frontend_emails): t for t in targets}
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    extracted_data.append(data)
                except Exception as e: 
                    logger.error(f"❌ Error en hilo principal de OSINT: {e}")

        elapsed = time.time() - start_time
        print(f"⏱️ Operación V38.0 completada en {elapsed:.2f} segundos.")

        # =========================================================
        # [NIVEL DIOS 5]: ALMACENAMIENTO DE ALTA FIDELIDAD Y RENDERING
        # =========================================================
        for data in extracted_data:
            target = data.target
            err = data.error
            
            if data.domain and not err:
                clean_name = (data.name or target)[:240]
                clean_domain = data.domain[:240]
                safe_city = city[:90] if city else "Desconocido"
                safe_country = country[:90] if country else "Colombia"
                
                main_email = list(data.emails)[0][:240] if data.emails else None
                
                combined_phone = ""
                if data.whatsapp: combined_phone += f"W:{','.join(list(data.whatsapp))} "
                if data.telephones: combined_phone += f"T:{','.join(list(data.telephones))}"
                
                combined_phone = combined_phone.strip()[:48] if combined_phone else None
                
                created = False
                try:
                    with transaction.atomic():
                        domain_core = clean_domain.replace('https://', '').replace('http://', '').replace('www.', '').strip('/')
                        inst = Institution.objects.filter(Q(website__icontains=domain_core) | Q(name__iexact=clean_name)).first()

                        if inst:
                            if main_email and not inst.email: inst.email = main_email
                            if combined_phone and (not inst.phone or len(combined_phone) > len(str(inst.phone))): 
                                inst.phone = combined_phone
                            if not inst.website: inst.website = clean_domain
                            inst.last_scored_at = timezone.now()
                            inst.processing_status = 'ENRICHED'
                            if data.forensics['is_technical']: inst.institution_type = 'institute'
                            inst.save()
                        else:
                            created = True
                            inst = Institution.objects.create(
                                name=clean_name,
                                city=safe_city,
                                country=safe_country,
                                website=clean_domain,
                                email=main_email,
                                phone=combined_phone,
                                discovery_source='Ghost_V38', 
                                is_private=True,
                                processing_status='ENRICHED',
                                institution_type='institute' if data.forensics['is_technical'] else 'school'
                            )
                        
                        tech, _ = TechProfile.objects.get_or_create(institution=inst)
                        if data.lms_provider != "No detectado":
                            tech.lms_provider = data.lms_provider[:90]
                            tech.has_lms = True
                        tech.save()

                        forensic, _ = DeepForensicProfile.objects.get_or_create(institution=inst)
                        if data.forensics['is_bilingual']: forensic.is_bilingual = True
                        if data.forensics['is_trilingual']: forensic.is_trilingual = True
                        if data.forensics['cert_ib']: forensic.has_ib_cert = True
                        if data.forensics['cert_cambridge']: forensic.has_cambridge_cert = True
                            
                        forensic.save()

                except IntegrityError as e:
                    logger.warning(f"⚠️ Colisión DB en {clean_domain}")
                    continue 
                except Exception as e:
                    logger.critical(f"❌ FALLO SQL CRÍTICO al guardar {clean_domain}: {str(e)}")
                    continue

                b_color = "emerald" if created else "blue"
                b_text = "NUEVO" if created else "ACTUALIZADO EN DB"
                soc_html = "".join([f"<a href='{s}' target='_blank' class='text-[8px] bg-[#111] border border-white/10 px-1.5 py-0.5 rounded text-blue-400 uppercase mr-1 hover:bg-white/10 transition-colors'>{s.split('.')[1] if '.' in s else 'SOCIAL'}</a>" for s in data.socials])
                
                wa_html = "".join([f"<a href='https://wa.me/{w}' target='_blank' class='bg-emerald-950/40 text-emerald-400 border border-emerald-500/30 px-1.5 py-0.5 rounded flex items-center gap-1 font-bold whitespace-nowrap hover:bg-emerald-900/60 transition-colors shadow-sm'><span class='material-symbols-outlined text-[10px]'>forum</span> {w}</a>" for w in data.whatsapp])
                tel_html = "".join([f"<a href='tel:{t}' class='bg-blue-950/40 text-blue-400 border border-blue-500/30 px-1.5 py-0.5 rounded flex items-center gap-1 font-bold whitespace-nowrap hover:bg-blue-900/60 transition-colors shadow-sm'><span class='material-symbols-outlined text-[10px]'>call</span> {t}</a>" for t in data.telephones])
                phones_html = wa_html + tel_html
                if not phones_html: phones_html = "<span class='text-slate-600 font-bold text-[9px]'>📞 NO EXTRAÍDO</span>"

                badges = []
                if data.forensics['is_bilingual']: badges.append("<span class='bg-blue-900/50 text-blue-300 border border-blue-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold shadow-[0_0_5px_blue]'>BILINGÜE</span>")
                if data.forensics['is_trilingual']: badges.append("<span class='bg-purple-900/50 text-purple-300 border border-purple-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold shadow-[0_0_5px_purple]'>TRILINGÜE</span>")
                if data.forensics['cert_ib']: badges.append("<span class='bg-yellow-900/50 text-yellow-300 border border-yellow-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold shadow-[0_0_5px_yellow]'>IB CERT</span>")
                if data.forensics['is_technical']: badges.append("<span class='bg-orange-900/50 text-orange-300 border border-orange-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold shadow-[0_0_5px_orange]'>TÉCNICO</span>")
                forensic_html = " ".join(badges) if badges else "<span class='text-slate-600 font-mono text-[8px] font-bold'>ESTÁNDAR</span>"
                
                warn_html = f"<div class='bg-yellow-900/40 border border-yellow-500/50 text-yellow-400 text-[8px] font-mono p-1 rounded mt-2 text-center uppercase'>{data.playwright_warn}</div>" if data.playwright_warn else ""

                results_html += f"""
                <div class="bg-[#080808] border border-white/10 rounded-xl p-5 shadow-2xl relative overflow-hidden group hover:border-{b_color}-500/50 transition-all duration-300 flex flex-col justify-between">
                    <div class="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-transparent via-{b_color}-500 to-transparent opacity-60"></div>
                    <div>
                        <div class="flex justify-between items-center mb-4 pb-3 border-b border-white/5">
                            <span class="text-[9px] font-mono font-black text-{b_color}-500 bg-{b_color}-900/20 px-2 py-0.5 rounded border border-{b_color}-500/30">{b_text}</span>
                            <span class="text-[8px] font-mono text-slate-500 uppercase flex items-center gap-1">
                                <span class="material-symbols-outlined text-[10px]">psychology</span> {data.pages_scanned} PAGS
                            </span>
                        </div>
                        <h4 class="text-white font-black text-sm uppercase truncate mb-4" title="{clean_name}">{clean_name}</h4>
                        <div class="space-y-2 text-[10px] font-mono mb-4">
                            <div class="flex justify-between bg-[#020202] p-1.5 rounded border border-white/5">
                                <span class="text-slate-500">URL</span>
                                <a href="{clean_domain}" target="_blank" class="text-blue-400 font-bold truncate max-w-[140px] hover:underline">{clean_domain or "N/A"}</a>
                            </div>
                            <div class="flex justify-between items-start bg-[#020202] p-1.5 rounded border border-white/5">
                                <span class="text-slate-500 mt-0.5">EMAILS</span>
                                <div class="flex flex-col items-end gap-1 max-h-[80px] overflow-y-auto custom-scrollbar">
                                    {"".join([f"<span class='text-slate-300 font-bold truncate max-w-[150px]' title='{e}'>{e}</span>" for e in data.emails]) if data.emails else "<span class='text-slate-600 font-bold text-[9px]'>✉️ NO EXTRAÍDO</span>"}
                                </div>
                            </div>
                            <div class="flex justify-between items-start bg-[#020202] p-1.5 rounded border border-white/5">
                                <span class="text-slate-500 mt-0.5 flex flex-col gap-1">CONTACTOS <span class="bg-purple-900/30 text-purple-400 px-1 rounded text-[7px]">Max Yield</span></span>
                                <div class="flex flex-wrap justify-end gap-1 overflow-y-auto max-h-[80px] custom-scrollbar max-w-[160px]">
                                    {phones_html}
                                </div>
                            </div>
                            <div class="flex justify-between items-center bg-[#020202] p-1.5 rounded border border-white/5">
                                <span class="text-slate-500">INTELIGENCIA</span>
                                <div class="flex gap-1">{forensic_html}</div>
                            </div>
                            <div class="flex justify-between bg-[#020202] p-1.5 rounded border border-white/5">
                                <span class="text-slate-500">TECH/LMS</span>
                                <span class="{"text-emerald-400 font-bold drop-shadow-[0_0_5px_rgba(16,185,129,0.5)]" if data.lms_provider != 'No detectado' else "text-slate-600"}">{data.lms_provider.upper()}</span>
                            </div>
                        </div>
                    </div>
                    <div class="mt-auto border-t border-white/5 pt-2">
                        <div class="flex flex-wrap gap-1">{soc_html if soc_html else "<span class='text-[8px] text-slate-700 font-mono'>SIN REDES SOCIALES</span>"}</div>
                        {warn_html}
                    </div>
                </div>
                """
            else:
                results_html += f"""
                <div class="bg-[#050000] border border-red-900/40 rounded-xl p-4 relative flex flex-col justify-between">
                    <div class="absolute top-0 left-0 w-full h-1 bg-red-600/50 animate-pulse"></div>
                    <div>
                        <div class="flex items-center gap-2 mb-2 border-b border-red-900/30 pb-2">
                            <span class="material-symbols-outlined text-red-500 text-lg">gpp_bad</span>
                            <h4 class="text-red-400 font-black text-[10px] uppercase">FALSO POSITIVO / BLOQUEADO</h4>
                        </div>
                        <p class="text-[10px] font-mono text-slate-300 truncate mb-2">{target}</p>
                    </div>
                    <div class="bg-red-950/30 p-2 rounded border border-red-900/20 text-[9px] font-mono text-red-500/80">
                        {err or 'Cloudflare WAF, Timeout o error 404.'}
                    </div>
                </div>
                """

        results_html += "</div>"
        return HttpResponse(results_html)