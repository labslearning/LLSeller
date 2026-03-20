"""
======================================================================
[GOD TIER ARCHITECTURE: LEVIATHAN CLASS V30.0 - THE PHANTOM MATRIX]
PROJECT: GHOST SWARM (SILICON WADI / UNIT 8200 SPEC)
MODULE: COGNITIVE OSINT ENGINE & DEEP RENDER JS (PLAYWRIGHT)
ENGINEERING: MULTI-FRAME BREACH, BROWSER-NATIVE INNERTEXT, BIOMETRICS
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
from django.db.models import F
from django.core.cache import cache
from django.utils import timezone

# [GOD TIER]: Playwright Import for Deep JS Rendering
try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.critical("❌ [FATAL] Playwright no detectado. El Deep Render fallará. Ejecuta: pip install playwright && playwright install chromium")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Importación estricta de Modelos 
from .models import Interaction, Institution, TechProfile

logger = logging.getLogger("Sovereign.Intelligence")

# =========================================================
# [NIVEL DIOS 1]: INICIALIZACIÓN RAM & REGEX FORENSE
# =========================================================
PIXEL_BYTES = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")

BOT_REGEX = re.compile(r'(googleimageproxy|proofpoint|mimecast|barracuda|slackbot|whatsapp|telegrambot|applebot|outlook-com|yahoo|yandex|microsoft|spider|bot|crawler|scanner|datanyze)', re.IGNORECASE)

EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

# Atrapa ofuscaciones más agresivas
OBFUSCATED_EMAIL_REGEX = re.compile(r'([a-zA-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\s+at\s+|@|\[arroba\]|\s+en\s+)\s*([a-zA-Z0-9.-]+)\s*(?:\[dot\]|\(dot\)|\s+dot\s+|\.|\[punto\]|\s+punto\s+)\s*([a-zA-Z]{2,})', re.IGNORECASE)
CLOUDFLARE_HEX_REGEX = re.compile(r'/cdn-cgi/l/email-protection#([a-fA-F0-9]{4,})')
BASE64_HEURISTIC_REGEX = re.compile(r'(?i)(?:[A-Za-z0-9+/]{4}){5,}(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?')
SOCIAL_REGEX = re.compile(r'https?:\/\/(?:www\.)?(linkedin\.com|facebook\.com|instagram\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com)\/[a-zA-Z0-9_.-]+')

# Regex telefónico tolerante a espacios invisibles, paréntesis, guiones y formatos atípicos
RAW_PHONE_REGEX = re.compile(r'(?:(?:\+|00)[1-9]\d{0,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?)?\d{3,4}[\s.-]?\d{3,4}')

SERP_EXCLUSIONS = {'facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'youtube.com', 'wikipedia.org', 'paginasamarillas', 'directory', 'infoisinfo', 'tripadvisor', 'foursquare', 'civico', 'losmejorescolegios', 'wiktionary', 'dictionary', 'google.com'}
EDU_KEYWORDS = {'colegio', 'school', 'educación', 'educacion', 'gimnasio', 'liceo', 'instituto', 'academy', 'admisiones', 'estudiantes', 'matrículas', 'rectoría', 'alumnos', 'contact'}

EMAIL_GARBAGE_EXT = {'.png', '.jpg', '.jpeg', '.gif', '.css', '.js', '.webp', '.svg', '.woff', '.ttf', '.mp4', '.pdf'}
EMAIL_GARBAGE_DOMAINS = {'sentry.io', 'wixpress.com', 'example.com', 'domain.com', 'email.com', 'wix.com', 'localhost', 'sentry.wixpress.com'}
EMAIL_GARBAGE_PREFIXES = {'icon', 'logo', 'image', 'test', 'yourname', 'no-reply', 'noreply', 'sentry', '12345'}

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

# =========================================================
# [NIVEL DIOS 2]: UTILIDADES DE RED & DECRYPTORS
# =========================================================
def get_tactical_session():
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=1.5, status_forcelist=[403, 406, 429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

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
    # Elimina absolutamente todo lo que no sea dígito o +, incluyendo zero-width spaces
    num = re.sub(r'[^\d+]', '', raw_num)
    if num.startswith('00'): num = '+' + num[2:]
    return num

# =========================================================
# [NIVEL DIOS 2B]: PIXEL TRACKING VIEW (RESTAURADO)
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

    def search_engine_redundancy(self, query):
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

    def extract_from_dom(self, html_content, browser_visible_text, soup, data, use_email, use_whatsapp, use_lms):
        """[GOD TIER 6-PASS ENGINE]: Extracción Híbrida (HTML Crudo + Browser Render)."""
        
        # 1. Preparación del HTML Crudo (Para atributos y links)
        decoded_html = html_lib.unescape(html_content)
        decoded_html = re.sub(r'[\u200B-\u200D\uFEFF]', '', decoded_html)
        tagless_html = re.sub(r'<[^>]+>', '', decoded_html)
        
        raw_html_soup = BeautifulSoup(decoded_html, 'html.parser')

        # 2. Atributos invisibles (alt, title, data-)
        hidden_attributes_text = ""
        for tag in raw_html_soup.find_all(['img', 'a', 'div', 'span']):
            hidden_attributes_text += f" {tag.get('alt', '')} {tag.get('title', '')} "
            for key, val in tag.attrs.items():
                if isinstance(val, str): hidden_attributes_text += f" {val} "

        # 3. El Master Text ahora incluye el texto PURO renderizado por Chromium (browser_visible_text)
        # Esto destruye la separación de <span> y ofuscaciones visuales porque Chromium ya lo unió.
        master_text = f"{browser_visible_text} {hidden_attributes_text} {soup.get_text(separator=' ', strip=True)}"
        master_text = re.sub(r'[\u200B-\u200D\uFEFF]', '', master_text)

        # ==========================================
        # PASE 1: EXTRACCIÓN JSON-LD (SCHEMA.ORG)
        # ==========================================
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

                if use_email:
                    for e in extract_json_keys(json_data, 'email'):
                        if isinstance(e, str) and EMAIL_REGEX.match(e): data['ems'].append(e.lower())
                if use_whatsapp:
                    for t in extract_json_keys(json_data, 'telephone'):
                        if isinstance(t, str):
                            num = clean_phone_number(t)
                            if len(num) >= 7: data['tel'].append(num)
            except Exception:
                pass

        # ==========================================
        # PASE 2: EXTRACCIÓN DE EMAILS (BLINDAJE TOTAL)
        # ==========================================
        if use_email: 
            raw_emails = set(data['ems'])
            
            # A) Semántica Absoluta (Mailto)
            for a in raw_html_soup.find_all('a', href=True):
                href = urllib.parse.unquote(a['href'].lower())
                if href.startswith('mailto:'):
                    raw_emails.add(href[7:].split('?')[0].strip())

            # B) Cloudflare Decryptor Nativo
            for hex_str in CLOUDFLARE_HEX_REGEX.findall(decoded_html):
                decoded = decode_cloudflare_email(hex_str)
                if decoded: raw_emails.add(decoded)

            # C) Base64 Heuristics
            for b64 in BASE64_HEURISTIC_REGEX.findall(decoded_html):
                decoded = try_decode_base64(b64)
                if decoded: raw_emails.add(decoded)

            # D) Escaneo en Texto Tagless, Browser Text y Master Text
            raw_emails.update(EMAIL_REGEX.findall(tagless_html))
            raw_emails.update(EMAIL_REGEX.findall(master_text))
            
            # E) Desofuscación Heurística
            for obf in OBFUSCATED_EMAIL_REGEX.findall(master_text):
                raw_emails.add(f"{obf[0]}@{obf[1]}.{obf[2]}".lower())

            # F) Sanitización Extrema
            clean_emails = []
            for e in raw_emails:
                e_lower = e.lower().strip()
                if any(e_lower.endswith(ext) for ext in EMAIL_GARBAGE_EXT): continue
                if any(garbage in e_lower for garbage in EMAIL_GARBAGE_DOMAINS): continue
                if any(e_lower.startswith(prefix) for prefix in EMAIL_GARBAGE_PREFIXES): continue
                if EMAIL_REGEX.match(e_lower) and len(e_lower) < 60:
                    clean_emails.append(e_lower)
            data['ems'] = clean_emails

        # ==========================================
        # PASE 3: EXTRACCIÓN DE TELÉFONOS Y WA
        # ==========================================
        if use_whatsapp:
            raw_wa = set(data['wa'])
            raw_tel = set(data['tel'])
            
            # A) Precisión Semántica API (WA y Tel)
            for a in raw_html_soup.find_all('a', href=True):
                href = urllib.parse.unquote(a['href'].lower())
                if 'wa.me/' in href or 'api.whatsapp.com' in href or 'whatsapp://' in href:
                    num = clean_phone_number(href.split('?')[0])
                    if len(num) >= 8: raw_wa.add(num)
                elif href.startswith('tel:'):
                    num = clean_phone_number(href[4:])
                    if len(num) >= 7: raw_tel.add(num)
            
            # B) Escaneo Agresivo en Master Text (Visión de Máquina + Visión Humana Chromium)
            clean_text = re.sub(r'(?i)(pbx|tel|teléfono|telefono|cel|celular|whatsapp|fax|ext|nit|rut)[:.\-]?\s*', ' ', master_text)
            text_phones = RAW_PHONE_REGEX.findall(clean_text)
            
            for p in text_phones:
                num = clean_phone_number(p)
                if 7 <= len(num) <= 15:
                    if len(num) == 10 and num.startswith('3'): 
                        raw_wa.add(num)
                    elif len(num) > 10 and num.startswith(('573', '346', '347', '526', '527')): 
                        raw_wa.add(num)
                    else:
                        raw_tel.add(num)
            
            data['wa'] = list(raw_wa)
            data['tel'] = list(raw_tel)

        # ==========================================
        # PASE 4: REDES Y LMS
        # ==========================================
        data['socs'].extend(list(set([s.lower() for s in SOCIAL_REGEX.findall(decoded_html)])))

        if use_lms and data['lms'] == 'No detectado':
            html_lower = decoded_html.lower()
            links = [a.get('href', '').lower() for a in raw_html_soup.find_all('a', href=True)]
            for lms, sigs in LMS_SIGNATURES.items():
                if any(s in html_lower for s in sigs) or any(any(s in link for s in sigs) for link in links):
                    data['lms'] = lms
                    break

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
                        '--disable-features=IsolateOrigins,site-per-process' # Vital para hackear iframes
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
                
                # INYECCIÓN STEALTH SUPREMA
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => false});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4]});
                    window.chrome = { runtime: {} };
                """)
                
                try:
                    await page.goto(url, timeout=35000, wait_until='domcontentloaded')
                except PlaywrightTimeoutError:
                    pass 
                
                # Biometría Humana Simulada (Engaña a WAFs)
                await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
                await page.mouse.down()
                await page.mouse.up()
                
                # SCROLL Y AUTO-CLICKER EN LAZY LOAD
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
                await asyncio.sleep(2.0) # Esperamos que los scripts y APIs respondan a los clics
                
                # EXTRACCIÓN MULTI-FRAME (El Agujero Negro)
                # Hackeamos todos los iframes (ej: Widgets de Contacto)
                frames_html = []
                for frame in page.frames:
                    try:
                        frames_html.append(await frame.content())
                    except: pass
                
                html_content = "\n".join(frames_html)
                
                # EXTRACCIÓN DE VISIÓN DE MÁQUINA
                # Forzamos a Chrome a entregarnos exactamente el texto visual renderizado (Bypass de Spans y Shadow DOM)
                browser_visible_text = await page.evaluate("() => document.body.innerText")
                title = await page.title()
                
                await context.close()
                await browser.close()
                
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

    def worker_scan(self, target, geo_context, city, country, use_email, use_whatsapp, use_lms, pre_emails, deep_render):
        data = {
            't': target, 'dom': '', 'name': '', 
            'ems': list(pre_emails), 
            'wa': [], 'tel': [],
            'socs': [], 'lms': 'No detectado', 
            'err': None, 'pages_scanned': 0
        }
        
        try:
            session = get_tactical_session()
            headers = {
                'User-Agent': random.choice(TACTICAL_UAS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }

            if not re.match(r'^(https?:\/\/|www\.)', target.lower()):
                query = f"{target} {geo_context}"
                winning_url = self.cognitive_url_scorer(self.search_engine_redundancy(query), target, city, country)
                if not winning_url:
                    data['err'] = "Puntaje de confianza muy bajo o motor SERP bloqueado."
                    return data
                data['dom'] = winning_url
            else:
                data['dom'] = target if target.startswith('http') else f"https://{target}"

            # ==========================================
            # FASE CRÍTICA: EXTRACCIÓN PRIMARIA
            # ==========================================
            html = ""
            browser_text = ""
            title = ""
            
            if deep_render and PLAYWRIGHT_AVAILABLE:
                try:
                    html, browser_text, title = self.run_deep_render_sync(data['dom'])
                    data['name'] = title if title else data['name']
                    data['pages_scanned'] += 1
                except Exception:
                    resp = session.get(data['dom'], headers=headers, timeout=12, verify=False)
                    html = resp.text
                    data['pages_scanned'] += 1
            else:
                resp = session.get(data['dom'], headers=headers, timeout=12, verify=False)
                html = resp.text
                data['pages_scanned'] += 1

            soup = BeautifulSoup(html, 'html.parser')
            if not data['name'] and soup.title:
                data['name'] = soup.title.get_text(strip=True)

            self.extract_from_dom(html, browser_text, soup, data, use_email, use_whatsapp, use_lms)

            # ==========================================
            # CRAWLING PROFUNDO (Contact Sub-pages)
            # ==========================================
            contact_links = set()
            keywords = {'contacto', 'contactenos', 'contact', 'nosotros', 'directorio', 'admisiones'}
            
            for link in soup.find_all('a', href=True):
                href = link['href'].lower()
                if any(kw in href for kw in keywords) and '#' not in href and not href.startswith('javascript:'):
                    full_url = urljoin(data['dom'], link['href'])
                    if full_url.startswith('http'):
                        contact_links.add(full_url)
            
            for sub_url in list(contact_links)[:3]:
                try:
                    time.sleep(random.uniform(0.5, 1.5))
                    sub_html = ""
                    sub_browser_text = ""
                    
                    if deep_render and PLAYWRIGHT_AVAILABLE:
                        try:
                            sub_html, sub_browser_text, _ = self.run_deep_render_sync(sub_url)
                            data['pages_scanned'] += 1
                        except: pass
                    
                    if not sub_html:
                        resp_sub = session.get(sub_url, headers=headers, timeout=10, verify=False)
                        sub_html = resp_sub.text
                        data['pages_scanned'] += 1
                        
                    soup_sub = BeautifulSoup(sub_html, 'html.parser')
                    self.extract_from_dom(sub_html, sub_browser_text, soup_sub, data, use_email, use_whatsapp, use_lms)
                except Exception: pass

            # ==========================================
            # FASE FINAL: LIMPIEZA AGRESIVA
            # ==========================================
            def prioritize_emails(emails_list):
                priorities = ['admision', 'info', 'contacto', 'secretaria', 'rectoria']
                unique = list(set(emails_list))
                unique.sort(key=lambda x: (not any(p in x for p in priorities), x))
                return unique

            data['ems'] = prioritize_emails(data['ems'])
            data['wa'] = list(set(data['wa']))
            data['tel'] = list(set(data['tel']))
            data['socs'] = list(set(data['socs']))[:5]

        except Exception as e:
            data['err'] = f"Fallo Crítico de Infraestructura: {str(e)[:60]}"

        return data

    def post(self, request, *args, **kwargs):
        print("\n" + "☢️"*5 + " [GHOST SWARM V30.0: THE PHANTOM MATRIX] " + "☢️"*5)
        
        city = request.POST.get('context_city', '').strip()
        country = request.POST.get('context_country', '').strip()
        geo_context = f"{city} {country}".strip()
        
        u_lms = request.POST.get('osint_lms') == '1'
        u_wa = request.POST.get('osint_whatsapp') == '1'
        u_em = request.POST.get('osint_email') == '1'
        u_deep = request.POST.get('deep_render') == '1'

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
        
        max_threads = 2 if u_deep else 4 
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {executor.submit(self.worker_scan, t, geo_context, city, country, u_em, u_wa, u_lms, frontend_emails, u_deep): t for t in targets}
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    extracted_data.append(data)
                except Exception as e: 
                    logger.error(f"❌ Error en hilo principal de OSINT: {e}")

        elapsed = time.time() - start_time
        print(f"⏱️ Operación V30 completada en {elapsed:.2f} segundos.")

        # Guardado Transaccional y Renderizado
        for data in extracted_data:
            target = data['t']
            err = data['err']
            
            if data['dom'] and not err:
                clean_name = (data['name'] or target)[:200]
                clean_domain = data['dom'][:250]
                
                main_email = data['ems'][0][:250] if data['ems'] else None
                
                # Límite ajustado a 250 para evitar fallo en PostgreSQL si no cambiaste el modelo
                combined_phone = ""
                if data['wa']: 
                    wa_str = ",".join(data['wa'])
                    combined_phone += f"W:{wa_str} "
                if data['tel']: 
                    tel_str = ",".join(data['tel'])
                    combined_phone += f"T:{tel_str}"
                    
                combined_phone = combined_phone.strip()[:250] if combined_phone else None
                
                try:
                    with transaction.atomic():
                        domain_core = clean_domain.replace('https://', '').replace('http://', '').replace('www.', '').strip('/')
                        inst = Institution.objects.filter(website__icontains=domain_core).first()
                        if not inst:
                            inst = Institution.objects.filter(name__iexact=clean_name).first()

                        created = False
                        if inst:
                            if main_email and not inst.email: inst.email = main_email
                            if combined_phone and (not inst.phone or len(combined_phone) > len(str(inst.phone))): 
                                inst.phone = combined_phone
                            if not inst.website: inst.website = clean_domain
                            inst.last_scored_at = timezone.now()
                            inst.save()
                        else:
                            created = True
                            inst = Institution.objects.create(
                                name=clean_name,
                                city=city[:90],
                                country=country[:90] if country else "Colombia",
                                website=clean_domain,
                                email=main_email,
                                phone=combined_phone,
                                discovery_source='Ghost_V30', 
                                is_private=True
                            )
                        
                        tech, _ = TechProfile.objects.get_or_create(institution=inst)
                        if data['lms'] != "No detectado":
                            tech.lms_provider = data['lms'][:90]
                            tech.has_lms = True
                        tech.save()

                    b_color = "emerald" if created else "blue"
                    b_text = "NUEVO" if created else "ACTUALIZADO EN DB"
                    soc_html = "".join([f"<a href='{s}' target='_blank' class='text-[8px] bg-[#111] border border-white/10 px-1.5 py-0.5 rounded text-blue-400 uppercase mr-1 hover:bg-white/10 transition-colors'>{s.split('.')[1] if '.' in s else 'SOCIAL'}</a>" for s in data['socs']])
                    
                    wa_html = "".join([f"<a href='https://wa.me/{w}' target='_blank' class='bg-emerald-950/40 text-emerald-400 border border-emerald-500/30 px-1.5 py-0.5 rounded flex items-center gap-1 font-bold whitespace-nowrap hover:bg-emerald-900/60 transition-colors shadow-sm'><span class='material-symbols-outlined text-[10px]'>forum</span> {w}</a>" for w in data['wa']])
                    tel_html = "".join([f"<a href='tel:{t}' class='bg-blue-950/40 text-blue-400 border border-blue-500/30 px-1.5 py-0.5 rounded flex items-center gap-1 font-bold whitespace-nowrap hover:bg-blue-900/60 transition-colors shadow-sm'><span class='material-symbols-outlined text-[10px]'>call</span> {t}</a>" for t in data['tel']])
                    phones_html = wa_html + tel_html
                    if not phones_html: phones_html = "<span class='text-slate-600 font-bold text-[9px]'>📞 NO EXTRAÍDO</span>"

                    results_html += f"""
                    <div class="bg-[#080808] border border-white/10 rounded-xl p-5 shadow-2xl relative overflow-hidden group hover:border-{b_color}-500/50 transition-all duration-300 flex flex-col justify-between">
                        <div class="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-transparent via-{b_color}-500 to-transparent opacity-60"></div>
                        <div>
                            <div class="flex justify-between items-center mb-4 pb-3 border-b border-white/5">
                                <span class="text-[9px] font-mono font-black text-{b_color}-500 bg-{b_color}-900/20 px-2 py-0.5 rounded border border-{b_color}-500/30">{b_text}</span>
                                <span class="text-[8px] font-mono text-slate-500 uppercase flex items-center gap-1">
                                    <span class="material-symbols-outlined text-[10px]">psychology</span> {data['pages_scanned']} PAGS
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
                                    <div class="flex flex-col items-end gap-1">
                                        {"".join([f"<span class='text-slate-300 font-bold truncate max-w-[150px]'>{e}</span>" for e in data['ems']]) if data['ems'] else "<span class='text-slate-600 font-bold text-[9px]'>✉️ NO EXTRAÍDO</span>"}
                                    </div>
                                </div>
                                <div class="flex justify-between items-start bg-[#020202] p-1.5 rounded border border-white/5">
                                    <span class="text-slate-500 mt-0.5 flex flex-col gap-1">CONTACTOS <span class="bg-purple-900/30 text-purple-400 px-1 rounded text-[7px]">ALL Extracted</span></span>
                                    <div class="flex flex-wrap justify-end gap-1 overflow-hidden max-w-[160px]">
                                        {phones_html}
                                    </div>
                                </div>
                                <div class="flex justify-between bg-[#020202] p-1.5 rounded border border-white/5">
                                    <span class="text-slate-500">TECH/LMS</span>
                                    <span class="{"text-emerald-400 font-bold drop-shadow-[0_0_5px_rgba(16,185,129,0.5)]" if data['lms'] != 'No detectado' else "text-slate-600"}">{data['lms'].upper()}</span>
                                </div>
                            </div>
                        </div>
                        <div class="mt-auto border-t border-white/5 pt-2">
                            <div class="flex flex-wrap gap-1">{soc_html if soc_html else "<span class='text-[8px] text-slate-700 font-mono'>SIN REDES SOCIALES</span>"}</div>
                        </div>
                    </div>
                    """
                except IntegrityError as e:
                    results_html += f"""
                    <div class="bg-yellow-950/20 border border-yellow-700/50 rounded-xl p-4 relative">
                        <div class="absolute top-0 left-0 w-full h-1 bg-yellow-500/50"></div>
                        <h4 class="text-yellow-500 font-black text-[10px] uppercase mb-1 flex items-center gap-1"><span class="material-symbols-outlined text-[14px]">warning</span> COLISIÓN EN DB</h4>
                        <p class="text-[9px] font-mono text-slate-400">Dominio <span class="text-white font-bold">{clean_domain}</span> en conflicto.</p>
                    </div>
                    """
                except Exception as e:
                    pass
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
                        {err or 'Cloudflare WAF o error 404.'}
                    </div>
                </div>
                """

        results_html += "</div>"
        return HttpResponse(results_html)