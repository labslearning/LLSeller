"""
======================================================================
[GOD TIER ARCHITECTURE: LEVIATHAN CLASS V20.0 - OMNISCIENT SCORER]
PROJECT: GHOST SWARM (SILICON WADI / UNIT 8200 SPEC)
MODULE: COGNITIVE OSINT ENGINE & SERP REDUNDANCY
ENGINEERING: HEAVY ARTILLERY PATTERN MATCHING & SCORING
======================================================================
"""

import re
import time
import base64
import logging
import uuid
import random
import requests
import urllib3
import concurrent.futures
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
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
PHONE_REGEX = re.compile(r'(?:wa\.me\/|api\.whatsapp\.com\/send\?phone=|\+?\d{2,4}[-\s]?\d{3}[-\s]?\d{3}[-\s]?\d{3,4})')
SOCIAL_REGEX = re.compile(r'https?:\/\/(?:www\.)?(linkedin\.com|facebook\.com|instagram\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com)\/[a-zA-Z0-9_.-]+')

# Dominios basura que jamás deben ganar el puntaje
SERP_EXCLUSIONS = ['facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'youtube.com', 'wikipedia.org', 'paginasamarillas', 'directory', 'infoisinfo', 'tripadvisor', 'foursquare', 'civico', 'losmejorescolegios', 'wiktionary', 'dictionary']

# Palabras clave de la industria (Aumentan el puntaje de la URL si se encuentran en la web)
EDU_KEYWORDS = ['colegio', 'school', 'educación', 'educacion', 'gimnasio', 'liceo', 'instituto', 'academy', 'admisiones', 'estudiantes', 'matrículas', 'rectoría', 'alumnos']

LMS_SIGNATURES = {
    # 🌍 TITANES GLOBALES
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
    
    # 🇨🇴 PESOS PESADOS COLOMBIA Y LATAM
    'Phidias': ['phidias.co', 'phidias.ac', 'phidias'],
    'Ciudad Educativa': ['ciudadeducativa.com'],
    'Sistema Saberes': ['sistemasaberes.com', 'saberes.com'],
    'Gnosoft': ['gnosoft', 'gnosoft.com.co'],
    'Pegaso': ['pegaso.com.co', 'pegaso', 'pegasopro'],
    'Cibercolegios': ['cibercolegios.com', 'cibercolegios'],
    'Q10 Académico': ['q10.com', 'q10academico', 'q10'],
    'Integra': ['plataformaintegra.net', 'plataformaintegra'],
    'SIGA': ['sigaweb', 'edusiga', 'siganet', 'sigaportal', 'sigaapp', ' Luis Albeiro Rios'],
    'Colegios Colombia': ['colegiosonline.com', 'portalcolegioscolombia', 'masteracademic', 'colegioscolombia.net'],
    'Ovy': ['ovy.co', 'plataforma ovy'],
    'WebColegios': ['webcolegios.com'],
    'Bicol': ['bicol.com.co'],
    'Schoolnet': ['schoolnet', 'colegios-online'],

    # 📚 PLATAFORMAS EDITORIALES Y ESPAÑA/PERÚ
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
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0'
]


# =========================================================
# [NIVEL DIOS 2]: UTILIDADES DE RED & TELEMETRÍA PIXEL
# =========================================================
def get_tactical_session():
    """Sesión HTTP con reintentos agresivos para penetrar Firewalls."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[403, 429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

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
        """Búsqueda Híbrida: Si la API falla, hace Web Scraping directo al buscador."""
        urls = []
        # Intento 1: DuckDuckGo API (Lite Backend para evadir bloqueos de Bing)
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(query, backend="lite", max_results=5))
                for r in results:
                    href = r.get('href', r.get('link', r.get('url', '')))
                    if href and not any(exc in href.lower() for exc in SERP_EXCLUSIONS):
                        urls.append(href)
        except Exception as e:
            print(f"      [!] API DDG Falló: {e}")

        # Intento 2: Raw HTML Scraping si la API devolvió 0
        if not urls:
            try:
                headers = {'User-Agent': random.choice(TACTICAL_UAS)}
                resp = requests.get(f"https://html.duckduckgo.com/html/?q={query}", headers=headers, timeout=5)
                soup = BeautifulSoup(resp.text, 'html.parser')
                for a in soup.find_all('a', class_='result__url'):
                    href = a.get('href', '')
                    if href and 'http' in href and not any(exc in href.lower() for exc in SERP_EXCLUSIONS):
                        urls.append(href)
            except Exception as e:
                print(f"      [!] Scraper DDG Falló: {e}")
        
        # Devolvemos máximo 3 URLs viables para analizarlas
        return list(dict.fromkeys(urls))[:3]

    def cognitive_url_scorer(self, urls, target, city, country):
        """MOTOR DE TRIANGULACIÓN: Lee el HTML de varias URLs y elige la correcta."""
        best_url = None
        best_score = -1
        best_soup = None
        best_html = ""
        best_title = ""
        
        session = get_tactical_session()
        headers = {'User-Agent': random.choice(TACTICAL_UAS)}

        target_words = [w.lower() for w in target.split() if len(w) > 2]

        for url in urls:
            try:
                resp = session.get(url, headers=headers, timeout=6, verify=False)
                html = resp.text
                soup = BeautifulSoup(html, 'html.parser')
                html_lower = html.lower()
                
                score = 0
                #title = soup.title.string.strip() if soup.title and soup.title.string else ""
                title = soup.title.get_text(strip=True) if soup.title else ""
                
                # 1. Puntuación de Coincidencia de Nombre (Title y Body)
                for word in target_words:
                    if word in title.lower(): score += 15
                    if word in html_lower: score += 5
                
                # 2. Puntuación Geográfica
                if city and city.lower() in html_lower: score += 20
                if country and country.lower() in html_lower: score += 10
                
                # 3. Puntuación de Industria (¿Es un colegio?)
                for kw in EDU_KEYWORDS:
                    if kw in html_lower: score += 5
                
                print(f"      [SCORE] {url} => {score} pts")

                # Actualizamos el ganador
                if score > best_score:
                    best_score = score
                    best_url = resp.url # URL final tras redirecciones
                    best_soup = soup
                    best_html = html
                    best_title = title

            except Exception as e:
                print(f"      [x] Error puntuando {url}: {str(e)[:30]}")
                continue

        # Si el mejor puntaje es muy bajo, lo descartamos (Falso Positivo)
        if best_score < 15:
            return None, None, "", ""
            
        return best_url, best_soup, best_html, best_title

    def extract_from_dom(self, html, soup, data, use_email, use_whatsapp, use_lms):
        """Extractor Forense Híbrido."""
        if use_email: 
            found = EMAIL_REGEX.findall(html)
            for a in soup.find_all('a', href=True):
                if a['href'].startswith('mailto:'):
                    clean_mail = a['href'].replace('mailto:', '').split('?')[0].strip()
                    if EMAIL_REGEX.match(clean_mail): found.append(clean_mail)
            
            basura = ('.png', '.jpg', '.jpeg', '.gif', '.css', '.js', '.webp', 'sentry.io', 'example.com', 'wixpress.com')
            clean = [e.lower() for e in found if not e.endswith(basura) and not e.startswith('info@wix')]
            data['ems'].extend(clean)

        if use_whatsapp:
            found = PHONE_REGEX.findall(html)
            for a in soup.find_all('a', href=True):
                if a['href'].startswith('tel:'): found.append(a['href'].replace('tel:', ''))
                elif 'wa.me/' in a['href'] or 'api.whatsapp.com' in a['href']: found.append(a['href'])
            clean = [re.sub(r'[^\d+]', '', p) for p in found if len(re.sub(r'[^\d]', '', p)) >= 8]
            data['phs'].extend(clean)

        found_socs = SOCIAL_REGEX.findall(html)
        data['socs'].extend([s.lower() for s in found_socs])

        if use_lms and data['lms'] == 'No detectado':
            html_lower = html.lower()
            links = [a.get('href', '').lower() for a in soup.find_all('a', href=True)]
            for lms, sigs in LMS_SIGNATURES.items():
                if any(s in html_lower for s in sigs) or any(any(s in link for s in sigs) for link in links):
                    data['lms'] = lms
                    break

    def worker_scan(self, target, geo_context, city, country, use_email, use_whatsapp, use_lms):
        """DRONE OMNISCIENTE. Búsqueda -> Puntuación -> Deep Crawl."""
        data = {'t': target, 'dom': '', 'name': '', 'ems': [], 'phs': [], 'socs': [], 'lms': 'No detectado', 'err': None, 'pages_scanned': 0}
        
        try:
            session = get_tactical_session()
            headers = {'User-Agent': random.choice(TACTICAL_UAS)}

            # ==========================================
            # FASE A: BÚSQUEDA Y PUNTUACIÓN COGNITIVA
            # ==========================================
            if not re.match(r'^(https?:\/\/|www\.)', target.lower()):
                # Usamos comillas para buscar la frase exacta si es posible
                query = f"{target} {city} {country}"
                print(f"   -> [BUSCANDO] {query}")
                urls_to_check = self.search_engine_redundancy(query)
                
                if not urls_to_check:
                    data['err'] = "Motor SERP bloqueado o 0 resultados viables."
                    return data

                # El motor evalúa las URLs y elige la oficial
                winning_url, soup, html, title = self.cognitive_url_scorer(urls_to_check, target, city, country)
                
                if not winning_url:
                    data['err'] = "Puntaje de confianza muy bajo. Posible Falso Positivo."
                    return data
                
                data['dom'] = winning_url
                data['name'] = title if title else target
                data['pages_scanned'] += 1
            else:
                # Si ya dieron la URL directa, la analizamos
                data['dom'] = target if target.startswith('http') else f"https://{target}"
                resp = session.get(data['dom'], headers=headers, timeout=10, verify=False)
                html = resp.text
                soup = BeautifulSoup(html, 'html.parser')
                data['dom'] = resp.url
                #data['name'] = soup.title.string.strip() if soup.title else target
                data['name'] = soup.title.get_text(strip=True) if soup.title else target
                data['pages_scanned'] += 1

            # ==========================================
            # FASE B: DEEP CRAWLING (NAVEGACIÓN)
            # ==========================================
            print(f"   -> [TRIANGULADO] URL Ganadora: {data['dom']}")
            
            # Extraemos de la Home (Ya la tenemos descargada)
            self.extract_from_dom(html, soup, data, use_email, use_whatsapp, use_lms)

            # NAVEGACIÓN A SUB-PÁGINAS (Contacto)
            if (use_email and not data['ems']) or (use_whatsapp and not data['phs']):
                contact_links = []
                keywords = ['contacto', 'contactenos', 'contact', 'nosotros', 'directorio', 'admisiones']
                
                for link in soup.find_all('a', href=True):
                    href = link['href'].lower()
                    if any(kw in href for kw in keywords):
                        full_url = urljoin(data['dom'], link['href'])
                        if full_url not in contact_links and '#' not in link['href'] and full_url.startswith('http'):
                            contact_links.append(full_url)
                
                for sub_url in contact_links[:2]:
                    try:
                        print(f"      [DEEP CRAWL] Scaneando: {sub_url}")
                        time.sleep(random.uniform(0.5, 1.5))
                        resp_sub = session.get(sub_url, headers=headers, timeout=8, verify=False)
                        data['pages_scanned'] += 1
                        soup_sub = BeautifulSoup(resp_sub.text, 'html.parser')
                        self.extract_from_dom(resp_sub.text, soup_sub, data, use_email, use_whatsapp, use_lms)
                        
                        if len(set(data['ems'])) >= 2: break
                    except Exception: pass

            # ==========================================
            # FASE C: LIMPIEZA Y PRIORIZACIÓN
            # ==========================================
            def prioritize_emails(emails):
                priorities = ['admision', 'info', 'contacto', 'secretaria', 'rectoria']
                unique = list(set(emails))
                unique.sort(key=lambda x: (not any(p in x for p in priorities), x))
                return unique

            data['ems'] = prioritize_emails(data['ems'])[:3]
            data['phs'] = list(set(data['phs']))[:2]
            data['socs'] = list(set(data['socs']))[:4]

        except Exception as e:
            data['err'] = f"Fallo Crítico: {str(e)[:60]}"

        return data

    def post(self, request, *args, **kwargs):
        print("\n" + "☢️"*5 + " [GHOST SWARM V20: OMNISCIENT ENGINE] " + "☢️"*5)
        
        raw_targets = request.POST.get('search_query', '')
        city = request.POST.get('context_city', '').strip()
        country = request.POST.get('context_country', '').strip()
        
        u_lms = request.POST.get('osint_lms') == 'true'
        u_wa = request.POST.get('osint_whatsapp') == 'true'
        u_em = request.POST.get('osint_email') == 'true'

        targets = list(set([t.strip() for t in re.split(r'[\n,;]+', raw_targets) if len(t.strip()) > 3]))

        if not targets:
            return HttpResponse('<div class="p-6 bg-red-950/80 border border-red-500 rounded text-red-500 font-mono text-center">ERROR: MATRIZ VACÍA</div>')

        geo_context = f"{city} {country}".strip()
        results_html = f"""
        <div class="mb-5 bg-[#050505] border border-emerald-500/40 p-4 rounded-xl flex justify-between items-center font-mono text-[11px] shadow-[0_0_20px_rgba(16,185,129,0.15)]">
            <span class="text-slate-400">ANÁLISIS COGNITIVO FINALIZADO | TARGETS: <span class="text-white font-black">{len(targets)}</span></span>
            <span class="text-emerald-400 font-bold flex items-center gap-2"><span class="material-symbols-outlined text-[14px]">psychology</span> SCORE VÁLIDO</span>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
        """

        start_time = time.time()
        extracted_data = []
        
        # Enjambre Multi-Hilo: 4 Workers es el equilibrio perfecto entre velocidad y evasión
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(self.worker_scan, t, geo_context, city, country, u_em, u_wa, u_lms): t for t in targets}
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    extracted_data.append(data)
                except Exception as e: 
                    print(f"❌ Error en hilo principal: {e}")

        elapsed = time.time() - start_time
        print(f"⏱️ Deep Crawl completado en {elapsed:.2f} segundos.")

        # Guardado en DB 
        for data in extracted_data:
            target = data['t']
            err = data['err']
            
            if data['dom'] and not err:
                clean_name = (data['name'] or target)[:200]
                clean_domain = data['dom'][:250]
                main_email = data['ems'][0][:250] if data['ems'] else None
                main_phone = data['phs'][0][:45] if data['phs'] else None
                
                try:
                    with transaction.atomic():
                        inst, created = Institution.objects.update_or_create(
                            name=clean_name,
                            city=city[:90],
                            defaults={
                                'country': country[:90] if country else "Colombia",
                                'website': clean_domain,
                                'email': main_email,
                                'phone': main_phone,
                                'discovery_source': 'Ghost_V20', 
                                'is_private': True,
                            }
                        )
                        
                        tech, _ = TechProfile.objects.get_or_create(institution=inst)
                        tech.lms_provider = data['lms'][:90]
                        tech.has_lms = (data['lms'] != "No detectado")
                        tech.save()

                    b_color = "emerald" if created else "blue"
                    b_text = "NUEVO" if created else "ACTUALIZADO"
                    
                    soc_html = "".join([f"<a href='{s}' target='_blank' class='text-[8px] bg-[#111] border border-white/10 px-1.5 py-0.5 rounded text-blue-400 uppercase mr-1'>{s.split('.')[1] if '.' in s else 'SOCIAL'}</a>" for s in data['socs']])

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
                                    <a href="{clean_domain}" target="_blank" class="text-blue-400 font-bold truncate max-w-[140px]">{clean_domain or "N/A"}</a>
                                </div>
                                <div class="flex justify-between bg-[#020202] p-1.5 rounded border border-white/5">
                                    <span class="text-slate-500">EMAIL</span>
                                    <span class="text-slate-300 font-bold">{main_email or "NULL"}</span>
                                </div>
                                <div class="flex justify-between bg-[#020202] p-1.5 rounded border border-white/5">
                                    <span class="text-slate-500">TELÉFONO</span>
                                    <span class="text-slate-300 font-bold">{main_phone or "NULL"}</span>
                                </div>
                                <div class="flex justify-between bg-[#020202] p-1.5 rounded border border-white/5">
                                    <span class="text-slate-500">TECH/LMS</span>
                                    <span class="{"text-emerald-400 font-bold drop-shadow-[0_0_5px_rgba(16,185,129,0.5)]" if data['lms'] != 'No detectado' else "text-slate-600"}">{data['lms'].upper()}</span>
                                </div>
                            </div>
                        </div>
                        <div class="mt-auto border-t border-white/5 pt-2">
                            <div class="flex flex-wrap">{soc_html if soc_html else "<span class='text-[8px] text-slate-700 font-mono'>SIN REDES SOCIALES</span>"}</div>
                        </div>
                    </div>
                    """
                except IntegrityError as e:
                    results_html += f"""
                    <div class="bg-yellow-950/20 border border-yellow-700/50 rounded-xl p-4 relative">
                        <div class="absolute top-0 left-0 w-full h-1 bg-yellow-500/50"></div>
                        <h4 class="text-yellow-500 font-black text-[10px] uppercase mb-1 flex items-center gap-1"><span class="material-symbols-outlined text-[14px]">warning</span> COLISIÓN EN DB</h4>
                        <p class="text-[9px] font-mono text-slate-400">El dominio <span class="text-white font-bold">{clean_domain}</span> ya existe en el directorio.</p>
                    </div>
                    """
                except Exception as e:
                    results_html += f"""
                    <div class="bg-red-950/20 border border-red-900/50 rounded-xl p-4 relative">
                        <div class="absolute top-0 left-0 w-full h-1 bg-red-600/50"></div>
                        <h4 class="text-red-400 font-black text-[10px] uppercase mb-1">ERROR DE INYECCIÓN</h4>
                        <p class="text-[9px] font-mono text-red-500/80">{str(e)[:100]}</p>
                    </div>
                    """
            else:
                results_html += f"""
                <div class="bg-[#050000] border border-red-900/40 rounded-xl p-4 relative flex flex-col justify-between">
                    <div class="absolute top-0 left-0 w-full h-1 bg-red-600/50 animate-pulse"></div>
                    <div>
                        <div class="flex items-center gap-2 mb-2 border-b border-red-900/30 pb-2">
                            <span class="material-symbols-outlined text-red-500 text-lg">gpp_bad</span>
                            <h4 class="text-red-400 font-black text-[10px] uppercase">FALSO POSITIVO DESCARTADO</h4>
                        </div>
                        <p class="text-[10px] font-mono text-slate-300 truncate mb-2">{target}</p>
                    </div>
                    <div class="bg-red-950/30 p-2 rounded border border-red-900/20 text-[9px] font-mono text-red-500/80">
                        {err or 'El motor no pudo verificar la identidad del colegio con precisión.'}
                    </div>
                </div>
                """

        results_html += "</div>"
        return HttpResponse(results_html)