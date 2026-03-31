"""
================================================================================
[TRANSCENDENT GOD TIER ARCHITECTURE: OMEGA QUANTUM LEVIATHAN CLASS ∞]
PROJECT: GHOST SWARM - COSMIC INTELLIGENCE HARVESTER
VERSION: 99.9.9.9.9
MODULE: COGNITIVE OSINT ENGINE + DEEP RENDER JS (PLAYWRIGHT) + COSMIC AI
ENGINEERING: MAXIMUM YIELD EXTRACTION, LLM SEMANTIC ANALYSIS, QUANTUM CACHING
DATABASE: SIDECAR MODEL ISOLATION (DeepForensicProfile)
STANDARD: SILICON VALLEY / TEL AVIV / WADI / SHANGHAI / TOKYO / DUBLIN / LONDON
================================================================================
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
import hashlib
import sqlite3
import traceback
from pathlib import Path
from typing import Tuple, Optional, Set, List, Dict, Any, Union
from urllib.parse import urlparse, urljoin
from datetime import datetime
from dataclasses import dataclass, field, asdict
from functools import wraps

from bs4 import BeautifulSoup, Comment
from duckduckgo_search import DDGS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from django.http import HttpResponse, HttpRequest, JsonResponse
from django.views import View
from django.views.generic import TemplateView
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET
from django.db import transaction, IntegrityError
from django.db.models import F, Q
from django.core.cache import cache
from django.utils import timezone
from django.conf import settings

# =========================================================
# [TIER 0]: COSMIC AI ANALYZER IMPORT
# =========================================================
try:
    from sales.engine.ai_analyzer import get_analyzer, analyze_institution, InstitutionProfile
    AI_ANALYZER_AVAILABLE = True
    logger_ai = logging.getLogger("Sovereign.CosmicAnalyzer")
    logger_ai.info("✅ Cosmic AI Analyzer loaded successfully")
except ImportError as e:
    AI_ANALYZER_AVAILABLE = False
    logger_ai = logging.getLogger("Sovereign.CosmicAnalyzer")
    logger_ai.warning(f"⚠️ Cosmic AI Analyzer not available: {e}")

# =========================================================
# [TIER 1]: PLAYWRIGHT ENGINE GUARD
# =========================================================
try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.critical("❌ [FATAL] Playwright no detectado. El Deep Render fallará. Ejecuta: pip install playwright && playwright install chromium")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# [CORRECCIÓN CRÍTICA]: Importación Estricta de la Capa de Datos
from .models import Interaction, Institution, TechProfile, DeepForensicProfile

logger = logging.getLogger("Sovereign.Intelligence")

# =========================================================
# [TIER 0.5]: QUANTUM PERSISTENT CACHE (GOD TIER)
# =========================================================
CACHE_DIR = Path("/tmp/cosmic_quantum_cache")
CACHE_DIR.mkdir(exist_ok=True)
DB_PATH = CACHE_DIR / "cosmic_cache.db"

class QuantumPersistentCache:
    """Cache persistente en disco con SQLite - GOD TIER OMEGA"""
    
    def __init__(self, db_path: Path = DB_PATH, ttl_seconds: int = 86400):
        self.db_path = db_path
        self.ttl = ttl_seconds
        self._memory_cache: Dict[str, Tuple[float, Any]] = {}
        self._init_db()
    
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quantum_cache (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    timestamp REAL,
                    ttl INTEGER,
                    access_count INTEGER DEFAULT 0,
                    last_access REAL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON quantum_cache(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_access ON quantum_cache(access_count)")
    
    def get(self, key: str) -> Optional[Any]:
        """Obtiene del cache con prioridad de acceso"""
        # Memory cache L1
        if key in self._memory_cache:
            timestamp, value = self._memory_cache[key]
            if time.time() - timestamp < self.ttl:
                return value
            del self._memory_cache[key]
        
        # Disk cache L2
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT value, timestamp FROM quantum_cache WHERE key = ?",
                    (key,)
                )
                row = cursor.fetchone()
                if row:
                    value, timestamp = row
                    if time.time() - timestamp < self.ttl:
                        # Promover a memoria L1
                        self._memory_cache[key] = (timestamp, json.loads(value))
                        # Incrementar contador de acceso
                        conn.execute(
                            "UPDATE quantum_cache SET access_count = access_count + 1, last_access = ? WHERE key = ?",
                            (time.time(), key)
                        )
                        conn.commit()
                        return json.loads(value)
        except Exception as e:
            logger.debug(f"Quantum cache read error: {e}")
        return None
    
    def set(self, key: str, value: Any):
        """Guarda en cache con promoción automática"""
        try:
            serialized = json.dumps(value, ensure_ascii=False)
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO quantum_cache 
                       (key, value, timestamp, ttl, access_count, last_access) 
                       VALUES (?, ?, ?, ?, 0, ?)""",
                    (key, serialized, time.time(), self.ttl, time.time())
                )
                conn.commit()
            
            # Mantener memoria L1 limitada a 200 items (LRU)
            self._memory_cache[key] = (time.time(), value)
            if len(self._memory_cache) > 200:
                oldest = min(self._memory_cache.keys(), 
                            key=lambda k: self._memory_cache[k][0])
                del self._memory_cache[oldest]
        except Exception as e:
            logger.debug(f"Quantum cache write error: {e}")
    
    def clear(self):
        self._memory_cache.clear()
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM quantum_cache")
                conn.commit()
        except Exception as e:
            logger.debug(f"Quantum cache clear error: {e}")

# Instancia global del cache cuántico
QUANTUM_CACHE = QuantumPersistentCache()

# =========================================================
# [TIER 2]: MATRIZ OOP DE EXPRESIONES REGULARES (SCOPE SHIELD) - MEJORADA
# =========================================================
# =========================================================
# [TIER 2]: MATRIZ OOP DE EXPRESIONES REGULARES (SCOPE SHIELD)
# =========================================================
class MatrixRegex:
    BOT = re.compile(r'(googleimageproxy|proofpoint|mimecast|barracuda|slackbot|whatsapp|telegrambot|applebot|outlook-com|yahoo|yandex|microsoft|spider|bot|crawler|scanner|datanyze)', re.IGNORECASE)
    EMAIL = re.compile(r'[a-zA-Z0-9._%+-\u00C0-\u00FF]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    OBFUSCATED_EMAIL = re.compile(r'([a-zA-Z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\s+at\s+|@|\[arroba\]|\s+en\s+|%40)\s*([a-zA-Z0-9.-]+)\s*(?:\[dot\]|\(dot\)|\s+dot\s+|\.|\[punto\]|\s+punto\s+|%2E)\s*([a-zA-Z]{2,})', re.IGNORECASE)
    CLOUDFLARE_HEX = re.compile(r'/cdn-cgi/l/email-protection#([a-fA-F0-9]{4,})')
    CLOUDFLARE_JS = re.compile(r'<script[^>]*type="text/javascript"[^>]*>.*?cf_email.*?</script>', re.IGNORECASE | re.DOTALL)
    BASE64_HEURISTIC = re.compile(r'(?i)(?:[A-Za-z0-9+/]{4}){5,}(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?')
    SOCIAL = re.compile(r'https?:\/\/(?:www\.)?(linkedin\.com|facebook\.com|instagram\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com|whatsapp\.com)\/[a-zA-Z0-9_.-]+')
    
    # Telefonía Colombia - Patrones mejorados
    CONTEXT_BLOCK = re.compile(r'(?i)(?:pbx|tel[ée]fono|tel|celular|cel|whatsapp|wa|contactenos|ll[áa]manos|l[íi]nea|contacto|comunicate|asistencia|soporte|emergencia)(?:[^a-zA-Z]{0,50})')
    RAW_PHONE_FINDER = re.compile(r'(?:(?:\+|00)[1-9]\d{0,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?)?\d{3,4}[\s.-]?\d{3,4}')
    STRICT_HREF_PHONE = re.compile(r'(?i)(?:tel:|wa\.me/|whatsapp://send\?phone=)([+0-9]+)')
    RAW_PHONE_FALLBACK = re.compile(r'(?:(?:\+|00)[1-9]\d{0,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?)?\d{3,4}[\s.-]?\d{3,4}')
    
    # 🔥 NUEVOS ATRIBUTOS PARA EXTRACURRICULARES (CORRECCIÓN)
    extracurricular_sports = re.compile(r'\b(deportes|sports|fútbol|soccer|baloncesto|basketball|voleibol|volleyball|natación|swimming|atletismo|athletics|tenis|tennis)\b', re.IGNORECASE)
    extracurricular_arts = re.compile(r'\b(artes|arts|música|music|teatro|theater|danza|dance|pintura|painting|dibujo|drawing|orquesta|orchestra|coral|choir)\b', re.IGNORECASE)
    extracurricular_clubs = re.compile(r'\b(clubes|clubs|talleres|workshops|ajedrez|chess|debate|robótica|robotica|programación|programacion|ciencias|science)\b', re.IGNORECASE)
    
    # Patrones de certificaciones académicas avanzadas
    IB_PATTERN = re.compile(r'\b(?:bachillerato internacional|international baccalaureate|ib world school|ib diploma|ib program|ib continuum|ib.org|ib programme)\b', re.IGNORECASE)
    CAMBRIDGE_PATTERN = re.compile(r'\b(?:cambridge english|cambridge assessment|cambridge international|cambridge igcse|cambridge a levels|cambridge o levels|cambridge primary|cambridge secondary|cambridge checkpoint)\b', re.IGNORECASE)
    OXFORD_PATTERN = re.compile(r'\b(?:oxford international|oxford aqa|oxford quality|oxford english|oxford university press)\b', re.IGNORECASE)
    STEM_PATTERN = re.compile(r'\b(?:stem|steam|ciencia y tecnología|tecnología educativa|science technology engineering math|ciencia tecnologia ingenieria matematicas)\b', re.IGNORECASE)
    ROBOTICS_PATTERN = re.compile(r'\b(?:robótica|robotica|robotics|lego education|vex|first lego league|arduino|maker|makerspace|robot|programación|programacion|coding|code|scratch|python|java|javascript)\b', re.IGNORECASE)
    ICFES_PATTERN = re.compile(r'\b(?:icfes|saber 11|saber pro|resultados icfes|puntaje icfes|ranking icfes|categoría [a-d]|[0-9]{2,3}\s*(?:puntos?|pts?))\b', re.IGNORECASE)
    
    # Patrones de convenios y alianzas
    AGREEMENT_PATTERN = re.compile(r'\b(?:convenio|alianza|acuerdo|partnership|agreement|colaboración|colaboracion|cooperación|cooperacion|articulación|articulacion|universidad|university|corporación|corporacion|empresa|company)\b', re.IGNORECASE)

PIXEL_BYTES = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")

# [INYECCIÓN FORENSE GOD-TIER]: Firmas Comerciales Deep B2B (AMPLIADO MÁXIMO)
BUSINESS_SIGNATURES = {
    'is_bilingual': re.compile(r'\b(bilingüe|bilingual school|dual language|inglés-español|formación bilingüe|english immersion|bilingual education|two languages)\b', re.IGNORECASE),
    'is_trilingual': re.compile(r'\b(trilingüe|trilingual school|tercer idioma|francés e inglés|german|french|portugués|tres idiomas)\b', re.IGNORECASE),
    'is_multilingual': re.compile(r'\b(multilingüe|multilingual|varios idiomas|multiple languages|international language program)\b', re.IGNORECASE),
    'cert_ib': re.compile(r'\b(bachillerato internacional|international baccalaureate|ib world school|ib diploma|ib continuum|ib.org)\b', re.IGNORECASE),
    'cert_cambridge': re.compile(r'\b(cambridge english|cambridge assessment|cambridge international|cambridge igcse|cambridge a levels|cambridge checkpoint)\b', re.IGNORECASE),
    'cert_oxford': re.compile(r'\b(oxford international|oxford aqa|oxford quality|oxford english|oxford university press)\b', re.IGNORECASE),
    'cert_efqm': re.compile(r'\b(efqm|iso 9001|great place to study|excelencia educativa|calidad educativa|acreditación de calidad|acreditacion de calidad)\b', re.IGNORECASE),
    'is_technical': re.compile(r'\b(instituto técnico|formación técnica|tecnológico|sena|politecnico|technical school|vocational training)\b', re.IGNORECASE),
    'has_robotics': re.compile(r'\b(robótica|robotica|robotics|lego|vex|first lego|arduino|maker|makerspace|robot|programación|programacion|coding|code|scratch|python)\b', re.IGNORECASE),
    'has_stem': re.compile(r'\b(stem|steam|ciencia y tecnología|tecnología educativa|science technology engineering math|ciencia tecnologia ingenieria matematicas|robotics lab|makerspace)\b', re.IGNORECASE),
    'has_programming': re.compile(r'\b(programación|programacion|programming|coding|code|scratch|python|java|javascript|html|css|web development|app development)\b', re.IGNORECASE),
    'calendar_a': re.compile(r'\b(calendario a|enero a noviembre|calendario A|calendario tradicional|january to november)\b', re.IGNORECASE),
    'calendar_b': re.compile(r'\b(calendario b|septiembre a junio|calendario B|calendario internacional|september to june)\b', re.IGNORECASE),
    'double_degree': re.compile(r'\b(doble titulación|doble titulacion|dual degree|double diploma|doble certificación|doble certificacion)\b', re.IGNORECASE),
    'exchange_program': re.compile(r'\b(intercambio|exchange|intercambio estudiantil|student exchange|international exchange|study abroad)\b', re.IGNORECASE),
    'language_immersion': re.compile(r'\b(inmersión|immersion|campamento de inglés|language camp|english camp|summer camp|winter camp)\b', re.IGNORECASE),
    'university_agreements': re.compile(r'\b(convenio universitario|articulación universitaria|acuerdo con universidad|university agreement|pathway program)\b', re.IGNORECASE),
    'corporate_agreements': re.compile(r'\b(convenio empresarial|alianza corporativa|corporate agreement|business partnership|prácticas empresariales|internships)\b', re.IGNORECASE),
    'extracurricular_sports': re.compile(r'\b(deportes|sports|fútbol|soccer|baloncesto|basketball|voleibol|volleyball|natación|swimming|atletismo|athletics|tenis|tennis)\b', re.IGNORECASE),
    'extracurricular_arts': re.compile(r'\b(artes|arts|música|music|teatro|theater|danza|dance|pintura|painting|dibujo|drawing|orquesta|orchestra|coral|choir)\b', re.IGNORECASE),
    'extracurricular_clubs': re.compile(r'\b(clubes|clubs|talleres|workshops|ajedrez|chess|debate|robótica|robotica|programación|programacion|ciencias|science)\b', re.IGNORECASE),
}

SERP_EXCLUSIONS = {'facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'youtube.com', 'wikipedia.org', 'paginasamarillas', 'directory', 'infoisinfo', 'tripadvisor', 'foursquare', 'civico', 'losmejorescolegios', 'wiktionary', 'dictionary', 'google.com', 'maps.google.com', 'blogspot.com', 'wordpress.com', 'wix.com'}
EDU_KEYWORDS = {'colegio', 'school', 'educación', 'educacion', 'gimnasio', 'liceo', 'instituto', 'academy', 'admisiones', 'estudiantes', 'matrículas', 'rectoría', 'alumnos', 'contact', 'acerca de', 'sobre nosotros', 'quienes somos'}

EMAIL_GARBAGE_EXT = {'.png', '.jpg', '.jpeg', '.gif', '.css', '.js', '.webp', '.svg', '.woff', '.ttf', '.mp4', '.pdf', '.doc', '.docx', '.xls', '.xlsx'}
EMAIL_GARBAGE_DOMAINS = {'sentry.io', 'wixpress.com', 'example.com', 'domain.com', 'email.com', 'wix.com', 'localhost', 'sentry.wixpress.com', 'tuweb.com', 'colombia.ver', 'test.com', 'demo.com', 'sample.com', 'mail.com', 'outlook.com', 'hotmail.com', 'gmail.com'}
EMAIL_GARBAGE_PREFIXES = {'icon', 'logo', 'image', 'test', 'yourname', 'no-reply', 'noreply', 'sentry', '12345', 'admin@', 'correo@', 'contacto@', 'info@', 'webmaster@', 'postmaster@'}

# =========================================================
# [TIER 3]: THE ULTIMATE GLOBAL EDTECH/LMS/SIS MATRIX (100+ Providers)
# =========================================================
LMS_SIGNATURES = {
    # --- LATAM & COLOMBIA SPECIFIC (Expanded) ---
    'Phidias': re.compile(r'(phidias\.co|phidias\.ac|phidias\.net|phidias\.cloud|\bphidias\b|phidias-static)', re.IGNORECASE),
    'Ciudad Educativa': re.compile(r'(ciudadeducativa\.com|cloud\.ciudadeducativa|\bciudad educativa\b|ciudadeducativa)', re.IGNORECASE),
    'Sistema Saberes': re.compile(r'(sistemasaberes\.com|saberes\.com|saberes\.net|\bsaberes\b|sistema saberes)', re.IGNORECASE),
    'Gnosoft': re.compile(r'(\bgnosoft\b|gnosoft\.com\.co|gnosoftportal)', re.IGNORECASE),
    'Pegaso': re.compile(r'(pegaso\.com\.co|\bpegasopro\b|pegaso cloud)', re.IGNORECASE),
    'Cibercolegios': re.compile(r'(cibercolegios\.com|v3\.cibercolegios|login\.cibercolegios|\bcibercolegios\b)', re.IGNORECASE),
    'Q10 Académico': re.compile(r'(q10\.com|q10academico|academico\.q10|\bq10\b)', re.IGNORECASE),
    'Integra': re.compile(r'(plataformaintegra\.net|integra\.com\.co|\bplataforma integra\b)', re.IGNORECASE),
    'SIGA': re.compile(r'(\bsigaweb\b|\bedusiga\b|\bsigaportal\b|\bsigaapp\b|desarrollosiga)', re.IGNORECASE),
    'Colegios Colombia': re.compile(r'(colegiosonline\.com|portalcolegioscolombia|masteracademic|colegioscolombia)', re.IGNORECASE),
    'Ovy': re.compile(r'(ovy\.co|\bplataforma ovy\b|ovyeducacion)', re.IGNORECASE),
    'WebColegios': re.compile(r'(webcolegios\.com|webcolegios\.net)', re.IGNORECASE),
    'Bicol': re.compile(r'(bicol\.com\.co|bicol\.net)', re.IGNORECASE),
    'SchoolNet': re.compile(r'(\bschoolnet\.(com|cl|co|pe)\b|colegios-online|schoolnetcloud|schoolnetlatam)', re.IGNORECASE),
    'SchoolTrack': re.compile(r'(schooltrack\.com|\bschooltrack\b|schooltracker)', re.IGNORECASE),
    'Santillana Compartir': re.compile(r'(santillanacompartir|stilus\.santillana|santillanaeducacion)', re.IGNORECASE),
    'SM Educamos': re.compile(r'(educamos\.com|sm educamos|educamos sm|plataformaeducamos)', re.IGNORECASE),
    'Educaria (Alexia)': re.compile(r'(alexiaeducacion\.com|\balexia\b|educaria)', re.IGNORECASE),
    'UNOi': re.compile(r'(unoi\.com|\bsistema uno\b|unoi education|unoi latam)', re.IGNORECASE),
    'Norma Educa': re.compile(r'(educanorma\.com|normaeduca)', re.IGNORECASE),
    'Sieweb': re.compile(r'(sieweb\.com\.pe|\bsieweb\b|sieweblatam)', re.IGNORECASE),
    'Gesta': re.compile(r'(gesta\.com\.co|gestaeducativa)', re.IGNORECASE),
    'EduPage': re.compile(r'(edupage\.org|\bedupage\b|edupage latam)', re.IGNORECASE),
    'Nodos': re.compile(r'(nodos\.com\.co|\bplataforma nodos\b|nodos educacion)', re.IGNORECASE),
    'VisualCX': re.compile(r'(\bvisualcx\b|visualcx education)', re.IGNORECASE),
    'Control Académico': re.compile(r'(controlacademico\.com|controlacademico co)', re.IGNORECASE),
    'Academica': re.compile(r'(\bacademica\.pe\b|academicasoft)', re.IGNORECASE),
    'Schoology LATAM': re.compile(r'(schoology\.com|schoology latam)', re.IGNORECASE),
    'Idukay': re.compile(r'(idukay\.com\.co|idukay\.net|\bidukay\b)', re.IGNORECASE),
    'Sapred': re.compile(r'(sapred\.com|sapred\.net|plataformadecolegios)', re.IGNORECASE),
    'Zona Educativa': re.compile(r'(zonaeducativa\.com|zonaeducativa\.net)', re.IGNORECASE),
    'Aula 365': re.compile(r'(aula365\.com|aula365 latam)', re.IGNORECASE),
    
    # --- GLOBAL GIANTS (LMS/SIS/ERP) ---
    'Moodle': re.compile(r'(\bmoodle\b|theme/moove|pluginfile\.php|\bmdl_|moodle\.org|moodlecloud)', re.IGNORECASE),
    'Canvas': re.compile(r'(instructure\.com|\bcanvas-lms\b|canvas_session|canvas\.instructure)', re.IGNORECASE),
    'Blackboard': re.compile(r'(blackboard\.com|bbcswebdav|learn\.blackboard|blackboardlearn|bb collaborate)', re.IGNORECASE),
    'Google Workspace Edu': re.compile(r'(classroom\.google\.com|workspace\.google\.com/education|google for education)', re.IGNORECASE),
    'MS Teams for Edu': re.compile(r'(teams\.microsoft|microsoft_teams|office365 education|microsoft education)', re.IGNORECASE),
    'D2L Brightspace': re.compile(r'(desire2learn\.com|brightspace\.com|\bd2l\b|d2l brightspace)', re.IGNORECASE),
    'Sakai': re.compile(r'(sakaiproject\.org|\bsakai\b|sakaicle)', re.IGNORECASE),
    'Chamilo': re.compile(r'(chamilo\.org|\bchamilo\b|chamilo lms)', re.IGNORECASE),
    'Ilias': re.compile(r'(ilias\.de|\bilias\b|ilias lms)', re.IGNORECASE),
    'Itslearning': re.compile(r'(itslearning\.com|itslearning lms)', re.IGNORECASE),
    'Docebo': re.compile(r'(docebo\.com|\bdocebo\b|docebo lms)', re.IGNORECASE),
    'Totara': re.compile(r'(totaralearning\.com|\btotara\b|totara lms)', re.IGNORECASE),
    'TalentLMS': re.compile(r'(talentlms\.com|\btalentlms\b|talent lms)', re.IGNORECASE),
    'Absorb LMS': re.compile(r'(absorblms\.com|\babsorblms\b|absorb lms)', re.IGNORECASE),
    'LearnDash': re.compile(r'(learndash\.com|\blearndash\b|learndash lms)', re.IGNORECASE),
    'Open LMS': re.compile(r'(openlms\.net|\bopen lms\b|\bopenlms\b|openlms)', re.IGNORECASE),
    'Edmodo': re.compile(r'(edmodo\.com|\bedmodo\b|edmodo lms)', re.IGNORECASE),
    
    # --- K-12 USA/EUROPE SYSTEMS ---
    'PowerSchool': re.compile(r'(powerschool\.com|\bpowerschool\b|powerschool sis)', re.IGNORECASE),
    'Infinite Campus': re.compile(r'(infinitecampus\.com|\binfinite campus\b|infinite campus sis)', re.IGNORECASE),
    'Skyward': re.compile(r'(skyward\.com|\bskyward\b|skyward sis)', re.IGNORECASE),
    'Clever': re.compile(r'(clever\.com|\bclever\b|clever login)', re.IGNORECASE),
    'Seesaw': re.compile(r'(seesaw\.me|\bseesaw\b|seesaw learning)', re.IGNORECASE),
    'ClassDojo': re.compile(r'(classdojo\.com|\bclassdojo\b|class dojo)', re.IGNORECASE),
    'ManageBac': re.compile(r'(managebac\.com|\bmanagebac\b|managebac ib)', re.IGNORECASE),
    'Toddle': re.compile(r'(toddleapp\.com|\btoddle\b|toddle ib)', re.IGNORECASE),
    'FACTS SIS': re.compile(r'(factsmgt\.com|\bfacts sis\b|facts education)', re.IGNORECASE),
    'Synergy (Skyward)': re.compile(r'(\bsynergy sis\b|synergy education)', re.IGNORECASE),
    'Alma SIS': re.compile(r'(getalma\.com|\balma sis\b|alma student)', re.IGNORECASE),
    'Veracross': re.compile(r'(veracross\.com|\bveracross\b|veracross sis)', re.IGNORECASE),
    'Blackbaud': re.compile(r'(myschoolapp\.com|blackbaud\.com|\bblackbaud\b|blackbaud education)', re.IGNORECASE),
    'Compass': re.compile(r'(compass\.education|\bcompass education\b|compass lms)', re.IGNORECASE),
    'Sentral': re.compile(r'(sentral\.com\.au|\bsentral\b|sentral sis)', re.IGNORECASE),
    'SIMS': re.compile(r'(sims-education|\bsims education\b|sims sis)', re.IGNORECASE),
    'Arbor': re.compile(r'(arbor-education|\barbor education\b|arbor sis)', re.IGNORECASE),
    'Bromcom': re.compile(r'(bromcom\.com|\bbromcom\b|bromcom sis)', re.IGNORECASE),
    'iSAMS': re.compile(r'(isams\.com|\bisams\b|isams sis)', re.IGNORECASE),
    
    # --- HIGHER ED & OTHERS ---
    'Jenzabar': re.compile(r'(jenzabar\.com|\bjenzabar\b|jenzabar erp)', re.IGNORECASE),
    'Anthology': re.compile(r'(anthology\.com|\banthology\b|anthology lms)', re.IGNORECASE),
    'Ellucian': re.compile(r'(elluciancloud\.com|ellucian\.com|\bellucian\b|ellucian erp)', re.IGNORECASE),
    'Populi': re.compile(r'(populiweb\.com|\bpopuli\b|populi lms)', re.IGNORECASE),
    'Kaltura': re.compile(r'(kaltura\.com|\bkaltura\b|kaltura video)', re.IGNORECASE),
    'Coursera': re.compile(r'(coursera\.org|\bcoursera\b|coursera campus)', re.IGNORECASE),
    'EdX': re.compile(r'(edx\.org|\bedx\b|edx platform)', re.IGNORECASE),
    'Udemy': re.compile(r'(udemy\.com|\budemy\b|udemy business)', re.IGNORECASE),
    'LinkedIn Learning': re.compile(r'(linkedin\.com/learning|linkedin learning|lynda\.com)', re.IGNORECASE),
}

TACTICAL_UAS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1',
]

# GLOBAL CONNECTION POOLING PARA VELOCIDAD EXTREMA
GLOBAL_SESSION = requests.Session()
_adapter = HTTPAdapter(
    max_retries=Retry(total=5, backoff_factor=1.5, status_forcelist=[403, 406, 429, 500, 502, 503, 504]),
    pool_connections=100,
    pool_maxsize=100
)
GLOBAL_SESSION.mount('http://', _adapter)
GLOBAL_SESSION.mount('https://', _adapter)

# =========================================================
# [TIER 4]: UTILIDADES DE RED, ESTRUCTURAS Y VALIDACIÓN (MEJORADAS)
# =========================================================
@dataclass
class ExtractedPayload:
    """Payload de extracción con todos los campos necesarios"""
    target: str = ""
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
    forensics: Dict[str, bool] = field(default_factory=dict)
    raw_text_corpus: str = ""
    full_html: str = ""
    ai_report: str = ""
    cosmic_report: str = ""
    icfes_score: str = ""
    icfes_category: str = ""
    has_ib: bool = False
    has_cambridge: bool = False
    has_oxford: bool = False
    has_stem: bool = False
    has_robotics: bool = False
    has_programming: bool = False
    extracurricular: List[str] = field(default_factory=list)
    agreements: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        for key in BUSINESS_SIGNATURES.keys():
            if key not in self.forensics:
                self.forensics[key] = False

def get_tactical_session() -> requests.Session:
    return GLOBAL_SESSION

def get_base_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.lower().replace('www.', '')
        return netloc.split('.')[0] if netloc else ""
    except Exception: 
        return ""

def decode_cloudflare_email(hex_string: str) -> Optional[str]:
    try:
        r = int(hex_string[:2], 16)
        email = ''.join([chr(int(hex_string[i:i+2], 16) ^ r) for i in range(2, len(hex_string), 2)])
        return email if MatrixRegex.EMAIL.match(email) else None
    except Exception: 
        return None

def try_decode_base64(b64_string: str) -> Optional[str]:
    try:
        decoded = base64.b64decode(b64_string).decode('utf-8')
        if MatrixRegex.EMAIL.match(decoded): 
            return decoded
    except Exception: 
        return None
    return None

def clean_phone_number(raw_num: str) -> str:
    num = re.sub(r'[^\d+]', '', raw_num)
    if num.startswith('00'): 
        num = '+' + num[2:]
    return num

def clean_and_validate_phone_strict(raw_num: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """Validador estricto Colombia con soporte internacional"""
    if not raw_num:
        return False, None, None
    
    clean = re.sub(r'\D', '', raw_num)
    
    # Detectar código de país
    if clean.startswith('57'):
        clean = clean[2:]
    elif clean.startswith('0057'):
        clean = clean[4:]
    
    length = len(clean)
    
    # WhatsApp (móvil Colombia: 10 dígitos empezando con 3)
    if length == 10 and clean.startswith('3'):
        return True, clean, 'W'
    
    # Teléfono fijo Colombia (10 dígitos empezando con 60 o 7 dígitos)
    if length == 10 and clean.startswith('60'):
        return True, clean, 'T'
    
    if length == 7 and clean[0] in ['2', '3', '4', '5', '6', '7', '8', '9']:
        return True, clean, 'T'
    
    # Línea gratuita Colombia
    if clean.startswith('018000') and length >= 10:
        return True, clean, 'T'
    
    return False, None, None

def extract_icfes_score(text: str) -> Tuple[str, str]:
    """Extrae puntaje ICFES y categoría del texto"""
    icfes_match = MatrixRegex.ICFES_PATTERN.search(text)
    if not icfes_match:
        return "", ""
    
    # Buscar puntaje numérico (2 o 3 dígitos)
    score_match = re.search(r'\b([5-9][0-9]|[0-9]{3})\s*(?:puntos?|pts?)\b', text, re.IGNORECASE)
    score = score_match.group(1) if score_match else ""
    
    # Buscar categoría (A+, A, B, C, D)
    category_match = re.search(r'\b(categor[ií]a\s+([A-D][+-]?)|([A-D][+-]?)\s+[Cc]ategor[ií]a)\b', text, re.IGNORECASE)
    category = category_match.group(2) or category_match.group(3) if category_match else ""
    
    return score.upper(), category.upper()



def extract_extracurricular(text: str) -> List[str]:
    """
    Extrae actividades extracurriculares del texto.
    [GOD TIER FIX] - Versión independiente
    """
    activities = []
    
    # Deportes
    sports_pattern = re.compile(
        r'\b(deportes|sports|fútbol|soccer|baloncesto|basketball|'
        r'voleibol|volleyball|natación|swimming|atletismo|athletics|'
        r'tenis|tennis|gimnasia|gymnastics|artes marciales|martial arts|'
        r'equitación|horseback riding|ciclismo|cycling|patinaje|skating)\b',
        re.IGNORECASE
    )
    sports_matches = sports_pattern.findall(text)
    if sports_matches:
        unique_sports = list(set(sports_matches))[:5]
        activities.append(f"Deportes: {', '.join(unique_sports)}")
    
    # Artes
    arts_pattern = re.compile(
        r'\b(artes|arts|música|music|teatro|theater|danza|dance|'
        r'pintura|painting|dibujo|drawing|escultura|sculpture|'
        r'orquesta|orchestra|coral|choir|banda|band|'
        r'dramatización|drama|fotografía|photography|cine|cinema)\b',
        re.IGNORECASE
    )
    arts_matches = arts_pattern.findall(text)
    if arts_matches:
        unique_arts = list(set(arts_matches))[:5]
        activities.append(f"Artes: {', '.join(unique_arts)}")
    
    # Clubs
    clubs_pattern = re.compile(
        r'\b(clubes|clubs|talleres|workshops|ajedrez|chess|debate|'
        r'robótica|robotica|programación|programacion|ciencias|science|'
        r'lectura|reading|idiomas|languages|matemáticas|mathematics|'
        r'ecología|ecology|voluntariado|volunteering|emprendimiento|'
        r'entrepreneurship|tecnología|technology|investigación|research)\b',
        re.IGNORECASE
    )
    clubs_matches = clubs_pattern.findall(text)
    if clubs_matches:
        unique_clubs = list(set(clubs_matches))[:5]
        activities.append(f"Clubes: {', '.join(unique_clubs)}")
    
    return activities[:10]


def extract_agreements(text: str) -> List[str]:
    """
    Extrae convenios y alianzas del texto.
    [GOD TIER FIX] - Versión independiente
    """
    agreements = []
    
    # Convenios universitarios
    uni_matches = re.findall(
        r'(?:convenio|alianza|acuerdo|articulaci[oó]n|colaboraci[oó]n)\s+'
        r'(?:con|entre|con la|con el|con)\s+'
        r'([^.,;]+(?:universidad|facultad|escuela|polit[eé]cnico|instituto|centro|academia)[^.,;]+)',
        text,
        re.IGNORECASE
    )
    agreements.extend([m.strip()[:100] for m in uni_matches[:5]])
    
    # Convenios empresariales
    corp_matches = re.findall(
        r'(?:convenio|alianza|acuerdo|articulaci[oó]n|colaboraci[oó]n)\s+'
        r'(?:con|entre|con la|con el|con)\s+'
        r'([^.,;]+(?:empresa|corporaci[oó]n|compañ[ií]a|inc|sas|ltda|fundación|asociación|grupo)[^.,;]+)',
        text,
        re.IGNORECASE
    )
    agreements.extend([m.strip()[:100] for m in corp_matches[:5]])
    
    # Convenios internacionales
    intl_matches = re.findall(
        r'(?:convenio|alianza|acuerdo|intercambio|programa)\s+'
        r'(?:internacional|global|mundial)\s+'
        r'(?:con|en)\s+([^.,;]+)',
        text,
        re.IGNORECASE
    )
    agreements.extend([m.strip()[:100] for m in intl_matches[:3]])
    
    # Convenios específicos (nombre de universidad)
    specific_matches = re.findall(
        r'(?:convenio|alianza|acuerdo)\s+(?:con|entre)\s+'
        r'(Universidad\s+[A-ZÁÉÍÓÚ][a-záéíóú]+(?:\s+[A-ZÁÉÍÓÚ][a-záéíóú]+)?)',
        text,
        re.IGNORECASE
    )
    agreements.extend([m.strip() for m in specific_matches[:3]])
    
    return list(set(agreements))[:10]

def _is_security_bot(user_agent: str, ip: str) -> bool:
    if not user_agent: 
        return True
    if MatrixRegex.BOT.search(user_agent): 
        return True
    return False

# =========================================================
# [TIER 5]: PIXEL TRACKING VIEW
# =========================================================
def _build_pixel_response() -> HttpResponse:
    response = HttpResponse(PIXEL_BYTES, content_type="image/gif")
    response['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    return response

@require_GET
def tracking_pixel_view(request: HttpRequest, interaction_id: str) -> HttpResponse:
    user_agent = request.META.get('HTTP_USER_AGENT', '')
    client_ip = request.META.get('HTTP_X_FORWARDED_FOR', request.META.get('REMOTE_ADDR', ''))

    if _is_security_bot(user_agent, client_ip): 
        return _build_pixel_response()

    try: 
        interaction_uuid = uuid.UUID(str(interaction_id))
    except ValueError: 
        return _build_pixel_response()

    lock_key = f"pixel_debounce_{interaction_uuid}"
    if cache.get(lock_key): 
        return _build_pixel_response()
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
    except Exception: 
        pass
    
    return _build_pixel_response()

# =========================================================
# [TIER 6]: COSMIC AI REPORT GENERATOR (VERSIÓN ULTRA COMPLETA)
# =========================================================
async def generate_cosmic_ai_report_async(
    name: str,
    city: str,
    country: str,
    webpage_text: str,
    raw_html: str,
    extracted_data: dict
) -> str:
    """Genera reporte cósmico completo usando el analizador avanzado"""
    if not AI_ANALYZER_AVAILABLE:
        return "⚠️ Cosmic AI Analyzer no disponible. Verifica la instalación de sales.engine.ai_analyzer"
    
    if not webpage_text or len(webpage_text.strip()) < 100:
        return "⚠️ Insufficient text for cosmic analysis (minimum 100 characters required)"
    
    # Verificar cache cuántico
    cache_key = hashlib.sha256(f"{name}_{city}_{country}_{webpage_text[:1000]}".encode()).hexdigest()[:32]
    cached = QUANTUM_CACHE.get(cache_key)
    if cached:
        logger.info(f"⚡ Quantum cache HIT for {name}")
        return cached
    
    try:
        analyzer = get_analyzer()
        profile = await analyzer.analyze(
            name=name,
            city=city,
            country=country,
            webpage_text=webpage_text,
            raw_html=raw_html,
            extracted_data=extracted_data
        )
        report = profile.to_markdown()
        
        # Guardar en cache cuántico
        QUANTUM_CACHE.set(cache_key, report)
        
        return report
    except Exception as e:
        logger.error(f"Error generando reporte cósmico: {e}")
        traceback.print_exc()
        return f"❌ Error en análisis cósmico: {str(e)[:500]}"

def generate_cosmic_ai_report_sync(
    name: str,
    city: str,
    country: str,
    webpage_text: str,
    raw_html: str,
    extracted_data: dict
) -> str:
    """Versión síncrona del generador de reporte cósmico"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(
            generate_cosmic_ai_report_async(name, city, country, webpage_text, raw_html, extracted_data)
        )
    finally:
        loop.close()

# =========================================================
# [TIER 7]: MOTOR COGNITIVO OSINT (GHOST SNIPER) - MEJORADO
# =========================================================
class SniperConsoleView(TemplateView):
    template_name = "admin/sales/sniper_console.html"

@method_decorator(csrf_exempt, name='dispatch')
class SniperSearchView(View):

    def resolve_domain_from_serp(self, query: str, city: str, country: str) -> Optional[List[str]]:
        """Resolución de dominio con múltiples backends y fallback"""
        urls = []
        
        # Backend 1: DuckDuckGo
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(query, backend="lite", max_results=8))
                for r in results:
                    href = r.get('href', r.get('link', r.get('url', '')))
                    if href and not any(exc in href.lower() for exc in SERP_EXCLUSIONS):
                        urls.append(href)
        except Exception: 
            pass

        # Backend 2: HTML DuckDuckGo (fallback)
        if not urls:
            try:
                headers = {'User-Agent': random.choice(TACTICAL_UAS)}
                resp = requests.get(f"https://html.duckduckgo.com/html/?q={query}", headers=headers, timeout=5)
                soup = BeautifulSoup(resp.text, 'html.parser')
                for a in soup.find_all('a', class_='result__url'):
                    href = a.get('href', '')
                    if href and 'http' in href and not any(exc in href.lower() for exc in SERP_EXCLUSIONS):
                        urls.append(href)
            except Exception: 
                pass
        
        # Backend 3: Brave Search (si está configurado)
        brave_api_key = getattr(settings, 'BRAVE_API_KEY', None)
        if brave_api_key and not urls:
            try:
                headers = {
                    'Accept': 'application/json',
                    'X-Subscription-Token': brave_api_key
                }
                resp = requests.get(
                    f"https://api.search.brave.com/res/v1/web/search?q={query}&count=5",
                    headers=headers,
                    timeout=5
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for result in data.get('web', {}).get('results', []):
                        url = result.get('url', '')
                        if url and not any(exc in url.lower() for exc in SERP_EXCLUSIONS):
                            urls.append(url)
            except Exception:
                pass
        
        return list(dict.fromkeys(urls))[:5]

    def cognitive_url_scorer(self, urls: List[str], target: str, city: str, country: str) -> Optional[str]:
        """Scoring cognitivo de URLs con heurística avanzada"""
        best_url, best_score = None, -1
        target_words = [w.lower() for w in target.split() if len(w) > 2]
        session = get_tactical_session()

        for url in urls:
            try:
                resp = session.get(
                    url, 
                    headers={'User-Agent': random.choice(TACTICAL_UAS)}, 
                    timeout=6, 
                    verify=False
                )
                html_lower = resp.text.lower()
                score = 0
                
                # Peso por palabras clave del nombre
                for word in target_words:
                    if word in html_lower: 
                        score += 8
                
                # Peso por ubicación
                if city and city.lower() in html_lower: 
                    score += 20
                if country and country.lower() in html_lower: 
                    score += 15
                
                # Peso por palabras clave educativas
                for kw in EDU_KEYWORDS:
                    if kw in html_lower: 
                        score += 8
                
                # Bonificación por dominio .edu o .edu.co
                if url.endswith('.edu.co') or url.endswith('.edu'):
                    score += 50
                elif url.endswith('.co') or url.endswith('.com.co'):
                    score += 25
                
                # Penalización por paths sospechosos
                if '/blog/' in url.lower() or '/noticias/' in url.lower():
                    score -= 30
                if '/wp-content/' in url.lower():
                    score -= 20

                if score > best_score:
                    best_score = score
                    best_url = resp.url
            except Exception: 
                continue

        if best_score < 15: 
            return None
        return best_url

    # =========================================================
    # [TIER 8]: NEURAL AI INJECTOR (VERSIÓN ULTRA COMPLETA)
    # =========================================================
    def generate_ai_forensic_report_ultra(self, scraped_text: str, institution_name: str = "", city: str = "", country: str = "") -> str:
        """Versión ultra completa del generador de reporte IA con todos los campos requeridos"""
        if not scraped_text or len(scraped_text) < 100:
            return "⚠️ Corpus insuficiente para análisis de IA Semántica (mínimo 100 caracteres)."

        safe_context = scraped_text[:15000]
        
        prompt = f"""
        Eres un Analista de Inteligencia B2B de élite, experto en educación y tecnología educativa.
        Tu misión es analizar en profundidad la siguiente institución educativa y extraer TODA la información posible.
        
        INSTITUCIÓN: {institution_name}
        UBICACIÓN: {city}, {country}
        
        TEXTO EXTRAÍDO DE LA PÁGINA WEB:
        {safe_context}
        
        ============================================================
        EXTRACCIÓN REQUERIDA (RESPONDE ÚNICAMENTE EN FORMATO JSON):
        ============================================================
        
        {{
            "academic_profile": {{
                "levels_offered": ["Lista de niveles educativos: Preescolar, Primaria, Bachillerato, etc."],
                "pedagogical_emphasis": "Énfasis pedagógico (Montessori, constructivista, etc.)",
                "calendar": "A o B (tradicional o internacional)",
                "foundation_year": "Año de fundación",
                "accreditation_level": "Nivel de acreditación (A+, A, B, etc.)"
            }},
            "certifications": {{
                "ib": {{"has_ib": true/false, "programs": ["PYP", "MYP", "DP"], "since": "año"}},
                "cambridge": {{"has_cambridge": true/false, "exams": ["PET", "FCE", "CAE", "CPE", "KET"]}},
                "oxford": {{"has_oxford": true/false}},
                "quality": {{"has_iso": true/false, "has_efqm": true/false, "others": ["ISO 14001", etc]}}
            }},
            "international_programs": {{
                "double_degree": {{"has_double_degree": true/false, "partners": ["Nombre universidad"], "countries": ["País"]}},
                "exchanges": {{"has_exchanges": true/false, "countries": ["País"], "universities": ["Universidad"]}},
                "language_immersion": {{"has_immersion": true/false, "destinations": ["Destino"], "duration": "1 semana / 1 mes"}},
                "international_agreements": ["Lista de acuerdos internacionales"]
            }},
            "technology": {{
                "stem": {{"has_stem": true/false, "programs": ["STEM lab", "Science fair"]}},
                "robotics": {{
                    "has_robotics": true/false,
                    "type": "LEGO Education / VEX / Arduino / FIRST",
                    "platforms": ["LEGO Mindstorms", "VEX IQ"],
                    "competitions": ["FIRST LEGO League", "RobotiX"],
                    "achievements": ["Campeones 2023"]
                }},
                "programming": {{
                    "has_programming": true/false,
                    "languages": ["Python", "Java", "Scratch", "JavaScript"],
                    "frameworks": ["Django", "React"],
                    "grade_levels": ["5° a 11°"]
                }},
                "laboratories": ["Robótica", "Ciencias", "Computación", "Física", "Química"],
                "classroom_tech": ["Smartboards", "Tablets", "Computadores"],
                "digital_platforms": ["Google Classroom", "Microsoft Teams", "Canvas"]
            }},
            "performance": {{
                "icfes": {{
                    "score": "puntaje numérico (ej: 78)",
                    "category": "A+, A, B, C, D",
                    "ranking": "posición en el país/ciudad",
                    "year": "2023",
                    "trend": "ascendente/estable/descendente"
                }},
                "awards": ["Premio a la excelencia académica", "Colegio destacado"],
                "university_admission_rate": "85%",
                "top_universities": ["Universidad de los Andes", "Universidad Nacional"]
            }},
            "extracurricular": {{
                "sports": ["Fútbol", "Baloncesto", "Natación", "Voleibol"],
                "arts": ["Música", "Teatro", "Danza", "Pintura"],
                "clubs": ["Ajedrez", "Robótica", "Debate", "Programación"],
                "camps": ["Campamento de inglés", "Campamento de verano"],
                "community_service": true/false,
                "competitions_won": ["Campeonato de robótica regional"]
            }},
            "infrastructure": {{
                "campus": {{"size": "20,000 m²", "locations": ["Sede principal", "Sede norte"], "facilities": []}},
                "green_areas": true/false,
                "sports_facilities": ["Cancha de fútbol", "Gimnasio", "Piscina"],
                "library": "Biblioteca con 5,000 volúmenes",
                "transport": true/false,
                "dining": "Restaurante escolar",
                "capacity": 1500
            }},
            "agreements": {{
                "university_agreements": ["Universidad de los Andes", "Universidad Javeriana"],
                "corporate_agreements": ["Microsoft", "LEGO Education"],
                "ngo_agreements": ["Fundación Telefónica"],
                "government_programs": ["Programa de alimentación escolar"]
            }},
            "sales_intelligence": {{
                "pain_points": ["Falta de plataforma digital unificada", "Sistemas obsoletos"],
                "sales_triggers": ["Expansión de campus", "Nueva sede", "Certificación IB reciente"],
                "opportunities": ["Migración a LMS moderno", "Implementación de robótica"],
                "risks": ["Presupuesto limitado", "Competencia con colegios cercanos"],
                "ideal_contact": "Rector o Director de Tecnología",
                "budget_indication": "Alto / Medio / Bajo",
                "decision_timeline": "Inmediato / 3-6 meses / 1 año",
                "recommended_approach": "Demostración técnica seguida de propuesta económica"
            }},
            "executive_summary": "Resumen ejecutivo de 2-3 líneas para el equipo de ventas",
            "lms_provider": "LMS detectado (ej: Moodle, Canvas, Phidias, SchoolNet)",
            "is_bilingual": true/false,
            "is_trilingual": true/false
        }}
        """
        
        try:
            deepseek_key = getattr(settings, 'DEEPSEEK_API_KEY', None)
            openai_key = getattr(settings, 'OPENAI_API_KEY', None)
            
            if deepseek_key:
                api_key = deepseek_key
                url = "https://api.deepseek.com/chat/completions"
                model_name = "deepseek-chat"
            elif openai_key:
                api_key = openai_key
                url = "https://api.openai.com/v1/chat/completions"
                model_name = "gpt-4o-mini"
            else:
                return "SISTEMA IA APAGADO: No se detectó API Key de DeepSeek ni de OpenAI en el servidor."
            
            headers = {
                "Authorization": f"Bearer {api_key}", 
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": model_name,
                "messages": [
                    {"role": "system", "content": "Eres un analista de inteligencia B2B de élite. Responde ÚNICAMENTE con JSON válido. No incluyas markdown, no incluyas texto explicativo. Solo JSON puro."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.1,
                "max_tokens": 4000,
                "response_format": {"type": "json_object"}
            }
            
            resp = requests.post(url, json=payload, headers=headers, timeout=35)
            
            if resp.status_code == 200:
                raw_json = resp.json()['choices'][0]['message']['content'].strip()
                # Limpiar posibles marcadores de markdown
                if raw_json.startswith('```json'):
                    raw_json = raw_json[7:]
                if raw_json.endswith('```'):
                    raw_json = raw_json[:-3]
                
                try:
                    data = json.loads(raw_json)
                    
                    # Construir reporte en markdown
                    markdown_report = self._build_markdown_report(data, institution_name, city, country)
                    return markdown_report
                except json.JSONDecodeError as je:
                    logger.error(f"JSON decode error: {je}")
                    return f"❌ Error parseando respuesta de IA: {str(je)[:100]}"
            else:
                return f"Anomalía Neural (HTTP {resp.status_code}): Error de Proveedor LLM. Detalles: {resp.text[:150]}"
                
        except requests.exceptions.Timeout:
            return "Error Crítico en IA: Timeout agotado (El proveedor LLM tardó más de 35 segundos en responder)."
        except Exception as e:
            return f"Error Crítico en Inyección IA: {str(e)}"
    
    def _build_markdown_report(self, data: Dict, name: str, city: str, country: str) -> str:
        """Construye el reporte en formato Markdown con todos los datos"""
        academic = data.get('academic_profile', {})
        certs = data.get('certifications', {})
        intl = data.get('international_programs', {})
        tech = data.get('technology', {})
        perf = data.get('performance', {})
        extra = data.get('extracurricular', {})
        infra = data.get('infrastructure', {})
        agree = data.get('agreements', {})
        sales = data.get('sales_intelligence', {})
        
        md = f"""
# 🌌 COSMIC INTELLIGENCE REPORT
## {name}
### {city}, {country}

---

## 🎯 EXECUTIVE SUMMARY
{data.get('executive_summary', 'No summary available.')}

---

## 📚 ACADEMIC PROFILE
| Field | Value |
|-------|-------|
| **Levels Offered** | {', '.join(academic.get('levels_offered', [])) or "Not specified"} |
| **Pedagogical Emphasis** | {academic.get('pedagogical_emphasis', 'Not specified')} |
| **Calendar** | {academic.get('calendar', 'Not specified')} |
| **Foundation Year** | {academic.get('foundation_year', 'Not specified')} |
| **Accreditation Level** | {academic.get('accreditation_level', 'Not specified')} |

---

## 🏆 CERTIFICATIONS & ACCREDITATIONS

### 🌍 International Certifications
| Certification | Status | Details |
|---------------|--------|---------|
| **IB** | {'✅' if certs.get('ib', {}).get('has_ib') else '❌'} | {', '.join(certs.get('ib', {}).get('programs', []))} |
| **Cambridge** | {'✅' if certs.get('cambridge', {}).get('has_cambridge') else '❌'} | {', '.join(certs.get('cambridge', {}).get('exams', []))} |
| **Oxford** | {'✅' if certs.get('oxford', {}).get('has_oxford') else '❌'} | - |

### 🏅 Quality Certifications
- **ISO 9001**: {'✅' if certs.get('quality', {}).get('has_iso') else '❌'}
- **EFQM**: {'✅' if certs.get('quality', {}).get('has_efqm') else '❌'}
- **Others**: {', '.join(certs.get('quality', {}).get('others', [])) or "None"}

---

## 🌎 INTERNATIONAL PROGRAMS
| Program | Status | Details |
|---------|--------|---------|
| **Double Degree** | {'✅' if intl.get('double_degree', {}).get('has_double_degree') else '❌'} | {', '.join(intl.get('double_degree', {}).get('partners', []))} |
| **Exchanges** | {'✅' if intl.get('exchanges', {}).get('has_exchanges') else '❌'} | {', '.join(intl.get('exchanges', {}).get('countries', []))} |
| **Language Immersion** | {'✅' if intl.get('language_immersion', {}).get('has_immersion') else '❌'} | {', '.join(intl.get('language_immersion', {}).get('destinations', []))} |

**International Agreements:**
{self._format_list(intl.get('international_agreements', []))}

---

## 🤖 TECHNOLOGY & INNOVATION

### Robotics
- **Status**: {'✅' if tech.get('robotics', {}).get('has_robotics') else '❌'}
- **Type**: {tech.get('robotics', {}).get('type', 'Not specified')}
- **Platforms**: {', '.join(tech.get('robotics', {}).get('platforms', []))}
- **Competitions**: {', '.join(tech.get('robotics', {}).get('competitions', []))}
- **Achievements**: {', '.join(tech.get('robotics', {}).get('achievements', []))}

### Programming & Coding
- **Status**: {'✅' if tech.get('programming', {}).get('has_programming') else '❌'}
- **Languages**: {', '.join(tech.get('programming', {}).get('languages', []))}
- **Frameworks**: {', '.join(tech.get('programming', {}).get('frameworks', []))}

### STEM
- **Status**: {'✅' if tech.get('stem', {}).get('has_stem') else '❌'}
- **Programs**: {', '.join(tech.get('stem', {}).get('programs', []))}

### Laboratories & Facilities
{self._format_list(tech.get('laboratories', []))}

### Classroom Technology
{self._format_list(tech.get('classroom_tech', []))}

### Digital Platforms
{self._format_list(tech.get('digital_platforms', []))}

---

## 📊 PERFORMANCE & ACHIEVEMENTS

| Metric | Value |
|--------|-------|
| **ICFES Score** | {perf.get('icfes', {}).get('score', 'Not specified')} |
| **ICFES Category** | {perf.get('icfes', {}).get('category', 'Not specified')} |
| **Ranking** | {perf.get('icfes', {}).get('ranking', 'Not specified')} |
| **University Admission Rate** | {perf.get('university_admission_rate', 'Not specified')} |

### Awards & Recognitions
{self._format_list(perf.get('awards', []))}

### Top Universities (Graduation Destinations)
{self._format_list(perf.get('top_universities', []))}

---

## 🎪 EXTRACURRICULAR ACTIVITIES

| Category | Activities |
|----------|------------|
| **Sports** | {', '.join(extra.get('sports', [])) or "None"} |
| **Arts** | {', '.join(extra.get('arts', [])) or "None"} |
| **Clubs** | {', '.join(extra.get('clubs', [])) or "None"} |
| **Camps** | {', '.join(extra.get('camps', [])) or "None"} |
| **Competitions Won** | {', '.join(extra.get('competitions_won', [])) or "None"} |

**Community Service**: {'✅' if extra.get('community_service') else '❌'}

---

## 🏛️ INFRASTRUCTURE

| Aspect | Details |
|--------|---------|
| **Campus** | {infra.get('campus', {}).get('size', 'Not specified')} |
| **Green Areas** | {'✅' if infra.get('green_areas') else '❌'} |
| **Sports Facilities** | {', '.join(infra.get('sports_facilities', [])) or "None"} |
| **Library** | {infra.get('library', 'Not specified')} |
| **Transport** | {'✅' if infra.get('transport') else '❌'} |
| **Dining** | {infra.get('dining', 'Not specified')} |
| **Capacity** | {infra.get('capacity', 'Not specified')} |

---

## 🤝 AGREEMENTS & PARTNERSHIPS

### University Agreements
{self._format_list(agree.get('university_agreements', []))}

### Corporate Agreements
{self._format_list(agree.get('corporate_agreements', []))}

### NGO Agreements
{self._format_list(agree.get('ngo_agreements', []))}

### Government Programs
{self._format_list(agree.get('government_programs', []))}

---

## 💼 SALES INTELLIGENCE

### 🔴 Pain Points
{self._format_list(sales.get('pain_points', []))}

### 🟢 Sales Triggers
{self._format_list(sales.get('sales_triggers', []))}

### 🚀 Opportunities
{self._format_list(sales.get('opportunities', []))}

### ⚠️ Risks
{self._format_list(sales.get('risks', []))}

### 👤 Ideal Contact
{sales.get('ideal_contact', 'Not specified')}

### 💰 Budget Indication
{sales.get('budget_indication', 'Not specified')}

### 📅 Decision Timeline
{sales.get('decision_timeline', 'Not specified')}

### 🎯 Recommended Approach
{sales.get('recommended_approach', 'Not specified')}

---

## 📈 METADATA
- **Confidence Score**: {data.get('confidence_score', 0.8):.1%}
- **Analysis Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---
*Report generated by Cosmic Intelligence Engine v99.9.9.9.9*
*Powered by DeepSeek AI*
"""
        return md
    
    def _format_list(self, items: List[str]) -> str:
        if not items:
            return "None"
        return "\n".join([f"- {item}" for item in items[:10]])

    def extract_from_dom(self, html_content: str, browser_visible_text: str, soup: BeautifulSoup, payload: ExtractedPayload, cfg: dict):
        """Extracción Híbrida Agresiva con análisis avanzado de certificaciones"""
        decoded_html = html_lib.unescape(html_content)
        decoded_html = re.sub(r'[\u200B-\u200D\uFEFF]', '', decoded_html)
        
        # [SANEAMIENTO ALFANUMÉRICO]: Destruye "mail12345"
        decoded_html = re.sub(r'(\D)(\d{5,})(\D)', r'\1 \2 \3', decoded_html)
        
        for tag in soup.find_all(True): 
            tag.insert_after(' ')
        soup_text = soup.get_text(separator=' | ', strip=True) 
        
        tagless_html = re.sub(r'<[^>]+>', ' ', decoded_html)
        raw_html_soup = BeautifulSoup(decoded_html, 'html.parser')

        hidden_attributes_text = ""
        for tag in raw_html_soup.find_all(['img', 'a', 'div', 'span']):
            hidden_attributes_text += f" {tag.get('alt', '')} {tag.get('title', '')} "
            for key, val in tag.attrs.items():
                if isinstance(val, str): 
                    hidden_attributes_text += f" {val} "

        master_text = f"{browser_visible_text} {hidden_attributes_text} | {soup_text}"
        master_text = re.sub(r'\s+', ' ', master_text)
        master_text = re.sub(r'[\u200B-\u200D\uFEFF]', '', master_text)

        # Alimentamos el Corpus RAM para el Agente IA
        payload.raw_text_corpus += " " + master_text
        payload.full_html += decoded_html[:15000]

        base_domain = get_base_domain(payload.domain)
        raw_emails = set(payload.emails)
        raw_wa = set(payload.whatsapp)
        raw_tel = set(payload.telephones)
        
        master_text_lower = master_text.lower()

        # =========================================================
        # ANÁLISIS AVANZADO DE CERTIFICACIONES
        # =========================================================
        payload.has_ib = bool(MatrixRegex.IB_PATTERN.search(master_text_lower))
        payload.has_cambridge = bool(MatrixRegex.CAMBRIDGE_PATTERN.search(master_text_lower))
        payload.has_oxford = bool(MatrixRegex.OXFORD_PATTERN.search(master_text_lower))
        payload.has_stem = bool(MatrixRegex.STEM_PATTERN.search(master_text_lower))
        payload.has_robotics = bool(MatrixRegex.ROBOTICS_PATTERN.search(master_text_lower))
        payload.has_programming = bool(re.search(r'\b(programaci[oó]n|programming|coding|python|java|javascript|scratch)\b', master_text_lower, re.IGNORECASE))
        
        # Extraer ICFES
        payload.icfes_score, payload.icfes_category = extract_icfes_score(master_text)
        
        # Extraer extracurriculares
        payload.extracurricular = extract_extracurricular(master_text)
        
        # Extraer convenios
        payload.agreements = extract_agreements(master_text)

        # =========================================================
        # PASE 1: JSON-LD
        # =========================================================
        for script in raw_html_soup.find_all('script', type='application/ld+json'):
            try:
                json_data = json.loads(script.string)
                def extract_json_keys(obj, key):
                    results = []
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if k.lower() == key: 
                                results.append(v)
                            elif isinstance(v, (dict, list)): 
                                results.extend(extract_json_keys(v, key))
                    elif isinstance(obj, list):
                        for item in obj: 
                            results.extend(extract_json_keys(item, key))
                    return results

                if cfg['use_email']:
                    for e in extract_json_keys(json_data, 'email'):
                        if isinstance(e, str) and MatrixRegex.EMAIL.match(e): 
                            raw_emails.add(e.lower())
                if cfg['use_whatsapp']:
                    for t in extract_json_keys(json_data, 'telephone'):
                        if isinstance(t, str):
                            is_val, num, t_type = clean_and_validate_phone_strict(t)
                            if is_val:
                                if t_type == 'W': 
                                    raw_wa.add(num)
                                else: 
                                    raw_tel.add(num)
            except Exception: 
                pass

        # =========================================================
        # PASE 2: EMAILS (MAXIMUM YIELD)
        # =========================================================
        if cfg['use_email']: 
            for a in raw_html_soup.find_all('a', href=True):
                href = urllib.parse.unquote(a['href'].lower())
                if href.startswith('mailto:'):
                    clean_mail = href[7:].split('?')[0].strip()
                    clean_mail = re.sub(r'^[^a-zA-Z0-9]+', '', clean_mail) 
                    if MatrixRegex.EMAIL.match(clean_mail): 
                        raw_emails.add(clean_mail)

            for hex_str in MatrixRegex.CLOUDFLARE_HEX.findall(decoded_html):
                if decoded := decode_cloudflare_email(hex_str): 
                    raw_emails.add(decoded)

            for b64 in MatrixRegex.BASE64_HEURISTIC.findall(decoded_html):
                if decoded := try_decode_base64(b64): 
                    raw_emails.add(decoded)

            raw_emails.update(MatrixRegex.EMAIL.findall(tagless_html))
            raw_emails.update(MatrixRegex.EMAIL.findall(master_text))
            
            for obf in MatrixRegex.OBFUSCATED_EMAIL.findall(master_text):
                raw_emails.add(f"{obf[0]}@{obf[1]}.{obf[2]}".lower())

            scored_emails = []
            for e in raw_emails:
                e_lower = e.lower().strip()
                saneamiento = re.search(r'(info|admisiones|contacto|secretaria|rectoria|gerencia|direcciones|colegio|academy|school)@.*', e_lower)
                if saneamiento: 
                    e_lower = saneamiento.group(0)

                if any(e_lower.endswith(ext) for ext in EMAIL_GARBAGE_EXT): 
                    continue
                if any(garbage in e_lower for garbage in EMAIL_GARBAGE_DOMAINS): 
                    continue
                if any(e_lower.startswith(prefix) for prefix in EMAIL_GARBAGE_PREFIXES): 
                    continue
                if not MatrixRegex.EMAIL.match(e_lower) or len(e_lower) >= 60: 
                    continue
                if re.match(r'^\d{6,}', e_lower): 
                    continue 
                
                if base_domain and base_domain in e_lower: 
                    scored_emails.insert(0, e_lower) 
                elif any(kw in e_lower for kw in ['info', 'contacto', 'admision', 'rectoria', 'secretaria', 'colegio']): 
                    scored_emails.insert(1, e_lower) 
                else: 
                    scored_emails.append(e_lower)

            payload.emails = set(dict.fromkeys(scored_emails))

        # =========================================================
        # PASE 3: TELÉFONOS Y WA (GREEDY MULTI-CLUSTER HARVESTING)
        # =========================================================
        if cfg['use_whatsapp']:
            for a in raw_html_soup.find_all('a', href=True):
                href = urllib.parse.unquote(a['href'].lower())
                if match := MatrixRegex.STRICT_HREF_PHONE.search(href):
                    is_val, num, t_type = clean_and_validate_phone_strict(match.group(1))
                    if is_val:
                        if t_type == 'W': 
                            raw_wa.add(num)
                        else: 
                            raw_tel.add(num)
            
            blocks = MatrixRegex.CONTEXT_BLOCK.split(master_text)
            for block in blocks:
                for match in MatrixRegex.RAW_PHONE_FINDER.findall(block):
                    is_val, num, t_type = clean_and_validate_phone_strict(match)
                    if is_val:
                        if t_type == 'W': 
                            raw_wa.add(num)
                        else: 
                            raw_tel.add(num)

            for match in MatrixRegex.RAW_PHONE_FALLBACK.findall(master_text):
                is_val, num, t_type = clean_and_validate_phone_strict(match)
                if is_val:
                    if t_type == 'W': 
                        raw_wa.add(num)
                    else: 
                        raw_tel.add(num)
            
            payload.whatsapp = raw_wa
            payload.telephones = raw_tel

        # =========================================================
        # PASE 4: REDES Y LMS
        # =========================================================
        payload.socials.update(list(set([s.lower() for s in MatrixRegex.SOCIAL.findall(decoded_html)])))

        if cfg['use_lms']:
            master_lms_text = decoded_html + " " + " ".join([a.get('href', '') for a in raw_html_soup.find_all('a', href=True)])
            detected_lms = set()
            for lms_name, lms_regex in LMS_SIGNATURES.items():
                if lms_regex.search(master_lms_text): 
                    detected_lms.add(lms_name)
            
            if detected_lms:
                if payload.lms_provider == 'No detectado': 
                    payload.lms_provider = ", ".join(detected_lms)
                else:
                    existing = set(payload.lms_provider.split(", "))
                    existing.update(detected_lms)
                    payload.lms_provider = ", ".join(existing)

        # =========================================================
        # PASE 5: BUSINESS INTELLIGENCE (AMPLIADO)
        # =========================================================
        for key, regex in BUSINESS_SIGNATURES.items():
            if regex.search(master_text_lower): 
                payload.forensics[key] = True
        
        # Actualizar campos derivados
        payload.forensics['has_robotics'] = payload.has_robotics
        payload.forensics['has_stem'] = payload.has_stem
        payload.forensics['has_programming'] = payload.has_programming
        payload.forensics['cert_ib'] = payload.has_ib
        payload.forensics['cert_cambridge'] = payload.has_cambridge
        payload.forensics['cert_oxford'] = payload.has_oxford

        raw_html_soup.decompose()

    # =========================================================
    # [TIER 9]: PLAYWRIGHT STEALTH MULTI-FRAME MANAGER (MEJORADO)
    # =========================================================
    async def async_deep_render(self, url: str, timeout_ms: int = 45000):
        """Renderizado profundo con Playwright y múltiples estrategias de evasión"""
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
                        '--disable-features=IsolateOrigins,site-per-process',
                        '--disable-dev-shm-usage',
                        '--disable-accelerated-2d-canvas',
                        '--disable-gpu',
                        '--window-size=1920,1080',
                        '--js-flags=--max-old-space-size=4096'
                    ]
                )
                context = await browser.new_context(
                    user_agent=random.choice(TACTICAL_UAS),
                    viewport={'width': 1920, 'height': 1080},
                    ignore_https_errors=True,
                    java_script_enabled=True,
                    locale='es-CO',
                    timezone_id='America/Bogota',
                    extra_http_headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Language': 'es-CO,es-419;q=0.9,es;q=0.8,en-US;q=0.7,en;q=0.6',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Sec-Ch-Ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                        'Sec-Ch-Ua-Mobile': '?0',
                        'Sec-Ch-Ua-Platform': '"Windows"',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                        'Connection': 'keep-alive'
                    }
                )
                page = await context.new_page()
                
                # Script de evasión avanzado
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => false});
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
                
                # Navegación con timeout y retry
                try:
                    response = await page.goto(url, timeout=timeout_ms, wait_until='domcontentloaded')
                    if response and response.status >= 400:
                        raise Exception(f"HTTP {response.status}")
                except PlaywrightTimeoutError:
                    pass
                
                # Simulación de comportamiento humano
                await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
                await asyncio.sleep(random.uniform(0.5, 1.5))
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 3)")
                await asyncio.sleep(random.uniform(0.3, 0.8))
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 2 / 3)")
                await asyncio.sleep(random.uniform(0.3, 0.8))
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(random.uniform(0.5, 1.0))
                
                # Script de desofuscación avanzado
                stealth_script = """
                () => {
                    // Click en botones que podrían revelar información oculta
                    const buttons = document.querySelectorAll('button, a, span, div');
                    for (const b of buttons) {
                        const txt = (b.innerText || '').toLowerCase();
                        if (/(ver|show|mostrar|revelar|tel|mail|contacto|llamar|ver más|leer más|contact)/.test(txt)) {
                            try { b.click(); } catch(e) {}
                        }
                    }
                    
                    // Desofuscar texto invertido
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
                    
                    // Esperar carga de elementos dinámicos
                    return new Promise(resolve => {
                        setTimeout(() => resolve(true), 2000);
                    });
                }
                """
                await page.evaluate(stealth_script)
                await asyncio.sleep(2.0)
                
                # Recolectar contenido de todos los frames
                frames_html = []
                for frame in page.frames:
                    try:
                        frames_html.append(await frame.content())
                    except: 
                        pass
                
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

    def run_deep_render_sync(self, url: str):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.async_deep_render(url))
        finally:
            loop.close()

    def process_target_worker(self, target: str, geo_context: str, city: str, country: str, cfg: dict, pre_emails: list) -> ExtractedPayload:
        """Procesamiento completo de un objetivo con todas las extracciones"""
        payload = ExtractedPayload(target=target)
        if pre_emails: 
            payload.emails.update(pre_emails)
        
        try:
            headers = {'User-Agent': random.choice(TACTICAL_UAS), 'Accept': 'text/html,*/*'}

            if not re.match(r'^(https?:\/\/|www\.)', target.lower()):
                query = f"{target} {geo_context} colegio sitio web oficial"
                urls = self.resolve_domain_from_serp(query)
                winning_url = self.cognitive_url_scorer(urls, target, city, country) if urls else None
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
                except Exception as e:
                    logger.debug(f"Deep render failed: {e}")
            
            if not html:
                resp = GLOBAL_SESSION.get(payload.domain, headers=headers, timeout=12, verify=False)
                html = resp.text
                payload.pages_scanned += 1

            soup = BeautifulSoup(html, 'html.parser')
            if not payload.name and soup.title:
                payload.name = soup.title.get_text(strip=True)

            self.extract_from_dom(html, browser_text, soup, payload, cfg)
            
            # Extraer deep links
            contact_links = set()
            keywords = {'contacto', 'contactenos', 'contact', 'nosotros', 'directorio', 'admisiones', 'about', 'quienes-somos', 'institucion', 'staff'}
            
            for link in soup.find_all('a', href=True):
                href = link['href'].lower()
                if any(kw in href for kw in keywords) and '#' not in href and not href.startswith('javascript:'):
                    full_url = urljoin(payload.domain, link['href'])
                    if full_url.startswith('http'):
                        contact_links.add(full_url)
            
            for sub_url in list(contact_links)[:5]:
                try:
                    time.sleep(random.uniform(0.5, 1.5))
                    sub_html = ""
                    sub_browser_text = ""
                    
                    if cfg['use_deep_render'] and PLAYWRIGHT_AVAILABLE and not payload.playwright_warn:
                        try:
                            sub_html, sub_browser_text, _ = self.run_deep_render_sync(sub_url)
                            payload.pages_scanned += 1
                        except: 
                            pass
                    
                    if not sub_html:
                        resp_sub = GLOBAL_SESSION.get(sub_url, headers=headers, timeout=10, verify=False)
                        sub_html = resp_sub.text
                        payload.pages_scanned += 1
                        
                    soup_sub = BeautifulSoup(sub_html, 'html.parser')
                    self.extract_from_dom(sub_html, sub_browser_text, soup_sub, payload, cfg)
                    soup_sub.decompose() 
                except Exception: 
                    pass
            
            soup.decompose() 
            payload.socials = set(list(payload.socials)[:5])

            # =========================================================
            # [GOD TIER]: GENERAR REPORTE COSMICO COMPLETO
            # =========================================================
            if payload.raw_text_corpus and len(payload.raw_text_corpus) > 100:
                try:
                    logger.info(f"🌌 Generating cosmic report for {payload.name or target}...")
                    extracted_for_ai = {
                        'lms_provider': payload.lms_provider,
                        'emails': list(payload.emails)[:10],
                        'phones': list(payload.telephones)[:10],
                        'whatsapp': list(payload.whatsapp)[:10]
                    }
                    
                    # Usar el reporte ultra completo con IA
                    payload.cosmic_report = self.generate_ai_forensic_report_ultra(
                        payload.raw_text_corpus,
                        institution_name=payload.name or target,
                        city=city or "Desconocido",
                        country=country or "Colombia"
                    )
                    
                    # También intentar con el cosmic analyzer si está disponible
                    if AI_ANALYZER_AVAILABLE:
                        try:
                            cosmic_report = generate_cosmic_ai_report_sync(
                                name=payload.name or target,
                                city=city or "Desconocido",
                                country=country or "Colombia",
                                webpage_text=payload.raw_text_corpus,
                                raw_html=payload.full_html,
                                extracted_data=extracted_for_ai
                            )
                            # Si el reporte ultra falló o es más corto, usar el cosmic
                            if len(cosmic_report) > len(payload.cosmic_report):
                                payload.cosmic_report = cosmic_report
                        except Exception as e:
                            logger.debug(f"Cosmic analyzer fallback failed: {e}")
                            
                except Exception as e:
                    logger.error(f"❌ Error generating cosmic report: {e}")
                    payload.cosmic_report = f"❌ Error: {str(e)[:200]}"
            else:
                payload.cosmic_report = "⚠️ No hay suficiente texto para análisis cósmico"

        except Exception as e:
            payload.error = f"Fallo Crítico de Infraestructura: {str(e)[:60]}"
            traceback.print_exc()

        return payload

    def post(self, request, *args, **kwargs):
        print("\n" + "🌌"*7 + " [GHOST SWARM V99.9.9.9.9: COSMIC INTELLIGENCE MATRIX OMEGA] " + "🌌"*7)
        
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
        <div class="mb-5 bg-gradient-to-r from-purple-950/30 to-indigo-950/30 border border-purple-500/40 p-4 rounded-xl flex justify-between items-center font-mono text-[11px] shadow-[0_0_20px_rgba(168,85,247,0.15)]">
            <span class="text-slate-400">🌌 COSMIC OMEGA HARVEST | TARGETS: <span class="text-white font-black">{len(targets)}</span></span>
            <span class="text-purple-400 font-bold flex items-center gap-2"><span class="material-symbols-outlined text-[14px]">psychology</span> COSMIC AI OMEGA EXTRACTION v99.9.9.9.9</span>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
        """

        start_time = time.time()
        extracted_data = []
        
        max_threads = 2 if cfg['use_deep_render'] else 5
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {executor.submit(self.process_target_worker, t, geo_context, city, country, cfg, frontend_emails): t for t in targets}
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    extracted_data.append(data)
                except Exception as e: 
                    logger.error(f"❌ Error en hilo principal de OSINT: {e}")
                    traceback.print_exc()

        elapsed = time.time() - start_time
        print(f"⏱️ Operación Cosmic OMEGA completada en {elapsed:.2f} segundos.")

        # =========================================================
        # [TIER 10]: ALMACENAMIENTO DE ALTA FIDELIDAD CON COSMIC AI OMEGA
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
                if data.whatsapp: 
                    combined_phone += f"W:{','.join(list(data.whatsapp))} "
                if data.telephones: 
                    combined_phone += f"T:{','.join(list(data.telephones))}"
                combined_phone = combined_phone.strip()[:120] if combined_phone else None
                
                created = False
                inst = None
                
                try:
                    with transaction.atomic():
                        domain_core = clean_domain.replace('https://', '').replace('http://', '').replace('www.', '').strip('/')
                        inst = Institution.objects.filter(Q(website__icontains=domain_core) | Q(name__iexact=clean_name)).first()

                        if inst:
                            if main_email and not inst.email: 
                                inst.email = main_email
                            if combined_phone and (not inst.phone or len(combined_phone) > len(str(inst.phone))): 
                                inst.phone = combined_phone
                            if not inst.website: 
                                inst.website = clean_domain
                            inst.last_scored_at = timezone.now()
                            inst.processing_status = 'ENRICHED'
                            if data.forensics.get('is_technical', False): 
                                inst.institution_type = 'institute'
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
                                discovery_source='Ghost_Cosmic_Omega',
                                is_private=True,
                                processing_status='ENRICHED',
                                institution_type='institute' if data.forensics.get('is_technical', False) else 'school'
                            )
                        
                        tech, _ = TechProfile.objects.get_or_create(institution=inst)
                        if data.lms_provider != "No detectado":
                            tech.lms_provider = data.lms_provider[:90]
                            tech.has_lms = True
                        tech.save()

                        forensic, _ = DeepForensicProfile.objects.get_or_create(institution=inst)
                        if data.forensics.get('is_bilingual', False): 
                            forensic.is_bilingual = True
                        if data.forensics.get('is_trilingual', False): 
                            forensic.is_trilingual = True
                        if data.forensics.get('cert_ib', False): 
                            forensic.has_ib_cert = True
                        if data.forensics.get('cert_cambridge', False): 
                            forensic.has_cambridge_cert = True
                        if data.has_robotics: 
                            forensic.ai_structured_data = forensic.ai_structured_data or {}
                            forensic.ai_structured_data['has_robotics'] = True
                        if data.has_stem:
                            forensic.ai_structured_data = forensic.ai_structured_data or {}
                            forensic.ai_structured_data['has_stem'] = True
                        if data.has_programming:
                            forensic.ai_structured_data = forensic.ai_structured_data or {}
                            forensic.ai_structured_data['has_programming'] = True
                        if data.icfes_score:
                            forensic.ai_structured_data = forensic.ai_structured_data or {}
                            forensic.ai_structured_data['icfes_score'] = data.icfes_score
                            forensic.ai_structured_data['icfes_category'] = data.icfes_category
                        
                        # GUARDAR EL REPORTE CÓSMICO OMEGA
                        if data.cosmic_report:
                            forensic.ai_comprehensive_report = data.cosmic_report
                        elif data.ai_report:
                            forensic.ai_comprehensive_report = data.ai_report
                        else:
                            forensic.ai_comprehensive_report = "⚠️ No se pudo generar reporte de IA"
                        
                        # Guardar datos estructurados adicionales
                        structured = forensic.ai_structured_data or {}
                        structured.update({
                            'lms_provider': data.lms_provider,
                            'has_ib': data.has_ib,
                            'has_cambridge': data.has_cambridge,
                            'has_oxford': data.has_oxford,
                            'has_stem': data.has_stem,
                            'has_robotics': data.has_robotics,
                            'has_programming': data.has_programming,
                            'extracurricular': data.extracurricular,
                            'agreements': data.agreements,
                            'icfes_score': data.icfes_score,
                            'icfes_category': data.icfes_category
                        })
                        forensic.ai_structured_data = structured
                        forensic.save()

                except IntegrityError as e:
                    logger.warning(f"⚠️ Colisión DB en {clean_domain}: {e}")
                    continue 
                except Exception as e:
                    logger.critical(f"❌ FALLO SQL CRÍTICO al guardar {clean_domain}: {str(e)}")
                    traceback.print_exc()
                    continue

                # =========================================================
                # [TIER 11]: HTML COMPILER ENGINE - CON REPORTE CÓSMICO OMEGA
                # =========================================================
                b_color = "emerald" if created else "blue"
                b_text = "🌌 OMEGA NUEVO" if created else "ACTUALIZADO EN DB"
                soc_html = "".join([f"<a href='{s}' target='_blank' class='text-[8px] bg-[#111] border border-white/10 px-1.5 py-0.5 rounded text-blue-400 uppercase mr-1 hover:bg-white/10 transition-colors'>{s.split('.')[1] if '.' in s else 'SOCIAL'}</a>" for s in data.socials])
                
                wa_html = "".join([f"<a href='https://wa.me/{w}' target='_blank' class='bg-emerald-950/40 text-emerald-400 border border-emerald-500/30 px-1.5 py-0.5 rounded flex items-center gap-1 font-bold whitespace-nowrap hover:bg-emerald-900/60 transition-colors shadow-sm'><span class='material-symbols-outlined text-[10px]'>forum</span> {w}</a>" for w in data.whatsapp])
                tel_html = "".join([f"<a href='tel:{t}' class='bg-blue-950/40 text-blue-400 border border-blue-500/30 px-1.5 py-0.5 rounded flex items-center gap-1 font-bold whitespace-nowrap hover:bg-blue-900/60 transition-colors shadow-sm'><span class='material-symbols-outlined text-[10px]'>call</span> {t}</a>" for t in data.telephones])
                phones_html = wa_html + tel_html
                if not phones_html: 
                    phones_html = "<span class='text-slate-600 font-bold text-[9px]'>📞 NO EXTRAÍDO</span>"

                badges = []
                if data.forensics.get('is_bilingual', False): 
                    badges.append("<span class='bg-blue-900/50 text-blue-300 border border-blue-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold shadow-[0_0_5px_blue]'>🗣️ BILINGÜE</span>")
                if data.forensics.get('is_trilingual', False): 
                    badges.append("<span class='bg-purple-900/50 text-purple-300 border border-purple-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold shadow-[0_0_5px_purple]'>🌍 TRILINGÜE</span>")
                if data.has_ib: 
                    badges.append("<span class='bg-yellow-900/50 text-yellow-300 border border-yellow-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold shadow-[0_0_5px_yellow]'>🏆 IB</span>")
                if data.has_cambridge: 
                    badges.append("<span class='bg-red-900/50 text-red-300 border border-red-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold shadow-[0_0_5px_red]'>🇬🇧 CAMBRIDGE</span>")
                if data.has_oxford: 
                    badges.append("<span class='bg-orange-900/50 text-orange-300 border border-orange-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold shadow-[0_0_5px_orange]'>📚 OXFORD</span>")
                if data.has_robotics: 
                    badges.append("<span class='bg-purple-900/50 text-purple-300 border border-purple-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold'>🤖 ROBÓTICA</span>")
                if data.has_stem: 
                    badges.append("<span class='bg-cyan-900/50 text-cyan-300 border border-cyan-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold'>🔬 STEM</span>")
                if data.has_programming: 
                    badges.append("<span class='bg-emerald-900/50 text-emerald-300 border border-emerald-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold'>💻 PROGRAMACIÓN</span>")
                if data.icfes_score:
                    badges.append(f"<span class='bg-indigo-900/50 text-indigo-300 border border-indigo-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold'>📊 ICFES: {data.icfes_score} {data.icfes_category}</span>")
                if data.forensics.get('is_technical', False): 
                    badges.append("<span class='bg-orange-900/50 text-orange-300 border border-orange-500/50 px-1.5 py-0.5 rounded text-[8px] font-bold'>⚙️ TÉCNICO</span>")
                
                forensic_html = " ".join(badges) if badges else "<span class='text-slate-600 font-mono text-[8px] font-bold'>ESTÁNDAR</span>"
                
                warn_html = f"<div class='bg-yellow-900/40 border border-yellow-500/50 text-yellow-400 text-[8px] font-mono p-1 rounded mt-2 text-center uppercase'>{data.playwright_warn}</div>" if data.playwright_warn else ""

                # Reporte cósmico formateado
                cosmic_preview = (data.cosmic_report or data.ai_report or "No disponible")[:800] + "..." if len(data.cosmic_report or data.ai_report or "") > 800 else (data.cosmic_report or data.ai_report or "No disponible")
                cosmic_html = f"""
                <div class="mt-3 p-2 bg-gradient-to-r from-purple-950/30 to-indigo-950/30 border border-purple-500/30 rounded shadow-[inset_0_0_10px_rgba(168,85,247,0.1)]">
                    <h5 class="text-[9px] font-mono font-black text-purple-400 uppercase flex items-center gap-1 mb-1 border-b border-purple-500/30 pb-1">
                        <span class="material-symbols-outlined text-[12px]">psychology</span> COSMIC OMEGA INTELLIGENCE REPORT
                    </h5>
                    <div class="text-[9px] font-mono text-purple-200/80 leading-tight max-h-[200px] overflow-y-auto custom-scrollbar">
                        {cosmic_preview.replace(chr(10), '<br/>')}
                    </div>
                    <div class="mt-2 text-right">
                        <span class="text-[7px] text-purple-500/70">🌌 Generated by Cosmic Omega AI Engine v99.9.9.9.9</span>
                    </div>
                </div>
                """

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
                                <span class="text-slate-500">INTELIGENCIA BÁSICA</span>
                                <div class="flex gap-1">{forensic_html}</div>
                            </div>
                            <div class="flex justify-between bg-[#020202] p-1.5 rounded border border-white/5">
                                <span class="text-slate-500">TECH/LMS</span>
                                <span class="{"text-emerald-400 font-bold drop-shadow-[0_0_5px_rgba(16,185,129,0.5)]" if data.lms_provider != 'No detectado' else "text-slate-600"}" title="{data.lms_provider.upper()}">{data.lms_provider.upper()}</span>
                            </div>
                        </div>
                        {cosmic_html}
                    </div>
                    <div class="mt-auto border-t border-white/5 pt-3">
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