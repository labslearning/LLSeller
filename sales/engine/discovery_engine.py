"""
======================================================================
[GOD TIER ARCHITECTURE: LEVIATHAN CLASS V86.0 - PROJECT OMNISCIENT]
PROJECT: GHOST SWARM (SILICON WADI / UNIT 8200 SPEC)
MODULE: GEO-SPATIAL DISCOVERY ENGINE + DEEPSEEK AI INTEGRATION
ENGINEERING: TRIPLE-TAP ADAPTIVE SERP PROBE, ADVANCED EMAIL DEOBFUSCATION, 
             HTTP/2 BYPASS, O(1) MEMORY, DYNAMIC WORKSPACE REFLECTION,
             MISSION_ID INJECTION, ABSOLUTE DATA SANITIZATION
======================================================================
"""

import logging
import re
import asyncio
import hashlib
import random
import html
import ujson as json
from typing import List, Dict, Any, Optional, Iterator
from urllib.parse import urlparse, unquote

# Dependencias de Misión Crítica
import httpx
from ddgs import DDGS
from openai import AsyncOpenAI
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_exponential_jitter, 
    retry_if_exception_type,
    before_sleep_log
)
from asgiref.sync import sync_to_async
from django.db import transaction
from django.db.utils import IntegrityError
from django.utils import timezone
from django.apps import apps # Reflexión dinámica para evitar ImportError circular

# Importamos ÚNICAMENTE Institution. Workspace se carga en runtime.
from sales.models import Institution

# =========================================================
# 1. TELEMETRÍA Y CREDENCIALES CLASIFICADAS
# =========================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s.%(msecs)03d - [%(levelname)s] [OVERSEER] - %(message)s', 
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.DiscoveryEngine")

# [GOD TIER KEY]: Inyección Directa del LLM Core
DEEPSEEK_API_KEY = "sk-b6020f82f33f445daae865f32d723a44"
DEEPSEEK_BASE_URL = "https://api.deepseek.com"

# =========================================================
# [NÚCLEO FORENSE]: Expresiones Regulares de Grado Militar (V86.0)
# =========================================================
# Basura tecnológica a evadir en las búsquedas (Búsqueda O(1) usando frozenset)
SERP_EXCLUSIONS = frozenset({
    'facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'youtube.com', 
    'wikipedia.org', 'paginasamarillas', 'directory', 'infoisinfo', 'tripadvisor',
    'foursquare', 'civico', 'losmejorescolegios', 'wiktionary', 'dictionary', 'google.com'
})

# Purga de correos inútiles y trampas de spam
GARBAGE_EMAILS = frozenset({
    'sentry', 'wixpress', 'example', 'domain', 'noreply', 'no-reply', 
    'hostmaster', 'postmaster', 'abuse', 'webmaster', 'mailer-daemon', 'contacto@tuweb',
    'admin@'
})

# =========================================================
# 🛡️ THE DATA SANITIZATION SHIELD
# =========================================================
def safe_truncate(val: Any, length: int) -> Optional[str]:
    """
    Fuerza bruta elegante: Corta el string para que nunca explote la DB.
    Garantiza que los datos corruptos de OSM/SERP no causen 'value too long'.
    """
    if not val:
        return None
    val_str = str(val).strip()
    return val_str[:length] if len(val_str) > length else val_str

# =========================================================
# 2. NÚCLEO COGNITIVO DEEPSEEK (AI FORENSIC EXTRACTOR)
# =========================================================
class DeepSeekCognitiveCore:
    """Motor de Extracción Semántica Híbrida (Lexical + LLM)."""
    
    def __init__(self):
        self.client = AsyncOpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
        # Control de estrangulamiento de la API para evitar baneos (HTTP 429)
        self.semaphore = asyncio.Semaphore(15) 

    def _pre_extract_emails(self, text: str) -> List[str]:
        """
        [FAST PATH V86.0]: Extracción matemática de alta velocidad y de-ofuscación pre-IA.
        Convierte la basura ofuscada en correos puros antes de usar el Regex.
        """
        # Sanitizamos el HTML y las URLs antes de buscar
        clean_text = html.unescape(unquote(text))
        
        # 1. Fuerza bruta: Convertimos cualquier ofuscación conocida a un arroba real y puntos reales
        clean_text = re.sub(r'(?i)(\s*\[at\]\s*|\s*\(at\)\s*|\s+at\s+|\s*arroba\s*|&#64;|%40)', '@', clean_text)
        clean_text = re.sub(r'(?i)(\s*\[dot\]\s*|\s*\(dot\)\s*|\s+dot\s+)', '.', clean_text)
        
        # 2. Extracción estricta
        found = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', clean_text)
        
        clean_emails = []
        for e in found:
            clean = str(e).lower().strip().rstrip('.,;:') 
            if '@' in clean and not any(g in clean for g in GARBAGE_EMAILS):
                # Verificación final de integridad estructural
                if re.match(r'^[a-z0-9_.+-]+@[a-z0-9-]+\.[a-z0-9-.]+$', clean):
                    clean_emails.append(clean)
                    
        return list(set(clean_emails))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(initial=1, max=5))
    async def extract_entities_from_text(self, text_chunk: str) -> dict:
        if not text_chunk or len(text_chunk.strip()) < 10:
            return {}

        # 1. Recuperación Léxica Inicial (O(N) Speed)
        pre_found_emails = self._pre_extract_emails(text_chunk)
        
        prompt = f"""
        Eres un extractor forense OSINT de élite de la Unit 8200. 
        Misión: Extraer el CORREO OFICIAL, teléfono, URL y LMS de este fragmento de motor de búsqueda.
        
        REGLAS DE ORO (PENALIZACIÓN SI FALLAS):
        1. BUSCA CORREOS CORTADOS: Si ves "info@" y luego el dominio del colegio separado, únelos.
        2. IGNORA correos falsos (example.com, sentry, wixpress, noreply).
        3. Prioriza correos que terminen en .edu.co, .edu, o dominios institucionales.
        4. Extrae el teléfono dejando solo números (ej. +573100000000).
        5. Detecta plataformas LMS (Moodle, Canvas, Phidias, Q10, Cibercolegios, Idukay, Santillana, Norma, Saberes).
        
        Devuelve ÚNICAMENTE un JSON válido con esta estructura exacta:
        {{
            "emails": ["lista", "de", "correos", "limpios"],
            "phones": ["lista", "de", "telefonos", "limpios"],
            "website": "url oficial limpia si existe o null",
            "lms_provider": "nombre del LMS o null"
        }}
        
        TEXTO A ANALIZAR:
        {text_chunk[:3500]}
        """

        async with self.semaphore:
            try:
                response = await self.client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {"role": "system", "content": "You are a precise JSON data extractor. Output strictly valid JSON without formatting blocks."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.0, # Cero alucinaciones
                    max_tokens=200,
                    response_format={"type": "json_object"}
                )
                
                raw_json = response.choices[0].message.content
                ai_data = json.loads(raw_json)
                
                # 2. Fusión Cognitiva: LLM + Regex (Garantiza que no se pierda el correo)
                final_emails = set(ai_data.get('emails', []))
                final_emails.update(pre_found_emails)
                # Purga final matemática
                ai_data['emails'] = [e for e in final_emails if '@' in e and not any(g in e for g in GARBAGE_EMAILS)]
                
                return ai_data
            except Exception as e:
                logger.debug(f"🧠 DeepSeek Core Falló: {e}")
                # Fallback Robusto: Devolvemos los correos extraídos por fuerza bruta matemática
                return {'emails': pre_found_emails, 'phones': [], 'website': None, 'lms_provider': None}

# =========================================================
# 3. MOTOR DE DESCUBRIMIENTO GEOESPACIAL (GOD TIER V86.0)
# =========================================================
class OSMDiscoveryEngine:
    """
    Combina OSM + DDG (TRIPLE-Tap Dorking) + DeepSeek AI Extractor.
    Arquitectura de Auto-Sanación Zero-Drop.
    """
    
    OVERPASS_ENDPOINTS = [
        "https://overpass-api.de/api/interpreter",          
        "https://lz4.overpass-api.de/api/interpreter",      
        "https://overpass.kumi.systems/api/interpreter",    
        "https://overpass.openstreetmap.ru/cgi/interpreter" 
    ]

    NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
    DB_BATCH_SIZE = 200 

    def __init__(self):
        self.ai_core = DeepSeekCognitiveCore()
        # Semáforo estricto para DuckDuckGo para evadir límites de Rate
        self.serp_semaphore = asyncio.Semaphore(3) 

    @staticmethod
    def _get_stealth_headers() -> Dict[str, str]:
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0"
        ]
        return {
            "User-Agent": random.choice(user_agents),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-CO,es;q=0.9,en-US;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }

    def _build_radial_query(self, city: str, country: str) -> str:
        """Búsqueda de Ancla Geográfica."""
        city_regex = "".join({'a':'[aáAÁ]', 'e':'[eéEÉ]', 'i':'[iíIÍ]', 'o':'[oóOÓ]', 'u':'[uúUÚ]'}.get(c, c) for c in city.strip().lower())
        return f"""
        [out:json][timeout:180];
        area["name"="{country.title()}"]["admin_level"="2"]->.country;
        node["place"~"city|town|village|municipality"]["name"~"{city_regex}", i](area.country)->.cityNode;
        ( nwr["amenity"~"school|kindergarten|university|college"](around.cityNode:20000); );
        out center tags;
        """

    @retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(initial=2, max=15))
    async def _race_endpoints_async(self, query: str) -> List[Dict]:
        """Tolerancia a fallos absoluta con múltiples endpoints de Overpass."""
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=40)
        timeout = httpx.Timeout(180.0, connect=15.0) 
        
        # Mantenemos http2=False para evadir el crash del compilador C en Docker
        async with httpx.AsyncClient(timeout=timeout, http2=False, limits=limits, headers=self._get_stealth_headers()) as client:
            tasks = [asyncio.create_task(self._fetch_single_node(client, ep, query)) for ep in self.OVERPASS_ENDPOINTS]
            for coro in asyncio.as_completed(tasks):
                try:
                    winner_node, elements = await coro
                    for t in tasks:
                        if not t.done(): t.cancel()
                    logger.info(f"🏆 [SWARM] Satélite OSM exitoso: {winner_node} | Carga Útil: {len(elements)} Instituciones.")
                    return elements
                except Exception: continue
            raise Exception("Todos los satélites OSM fallaron.")

    async def _fetch_single_node(self, client: httpx.AsyncClient, endpoint: str, query: str) -> tuple:
        response = await client.post(endpoint, data={'data': query})
        response.raise_for_status()
        data = response.json()
        if "remark" in data and "runtime error" in data["remark"].lower():
            raise Exception("Overpass DB Crash")
        return endpoint, data.get("elements", [])

    # -----------------------------------------------------
    # ENRIQUECIMIENTO ASÍNCRONO + IA (THE TRIPLE-TAP SNIPER)
    # -----------------------------------------------------
    async def _enrich_with_ai(self, name: str, city: str, country: str, existing_email: str, existing_website: str) -> dict:
        """
        [GOD TIER V86.0]: Implementa The Triple-Tap Adaptive SERP Probe.
        La máxima obra de ingeniería OSINT. Si el buscador oculta el correo, 
        lo bombardeamos con 3 capas de Dorking hasta que ceda.
        """
        if existing_email and existing_website:
            return {'website': existing_website, 'email': existing_email, 'phone': None, 'lms': None}

        # Phase 1: Búsqueda General (Para hallar la URL y el LMS)
        query_phase_1 = f'"{name}" {city} colegio'
        
        text_corpus_blocks = []
        enriched = {'website': existing_website, 'email': existing_email, 'phone': None, 'lms': None}

        async with self.serp_semaphore:
            await asyncio.sleep(random.uniform(1.2, 3.5)) # Simulación humana táctica
            
            try:
                def search_ddg(q, max_res):
                    with DDGS(headers=self._get_stealth_headers()) as ddgs:
                        return list(ddgs.text(q, max_results=max_res))
                
                # Ejecutamos el Phase 1
                results_p1 = await asyncio.to_thread(search_ddg, query_phase_1, 5)

                for r in results_p1:
                    href = r.get('href', r.get('url', ''))
                    if href and not enriched['website'] and not any(exc in href.lower() for exc in SERP_EXCLUSIONS):
                        enriched['website'] = href
                    text_corpus_blocks.append(f"{r.get('title', '')} | {r.get('body', '')}")

                raw_corpus_p1 = " ".join(text_corpus_blocks).lower()

                # [THE TRIPLE-TAP SNIPER TRIGGER]
                # Evaluamos si DuckDuckGo censuró el correo en los resultados
                if not any(indicator in raw_corpus_p1 for indicator in ['@', 'correo', 'email']):
                    logger.debug(f"🎯 [SNIPER TIER-2] Correo ausente para {name}. Iniciando Dorking Fase 2...")
                    await asyncio.sleep(random.uniform(1.0, 2.0)) 
                    
                    # Phase 2: Dorking Agresivo Genérico
                    query_phase_2 = f'"{name}" {city} ("@gmail.com" OR "@hotmail.com" OR "correo" OR "email")'
                    results_p2 = await asyncio.to_thread(search_ddg, query_phase_2, 3)
                    
                    corpus_p2 = " ".join([f"{r.get('title', '')} | {r.get('body', '')}" for r in results_p2])
                    text_corpus_blocks.append(corpus_p2)
                    
                    # Si la Fase 2 falla y el colegio TIENE sitio web, lanzamos el ataque nuclear (Phase 3)
                    if enriched['website'] and not any(ind in corpus_p2.lower() for ind in ['@', 'correo']):
                        logger.debug(f"☢️ [SNIPER TIER-3] Aplicando Site-Dorking en {enriched['website']}...")
                        await asyncio.sleep(random.uniform(1.5, 3.0))
                        
                        domain = urlparse(enriched['website']).netloc.replace('www.', '')
                        query_phase_3 = f'site:{domain} "@" OR "correo" OR "contacto"'
                        results_p3 = await asyncio.to_thread(search_ddg, query_phase_3, 3)
                        text_corpus_blocks.append(" ".join([f"{r.get('title', '')} | {r.get('body', '')}" for r in results_p3]))

            except Exception as e:
                logger.debug(f"⚠️ SERP Rate Limit/Timeout para {name}: {e}")

        raw_text_corpus = " ".join(text_corpus_blocks)

        # Invocamos al Dios de la IA
        if raw_text_corpus and (not enriched['email'] or not enriched['website']):
            ai_data = await self.ai_core.extract_entities_from_text(raw_text_corpus)
            
            if not enriched['email'] and ai_data.get('emails'):
                enriched['email'] = safe_truncate(ai_data['emails'][0].lower(), 250)
            
            if not enriched['website'] and ai_data.get('website'):
                url_clean = self._sanitize_website(ai_data['website'])
                if url_clean: enriched['website'] = safe_truncate(url_clean, 250)

            if ai_data.get('phones'):
                enriched['phone'] = safe_truncate(",".join(ai_data['phones']), 50)
                
            if ai_data.get('lms_provider'):
                enriched['lms'] = safe_truncate(str(ai_data['lms_provider']), 90)

        return enriched

    async def _process_stream_with_ai(self, raw_instances: List[Institution], city: str, country: str) -> List[Institution]:
        """Orquestación del enjambre cognitivo de forma concurrente."""
        tasks = []
        for inst in raw_instances:
            async def process(i: Institution):
                if not i.email or not i.website:
                    enriched = await self._enrich_with_ai(i.name, city, country, i.email, i.website)
                    if enriched['website']: i.website = safe_truncate(enriched['website'], 250)
                    if enriched['email']: i.email = safe_truncate(enriched['email'], 250)
                    if enriched.get('phone') and not i.phone: i.phone = safe_truncate(enriched['phone'], 50)
                return i
            tasks.append(asyncio.create_task(process(inst)))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if not isinstance(r, Exception)]

    # -----------------------------------------------------
    # NORMALIZACIÓN Y GUARDADO DE DATOS (SANITIZACIÓN EXTREMA)
    # -----------------------------------------------------
    def _sanitize_website(self, url: str) -> Optional[str]:
        if not url: return None
        url = str(url).strip().lower()
        url = re.sub(r'^(https?://)+', '', url) 
        if not url or len(url) < 4: return None
        url = f"https://{url}" if not url.startswith('http') else url
        try:
            parsed = urlparse(url)
            if len(url) > 250 or not parsed.netloc or '.' not in parsed.netloc: return None
            return url
        except Exception: return None

    def _sanitize_phone(self, phone: str) -> Optional[str]:
        if not phone: return None
        clean = re.sub(r'[^\d\+\-\s\(\)]', '', str(phone)).strip()
        if len(re.sub(r'\D', '', clean)) < 6: return None
        # TRUNCADO ESTRICTO DE SEGURIDAD
        return safe_truncate(f"T:{clean}", 50)

    def _generate_fingerprint(self, name: str, city: str, country: str) -> str:
        raw_string = f"{name.strip().lower()}|{city.strip().lower()}|{country.strip().lower()}"
        return hashlib.sha256(raw_string.encode('utf-8')).hexdigest()

    def _normalize_stream(self, elements: List[Dict], city: str, country: str, mission_id: Optional[str] = None) -> List[Institution]:
        """
        [VITAL FIX INCLUDED]: Propagación de mission_id para UI Tracker.
        + SANITIZACIÓN EN TIEMPO REAL.
        """
        institutions = []
        for element in elements:
            tags = element.get("tags", {})
            raw_name = tags.get("name") or tags.get("official_name")
            if not raw_name or len(raw_name) < 4: continue

            amenity = tags.get("amenity", "school")
            inst_type = 'school'
            if amenity == "kindergarten": inst_type = 'kindergarten'
            elif amenity in ["university", "college"]: inst_type = 'university'

            lat = element.get("lat") or element.get("center", {}).get("lat")
            lon = element.get("lon") or element.get("center", {}).get("lon")

            website = self._sanitize_website(tags.get("website") or tags.get("contact:website") or tags.get("url"))
            phone = self._sanitize_phone(tags.get("phone") or tags.get("contact:phone"))
            
            raw_email = tags.get("email") or tags.get("contact:email")
            email = safe_truncate(str(raw_email).strip().lower(), 250) if raw_email and '@' in str(raw_email) else None

            raw_address = f"{tags.get('addr:street', '')} {tags.get('addr:housenumber', '')} {tags.get('addr:postcode', '')}".strip()

            # CONSTRUCCIÓN BLINDADA (The Shield)
            institutions.append(Institution(
                name=safe_truncate(raw_name.strip(), 250), 
                website=website, 
                email=email, 
                phone=phone,
                institution_type=inst_type, 
                country=safe_truncate(country, 100), 
                city=safe_truncate(tags.get("addr:city", city), 100),
                address=safe_truncate(raw_address, 250) if raw_address else None, 
                latitude=lat, 
                longitude=lon,
                discovery_source='GeoRadar_DeepSeek_V60', 
                processing_status='RAW', 
                is_private=True, 
                is_active=True,
                mission_id=mission_id
            ))
        return institutions

    @sync_to_async
    def _save_to_db(self, instances: List[Institution], city: str) -> int:
        total_valid = len(instances)
        logger.info(f"⚙️ Inyectando {total_valid} Leads Híbridos a PostgreSQL en Bloque...")
        
        # Filtro final de seguridad antes de insertar en PostgreSQL
        safe_instances = []
        for inst in instances:
            # Reafirmar límites de varchar para los campos críticos
            inst.name = safe_truncate(inst.name, 250)
            inst.phone = safe_truncate(inst.phone, 50)
            inst.website = safe_truncate(inst.website, 250)
            inst.email = safe_truncate(inst.email, 250)
            inst.city = safe_truncate(inst.city, 100)
            inst.country = safe_truncate(inst.country, 100)
            inst.address = safe_truncate(inst.address, 250)
            safe_instances.append(inst)

        try:
            with transaction.atomic():
                # ignore_conflicts=True reemplaza a update_conflicts para máxima tolerancia a fallos masivos
                Institution.objects.bulk_create(
                    safe_instances, 
                    batch_size=self.DB_BATCH_SIZE, 
                    ignore_conflicts=True
                )
            logger.info(f"🏁 [APEX VICTORY] {city.upper()} | {total_valid} LEADS COSECHADOS Y ASEGURADOS.")
            return total_valid
        except Exception as e:
            logger.error(f"🧨 FATAL BULK CREATE ERROR: {str(e)}. Activando Contingencia Secuencial...")
            return self._fallback_sequential_inject(safe_instances, city)

    def _fallback_sequential_inject(self, instances: List[Institution], city: str) -> int:
        inserted = 0
        for inst in instances:
            try:
                with transaction.atomic():
                    # Usamos .save() directo envuelto en atomic para aislar fallos
                    inst.save()
                    inserted += 1
            except IntegrityError:
                pass # Duplicado, ignorar silenciosamente
            except Exception as e:
                logger.error(f"Fallo individual en {inst.name[:15]}: {str(e)}")
        return inserted

    async def run_radar(self, location_name: str, country: str = "Colombia", limit: int = 50, mission_id: Optional[str] = None) -> int:
        """
        Punto de ejecución maestro. Incorpora mission_id en la firma.
        """
        logger.info(f"🚀 INICIANDO RECONOCIMIENTO COGNITIVO V86.0: {location_name.upper()}, {country.upper()}")
        
        query = self._build_radial_query(location_name, country)
        
        try:
            raw_elements = await self._race_endpoints_async(query)
        except Exception as e:
            logger.error(f"❌ [CRÍTICO] Colapso total del Escudo OSM: {str(e)}")
            return 0
        
        if not raw_elements: return 0

        # Memoria de Enjambre: O(1) Búsqueda para NO REPETIR colegios
        existing_names = await sync_to_async(lambda: set(
            Institution.objects.filter(city__iexact=location_name, country__iexact=country)
            .values_list('name', flat=True)
        ))()

        raw_instances = self._normalize_stream(raw_elements, location_name, country, mission_id=mission_id)
        
        unique_instances_map = {}
        for inst in raw_instances:
            # Filtro de Memoria: Si ya está en la DB, lo descartamos al instante
            if inst.name in existing_names:
                continue
                
            fingerprint = self._generate_fingerprint(inst.name, inst.city, inst.country)
            if fingerprint not in unique_instances_map: 
                unique_instances_map[fingerprint] = inst
            else:
                existing = unique_instances_map[fingerprint]
                if not existing.website and inst.website: existing.website = inst.website
                if not existing.email and inst.email: existing.email = inst.email
                if not existing.phone and inst.phone: existing.phone = inst.phone
                    
        instances_to_enrich = list(unique_instances_map.values())
        
        if not instances_to_enrich: 
            logger.info("✅ Todos los colegios encontrados ya están en la Base de Datos.")
            return 0

        # LÍMITE EXACTO: Cortamos la lista ANTES de gastar tokens de DeepSeek
        target_leads = instances_to_enrich[:limit]
        logger.info(f"🧠 {len(target_leads)} leads nuevos listos. Desplegando DeepSeek Swarm...")
        
        final_instances = await self._process_stream_with_ai(target_leads, location_name, country)

        return await self._save_to_db(final_instances, location_name)

def run_osm_radar(workspace_id=None, city="Bogota", country="Colombia", limit=50):
    """
    Punto de entrada compatible con Celery. Orquesta el motor asíncrono desde el mundo síncrono.
    """
    radar = OSMDiscoveryEngine()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # [GOD TIER FIX]: Importación Dinámica segura en Runtime. Adiós ImportErrors!
    WorkspaceModel = None
    for model_name in ['Workspace', 'GeoRadarWorkspace', 'CommandCenter']:
        try:
            WorkspaceModel = apps.get_model('sales', model_name)
            break
        except LookupError: pass

    mission_uuid = None
    if WorkspaceModel and workspace_id:
        try:
            workspace = WorkspaceModel.objects.get(id=workspace_id)
            if hasattr(workspace, 'status'): workspace.status = 'SCANNING'
            workspace.save()
            city = getattr(workspace, 'target_city', city)
            country = getattr(workspace, 'target_country', country)
            limit = getattr(workspace, 'limit_count', limit)
            mission_uuid = str(workspace.id)
        except Exception: pass

    try:
        inserted = loop.run_until_complete(radar.run_radar(city, country, limit, mission_uuid))
        
        if WorkspaceModel and workspace_id:
            try:
                workspace = WorkspaceModel.objects.get(id=workspace_id)
                if hasattr(workspace, 'status'): workspace.status = 'COMPLETED'
                workspace.save()
            except Exception: pass
            
        return inserted
    except Exception as e:
        logger.error(f"💥 FALLO CRÍTICO EN GEORADAR V86.0: {str(e)}")
        if WorkspaceModel and workspace_id:
            try:
                workspace = WorkspaceModel.objects.get(id=workspace_id)
                if hasattr(workspace, 'status'): workspace.status = 'FAILED'
                workspace.save()
            except Exception: pass
        return 0
    finally:
        loop.close()