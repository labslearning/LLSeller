"""
================================================================================
[GOD TIER OMEGA ABSOLUTE ARCHITECTURE: TRANSCENDENT QUANTUM LEVIATHAN CLASS]
PROJECT: GHOST SWARM - COSMIC INTELLIGENCE HARVESTER
MODULE: GEO-SPATIAL DISCOVERY ENGINE + DEEPSEEK AI INTEGRATION
VERSION: 99.9.9.9.9.OMEGA.ABSOLUTE
================================================================================
"""

import logging
import re
import asyncio
import hashlib
import random
import html
import ujson as json
import time
import traceback
import secrets
from typing import List, Dict, Any, Optional, Iterator, Tuple, Set
from urllib.parse import urlparse, unquote
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

# Dependencias de Misión Crítica
import httpx
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_exponential_jitter, 
    retry_if_exception_type,
    before_sleep_log,
    retry_if_result
)
from asgiref.sync import sync_to_async
from django.db import transaction
from django.db.utils import IntegrityError
from django.utils import timezone
from django.apps import apps
from django.core.cache import cache

# Importamos modelos
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
import os
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "sk-b6020f82f33f445daae865f32d723a44")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"

# =========================================================
# [NÚCLEO FORENSE]: Expresiones Regulares de Grado Militar
# =========================================================

SERP_EXCLUSIONS = frozenset({
    'facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'youtube.com', 
    'wikipedia.org', 'paginasamarillas', 'directory', 'infoisinfo', 'tripadvisor',
    'foursquare', 'civico', 'losmejorescolegios', 'wiktionary', 'dictionary', 'google.com'
})

GARBAGE_EMAILS = frozenset({
    'sentry', 'wixpress', 'example', 'domain', 'noreply', 'no-reply', 
    'hostmaster', 'postmaster', 'abuse', 'webmaster', 'mailer-daemon', 'contacto@tuweb',
    'admin@'
})

def safe_truncate(val: Any, length: int) -> Optional[str]:
    """Corta string para que nunca explote la DB."""
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
        from openai import AsyncOpenAI
        self.client = AsyncOpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
        self.semaphore = asyncio.Semaphore(15)

    def _pre_extract_emails(self, text: str) -> List[str]:
        """Extracción matemática de alta velocidad pre-IA."""
        if not text:
            return []
        clean_text = html.unescape(unquote(text))
        
        # Patrones de ofuscación
        clean_text = re.sub(r'(?i)(\s*\[at\]\s*|\s*\(at\)\s*|\s+at\s+|\s*arroba\s*|&#64;|%40)', '@', clean_text)
        clean_text = re.sub(r'(?i)(\s*\[dot\]\s*|\s*\(dot\)\s*|\s+dot\s+)', '.', clean_text)
        
        found = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', clean_text)
        
        clean_emails = []
        for e in found:
            clean = str(e).lower().strip().rstrip('.,;:')
            if '@' in clean and not any(g in clean for g in GARBAGE_EMAILS):
                if re.match(r'^[a-z0-9_.+-]+@[a-z0-9-]+\.[a-z0-9-.]+$', clean):
                    clean_emails.append(clean)
        return list(set(clean_emails))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(initial=1, max=5))
    async def extract_entities_from_text(self, text_chunk: str) -> dict:
        if not text_chunk or len(text_chunk.strip()) < 10:
            return {}

        pre_found_emails = self._pre_extract_emails(text_chunk)
        
        prompt = f"""
        Eres un extractor forense OSINT de élite de la Unit 8200.
        Misión: Extraer el CORREO OFICIAL, teléfono, URL y LMS de este fragmento.
        
        REGLAS DE ORO:
        1. BUSCA CORREOS CORTADOS: Si ves "info@" y luego el dominio separado, únelos.
        2. IGNORA correos falsos (example.com, sentry, wixpress, noreply).
        3. Prioriza correos que terminen en .edu.co, .edu, o dominios institucionales.
        4. Extrae el teléfono dejando solo números (ej. +573100000000).
        5. Detecta plataformas LMS (Moodle, Canvas, Phidias, Schoolnet, Cibercolegios, etc.)
        
        Devuelve ÚNICAMENTE un JSON válido con esta estructura:
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
                    temperature=0.0,
                    max_tokens=200,
                    response_format={"type": "json_object"}
                )
                
                raw_json = response.choices[0].message.content
                ai_data = json.loads(raw_json)
                
                final_emails = set(ai_data.get('emails', []))
                final_emails.update(pre_found_emails)
                ai_data['emails'] = [e for e in final_emails if '@' in e and not any(g in e for g in GARBAGE_EMAILS)]
                
                return ai_data
            except Exception as e:
                logger.debug(f"🧠 DeepSeek Core Falló: {e}")
                return {'emails': pre_found_emails, 'phones': [], 'website': None, 'lms_provider': None}


# =========================================================
# 3. CLASE ORIGINAL OSMDiscoveryEngine (MANTENIDA PARA COMPATIBILIDAD)
# =========================================================

class OSMDiscoveryEngine:
    """
    Motor de descubrimiento ORIGINAL - Mantenido para compatibilidad con código existente.
    Esta clase es un wrapper que llama a la versión Omega.
    """
    
    OVERPASS_ENDPOINTS = [
        "https://overpass-api.de/api/interpreter",
        "https://lz4.overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass.openstreetmap.ru/cgi/interpreter"
    ]
    
    DB_BATCH_SIZE = 200

    def __init__(self):
        self.ai_core = DeepSeekCognitiveCore()
        self.serp_semaphore = asyncio.Semaphore(3)

    @staticmethod
    def _get_stealth_headers() -> Dict[str, str]:
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        ]
        return {
            "User-Agent": random.choice(user_agents),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-CO,es;q=0.9,en-US;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }

    def _build_radial_query(self, city: str, country: str) -> str:
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
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=40)
        timeout = httpx.Timeout(180.0, connect=15.0)
        
        async with httpx.AsyncClient(timeout=timeout, http2=False, limits=limits, headers=self._get_stealth_headers()) as client:
            tasks = [asyncio.create_task(self._fetch_single_node(client, ep, query)) for ep in self.OVERPASS_ENDPOINTS]
            for coro in asyncio.as_completed(tasks):
                try:
                    winner_node, elements = await coro
                    for t in tasks:
                        if not t.done(): t.cancel()
                    logger.info(f"🏆 [SWARM] Satélite OSM exitoso: {winner_node} | {len(elements)} elementos")
                    return elements
                except Exception: continue
            raise Exception("Todos los satélites OSM fallaron")

    async def _fetch_single_node(self, client: httpx.AsyncClient, endpoint: str, query: str) -> tuple:
        response = await client.post(endpoint, data={'data': query})
        response.raise_for_status()
        data = response.json()
        if "remark" in data and "runtime error" in data["remark"].lower():
            raise Exception("Overpass DB Crash")
        return endpoint, data.get("elements", [])

    async def _enrich_with_ai(self, name: str, city: str, country: str, existing_email: str, existing_website: str) -> dict:
        if existing_email and existing_website:
            return {'website': existing_website, 'email': existing_email, 'phone': None, 'lms': None}

        text_corpus_blocks = []
        enriched = {'website': existing_website, 'email': existing_email, 'phone': None, 'lms': None}

        async with self.serp_semaphore:
            await asyncio.sleep(random.uniform(1.2, 3.5))
            
            try:
                from ddgs import DDGS
                
                def search_ddg(q, max_res):
                    with DDGS(headers=self._get_stealth_headers()) as ddgs:
                        return list(ddgs.text(q, max_results=max_res))
                
                results_p1 = await asyncio.to_thread(search_ddg, f'"{name}" {city} colegio', 5)
                for r in results_p1:
                    href = r.get('href', r.get('url', ''))
                    if href and not enriched['website'] and not any(exc in href.lower() for exc in SERP_EXCLUSIONS):
                        enriched['website'] = href
                    text_corpus_blocks.append(f"{r.get('title', '')} | {r.get('body', '')}")
                
                raw_corpus_p1 = " ".join(text_corpus_blocks).lower()
                
                if not any(indicator in raw_corpus_p1 for indicator in ['@', 'correo', 'email']):
                    await asyncio.sleep(random.uniform(1.0, 2.0))
                    results_p2 = await asyncio.to_thread(search_ddg, f'"{name}" {city} ("@gmail.com" OR "@hotmail.com" OR "correo" OR "email")', 3)
                    corpus_p2 = " ".join([f"{r.get('title', '')} | {r.get('body', '')}" for r in results_p2])
                    text_corpus_blocks.append(corpus_p2)
                    
                    if enriched['website'] and not any(ind in corpus_p2.lower() for ind in ['@', 'correo']):
                        await asyncio.sleep(random.uniform(1.5, 3.0))
                        domain = urlparse(enriched['website']).netloc.replace('www.', '')
                        results_p3 = await asyncio.to_thread(search_ddg, f'site:{domain} "@" OR "correo" OR "contacto"', 3)
                        text_corpus_blocks.append(" ".join([f"{r.get('title', '')} | {r.get('body', '')}" for r in results_p3]))
            except Exception as e:
                logger.debug(f"⚠️ SERP Rate Limit/Timeout para {name}: {e}")

        raw_text_corpus = " ".join(text_corpus_blocks)

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
        return safe_truncate(f"T:{clean}", 50)

    def _generate_fingerprint(self, name: str, city: str, country: str) -> str:
        raw_string = f"{name.strip().lower()}|{city.strip().lower()}|{country.strip().lower()}"
        return hashlib.sha256(raw_string.encode('utf-8')).hexdigest()

    def _normalize_stream(self, elements: List[Dict], city: str, country: str, mission_id: Optional[str] = None, search_hash: Optional[str] = None) -> List[Institution]:
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
                mission_id=mission_id,
                search_hash=search_hash  # 🔥 GOD TIER: Anti-duplicación por lote
            ))
        return institutions

    @sync_to_async
    def _save_to_db(self, instances: List[Institution], city: str) -> int:
        total_valid = len(instances)
        logger.info(f"⚙️ Inyectando {total_valid} Leads a PostgreSQL...")
        
        safe_instances = []
        for inst in instances:
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
                Institution.objects.bulk_create(
                    safe_instances, 
                    batch_size=self.DB_BATCH_SIZE, 
                    ignore_conflicts=True
                )
            logger.info(f"🏁 [APEX VICTORY] {city.upper()} | {total_valid} LEADS COSECHADOS")
            return total_valid
        except Exception as e:
            logger.error(f"🧨 FATAL BULK CREATE ERROR: {str(e)}. Activando fallback...")
            return self._fallback_sequential_inject(safe_instances, city)

    def _fallback_sequential_inject(self, instances: List[Institution], city: str) -> int:
        inserted = 0
        for inst in instances:
            try:
                with transaction.atomic():
                    inst.save()
                    inserted += 1
            except IntegrityError:
                pass
            except Exception as e:
                logger.error(f"Fallo individual en {inst.name[:15]}: {str(e)}")
        return inserted

    async def run_radar(self, location_name: str, country: str = "Colombia", 
                        limit: int = 50, mission_id: Optional[str] = None,
                        search_hash: Optional[str] = None) -> int:
        """
        Punto de ejecución maestro con soporte para search_hash (anti-duplicación).
        """
        logger.info(f"🚀 INICIANDO RECONOCIMIENTO COGNITIVO: {location_name.upper()}, {country.upper()}")
        
        query = self._build_radial_query(location_name, country)
        
        try:
            raw_elements = await self._race_endpoints_async(query)
        except Exception as e:
            logger.error(f"❌ [CRÍTICO] Colapso total del Escudo OSM: {str(e)}")
            return 0
        
        if not raw_elements: return 0

        # Filtrar duplicados por search_hash
        if search_hash:
            existing_names = await sync_to_async(lambda: set(
                Institution.objects.filter(
                    search_hash=search_hash,
                    city__iexact=location_name,
                    country__iexact=country
                ).values_list('name', flat=True)
            ))()
            logger.info(f"🔍 {len(existing_names)} duplicados en este lote (hash: {search_hash[:16]}...)")
        else:
            existing_names = await sync_to_async(lambda: set(
                Institution.objects.filter(
                    city__iexact=location_name,
                    country__iexact=country
                ).values_list('name', flat=True)
            ))()
            logger.info(f"🔍 {len(existing_names)} duplicados en DB global")

        raw_instances = self._normalize_stream(raw_elements, location_name, country, mission_id=mission_id, search_hash=search_hash)
        
        unique_instances_map = {}
        for inst in raw_instances:
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

        target_leads = instances_to_enrich[:limit]
        logger.info(f"🧠 {len(target_leads)} leads nuevos. Desplegando IA...")
        
        final_instances = await self._process_stream_with_ai(target_leads, location_name, country)

        return await self._save_to_db(final_instances, location_name)


# =========================================================
# 4. FUNCIONES DE EXPORTACIÓN (Compatibilidad)
# =========================================================

def run_osm_radar(workspace_id=None, city="Bogota", country="Colombia", limit=50):
    """
    Punto de entrada compatible con Celery.
    """
    engine = OSMDiscoveryEngine()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
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
        inserted = loop.run_until_complete(engine.run_radar(city, country, limit, mission_uuid))
        
        if WorkspaceModel and workspace_id:
            try:
                workspace = WorkspaceModel.objects.get(id=workspace_id)
                if hasattr(workspace, 'status'): workspace.status = 'COMPLETED'
                workspace.save()
            except Exception: pass
            
        return inserted
    except Exception as e:
        logger.error(f"💥 FALLO CRÍTICO EN GEORADAR: {str(e)}")
        if WorkspaceModel and workspace_id:
            try:
                workspace = WorkspaceModel.objects.get(id=workspace_id)
                if hasattr(workspace, 'status'): workspace.status = 'FAILED'
                workspace.save()
            except Exception: pass
        return 0
    finally:
        loop.close()


# =========================================================
# 5. SELF-TEST
# =========================================================

async def self_test():
    logger.info("🧪 [SELF-TEST] Verificando integridad...")
    try:
        engine = OSMDiscoveryEngine()
        assert engine is not None, "Engine no inicializado"
        logger.info("✅ [SELF-TEST] Todas las verificaciones pasaron")
        return True
    except Exception as e:
        logger.error(f"❌ [SELF-TEST] Falló: {e}")
        return False

