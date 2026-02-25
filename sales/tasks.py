import time
import logging
import asyncio
import requests
import uuid
import re
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse

from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from requests.exceptions import RequestException, HTTPError, Timeout
from django.core.cache import cache
from django.db import transaction
from django.utils import timezone

# Importaciones locales
from .models import Institution
from .engine.serp_resolver import SERPResolverEngine
from .engine.recon_engine import _orchestrate, execute_recon

# Logger estructurado de grado empresarial
logger = logging.getLogger("Sovereign.CeleryWorkers")

# =========================================================
# MISIÓN 0: OMNI-SCAN (TIER GOD RECON ENGINE)
# =========================================================
@shared_task(
    bind=True, 
    queue='scraping_queue',
    max_retries=3,  # Auto-recuperación de Celery
    autoretry_for=(RequestException, HTTPError),
    retry_backoff=True, # Exponential Backoff Nativo
    retry_backoff_max=60,
    soft_time_limit=300, 
    time_limit=360,
    name="sales.tasks.task_run_single_recon"
)
def task_run_single_recon(self, inst_id: str):
    """
    Motor OMNI-SCAN de Grado Empresarial.
    Pipeline: Resolución Heurística (SERP) -> Sanitización de Datos -> Infiltración Profunda (Playwright).
    """
    start_time = time.time()

    def log_telemetry(message: str, level: str = "SYS"):
        """Telemetría de ultra-baja latencia con precisión de milisegundos para HTMX."""
        cache_key = f"telemetry_{inst_id}"
        current_logs = cache.get(cache_key, [])
        # Timestamp de alta precisión
        timestamp = timezone.now().strftime('%H:%M:%S.%f')[:-3]
        
        formatted_msg = f"[{timestamp}] [{level}] {message}"
        current_logs.append(formatted_msg)
        
        # Sliding Window optimizada (solo guarda los últimos 8 en RAM)
        cache.set(cache_key, current_logs[-8:], timeout=600)
        logger.info(f"[OMNI-SCAN][{inst_id}]: {message}")

    # 1. Recuperación Segura y Bloqueo Distribuido Inmediato
    lock_id = f"lock_recon_{inst_id}"
    if not cache.add(lock_id, "processing", timeout=600):
        log_telemetry("Misión interceptada: Objetivo bajo escaneo concurrente.", "WARN")
        return "Locked by another worker"

    try:
        # Obtenemos el registro (solo lectura por ahora para no bloquear DB)
        inst = Institution.objects.get(id=inst_id)
        log_telemetry(f"⚡ OMNI-SCAN DESPLEGADO: {inst.name[:25]}", "INIT")
        
        # ---------------------------------------------------------
        # FASE 1: RESOLUCIÓN SERP (Auto-Sanable y Sanitizada)
        # ---------------------------------------------------------
        if not inst.website:
            log_telemetry("Buscando huella digital en redes SERP (DuckDuckGo)...", "NET")
            
            engine = SERPResolverEngine()
            keyword = "jardín infantil" if inst.institution_type == 'kindergarten' else "universidad" if inst.institution_type == 'university' else "colegio"
            query = f'"{inst.name}" {inst.city} {inst.country} {keyword} sitio web oficial'
            
            found_url = None
            
            # Circuit Breaker Manual: 3 Intentos con retraso por si DuckDuckGo nos bloquea
            for attempt in range(1, 4):
                try:
                    results = engine._sync_ddg_search(query)
                    if results:
                        for r in results:
                            candidate = r.get('href', '')
                            if engine._is_valid_candidate(candidate):
                                # SANITIZACIÓN DE GRADO EMPRESARIAL:
                                # Elimina UTMs, anclas y basura (ej: colegio.com/?ref=google -> colegio.com)
                                parsed = urlparse(candidate)
                                clean_url = f"{parsed.scheme}://{parsed.netloc}".lower()
                                found_url = clean_url
                                break
                    break # Si no hubo error, salimos del loop de reintentos
                except Exception as e:
                    log_telemetry(f"Sobrecarga SERP. Retrying ({attempt}/3)...", "WARN")
                    time.sleep(2 ** attempt) # Exponential backoff: 2s, 4s, 8s
            
            if found_url:
                # INYECCIÓN ATÓMICA: Evita condiciones de carrera en PostgreSQL
                with transaction.atomic():
                    # Bloqueamos la fila exclusiva para esta actualización
                    locked_inst = Institution.objects.select_for_update().get(id=inst_id)
                    locked_inst.website = found_url
                    locked_inst.save(update_fields=['website'])
                    
                log_telemetry(f"Enlace establecido: {found_url}", "OK")
                inst.website = found_url # Actualizamos memoria local
            else:
                log_telemetry("Objetivo fantasma. Misión cancelada.", "FAIL")
                return "Ghost Target"

        # ---------------------------------------------------------
        # FASE 2: GHOST SNIPER (Infiltración y Extracción)
        # ---------------------------------------------------------
        log_telemetry("Bypass de WAF y extracción forense en curso...", "HACK")
        
        # Llamada al motor pesado de Playwright
        execute_recon(inst_id)
        
        elapsed = round(time.time() - start_time, 2)
        log_telemetry(f"MISIÓN CUMPLIDA. Extracción en {elapsed}s", "SUCCESS")
        return f"Omni-Scan Complete: {elapsed}s"
        
    except Institution.DoesNotExist:
        logger.error(f"Falla crítica: ID {inst_id} no existe.")
        return "404 Not Found"
    except SoftTimeLimitExceeded:
        log_telemetry("Tiempo de infiltración excedido. Abortando...", "TIMEOUT")
        return "Soft Timeout"
    except Exception as e:
        log_telemetry(f"ERROR ESTRUCTURAL: {str(e)[:40]}", "CRITICAL")
        logger.exception(f"OMNI-SCAN Crash en {inst_id}")
        raise self.retry(exc=e) # Delega el error a Celery para que reintente todo desde cero
    finally:
        # PURGA DE MEMORIA: Garantiza que el botón se libere sin importar qué pase
        cache.delete(f"scan_in_progress_{inst_id}")
        cache.delete(lock_id)

# =========================================================
# MISIÓN 1: RADAR OPENSTREETMAP (DATA WAREHOUSE INGESTION)
# =========================================================
@shared_task(
    bind=True, 
    queue='discovery_queue', 
    max_retries=5,
    default_retry_delay=45, # Backoff generoso para no ser baneados por Overpass
    autoretry_for=(RequestException, Timeout),
    soft_time_limit=600,
    time_limit=660
)
def task_run_osm_radar(self, country: str, city: str, mission_id: Optional[str] = None):
    """
    Extracción Geoespacial Tier God.
    Normaliza datos (URLs, Emails) e infiere el tipo de sector (Privado/Público).
    """
    batch_uuid = mission_id or str(uuid.uuid4())
    logger.info(f"🛰️ [OSM RADAR] Inserción Orbital en {city}, {country} | Misión ID: {batch_uuid}")
    
    # Fuzzy Regex avanzado
    city_fuzzy = re.sub(r'[aAáÁ]', '[aAáÁ]', city)
    city_fuzzy = re.sub(r'[eEéÉ]', '[eEéÉ]', city_fuzzy)
    city_fuzzy = re.sub(r'[iIíÍ]', '[iIíÍ]', city_fuzzy)
    city_fuzzy = re.sub(r'[oOóÓ]', '[oOóÓ]', city_fuzzy)
    city_fuzzy = re.sub(r'[uUúÚ]', '[uUúÚ]', city_fuzzy)
    
    query = f"""
    [out:json][timeout:180];
    area["name"~"^{city_fuzzy}$",i]->.searchArea;
    (
      nwr["amenity"~"school|kindergarten|university|college"](area.searchArea);
    );
    out center tags;
    """
    
    try:
        response = requests.post("https://overpass-api.de/api/interpreter", data={'data': query}, timeout=185)
        response.raise_for_status()
        elements = response.json().get('elements', [])
        
        if not elements:
            logger.warning(f"⚠️ [OSM RADAR] Zona muerta detectada. Cero resultados en {city}.")
            return f"Cero resultados en {city}."

        institutions_to_create = []
        names_seen = set()
        
        # Contadores analíticos
        stats = {"con_web": 0, "sin_web": 0, "privados": 0}
        
        for el in elements:
            tags = el.get('tags', {})
            name = tags.get('name') or tags.get('official_name')
            if not name or name.lower() in names_seen: continue
            
            names_seen.add(name.lower())
            
            # --- 1. NORMALIZACIÓN DE URL ---
            raw_url = tags.get('website') or tags.get('contact:website') or tags.get('url')
            if raw_url:
                raw_url = raw_url.strip().lower()
                if not raw_url.startswith(('http://', 'https://')):
                    raw_url = f"https://{raw_url}"
                stats["con_web"] += 1
            else:
                stats["sin_web"] += 1

            # --- 2. NORMALIZACIÓN DE EMAIL ---
            raw_email = tags.get('email') or tags.get('contact:email')
            if raw_email:
                raw_email = raw_email.strip().lower()

            # --- 3. INFERENCIA HEURÍSTICA (Público vs Privado) ---
            operator_type = tags.get('operator:type', '').lower()
            fee = tags.get('fee', '').lower()
            
            # Asumimos que no es privado por defecto, pero buscamos pistas
            is_private = False
            if operator_type in ['private', 'ngo', 'religious'] or fee == 'yes':
                is_private = True
            elif operator_type in ['public', 'government']:
                is_private = False
                
            if is_private: stats["privados"] += 1

            # --- 4. RECONSTRUCCIÓN FÍSICA ---
            street = tags.get('addr:street', '')
            housenumber = tags.get('addr:housenumber', '')
            address = f"{street} {housenumber}".strip() or None
            
            institutions_to_create.append(
                Institution(
                    name=name.strip(),
                    city=city.strip().title(),
                    country=country.strip().title(),
                    website=raw_url,
                    phone=tags.get('phone') or tags.get('contact:phone'),
                    email=raw_email,
                    address=address,
                    institution_type=tags.get('amenity', 'school'),
                    discovery_source='osm',
                    mission_id=batch_uuid,
                    is_active=True,
                    is_private=is_private  # Guardamos la inferencia
                )
            )

        # Inserción Bulk Atómica
        with transaction.atomic():
            Institution.objects.bulk_create(
                institutions_to_create, 
                ignore_conflicts=True, 
                batch_size=500
            )
        
        logger.info(f"🎯 [OSM RADAR] ÉXITO. {len(institutions_to_create)} leads. ({stats['con_web']} Webs, {stats['privados']} Privados).")

        # =========================================================
        # ENRUTAMIENTO INTELIGENTE (SMART ROUTING)
        # =========================================================
        # Solo disparamos el buscador de IA si realmente hay colegios ciegos
        if stats["sin_web"] > 0:
            logger.info(f"🤖 [SMART ROUTE] Desplegando SERP para buscar {stats['sin_web']} webs faltantes.")
            task_run_serp_resolver.apply_async(kwargs={'limit': min(stats["sin_web"], 150)}, countdown=2)
        
        # Solo disparamos el escáner forense si encontramos webs directamente
        if stats["con_web"] > 0:
            logger.info(f"🕵️‍♂️ [SMART ROUTE] Desplegando Ghost Sniper para validar {stats['con_web']} webs nativas.")
            task_run_ghost_sniper.apply_async(kwargs={'mission_id': batch_uuid, 'limit': min(stats["con_web"], 50)}, countdown=5)

        return {"mission_id": batch_uuid, "total": len(institutions_to_create), "stats": stats}

    except Exception as e:
        logger.error(f"❌ [OSM RADAR] Crash Crítico: {str(e)}")
        raise self.retry(exc=e)


# =========================================================
# MISIÓN 2: RESOLUCIÓN DE URLs (SERP CLUSTER)
# =========================================================
@shared_task(
    bind=True, 
    queue='default',
    soft_time_limit=900,
    time_limit=950
)
def task_run_serp_resolver(self, limit: int = 50):
    """Buscador autónomo. Al encontrar URLs, delega inmediatamente al escáner LMS."""
    logger.info(f"🔍 [SERP RESOLVER] Cacería iniciada para {limit} objetivos ciegos.")
    try:
        engine = SERPResolverEngine(concurrency_limit=3)
        # IMPORTANTE: resolve_missing_urls debe retornar cuántos resolvió con éxito
        # Asumiendo que internamente guarda en BD, delegamos la siguiente fase:
        engine.resolve_missing_urls(limit=limit)
        
        # Pasamos la antorcha al Ghost Sniper para que revise lo que el SERP acaba de encontrar
        logger.info("🕵️‍♂️ [CHAIN REACTION] SERP finalizó. Despertando a Ghost Sniper para escaneo de LMS...")
        task_run_ghost_sniper.apply_async(kwargs={'limit': limit}, countdown=5)
        
        return f"Resolución SERP Finalizada. Pasando a fase Forense."
    except Exception as e:
        logger.error(f"❌ [SERP] Error en motor de búsqueda: {str(e)}")
        raise


# =========================================================
# MISIÓN 3: BATCH GHOST SNIPER (ASYNCHRONOUS BATCHING)
# =========================================================
@shared_task(
    bind=True, 
    queue='scraping_queue',
    soft_time_limit=2400,
    time_limit=2500
)
def task_run_ghost_sniper(self, limit: int = 25, mission_id: Optional[str] = None):
    """
    Orquestador Forense Masivo.
    Se alimenta de las bases de datos previamente curadas por OSM y SERP.
    """
    logger.info(f"🕵️‍♂️ [GHOST SNIPER] Iniciando escaneo LMS masivo (Batch: {limit})")
    
    # Filtro: Tienen web, están activos, pero NO tienen un Tech Stack registrado aún.
    query = Institution.objects.filter(website__isnull=False, is_active=True).exclude(website='')
    
    if mission_id:
        query = query.filter(mission_id=mission_id, tech_profile__isnull=True)
    else:
        query = query.filter(tech_profile__isnull=True)

    # Añadimos un fallback: si no hay nulos en tech_profile, buscamos los no analizados por fecha
    if not query.exists():
        query = Institution.objects.filter(website__isnull=False, last_scored_at__isnull=True).exclude(website='')

    qs = query[:limit]
    
    if not qs.exists():
        logger.info("✅ [GHOST SNIPER] Inbox Zero. Todo el pipeline está enriquecido.")
        return "Inbox Zero."

    targets = [
        {'id': inst.id, 'name': inst.name, 'url': inst.website, 'city': inst.city}
        for inst in qs
    ]

    try:
        asyncio.run(_orchestrate(targets))
        return f"Misión cumplida: {len(targets)} colegios inyectados con Tech Stack."
    except SoftTimeLimitExceeded:
        logger.warning("⏳ [GHOST SNIPER] Cut-off por tiempo límite. Lote guardado parcialmente.")
        return "Timeout. Guardado parcial."
    except Exception as e:
        logger.error(f"❌ [GHOST SNIPER] Crash en orquestador Playwright: {str(e)}")
        raise