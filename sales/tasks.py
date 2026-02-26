import time
import logging
import asyncio
import requests
import uuid
import re
import gc
import random
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse
from sales.engine.reply_catcher import run_inbound_catcher

# Celery & Django Imports
from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError

from django.core.cache import cache
from django.db import transaction
from django.utils import timezone
from django.db.models import Q

# Local Engine Imports
from .models import Institution
from .engine.serp_resolver import SERPResolverEngine
from .engine.recon_engine import _orchestrate, execute_recon
from .engine.ml_scoring import train_model, score_unrated_leads

# =========================================================
# ⚙️ TIER GOD CONFIGURATION & TELEMETRY
# =========================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s.%(msecs)03d - [%(levelname)s] [Sovereign-Workers] %(message)s', 
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.CeleryWorkers")

def create_resilient_session() -> requests.Session:
    """Configura una sesión HTTP con Circuit Breaker, Connection Pooling y Retries."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2, # Esperas de 2s, 4s, 8s, 16s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        'User-Agent': 'Sovereign-B2B-Intelligence-Engine/1.0 (Enterprise Data Aggregator)'
    })
    return session

# =========================================================
# 🕵️‍♂️ MISIÓN 0: OMNI-SCAN (TIER GOD RECON ENGINE)
# =========================================================
@shared_task(
    bind=True, 
    queue='scraping_queue',
    max_retries=3,
    autoretry_for=(RequestException, HTTPError, Timeout),
    retry_backoff=True,
    retry_backoff_max=120,
    retry_jitter=True, # Previene el problema del 'Thundering Herd'
    soft_time_limit=300, 
    time_limit=360,
    name="sales.tasks.task_run_single_recon"
)
def task_run_single_recon(self, inst_id: str):
    """
    Motor OMNI-SCAN de Grado Empresarial.
    Resolución SERP -> Sanitización -> Extracción Forense -> Limpieza de Memoria.
    """
    start_time = time.time()
    lock_id = f"mutex_recon_{inst_id}"

    def log_telemetry(message: str, level: str = "SYS"):
        cache_key = f"telemetry_{inst_id}"
        current_logs = cache.get(cache_key, [])
        timestamp = timezone.now().strftime('%H:%M:%S.%f')[:-3]
        current_logs.append(f"[{timestamp}] [{level}] {message}")
        cache.set(cache_key, current_logs[-8:], timeout=600)
        logger.info(f"[OMNI-SCAN][{inst_id}]: {message}")

    # Bloqueo Atómico Distribuido (Imposibilita colisiones)
    if not cache.add(lock_id, "processing", timeout=360):
        log_telemetry("Misión interceptada: Objetivo bajo escaneo concurrente.", "WARN")
        return "Locked by another worker"

    try:
        inst = Institution.objects.get(id=inst_id)
        log_telemetry(f"⚡ OMNI-SCAN DESPLEGADO: {inst.name[:25]}", "INIT")
        
        # --- FASE 1: RESOLUCIÓN SERP ---
        if not inst.website:
            log_telemetry("Buscando huella digital en redes SERP (DuckDuckGo)...", "NET")
            engine = SERPResolverEngine()
            
            # Contextualización Semántica
            keyword = {
                'kindergarten': 'jardín infantil',
                'university': 'universidad',
                'institute': 'instituto'
            }.get(inst.institution_type, 'colegio')
            
            query = f'"{inst.name}" {inst.city} {inst.country} {keyword} sitio web oficial'
            found_url = None
            
            for attempt in range(1, 4):
                try:
                    results = engine._sync_ddg_search(query)
                    if results:
                        for r in results:
                            candidate = r.get('href', '')
                            if engine._is_valid_candidate(candidate):
                                parsed = urlparse(candidate)
                                found_url = f"{parsed.scheme}://{parsed.netloc}".lower()
                                break
                    break 
                except Exception as e:
                    log_telemetry(f"Sobrecarga SERP. Retrying ({attempt}/3)...", "WARN")
                    time.sleep((2 ** attempt) + random.uniform(0, 1)) 
            
            if found_url:
                with transaction.atomic():
                    # Bloqueo estricto de fila para escritura limpia
                    locked_inst = Institution.objects.select_for_update().get(id=inst_id)
                    locked_inst.website = found_url
                    locked_inst.save(update_fields=['website', 'updated_at'])
                    
                log_telemetry(f"Enlace establecido: {found_url}", "OK")
                inst.website = found_url
            else:
                log_telemetry("Objetivo fantasma. Misión cancelada.", "FAIL")
                return "Ghost Target"

        # --- FASE 2: GHOST SNIPER ---
        log_telemetry("Bypass de WAF y extracción forense en curso...", "HACK")
        execute_recon(inst_id)
        
        elapsed = round(time.time() - start_time, 2)
        log_telemetry(f"MISIÓN CUMPLIDA. Operación finalizada en {elapsed}s", "SUCCESS")
        return f"Omni-Scan Complete: {elapsed}s"
        
    except Institution.DoesNotExist:
        logger.error(f"❌ Falla crítica: ID {inst_id} no existe.")
        return "404 Not Found"
    except SoftTimeLimitExceeded:
        log_telemetry("Cut-off de recursos. Proceso abortado para proteger el nodo.", "TIMEOUT")
        return "Soft Timeout"
    except Exception as e:
        log_telemetry(f"ERROR ESTRUCTURAL: {str(e)[:40]}", "CRITICAL")
        logger.exception(f"OMNI-SCAN Crash Crítico en {inst_id}")
        raise self.retry(exc=e) 
    finally:
        # Destrucción Absoluta de Artefactos de Memoria
        cache.delete(f"scan_in_progress_{inst_id}")
        cache.delete(lock_id)
        gc.collect() # Libera RAM del Celery Worker


# =========================================================
# 🛰️ MISIÓN 1: RADAR OPENSTREETMAP (DATA INGESTION)
# =========================================================
@shared_task(
    bind=True, 
    queue='discovery_queue', 
    max_retries=5,
    autoretry_for=(RequestException, Timeout, ConnectionError),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
    soft_time_limit=600,
    time_limit=660
)
def task_run_osm_radar(self, country: str, city: str, mission_id: Optional[str] = None):
    """
    Extracción Geoespacial con Upserts Masivos Optimizados y Auto-Sanación de API.
    """
    batch_uuid = mission_id or str(uuid.uuid4())
    logger.info(f"🛰️ [OSM RADAR] Inserción Orbital en {city}, {country} | Misión ID: {batch_uuid}")
    
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
        session = create_resilient_session()
        response = session.post("https://overpass-api.de/api/interpreter", data={'data': query}, timeout=185)
        response.raise_for_status()
        elements = response.json().get('elements', [])
        
        if not elements:
            logger.warning(f"⚠️ [OSM RADAR] Zona muerta detectada. Cero resultados en {city}.")
            return f"Cero resultados en {city}."

        institutions_to_create = []
        names_seen = set()
        stats = {"con_web": 0, "sin_web": 0, "privados": 0}
        
        for el in elements:
            tags = el.get('tags', {})
            name = tags.get('name') or tags.get('official_name')
            if not name or name.lower() in names_seen: continue
            
            names_seen.add(name.lower())
            
            raw_url = tags.get('website') or tags.get('contact:website') or tags.get('url')
            if raw_url:
                raw_url = raw_url.strip().lower()
                if not raw_url.startswith(('http://', 'https://')):
                    raw_url = f"https://{raw_url}"
                stats["con_web"] += 1
            else:
                stats["sin_web"] += 1

            raw_email = tags.get('email') or tags.get('contact:email')
            if raw_email:
                raw_email = raw_email.strip().lower()

            operator_type = tags.get('operator:type', '').lower()
            fee = tags.get('fee', '').lower()
            
            is_private = False
            if operator_type in ['private', 'ngo', 'religious'] or fee == 'yes':
                is_private = True
            elif operator_type in ['public', 'government']:
                is_private = False
                
            if is_private: stats["privados"] += 1

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
                    is_private=is_private
                )
            )

        # Bulk Upsert de Nivel Enterprise (PostgreSQL 15+)
        with transaction.atomic():
            Institution.objects.bulk_create(
                institutions_to_create, 
                update_conflicts=True, 
                unique_fields=['website'], # Asume que website es unique, si no, usa ['name', 'city']
                update_fields=['address', 'phone', 'mission_id', 'is_private'], 
                batch_size=1000
            )
        
        logger.info(f"🎯 [OSM RADAR] ÉXITO. {len(institutions_to_create)} leads procesados. ({stats['con_web']} Webs).")

        # Smart Routing (Chain Orchestration)
        if stats["sin_web"] > 0:
            logger.info(f"🤖 [SMART ROUTE] Encendiendo SERP Engine para {stats['sin_web']} objetivos ciegos.")
            task_run_serp_resolver.apply_async(kwargs={'limit': min(stats["sin_web"], 200)}, countdown=5)
        
        if stats["con_web"] > 0:
            logger.info(f"🕵️‍♂️ [SMART ROUTE] Desplegando Ghost Sniper Fleet para {stats['con_web']} webs nativas.")
            task_run_ghost_sniper.apply_async(kwargs={'mission_id': batch_uuid, 'limit': min(stats["con_web"], 100)}, countdown=15)

        return {"mission_id": batch_uuid, "total": len(institutions_to_create), "stats": stats}

    except Exception as e:
        logger.error(f"❌ [OSM RADAR] Crash de Red/API: {str(e)}")
        raise self.retry(exc=e)
    finally:
        gc.collect()


# =========================================================
# 🔍 MISIÓN 2: RESOLUCIÓN DE URLs (SERP CLUSTER)
# =========================================================
@shared_task(
    bind=True, 
    queue='default',
    soft_time_limit=1800,
    time_limit=1860,
    name="sales.tasks.task_run_serp_resolver"
)
def task_run_serp_resolver(self, limit: int = 50):
    """Cluster autónomo de resolución. Limitado con Mutex para no banear IPs locales."""
    lock_id = "mutex_global_serp_cluster"
    
    if not cache.add(lock_id, "processing", timeout=1800):
        logger.warning("⚠️ [SERP RESOLVER] Clúster actualmente saturado. Cancelando redundancia.")
        return "Cluster Occupied."

    logger.info(f"🔍 [SERP RESOLVER] Cacería iniciada. Límite de carga: {limit} objetivos.")
    try:
        engine = SERPResolverEngine(concurrency_limit=3)
        engine.resolve_missing_urls(limit=limit)
        
        logger.info("🕵️‍♂️ [CHAIN REACTION] SERP Finalizó. Transfiriendo targets resueltos a Playwright...")
        task_run_ghost_sniper.apply_async(kwargs={'limit': limit}, countdown=10)
        return "Resolución SERP Finalizada con éxito."
        
    except SoftTimeLimitExceeded:
        logger.warning("⏳ [SERP RESOLVER] Interrupción por límite de tiempo. Guardando estado.")
        return "Soft Timeout."
    except Exception as e:
        logger.error(f"❌ [SERP] Fallo de motor de búsqueda: {str(e)}")
        raise self.retry(exc=e, countdown=120)
    finally:
        cache.delete(lock_id)
        gc.collect()


# =========================================================
# 👻 MISIÓN 3: BATCH GHOST SNIPER (PLAYWRIGHT FLEET)
# =========================================================
@shared_task(
    bind=True, 
    queue='scraping_queue',
    soft_time_limit=3600, # Hasta 1 hora de scrapeo masivo
    time_limit=3660,
    name="sales.tasks.task_run_ghost_sniper"
)
def task_run_ghost_sniper(self, limit: int = 50, mission_id: Optional[str] = None):
    """Orquestador Forense Masivo asíncrono. Extracción de Tech Stack profunda."""
    logger.info(f"🕵️‍♂️ [GHOST SNIPER] Iniciando Infiltración Masiva (Batch MAX: {limit})")
    
    query = Institution.objects.filter(website__isnull=False, is_active=True).exclude(website='')
    
    if mission_id:
        query = query.filter(mission_id=mission_id, tech_profile__isnull=True)
    else:
        query = query.filter(tech_profile__isnull=True)

    if not query.exists():
        query = Institution.objects.filter(website__isnull=False, last_scored_at__isnull=True).exclude(website='')

    # Optimización de DB: values_list es infinitamente más rápido que instanciar el ORM entero
    qs = list(query.values('id', 'name', 'website', 'city')[:limit])
    
    if not qs:
        logger.info("✅ [GHOST SNIPER] Inbox Zero. Todo el pipeline está enriquecido.")
        return "Inbox Zero."

    targets = [
        {'id': str(item['id']), 'name': item['name'], 'url': item['website'], 'city': item['city']}
        for item in qs
    ]

    try:
        # Loop Asíncrono de alto rendimiento
        asyncio.run(_orchestrate(targets))
        return f"Misión cumplida: {len(targets)} nodos infiltrados."
        
    except SoftTimeLimitExceeded:
        logger.warning("⏳ [GHOST SNIPER] Cut-off por tiempo de ciclo de servidor. Datos parciales asegurados.")
        return "Timeout. Guardado parcial."
    except Exception as e:
        logger.error(f"❌ [GHOST SNIPER] Crash Crítico en Playwright Runtime: {str(e)}")
        raise self.retry(exc=e, countdown=180)
    finally:
        gc.collect()


# =========================================================
# 🧠 MISIÓN 4: PREDICTIVE ML SCORING (THE AUTONOMOUS UNICORN)
# =========================================================
@shared_task(
    bind=True,
    queue='default', # Escalar a queue dedicada en Kubernetes/ECS si es necesario
    max_retries=3,
    retry_backoff=True, 
    retry_backoff_max=600,
    soft_time_limit=1800, # 30 min max
    time_limit=1860,
    name="sales.tasks.task_retrain_ai_model"
)
def task_retrain_ai_model(self):
    """
    [WEEKLY MLOPS OPERATION]
    Reentrenamiento de la matriz de Bosques Aleatorios calibrada.
    Protegida por Mutex Locks distribuidos para prevenir OOM (Out Of Memory).
    """
    start_time = time.time()
    lock_id = "mutex_ml_training_lock"
    
    if not cache.add(lock_id, "locked", timeout=2100):
        logger.warning("⚠️ [ML-OPS] Operación de entrenamiento rechazada: Nodo actual ocupado.")
        return "Locked by another worker."

    logger.info("🧠 [ML-OPS] Lock Distribuido Asegurado. Recompilando Matriz Neuronal...")
    
    try:
        success = train_model()
        elapsed = round((time.time() - start_time) / 60, 2)
        
        if success:
            logger.info(f"🏆 [ML-OPS] Champion Model desplegado exitosamente en {elapsed} mins.")
            return f"Model retrained in {elapsed}m."
        else:
            logger.info("⏸️ [ML-OPS] Varianza de datos insuficiente. Pospuesto para el próximo ciclo.")
            return "Insufficient data."
            
    except SoftTimeLimitExceeded:
        logger.error("⏳ [ML-OPS] FATAL: Límite de procesamiento (30m) excedido. Previniendo deadlock.")
        return "Soft Timeout Exceeded."
    except Exception as e:
        logger.error(f"❌ [ML-OPS] Falla estructural crítica durante el entrenamiento: {str(e)}")
        raise self.retry(exc=e)
    finally:
        cache.delete(lock_id)
        gc.collect() # Crítico para liberar DataFrames de Pandas de la memoria RAM


@shared_task(
    bind=True,
    queue='default',
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    soft_time_limit=600, 
    time_limit=660,
    name="sales.tasks.task_batch_score_leads"
)
def task_batch_score_leads(self, limit: int = 2000):
    """
    [DAILY MLOPS OPERATION]
    Inferencia Masiva. Asigna un score de 0-100 a los leads frescos de manera atómica.
    """
    start_time = time.time()
    lock_id = "mutex_ml_inference_lock"
    
    if not cache.add(lock_id, "processing", timeout=600):
        logger.warning("⚠️ [ML-OPS] Inferencia bloqueada: Matriz actualmente evaluando en otro nodo.")
        return "Locked by another worker."

    logger.info(f"🔮 [ML-OPS] Iniciando Inferencia Vectorial de Alto Rendimiento ({limit} targets)...")
    
    try:
        score_unrated_leads(limit=limit)
        elapsed = round(time.time() - start_time, 2)
        logger.info(f"⚡ [ML-OPS] Inferencia Completada en {elapsed} segundos.")
        return f"Inferencia complete: {elapsed}s."
        
    except SoftTimeLimitExceeded:
        logger.error("⏳ [ML-OPS] Interrupción de Inferencia. Datos parciales guardados.")
        return "Soft Timeout Exceeded."
    except Exception as e:
        logger.error(f"❌ [ML-OPS] Fallo en pipeline de inferencia: {str(e)}")
        raise self.retry(exc=e)
    finally:
        cache.delete(lock_id)
        gc.collect()


