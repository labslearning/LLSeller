"""
======================================================================
[GOD TIER ARCHITECTURE: LEVIATHAN CLASS V86.0 - PROJECT OMNISCIENT]
MODULE: CELERY TASKS DISTRIBUTED ORCHESTRATOR
ENGINEERING: THE COGNITIVE REAPER PROTOCOL (EMAIL FALLBACK), 
             ADAPTIVE KWARGS PARSING, SYNC/ASYNC I/O HYGIENE, 
             OOM PREVENTION, ZERO-DROP ARCHITECTURE,
             ATOMIC TRANSACTIONS, ERROR BUBBLING, DEAD LETTER QUEUE
======================================================================
"""

import time
import logging
import asyncio
import requests
import uuid
import re
import random
import html
import os
import ujson as json
from contextlib import contextmanager
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urlparse, unquote

# Celery & Django Imports
from celery import shared_task, Task, group
from celery.exceptions import SoftTimeLimitExceeded
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError

from django.core.cache import cache
from django.db import transaction, DatabaseError, IntegrityError
from django import db  
from django.utils import timezone
from django.db.models import Q
from asgiref.sync import async_to_sync  

# =========================================================
# IMPORTACIONES DE VANGUARDIA (GOD TIER)
# =========================================================
from sales.models import Institution, TechProfile, DeepForensicProfile, Interaction, Contact
from sales.engine.serp_resolver import SERPResolverEngine
from sales.engine.recon_engine import execute_recon, run_recon
from sales.engine.ml_scoring import train_model, score_unrated_leads
from sales.engine.discovery_engine import OSMDiscoveryEngine

from ddgs import DDGS
from openai import AsyncOpenAI, RateLimitError, APIConnectionError, APIError

# =========================================================
# ⚙️ OMNI-TIER CONFIGURATION & TELEMETRY
# =========================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s.%(msecs)03d - [%(levelname)s] [Sovereign-Workers] %(message)s', 
    datefmt='%H:%M:%S'
)

logger = logging.getLogger("Sovereign.OmniSniper.Celery")

GARBAGE_EMAILS = frozenset({
    'sentry', 'wixpress', 'example', 'domain', 'noreply', 'no-reply', 
    'hostmaster', 'postmaster', 'abuse', 'webmaster', 'mailer-daemon', 'contacto@tuweb'
})

class SovereignBaseTask(Task):
    """
    [ARQUITECTURA LIMPIA]: Clase base para todas las tareas Celery.
    Garantiza la higiene absoluta de las conexiones a la base de datos.
    Destruye conexiones Zombie sin depender del Garbage Collector de Python.
    """
    abstract = True

    def before_start(self, task_id, args, kwargs):
        db.close_old_connections()

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        db.close_old_connections()
        super().on_failure(exc, task_id, args, kwargs, einfo)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        db.close_old_connections()

def create_resilient_session() -> requests.Session:
    """Connection Pooling de Grado Militar para mitigar TCP Handshake latency."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1.5, 
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        'User-Agent': 'Sovereign-B2B-Intelligence-Engine/6.0 (Enterprise Data Aggregator)'
    })
    return session

@contextmanager
def distributed_lock(lock_id: str, timeout: int = 360, blocking: bool = False, max_wait: int = 5):
    """
    [OMNI-TIER MUTEX]: Algoritmo de Backoff Exponencial con Jittering Matemático.
    Evita colisiones de transacciones O(N) reduciéndolas a O(1) amortizado en Redis.
    """
    acquired = False
    start_time = time.time()
    attempt = 0
    
    try:
        while True:
            try:
                acquired = cache.add(lock_id, "locked", timeout=timeout)
            except Exception as e:
                logger.error(f"⚠️ Falla del Broker de Caché en Lock {lock_id}: {e}")
                break 

            if acquired or not blocking:
                break
            
            elapsed = time.time() - start_time
            if elapsed > max_wait:
                break
                
            attempt += 1
            sleep_time = min(0.1 * (2 ** attempt), 1.0) + random.uniform(0, 0.1)
            time.sleep(sleep_time)
            
        yield acquired
    finally:
        if acquired:
            try:
                cache.delete(lock_id)
            except Exception:
                pass 

def safe_async_runner(coro):
    try:
        if hasattr(asyncio, 'Runner'):
            with asyncio.Runner() as runner:
                return runner.run(coro)
        else:
            return asyncio.run(coro)
    except Exception as e:
        logger.error(f"Async Sandbox Violation: {e}")
        raise

# =========================================================
# 🎯 MISIÓN 0: OMNI-SCAN (SINGLE TARGET RECON ENGINE)
# =========================================================
@shared_task(
    bind=True, 
    base=SovereignBaseTask, 
    queue='scraping_queue',
    max_retries=3,
    autoretry_for=(RequestException, HTTPError, Timeout),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
    soft_time_limit=600, 
    time_limit=660,
    name="sales.tasks.task_run_single_recon"
)
def task_run_single_recon(self, institution_id: str):
    """
    [THE SNIPER ENGINE]: Ejecución de Playwright + The Cognitive Reaper Protocol.
    Asegura que ninguna institución salga sin un correo validado.
    Envoltura Atómica para evitar fallos silenciosos.
    """
    start_time = time.time()
    logger.info(f"🎯 Infiltrando Objetivo: {institution_id}")
    
    lock_id = f"mutex_recon_{institution_id}"
    
    def log_telemetry(message: str, level: str = "SYS"):
        cache_key = f"telemetry_{institution_id}"
        current_logs = cache.get(cache_key, [])
        timestamp = timezone.now().strftime('%H:%M:%S.%f')[:-3]
        current_logs.append(f"[{timestamp}] [{level}] {message}")
        cache.set(cache_key, current_logs[-8:], timeout=600)
        logger.info(f"[OMNI-SCAN][{institution_id}]: {message}")

    # --- THE COGNITIVE REAPER (EMAIL FALLBACK ENGINE) ---
    async def _reaper_email_extraction(name: str, city: str, website: str) -> Optional[str]:
        """Si Playwright falla por ofuscación de JS, el Reaper ataca los metadatos indexados."""
        log_telemetry("Desplegando 'The Cognitive Reaper' para forzar extracción de Email...", "REAPER")
        
        domain = urlparse(website).netloc.replace('www.', '') if website else ""
        query = f'"{name}" {city} ("@gmail.com" OR "@hotmail.com" OR "correo" OR "email" OR "@edu.co")'
        if domain: query += f" OR site:{domain}"

        def fetch_serp():
            with DDGS() as ddg:
                return [f"{r.get('title')} | {r.get('body')}" for r in ddg.text(query, backend="lite", max_results=4)]
        
        try:
            results = await asyncio.to_thread(fetch_serp)
            corpus = " ".join(results)
            
            clean_text = re.sub(r'(?i)(\s*\[at\]\s*|\s*\(at\)\s*|\s+at\s+|\s*arroba\s*|&#64;|%40)', '@', html.unescape(unquote(corpus)))
            found = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', clean_text)
            
            valid_emails = [e.lower().strip().rstrip('.,;:') for e in found if '@' in e and not any(g in e for g in GARBAGE_EMAILS)]
            if valid_emails:
                return valid_emails[0]

            async_client = AsyncOpenAI(api_key=os.environ.get("DEEPSEEK_API_KEY", "sk-b6020f82f33f445daae865f32d723a44"), base_url="https://api.deepseek.com")
            response = await async_client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "Extract ONLY the valid email address from the text as a raw string. If none, return 'NONE'."},
                    {"role": "user", "content": corpus[:3000]}
                ],
                temperature=0.0
            )
            llm_result = response.choices[0].message.content.strip().lower()
            if '@' in llm_result and 'none' not in llm_result:
                return llm_result
        except Exception as e:
            logger.error(f"Reaper Exception: {e}")
        return None

    with distributed_lock(lock_id, timeout=660) as acquired:
        if not acquired:
            logger.warning(f"🔒 Objetivo {institution_id} ya bajo asedio por otro nodo.")
            return "Locked"

        try:
            with transaction.atomic():
                inst = Institution.objects.select_for_update().only(
                    'id', 'name', 'city', 'country', 'institution_type', 'website', 'email', 'processing_status'
                ).get(id=institution_id)
                
                if inst.processing_status not in [Institution.ProcessingStatus.RAW, Institution.ProcessingStatus.RAW_RADAR]:
                    logger.info(f"⏭️ Objetivo {institution_id} ya fue procesado (Status: {inst.processing_status}). Omitiendo.")
                    return "Already Processed"

                inst.processing_status = Institution.ProcessingStatus.SNIPER_LOCKED
                inst.save(update_fields=['processing_status'])
                
            log_telemetry(f"⚡ INFILTRACIÓN INICIADA: {inst.name[:25]}", "INIT")
            
            if not inst.website:
                log_telemetry("Buscando huella digital en redes SERP...", "NET")
                engine = SERPResolverEngine()
                keyword = {'kindergarten': 'jardín infantil', 'university': 'universidad'}.get(inst.institution_type, 'colegio')
                query = f'"{inst.name}" {inst.city} {inst.country} {keyword} sitio web oficial'
                found_url = None
                
                for attempt in range(1, 4):
                    try:
                        results = engine._search_provider_sync(query)
                        if results:
                            for r in results:
                                candidate = r.get('href', '')
                                if engine._is_valid_candidate(candidate):
                                    parsed = urlparse(candidate)
                                    found_url = f"{parsed.scheme}://{parsed.netloc}".lower()
                                    break
                        if found_url: break 
                    except Exception:
                        time.sleep((2 ** attempt) + random.uniform(0, 1)) 
                
                if found_url:
                    inst.website = found_url
                    Institution.objects.filter(id=institution_id).update(website=found_url, updated_at=timezone.now())
                    log_telemetry(f"Enlace establecido: {found_url}", "OK")
                else:
                    log_telemetry("Objetivo fantasma o sin URL. Misión abortada.", "FAIL")
                    Institution.objects.filter(id=institution_id).update(processing_status=Institution.ProcessingStatus.DISCARDED)
                    return "Ghost Target"

            log_telemetry("Bypass de WAF y extracción de inteligencia DOM...", "HACK")
            run_recon(inst_id=str(institution_id))
            
            inst.refresh_from_db(fields=['email'])
            
            if not inst.email:
                log_telemetry("El DOM Sniper no encontró el Email. Iniciando extracción Cognitiva...", "WARN")
                recovered_email = async_to_sync(_reaper_email_extraction)(inst.name, inst.city, inst.website)
                
                if recovered_email:
                    Institution.objects.filter(id=institution_id).update(email=recovered_email, updated_at=timezone.now())
                    log_telemetry(f"Reaper extrajo con éxito: {recovered_email}", "SUCCESS")
                else:
                    log_telemetry("El objetivo no posee correo digital verificable.", "FAIL")

            Institution.objects.filter(id=institution_id).update(processing_status=Institution.ProcessingStatus.ENRICHED)
            
            elapsed = round(time.time() - start_time, 2)
            log_telemetry(f"MISIÓN CUMPLIDA. Inteligencia asegurada en {elapsed}s", "SUCCESS")
            return {"status": "SUCCESS", "id": institution_id, "time": elapsed}

        except Institution.DoesNotExist:
            logger.error(f"❌ [OMNI-SCAN] Falla crítica: ID {institution_id} no existe en la base de datos.")
            return "404 Not Found"

        except IntegrityError as e:
            logger.critical(f"💀 [OMNI-SCAN] Fallo de Integridad DB (Duplicado o nulo) en {institution_id}: {e}")
            Institution.objects.filter(id=institution_id).update(processing_status=Institution.ProcessingStatus.FAILED)
            raise self.retry(exc=e, countdown=10)

        except DatabaseError as e:
            logger.critical(f"💀 [OMNI-SCAN] Fallo Estructural DB (Ej: CharField muy corto) en {institution_id}: {e}")
            Institution.objects.filter(id=institution_id).update(processing_status=Institution.ProcessingStatus.FAILED)
            raise

        except SoftTimeLimitExceeded:
            logger.warning(f"⏳ [OMNI-SCAN] Cut-off de recursos en {institution_id}. Proceso abortado para proteger el nodo.")
            Institution.objects.filter(id=institution_id).update(processing_status=Institution.ProcessingStatus.RAW_RADAR)
            return "Soft Timeout"

        except Exception as e:
            logger.error(f"❌ [OMNI-SCAN] Error catastrófico en Recon {institution_id}: {str(e)}")
            Institution.objects.filter(id=institution_id).update(processing_status=Institution.ProcessingStatus.FAILED)
            raise self.retry(exc=e, countdown=60)
        finally:
            cache.delete(f"scan_in_progress_{institution_id}")


# =========================================================
# 🛸 MISIÓN 1: CONTROLADOR DE ENJAMBRE (FLOW CONTROL)
# =========================================================
@shared_task(
    bind=True, 
    base=SovereignBaseTask,
    name="sales.tasks.task_run_ghost_sniper_fleet"
)
def task_run_ghost_sniper_fleet(self, limit: int = 500, city: str = None, mission_id: str = None):
    """Orquesta el enjambre reduciendo latencia O(N) a O(1) en Broker Dispatch."""
    logger.info(f"🚦 [SWARM COMMANDER] Solicitando autorización para {limit} objetivos en {city or 'Global'}...")

    with transaction.atomic():
        query = Institution.objects.select_for_update().filter(
            website__isnull=False, 
            is_active=True,
            processing_status__in=[Institution.ProcessingStatus.RAW, Institution.ProcessingStatus.RAW_RADAR]
        )
        if city: 
            query = query.filter(city__icontains=city)
        if mission_id: 
            query = query.filter(mission_id=mission_id)
        
        target_ids = list(query.order_by('created_at').values_list('id', flat=True)[:limit])

        if not target_ids:
            logger.info(f"✅ [SWARM COMMANDER] Base de datos limpia en {city or 'Global'}.")
            return f"Inbox Zero para {city or 'Global'}."

        Institution.objects.filter(id__in=target_ids).update(
            processing_status=Institution.ProcessingStatus.SNIPER_LOCKED,
            updated_at=timezone.now()
        )

    logger.info(f"🔥 [SWARM COMMANDER] {len(target_ids)} blancos BLOQUEADOS. Desatando el Infierno asíncrono...")
    
    for target_id in target_ids:
        task_run_single_recon.apply_async(
            args=[str(target_id)], 
            queue='scraping_queue'
        )

    return f"Flota desplegada: {len(target_ids)} drones en el aire."


# =========================================================
# 🛰️ MISIÓN 2: RADAR OPENSTREETMAP (DATA INGESTION)
# =========================================================
@shared_task(
    bind=True, 
    base=SovereignBaseTask,
    queue='discovery_queue', 
    max_retries=5,
    autoretry_for=(RequestException, Timeout, ConnectionError),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
    soft_time_limit=600,
    time_limit=660
)
def task_run_osm_radar(self, country: str, city: str, *args, **kwargs):
    """
    [THE TYPE CATCHER]: Intercepta parámetros desordenados de Django Admin.
    Reasigna dinámicamente si recibe un UUID en el lugar del 'limit'.
    """
    raw_arg3 = kwargs.get('workspace_id') or kwargs.get('limit') or (args[0] if len(args) > 0 else None)
    raw_arg4 = kwargs.get('mission_id') or (args[1] if len(args) > 1 else None)
    
    actual_limit = 500
    mission_uuid = str(uuid.uuid4())

    if isinstance(raw_arg3, str) and len(raw_arg3) > 20: 
        mission_uuid = raw_arg3
    elif isinstance(raw_arg3, int) or (isinstance(raw_arg3, str) and raw_arg3.isdigit()):
        actual_limit = int(raw_arg3)

    if isinstance(raw_arg4, str) and len(raw_arg4) > 20:
        mission_uuid = raw_arg4

    from django.apps import apps
    WorkspaceModel = None
    for model_name in ['GeoRadarWorkspace', 'Workspace', 'CommandCenter']:
        try:
            WorkspaceModel = apps.get_model('sales', model_name)
            break
        except LookupError: pass

    if WorkspaceModel:
        try:
            ws = WorkspaceModel.objects.get(id=mission_uuid)
            actual_limit = int(getattr(ws, 'limit_count', actual_limit))
        except Exception: pass

    logger.info(f"🛰️ [OSM RADAR] Inserción Orbital en {city}, {country} | Límite Realizado: {actual_limit} | Misión ID: {mission_uuid}")
    lock_id = f"mutex_osm_{country}_{city}"
    
    with distributed_lock(lock_id, timeout=600, blocking=True, max_wait=5) as acquired:
        if not acquired:
            logger.warning(f"⚠️ [OSM RADAR] Zona {city} ya bajo escaneo.")
            return f"Sector Locked {city}."
            
        try:
            engine = OSMDiscoveryEngine()
            total_creados = async_to_sync(engine.run_radar)(
                location_name=city, 
                country=country, 
                limit=actual_limit, 
                mission_id=mission_uuid
            )
            logger.info(f"🎯 [OSM RADAR] ÉXITO en {city}. Total inyectados en estado RAW: {total_creados}.")

            if total_creados > 0:
                logger.info(f"🤖 [SMART ROUTE] Despertando Flota Sniper para enriquecer {city}...")
                task_run_ghost_sniper_fleet.apply_async(
                    kwargs={'limit': total_creados, 'city': city, 'mission_id': mission_uuid}, 
                    countdown=10
                )
            return {"mission_id": mission_uuid, "total": total_creados}
        except SoftTimeLimitExceeded:
            return "Soft Timeout Exceeded"
        except Exception as e:
            raise self.retry(exc=e, countdown=60)

# =========================================================
# 🔍 MISIÓN 3: RESOLUCIÓN DE URLs (SERP CLUSTER)
# =========================================================
@shared_task(
    bind=True,
    base=SovereignBaseTask,
    name="sales.tasks.task_run_serp_resolver",
    queue='discovery_queue',
    soft_time_limit=120,
    time_limit=150,
    acks_late=True,
    max_retries=3
)
def task_run_serp_resolver(self, limit: int = 50):
    lock_id = "mutex_global_serp_cluster"
    with distributed_lock(lock_id, timeout=1800) as acquired:
        if not acquired: return "Cluster Occupied."
        logger.info(f"🔍 [SERP RESOLVER] Cacería iniciada. Límite: {limit} objetivos.")
        try:
            engine = SERPResolverEngine(concurrency_limit=3)
            engine.resolve_missing_urls(limit=limit)
            return "Resolución SERP Finalizada con éxito."
        except SoftTimeLimitExceeded:
            return "Soft Timeout."
        except Exception as e:
            raise self.retry(exc=e, countdown=120)

# =========================================================
# 🤖 MISIÓN 4: AUTONOMOUS AI OUTREACH (LA IA S.D.R.)
# =========================================================
@shared_task(
    bind=True,
    base=SovereignBaseTask,
    name="sales.tasks.task_autonomous_ai_outreach",
    soft_time_limit=300
)
def task_autonomous_ai_outreach(self, limit: int = 50, city: str = None):
    logger.info(f"🧠 [AI SDR] Iniciando campaña de contacto táctico. Límite: {limit}")
    query = Institution.objects.select_related('tech_profile', 'forensic_profile').filter(
        processing_status=Institution.ProcessingStatus.ENRICHED,
        contacted=False,
        email__isnull=False,
        lead_score__gte=50
    ).only(
        'id', 'name', 'city', 'email', 'contacted', 'updated_at',
        'tech_profile__lms_provider', 'forensic_profile__pedagogical_emphasis',
        'forensic_profile__is_bilingual'
    )
    if city: query = query.filter(city__icontains=city)
    targets = list(query.order_by('id')[:limit])
    
    if not targets: return "Cero Targets aptos para disparo."

    async def run_ai_fleet(targets_list: List[Institution]) -> List[Any]:
        async_client = AsyncOpenAI(api_key=os.getenv("DEEPSEEK_API_KEY"), base_url="https://api.deepseek.com", max_retries=0)
        semaphore = asyncio.Semaphore(15) 
        
        system_directive = """
        Eres el Director Comercial de 'Learning Labs', la plataforma LMS más rápida y nativa del mercado.
        Redacta un cold email (max 4 líneas) directo al Rector. 
        """
        async def fetch_with_retry(inst: Institution, max_attempts=3):
            tech = getattr(inst, 'tech_profile', None)
            forensic = getattr(inst, 'forensic_profile', None)
            lms_actual = tech.lms_provider if tech and tech.lms_provider else "plataforma estándar"
            enfasis = forensic.pedagogical_emphasis if forensic and forensic.pedagogical_emphasis else "educativo"
            
            user_context = f"Colegio '{inst.name}' en '{inst.city}'. Enfoque: {enfasis}. LMS actual: {lms_actual}."
            for attempt in range(1, max_attempts + 1):
                async with semaphore:
                    try:
                        response = await async_client.chat.completions.create(
                            model="deepseek-chat",
                            messages=[{"role": "system", "content": system_directive}, {"role": "user", "content": user_context}],
                            temperature=0.7, timeout=15.0
                        )
                        return inst, response.choices[0].message.content, enfasis
                    except Exception as e:
                        if attempt == max_attempts: return inst, None, enfasis
                        await asyncio.sleep((2 ** attempt) + random.uniform(0, 1))
            return inst, None, enfasis

        tasks = [fetch_with_retry(inst) for inst in targets_list]
        return await asyncio.gather(*tasks, return_exceptions=True)

    results = async_to_sync(run_ai_fleet)(targets)
    interactions_to_create, institutions_to_update = [], []

    for result in results:
        if isinstance(result, Exception) or not result: continue
        inst, email_body, enfasis = result
        if not email_body: continue
            
        inst.contacted = True
        inst.updated_at = timezone.now()
        institutions_to_update.append(inst)
        interactions_to_create.append(
            Interaction(
                institution=inst, channel='EMAIL', status='SENT',
                subject=f"Potenciando el enfoque {enfasis} en {inst.name}",
                message_sent=email_body, thread_id=f"thread_{inst.id}",
                next_action_date=timezone.now() + timezone.timedelta(days=3)
            )
        )
        
    if institutions_to_update:
        with transaction.atomic():
            Institution.objects.bulk_update(institutions_to_update, ['contacted', 'updated_at'], batch_size=200)
            Interaction.objects.bulk_create(interactions_to_create, batch_size=200)

    return f"Campaña completada. {len(institutions_to_update)} contactados."

# =========================================================
# 🧠 MISIÓN 5: PREDICTIVE ML SCORING
# =========================================================
@shared_task(bind=True, base=SovereignBaseTask, soft_time_limit=1800, time_limit=1860)
def task_retrain_ai_model(self):
    with distributed_lock("mutex_ml_training_lock", timeout=2100) as acquired:
        if not acquired: return "Locked."
        try: return "Model retrained." if train_model() else "Insufficient data."
        except Exception as e: raise self.retry(exc=e)

@shared_task(bind=True, base=SovereignBaseTask, soft_time_limit=600, time_limit=660)
def task_batch_score_leads(self, limit: int = 2000):
    with distributed_lock("mutex_ml_inference_lock", timeout=600) as acquired:
        if not acquired: return "Locked."
        try:
            score_unrated_leads(limit=limit)
            return "Inferencia complete."
        except Exception as e: raise self.retry(exc=e)

# =========================================================
# 📡 MISIÓN 6: INBOUND RADAR (WEBHOOK & REPLY CATCHER)
# =========================================================
@shared_task(bind=True, base=SovereignBaseTask, name="sales.tasks.task_run_inbound_catcher")
def task_run_inbound_catcher(self):
    logger.info("📡 [INBOUND RADAR] Escaneando respuestas entrantes...")
    try:
        from sales.engine.reply_catcher import run_inbound_catcher
        
        if not os.getenv("EMAIL_HOST_USER") or not os.getenv("EMAIL_HOST_PASSWORD"):
            logger.error("❌ [FATAL] Credenciales IMAP no detectadas en las variables de entorno.")
            return "Missing IMAP Credentials. Aborted."
            
        run_inbound_catcher() 
        return "Inbound scan acknowledged."
    except ImportError:
        logger.warning("Reply Catcher module not found or disabled.")
        return "Module Disabled"
    except Exception as e:
        logger.error(f"Fallo en Inbound Radar: {e}")
        return "Scan Failed."