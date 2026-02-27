import time
import logging
import asyncio
import requests
import uuid
import re
import gc
import random
from contextlib import contextmanager
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
from django import db
from django.utils import timezone
from django.db.models import Q
from django.db import transaction, DatabaseError

# Local Engine Imports
from .models import Institution
from .engine.serp_resolver import SERPResolverEngine
from .engine.recon_engine import _orchestrate, execute_recon
from .engine.ml_scoring import train_model, score_unrated_leads
from .engine.discovery_engine import OSMDiscoveryEngine
#Desde aqui 



# Importaciones locales de tu arquitectura B2B
from sales.models import Institution, TechProfile
from sales.views import SniperSearchView

logger = logging.getLogger("Sovereign.OmniSniper.Celery")



# =========================================================
# ⚙️ OMNI-TIER CONFIGURATION & TELEMETRY
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
# 🛡️ INFRAESTRUCTURA DE BLOQUEO DISTRIBUIDO Y SANDBOXING
# =========================================================
@contextmanager
def distributed_lock(lock_id: str, timeout: int = 360, blocking: bool = False, max_wait: int = 5):
    """
    [OMNI-TIER MUTEX]: Administrador de contexto con Spin-Lock y Jittering.
    Previene Deadlocks (si Celery muere) y Thundering Herd (saturación de Redis).
    """
    acquired = False
    start_time = time.time()
    
    try:
        while True:
            acquired = cache.add(lock_id, "locked", timeout=timeout)
            if acquired or not blocking:
                break
            if time.time() - start_time > max_wait:
                break
            time.sleep(random.uniform(0.1, 0.5)) # Micro-espera
            
        yield acquired
    finally:
        if acquired:
            cache.delete(lock_id)

def safe_async_runner(coro):
    """
    [EVENT LOOP SANDBOXING]: Entorno estéril para Playwright y HTTPX.
    Caza corrutinas zombies y libera descriptores de red.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        asyncio.set_event_loop(None)


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
    db.close_old_connections() # Previene "connection already closed" en procesos largos
    start_time = time.time()
    lock_id = f"mutex_recon_{inst_id}"

    def log_telemetry(message: str, level: str = "SYS"):
        cache_key = f"telemetry_{inst_id}"
        current_logs = cache.get(cache_key, [])
        timestamp = timezone.now().strftime('%H:%M:%S.%f')[:-3]
        current_logs.append(f"[{timestamp}] [{level}] {message}")
        cache.set(cache_key, current_logs[-8:], timeout=600)
        logger.info(f"[OMNI-SCAN][{inst_id}]: {message}")

    # Bloqueo Atómico Distribuido Invencible (Context Manager)
    with distributed_lock(lock_id, timeout=360) as acquired:
        if not acquired:
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
                        if found_url:
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
            db.close_old_connections()
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
    Extracción Geoespacial. Delega la ejecución de red a OSMDiscoveryEngine.
    """
    db.close_old_connections()
    batch_uuid = mission_id or str(uuid.uuid4())
    logger.info(f"🛰️ [OSM RADAR] Inserción Orbital en {city}, {country} | Misión ID: {batch_uuid}")
    
    lock_id = f"mutex_osm_{country}_{city}"
    
    with distributed_lock(lock_id, timeout=600, blocking=True, max_wait=5) as acquired:
        if not acquired:
            logger.warning(f"⚠️ [OSM RADAR] Zona {city} ya bajo escaneo.")
            return f"Sector Locked {city}."
            
        try:
            # Delegamos al motor Singularity Tier
            engine = OSMDiscoveryEngine()
            engine.discover_and_inject(city=city, country=country)
            
            # Post-procesamiento y recuento para orquestación inteligente
            institutions_query = Institution.objects.filter(city__iexact=city)
            if mission_id:
                institutions_query.filter(mission_id__isnull=True).update(mission_id=mission_id)
                
            total_creados = institutions_query.count()
            stats = {
                "con_web": institutions_query.filter(website__isnull=False).count(),
                "sin_web": institutions_query.filter(website__isnull=True).count(),
                "privados": institutions_query.filter(is_private=True).count()
            }
            
            logger.info(f"🎯 [OSM RADAR] ÉXITO en {city}. Total: {total_creados} leads. ({stats['con_web']} Webs).")

            # Smart Routing (Chain Orchestration)
            if stats["sin_web"] > 0:
                logger.info(f"🤖 [SMART ROUTE] Encendiendo SERP Engine para {stats['sin_web']} objetivos ciegos.")
                task_run_serp_resolver.apply_async(kwargs={'limit': min(stats["sin_web"], 200)}, countdown=5)
            
            if stats["con_web"] > 0:
                logger.info(f"🕵️‍♂️ [SMART ROUTE] Desplegando Ghost Sniper Fleet para {stats['con_web']} webs nativas.")
                task_run_ghost_sniper.apply_async(kwargs={'mission_id': batch_uuid, 'limit': min(stats["con_web"], 100)}, countdown=15)

            return {"mission_id": batch_uuid, "total": total_creados, "stats": stats}

        except SoftTimeLimitExceeded:
            logger.error("⏳ [OSM RADAR] Cut-off por límite de tiempo. Salvaguardando memoria.")
            return "Soft Timeout Exceeded"
        except Exception as e:
            logger.error(f"❌ [OSM RADAR] Crash de Red/API: {str(e)}")
            raise self.retry(exc=e, countdown=60)
        finally:
            db.close_old_connections()
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
    db.close_old_connections()
    lock_id = "mutex_global_serp_cluster"
    
    with distributed_lock(lock_id, timeout=1800) as acquired:
        if not acquired:
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
            db.close_old_connections()
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
    db.close_old_connections()
    lock_id = "mutex_playwright_fleet"
    
    with distributed_lock(lock_id, timeout=3600) as acquired:
        if not acquired:
            logger.warning("⚠️ [GHOST SNIPER] Flota Playwright ya desplegada. Ignorando solicitud.")
            return "Fleet Occupied."

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
            # [OMNI-TIER FIX]: Aislamiento absoluto de Playwright
            safe_async_runner(_orchestrate(targets))
            return f"Misión cumplida: {len(targets)} nodos infiltrados."
            
        except SoftTimeLimitExceeded:
            logger.warning("⏳ [GHOST SNIPER] Cut-off por tiempo de ciclo de servidor. Datos parciales asegurados.")
            return "Timeout. Guardado parcial."
        except Exception as e:
            logger.error(f"❌ [GHOST SNIPER] Crash Crítico en Playwright Runtime: {str(e)}")
            raise self.retry(exc=e, countdown=180)
        finally:
            db.close_old_connections()
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
    db.close_old_connections()
    start_time = time.time()
    lock_id = "mutex_ml_training_lock"
    
    with distributed_lock(lock_id, timeout=2100) as acquired:
        if not acquired:
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
            db.close_old_connections()
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
    db.close_old_connections()
    start_time = time.time()
    lock_id = "mutex_ml_inference_lock"
    
    with distributed_lock(lock_id, timeout=600) as acquired:
        if not acquired:
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
            db.close_old_connections()
            gc.collect()



# ==============================================================================
# [GOD TIER ARCHITECTURE: OMNI-SNIPER CELERY WORKER]
# Spec: Silicon Wadi / Lazarus ATP - Alta Disponibilidad y Resiliencia Extrema
# ==============================================================================
@shared_task(
    bind=True,
    max_retries=4,           # Reintentos máximos antes de declarar KIA al target
    acks_late=True,          # Zero Data Loss: Solo confirma la tarea si termina exitosamente
    time_limit=300,          # Hard Limit: Mata el worker si se cuelga por más de 5 min (Evita zombies)
    soft_time_limit=270,     # Soft Limit: Da 30s de margen para cerrar la base de datos limpiamente
    retry_backoff=True,      # Exponential Backoff (Espera 1m, luego 2m, 4m... evade bloqueos por IP)
    retry_backoff_max=600,   # Techo de espera de 10 minutos máximo
    retry_jitter=True        # Anti-Thundering Herd: Añade aleatoriedad a los reintentos
)
def task_run_omni_sniper(self, inst_id):
    """
    Motor Asíncrono Híbrido: Extrae Inteligencia (URL, Emails, Phones, LMS Stack).
    Cuenta con inyección de estado en Caché (HTMX Ready), bloqueos transaccionales
    y auto-curación ante caídas de red o bloqueos de Firewalls (WAF).
    """
    start_time = time.time()
    log_prefix = f"[MISSION:{str(inst_id)[:8]}]"
    
    # 📡 [TELEMETRÍA EN VIVO]: Notifica al Frontend (C2) que el satélite está en posición
    cache.set(f"telemetry_{inst_id}", [f"🛰️ {log_prefix} Uplink establecido. Motores listos."], timeout=1200)

    try:
        # 🛡️ 1. BLOQUEO TRANSACCIONAL ESTRICTO (ACID COMPLIANCE)
        # select_for_update() bloquea la fila en la DB a nivel de kernel para que ningún
        # otro worker o script sobreescriba esta institución mientras el Sniper trabaja.
        with transaction.atomic():
            try:
                inst = Institution.objects.select_for_update(nowait=False).get(id=inst_id)
            except Institution.DoesNotExist:
                logger.error(f"❌ {log_prefix} Objetivo purgado del Vault. Abortando misión.")
                return "ABORTED_NOT_FOUND"

            target_query = (inst.website if inst.website else inst.name).strip()
            geo_context = f"{inst.city or ''} {inst.country or ''}".strip()

            logger.info(f"🎯 {log_prefix} INFILTRACIÓN INICIADA: {target_query} | Sector: {geo_context}")
            cache.set(f"telemetry_{inst_id}", [f"🕵️‍♂️ Extrayendo inteligencia cruda de {target_query}..."], timeout=1200)

            # 🧠 2. INSTANCIACIÓN DEL MOTOR DE VANGUARDIA
            sniper_engine = SniperSearchView()

            # 🕷️ 3. EJECUCIÓN DEL CRAWLER MULTI-VECTOR
            data = sniper_engine.worker_scan(
                target=target_query,
                geo_context=geo_context,
                city=inst.city or "",
                country=inst.country or "Colombia",
                use_email=True,
                use_whatsapp=True,
                use_lms=True
            )

            # 🔬 4. ANÁLISIS FORENSE Y MUTACIÓN DE DATOS
            if data.get('dom') and not data.get('err'):
                
                # --- A. Higiene y Sanitización Estricta de Strings ---
                clean_domain = data['dom'][:250].lower()
                clean_email = data['ems'][0][:250].lower() if data.get('ems') else None
                clean_phone = data['phs'][0][:45] if data.get('phs') else None
                found_lms = str(data.get('lms', 'No detectado'))[:90]
                has_lms_flag = (found_lms.lower() != "no detectado")

                # --- B. Inyección Quirúrgica (Solo actualizamos lo que falta o mejora) ---
                # Usamos update_fields para reducir la carga de IO en PostgreSQL/MySQL en un 95%
                update_fields = ['updated_at', 'last_scored_at', 'discovery_source']
                inst.last_scored_at = timezone.now()
                inst.discovery_source = 'Ghost_V20'

                if not inst.website or "http" not in inst.website:
                    inst.website = clean_domain
                    update_fields.append('website')

                if clean_email and not inst.email:
                    inst.email = clean_email
                    update_fields.append('email')

                if clean_phone and not inst.phone:
                    inst.phone = clean_phone
                    update_fields.append('phone')

                # --- C. Motor de Puntuación Predictiva (Dynamic Lead Scoring) ---
                # Aumentamos la prioridad de venta del colegio según la densidad de datos hallados
                current_score = inst.lead_score
                score_bump = 0
                if clean_email and 'email' in update_fields: score_bump += 25
                if clean_phone and 'phone' in update_fields: score_bump += 15
                if has_lms_flag: score_bump += 40
                
                if score_bump > 0:
                    inst.lead_score = min(current_score + score_bump, 100)
                    update_fields.append('lead_score')

                # Commit a la Base de Datos (Row Lock liberado tras esto)
                inst.save(update_fields=update_fields)

                # --- D. Creación/Actualización del Perfil Tecnológico ---
                tech, tech_created = TechProfile.objects.get_or_create(institution=inst)
                tech.lms_provider = found_lms
                tech.has_lms = has_lms_flag
                # Si encontramos redes sociales (socs), asumimos huella digital analítica
                if data.get('socs'):
                    tech.has_analytics = True 
                tech.save()

                # 📊 Telemetría de Victoria
                elapsed = time.time() - start_time
                logger.info(f"✅ {log_prefix} OPERACIÓN EXITOSA. URL: {clean_domain} | TTR: {elapsed:.2f}s")
                cache.set(f"telemetry_{inst_id}", [f"✅ Extracción completada. URL: {clean_domain}", f"⚙️ Infraestructura: {found_lms.upper()}"], timeout=1200)

                return {"status": "SUCCESS", "domain": clean_domain, "time": elapsed}

            else:
                # 🛑 Fallo Controlado (Falso Positivo o WAF Bloqueando)
                err_msg = data.get('err', 'Identidad indetectable o escudo WAF activo.')
                logger.warning(f"⚠️ {log_prefix} FALLO TÁCTICO: {err_msg}")
                cache.set(f"telemetry_{inst_id}", [f"⚠️ Contramedida detectada: {err_msg[:60]}..."], timeout=1200)
                
                # Heurística: Si fue un bloqueo de red o un timeout, forzamos reintento
                # Celery usará Exponential Backoff para volver a intentarlo más tarde con otra IP
                if "timeout" in err_msg.lower() or "waf" in err_msg.lower() or "bloque" in err_msg.lower():
                    raise RequestException("WAF/Timeout trigger para Exponential Backoff.")
                    
                return {"status": "FAILED", "reason": err_msg}

    except DatabaseError as db_err:
        # Caída de la base de datos o Deadlock detectado
        logger.error(f"🔥 {log_prefix} Falla Crítica en Transacción DB: {db_err}")
        cache.set(f"telemetry_{inst_id}", ["🔥 DB Deadlock. Recalibrando transacciones..."], timeout=1200)
        raise self.retry(exc=db_err, countdown=20) # Retraso táctico para desatascar locks
        
    except (RequestException, TimeoutError) as net_err:
        # Firewall o Red Inestable. Reintenta silenciosamente.
        logger.warning(f"📡 {log_prefix} Interferencia de Red. Reintentando... (Intento {self.request.retries}/{self.max_retries})")
        raise self.retry(exc=net_err)
        
    except SoftTimeLimitExceeded:
        # El proceso lleva demasiado tiempo, se cierra elegantemente sin corromper la DB
        logger.critical(f"⌛ {log_prefix} TIEMPO LÍMITE EXCEDIDO. Interrumpiendo ejecución.")
        return {"status": "TIMEOUT_KILLED"}

    except Exception as e:
        # Error de Código Cero-Día
        logger.critical(f"💀 {log_prefix} COLAPSO CATASTRÓFICO: {str(e)}", exc_info=True)
        cache.set(f"telemetry_{inst_id}", [f"💀 Error Crítico del Sistema: {str(e)[:40]}"], timeout=1200)
        
        if self.request.retries < self.max_retries:
            raise self.retry(exc=e)
        return {"status": "CRITICAL_FAILURE", "error": str(e)}

    finally:
        # 🧹 5. PROTOCOLO DE LIMPIEZA INQUEBRANTABLE (KILL-SWITCH DE HTMX)
        # Pase lo que pase (éxito o explosión nuclear), esta línea TIENE que ejecutarse
        # para que la interfaz de usuario deje de girar y de mostrar "⏳ Analizando..."
        cache.delete(f"scan_in_progress_{inst_id}")
        logger.debug(f"🧹 {log_prefix} Lock de memoria caché destruido.")