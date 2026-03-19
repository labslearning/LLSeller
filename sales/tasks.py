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

# Celery & Django Imports
from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError

from django.core.cache import cache
from django.db import transaction, DatabaseError
from django import db
from django.utils import timezone
from django.db.models import Q

# =========================================================
# IMPORTACIONES DE VANGUARDIA (GOD TIER)
# =========================================================
from sales.models import Institution, TechProfile, DeepForensicProfile, Interaction, Contact
from sales.engine.serp_resolver import SERPResolverEngine
from sales.engine.recon_engine import execute_recon
from sales.engine.ml_scoring import train_model, score_unrated_leads
from sales.engine.discovery_engine import OSMDiscoveryEngine

# (Motor de Respuesta Inbound - Si lo tienes activo)
from sales.engine.reply_catcher import run_inbound_catcher

logger = logging.getLogger("Sovereign.OmniSniper.Celery")

# =========================================================
# ⚙️ OMNI-TIER CONFIGURATION & TELEMETRY
# =========================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s.%(msecs)03d - [%(levelname)s] [Sovereign-Workers] %(message)s', 
    datefmt='%H:%M:%S'
)

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
        'User-Agent': 'Sovereign-B2B-Intelligence-Engine/2.0 (Enterprise Data Aggregator)'
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
# 🎯 MISIÓN 0: OMNI-SCAN (SINGLE TARGET RECON ENGINE)
# =========================================================
@shared_task(
    bind=True, 
    queue='scraping_queue',
    max_retries=3,
    autoretry_for=(RequestException, HTTPError, Timeout),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True, # Previene el problema del 'Thundering Herd'
    soft_time_limit=300, 
    time_limit=360,
    name="sales.tasks.task_run_single_recon"
)
def task_run_single_recon(self, inst_id: str):
    """
    [El Núcleo del Francotirador]
    Resolución SERP -> Extracción Forense Playwright -> Análisis de Idiomas/LMS/Énfasis -> Memoria.
    """
    db.close_old_connections()
    start_time = time.time()
    lock_id = f"mutex_recon_{inst_id}"

    def log_telemetry(message: str, level: str = "SYS"):
        cache_key = f"telemetry_{inst_id}"
        current_logs = cache.get(cache_key, [])
        timestamp = timezone.now().strftime('%H:%M:%S.%f')[:-3]
        current_logs.append(f"[{timestamp}] [{level}] {message}")
        cache.set(cache_key, current_logs[-8:], timeout=600)
        logger.info(f"[OMNI-SCAN][{inst_id}]: {message}")

    with distributed_lock(lock_id, timeout=360) as acquired:
        if not acquired:
            log_telemetry("Misión interceptada: Objetivo bajo escaneo concurrente.", "WARN")
            return "Locked by another worker"

        try:
            # 1. BLOQUEO TRANSACCIONAL (ACID)
            with transaction.atomic():
                inst = Institution.objects.select_for_update().get(id=inst_id)
                # Pasar a estado LOCKED para que nadie más lo toque
                if inst.processing_status != Institution.ProcessingStatus.SNIPER_LOCKED:
                    inst.processing_status = Institution.ProcessingStatus.SNIPER_LOCKED
                    inst.save(update_fields=['processing_status'])
                    
            log_telemetry(f"⚡ INFILTRACIÓN INICIADA: {inst.name[:25]}", "INIT")
            
            # --- FASE 1: RESOLUCIÓN SERP ---
            if not inst.website:
                log_telemetry("Buscando huella digital en redes SERP (DuckDuckGo)...", "NET")
                engine = SERPResolverEngine()
                
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
                        if found_url: break 
                    except Exception as e:
                        log_telemetry(f"Sobrecarga SERP. Retrying ({attempt}/3)...", "WARN")
                        time.sleep((2 ** attempt) + random.uniform(0, 1)) 
                
                if found_url:
                    inst.website = found_url
                    inst.save(update_fields=['website', 'updated_at'])
                    log_telemetry(f"Enlace establecido: {found_url}", "OK")
                else:
                    log_telemetry("Objetivo fantasma o sin URL. Misión abortada.", "FAIL")
                    inst.processing_status = Institution.ProcessingStatus.DISCARDED
                    inst.save(update_fields=['processing_status'])
                    return "Ghost Target"

            # --- FASE 2: GHOST SNIPER (PLAYWRIGHT + IA DEEPSEEK) ---
            log_telemetry("Bypass de WAF y extracción de LMS, Idiomas y Certificaciones...", "HACK")
            
            # Aquí es donde ocurre la magia real de extracción de datos
            execute_recon(inst_id=str(inst.id))
            
            # Tras extraer la data, el colegio pasa a ENRICHED (Listo para la IA de Ventas)
            inst.refresh_from_db()
            inst.processing_status = Institution.ProcessingStatus.ENRICHED
            inst.save(update_fields=['processing_status'])
            
            elapsed = round(time.time() - start_time, 2)
            log_telemetry(f"MISIÓN CUMPLIDA. Inteligencia asegurada en {elapsed}s", "SUCCESS")
            return f"Omni-Scan Complete: {elapsed}s"
            
        except Institution.DoesNotExist:
            logger.error(f"❌ Falla crítica: ID {inst_id} no existe.")
            return "404 Not Found"
        except SoftTimeLimitExceeded:
            log_telemetry("Cut-off de recursos. Proceso abortado para proteger el nodo.", "TIMEOUT")
            Institution.objects.filter(id=inst_id).update(processing_status=Institution.ProcessingStatus.RAW_RADAR)
            return "Soft Timeout"
        except Exception as e:
            log_telemetry(f"ERROR ESTRUCTURAL: {str(e)[:40]}", "CRITICAL")
            Institution.objects.filter(id=inst_id).update(processing_status=Institution.ProcessingStatus.RAW_RADAR)
            logger.exception(f"OMNI-SCAN Crash Crítico en {inst_id}")
            raise self.retry(exc=e) 
        finally:
            # Destrucción Absoluta de Artefactos de Memoria
            cache.delete(f"scan_in_progress_{inst_id}")
            db.close_old_connections()
            gc.collect() 


# =========================================================
# 🛸 MISIÓN 1: CONTROLADOR DE ENJAMBRE (FLOW CONTROL)
# =========================================================
@shared_task(
    bind=True, 
    name="sales.tasks.task_run_ghost_sniper_fleet"
)
def task_run_ghost_sniper_fleet(self, limit: int = 500, city: str = None, mission_id: str = None):
    """
    [EL CONTROLADOR DEFINITIVO DE FLUJO]
    Responde a la orden: "Dame 500 colegios de Cajicá, y no me des los repetidos".
    Asegura los colegios en memoria, los bloquea y lanza el enjambre de escaneo.
    """
    db.close_old_connections()
    logger.info(f"🚦 [SWARM COMMANDER] Solicitando autorización para {limit} objetivos en {city or 'Global'}...")

    with transaction.atomic():
        # 1. Filtramos estrictamente los colegios VÍRGENES (RAW)
        query = Institution.objects.select_for_update().filter(
            website__isnull=False, 
            is_active=True,
            processing_status=Institution.ProcessingStatus.RAW_RADAR
        )
        if city: 
            query = query.filter(city__icontains=city)
        if mission_id: 
            query = query.filter(mission_id=mission_id)
        
        # 2. Limitamos a la cantidad solicitada (Ej: 500)
        query = query.order_by('created_at')[:limit]
        target_ids = list(query.values_list('id', flat=True))

        if not target_ids:
            logger.info(f"✅ [SWARM COMMANDER] Base de datos limpia en {city}. No hay colegios crudos pendientes.")
            return f"Inbox Zero para {city}."

        # 3. [CANDADO MÁGICO]: Cambiamos el estado a LOCKED.
        # Ningún otro worker ni tú al darle "click" volverá a tocar estos colegios.
        Institution.objects.filter(id__in=target_ids).update(
            processing_status=Institution.ProcessingStatus.SNIPER_LOCKED,
            updated_at=timezone.now()
        )

    logger.info(f"🔥 [SWARM COMMANDER] {len(target_ids)} blancos BLOQUEADOS. Desatando el Infierno asíncrono...")

    # 4. Desplegamos el enjambre de tareas en paralelo
    for t_id in target_ids:
        task_run_single_recon.delay(str(t_id))

    return f"Flota desplegada: {len(target_ids)} drones en el aire."


# =========================================================
# 🛰️ MISIÓN 2: RADAR OPENSTREETMAP (DATA INGESTION)
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
    Extracción Geoespacial. Llena la base de datos de manera bruta y lanza a los Snipers.
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
            total_creados = safe_async_runner(engine.run_radar(location_type='city', location_name=city))
            
            if mission_id and total_creados > 0:
                Institution.objects.filter(city__iexact=city, mission_id__isnull=True).update(mission_id=batch_uuid)

            logger.info(f"🎯 [OSM RADAR] ÉXITO en {city}. Total inyectados en estado RAW: {total_creados}.")

            # [REACCIÓN EN CADENA] Una vez descubiertos, manda a los Snipers a atacarlos
            if total_creados > 0:
                logger.info(f"🤖 [SMART ROUTE] Despertando Flota Sniper para enriquecer {city}...")
                task_run_ghost_sniper_fleet.apply_async(kwargs={'limit': min(total_creados, 500), 'city': city}, countdown=10)

            return {"mission_id": batch_uuid, "total": total_creados}

        except SoftTimeLimitExceeded:
            return "Soft Timeout Exceeded"
        except Exception as e:
            raise self.retry(exc=e, countdown=60)
        finally:
            db.close_old_connections()
            gc.collect()


# =========================================================
# 🔍 MISIÓN 3: RESOLUCIÓN DE URLs (SERP CLUSTER)
# =========================================================
@shared_task(
    bind=True,
    name="sales.tasks.task_run_serp_resolver",
    queue='discovery_queue', # Usamos la cola de descubrimiento, no la default
    soft_time_limit=120,     # 2 minutos para avisar (Soft)
    time_limit=150,          # 2.5 minutos para matar (Hard)
    acks_late=True,          # Si el worker muere, la tarea vuelve a la cola
    max_retries=3            # Si falla por red, reintentamos
)
def task_run_serp_resolver(self, limit: int = 50):
    """Cluster autónomo de resolución. Limitado con Mutex para no banear IPs locales."""
    db.close_old_connections()
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
        finally:
            db.close_old_connections()
            gc.collect()


# =========================================================
# 🤖 MISIÓN 4: AUTONOMOUS AI OUTREACH (LA IA S.D.R.)
# =========================================================
@shared_task(
    bind=True,
    name="sales.tasks.task_autonomous_ai_outreach"
)
def task_autonomous_ai_outreach(self, limit: int = 50, city: str = None):
    """
    [LA JOYA DE LA CORONA]
    Esta tarea lee los colegios ENRICHED (ya escaneados), evalúa su Moodle,
    su nivel Bilingüe, y les envía un correo hiper-personalizado ofreciendo Learning Labs.
    Genera la memoria de contexto para hacer seguimiento continuo.
    """
    db.close_old_connections()
    logger.info(f"🧠 [AI SDR] Iniciando campaña de contacto táctico. Límite: {limit}")

    # Buscamos colegios listos para atacar (No contactados, Enriquecidos, Buen Score)
    targets = Institution.objects.select_related('tech_profile', 'forensic_profile').filter(
        processing_status=Institution.ProcessingStatus.ENRICHED,
        contacted=False,
        email__isnull=False,
        lead_score__gte=50 # Solo atacamos a colegios que valgan la pena
    )
    
    if city: targets = targets.filter(city__icontains=city)
    targets = targets[:limit]

    if not targets:
        logger.info("⏸️ [AI SDR] No hay prospectos con perfil apto para contacto hoy.")
        return "Cero Targets aptos para disparo."

    import os
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    success_count = 0
    for inst in targets:
        try:
            # 1. Recuperar Contexto del Target
            tech = getattr(inst, 'tech_profile', None)
            forensic = getattr(inst, 'forensic_profile', None)
            
            lms_actual = tech.lms_provider if tech and tech.lms_provider else "una plataforma estándar"
            enfasis = forensic.pedagogical_emphasis if forensic and forensic.pedagogical_emphasis else "educativo"
            es_bilingue = forensic.is_bilingual if forensic else False

            # 2. Ingeniería de Prompts (Red Teaming Cognitivo)
            prompt = f"""
            Eres el Director Comercial de 'Learning Labs', la plataforma LMS más rápida y nativa del mercado.
            Analiza este prospecto: Colegio '{inst.name}' en '{inst.city}'.
            - Enfoque Pedagógico detectado: {enfasis}.
            - LMS actual: {lms_actual}.
            - ¿Es Bilingüe?: {'Sí' if es_bilingue else 'No'}.

            Redacta un cold email (max 4 líneas) directo al Rector. 
            No uses saludos formales aburridos.
            Si usan Moodle, diles que Learning Labs no requiere servidores complicados.
            Si son bilingües, menciona nuestro soporte nativo en inglés.
            Termina con un Call to Action preguntando si tienen 10 minutos este martes.
            """

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7
            )
            email_body = response.choices[0].message.content

            # 3. Marcar como Contactado e Iniciar el Hilo de Memoria
            with transaction.atomic():
                inst.contacted = True
                inst.save(update_fields=['contacted', 'updated_at'])
                
                # Creamos la Interacción de Memoria (Thread)
                Interaction.objects.create(
                    institution=inst,
                    channel='EMAIL',
                    status='SENT',
                    subject=f"Potenciando el enfoque {enfasis} en {inst.name}",
                    message_sent=email_body,
                    thread_id=f"thread_{inst.id}",
                    next_action_date=timezone.now() + timezone.timedelta(days=3) # Follow-up automático en 3 días
                )
            
            logger.info(f"📨 Misil enviado a {inst.email} ({inst.name})")
            success_count += 1
            time.sleep(1) # Delay táctico para no saturar SMTP

        except Exception as e:
            logger.error(f"❌ Fallo al contactar {inst.name}: {e}")

    return f"Campaña completada. {success_count} colegios contactados con IA."


# =========================================================
# 🧠 MISIÓN 5: PREDICTIVE ML SCORING
# =========================================================
@shared_task(bind=True, soft_time_limit=1800, time_limit=1860)
def task_retrain_ai_model(self):
    """Reentrenamiento de la matriz de Bosques Aleatorios."""
    db.close_old_connections()
    with distributed_lock("mutex_ml_training_lock", timeout=2100) as acquired:
        if not acquired: return "Locked."
        try:
            success = train_model()
            return "Model retrained." if success else "Insufficient data."
        except Exception as e:
            raise self.retry(exc=e)
        finally:
            gc.collect()

@shared_task(bind=True, soft_time_limit=600, time_limit=660)
def task_batch_score_leads(self, limit: int = 2000):
    """Inferencia Masiva de Score de Ventas."""
    db.close_old_connections()
    with distributed_lock("mutex_ml_inference_lock", timeout=600) as acquired:
        if not acquired: return "Locked."
        try:
            score_unrated_leads(limit=limit)
            return "Inferencia complete."
        except Exception as e:
            raise self.retry(exc=e)
        finally:
            gc.collect()