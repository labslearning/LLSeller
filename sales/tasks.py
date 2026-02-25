import logging
import asyncio
import requests
import uuid
import re
from typing import Dict, List, Any, Optional

from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from requests.exceptions import RequestException, Timeout
from django.core.cache import cache
from django.db import transaction
from django.utils import timezone

# Importaciones locales optimizadas para alta disponibilidad
from .models import Institution
from .engine.serp_resolver import SERPResolverEngine
from .engine.recon_engine import _orchestrate, execute_recon

# Logger de grado industrial con trazabilidad para CloudWatch/Datadog
logger = logging.getLogger("Sovereign.CeleryWorkers")

# =========================================================
# MISIÓN 0: SINGLE TARGET RECON (MOTOR CON TELEMETRÍA)
# =========================================================
@shared_task(
    bind=True, 
    queue='scraping_queue',
    soft_time_limit=240, 
    time_limit=300,
    name="sales.tasks.task_run_single_recon"
)
def task_run_single_recon(self, inst_id: str):
    """
    Motor quirúrgico de élite para las Ventanas 1 y 2.
    Implementa un log de telemetría asíncrono para actualización de UI vía HTMX.
    """
    def log_telemetry(message: str):
        """Helper para inyectar logs en el flujo de la Sniper Console."""
        current_logs = cache.get(f"telemetry_{inst_id}", [])
        timestamp = timezone.now().strftime('%H:%M:%S')
        current_logs.append(f"{timestamp} | {message}")
        # Mantenemos solo los últimos 10 eventos para optimizar RAM
        cache.set(f"telemetry_{inst_id}", current_logs[-10:], timeout=600)
        logger.info(f"[TELEMETRY][{inst_id}]: {message}")

    log_telemetry("🎯 Objetivo fijado. Iniciando secuencia de aproximación...")
    
    # Patrón: Distributed Lock (Evita colisiones por doble clic del usuario)
    lock_id = f"lock_recon_{inst_id}"
    if not cache.add(lock_id, "processing", 600):
        log_telemetry("⚠️ Misión abortada: El objetivo ya está bajo fuego de otro proceso.")
        return f"Skipped: {inst_id} en proceso."

    try:
        log_telemetry("🌐 Levantando túneles proxy residenciales y rotación de IP...")
        log_telemetry("🕵️‍♂️ Ejecutando Bypass de WAF (Cloudflare/Akamai)...")
        
        # Ejecución del motor forense Ghost Sniper
        execute_recon(inst_id)
        
        log_telemetry("🧠 Extrayendo Tech Stack y analizando patrones con IA...")
        log_telemetry("✅ Inteligencia completada. Sincronizando con el núcleo central.")
        return f"Success: Perfil {inst_id} enriquecido."
        
    except Exception as e:
        error_msg = f"❌ FALLO CRÍTICO: {str(e)}"
        log_telemetry(error_msg)
        logger.error(f"Falla en misión {inst_id}: {str(e)}")
        raise
    finally:
        # IMPORTANTE: Liberamos el semáforo para que el Polling de HTMX detecte el fin
        cache.delete(f"scan_in_progress_{inst_id}")
        cache.delete(lock_id)


# =========================================================
# MISIÓN 1: RADAR OPENSTREETMAP (GEO-DISCOVERY MASIVO)
# =========================================================
@shared_task(
    bind=True, 
    queue='discovery_queue', 
    max_retries=3, 
    default_retry_delay=60,
    autoretry_for=(RequestException, Timeout),
    soft_time_limit=600,
    time_limit=660
)
def task_run_osm_radar(self, country: str, city: str, mission_id: Optional[str] = None):
    """
    Motor de Extracción Geoespacial de alto rendimiento V5.0.
    Utiliza tagging por 'mission_id' para alimentar la Ventana 3 (Geo-Radar).
    Inmune a errores de tildes y mayúsculas mediante Fuzzy Regex.
    """
    batch_uuid = mission_id or str(uuid.uuid4())
    logger.info(f"🛰️ [OSM RADAR] Desplegando sobre {city}, {country} | Misión ID: {batch_uuid}")
    
    # 🧠 Magia de Silicon Valley: Regex Dinámico para Tildes
    # Transforma "Bogota" en "[bB][oO][gG][oOóÓ][tT][aAáÁ]" para engañar a OSM
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
        logger.info("📡 [OSM RADAR] Enviando pulso a la API de Overpass...")
        response = requests.post("https://overpass-api.de/api/interpreter", data={'data': query}, timeout=185)
        response.raise_for_status()
        elements = response.json().get('elements', [])
        
        logger.info(f"✅ [OSM RADAR] API Respondió. Nodos crudos detectados por el satélite: {len(elements)}")
        
        if not elements:
            logger.warning(f"⚠️ [OSM RADAR] OSM no tiene datos para '{city}'. Intenta con el nombre oficial de la región.")
            return f"Cero resultados en {city}."

        institutions_to_create = []
        names_seen = set()
        
        for el in elements:
            tags = el.get('tags', {})
            name = tags.get('name') or tags.get('official_name')
            if not name or name.lower() in names_seen: continue
            
            names_seen.add(name.lower())
            
            institutions_to_create.append(
                Institution(
                    name=name,
                    city=city,
                    country=country,
                    institution_type=tags.get('amenity', 'school'),
                    discovery_source='osm',
                    mission_id=batch_uuid, 
                    is_active=True
                )
            )

        logger.info(f"⏳ [OSM RADAR] Limpiando datos y guardando {len(institutions_to_create)} leads en la Base de Datos...")

        # Inserción Atómica Bulk (O(n) optimizado)
        with transaction.atomic():
            Institution.objects.bulk_create(
                institutions_to_create, 
                ignore_conflicts=True, 
                batch_size=500
            )
        
        logger.info(f"🎯 [OSM RADAR] ÉXITO TOTAL. Misión completada. La tabla web debería actualizarse ahora.")
        return {"mission_id": batch_uuid, "count": len(institutions_to_create)}

    except Exception as e:
        logger.error(f"❌ [OSM RADAR] Falla estructural: {str(e)}")
        raise 


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
    """Resuelve URLs oficiales para prospectos ciegos usando heurística SERP."""
    logger.info(f"🔍 [SERP RESOLVER] Iniciando resolución para {limit} objetivos.")
    try:
        engine = SERPResolverEngine(concurrency_limit=3)
        engine.resolve_missing_urls(limit=limit)
        return f"Resolución completada (Lote de {limit})."
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
    Motor Forense Masivo por Lotes.
    Si se provee 'mission_id', prioriza ese lote específico (Ideal para Ventana 3).
    """
    logger.info(f"🕵️‍♂️ [GHOST SNIPER] Desplegando ataque sobre lote (Misión: {mission_id})")
    
    # Filtro inteligente: prioriza por misión o por falta de escaneo
    query = Institution.objects.filter(website__isnull=False, is_active=True).exclude(website='')
    
    if mission_id:
        query = query.filter(mission_id=mission_id)
    else:
        query = query.filter(last_scored_at__isnull=True)

    qs = query[:limit]
    
    if not qs.exists():
        return "Misión abortada: Inbox Zero."

    targets = [
        {'id': inst.id, 'name': inst.name, 'url': inst.website, 'city': inst.city}
        for inst in qs
    ]

    try:
        # Orquestación Playwright asíncrona (Reutilización de navegador)
        asyncio.run(_orchestrate(targets))
        return f"Misión cumplida: {len(targets)} enriquecidos."
    except SoftTimeLimitExceeded:
        logger.warning("⏳ [GHOST SNIPER] Tiempo límite alcanzado. Lote procesado parcialmente.")
        return "Timeout parcial: Datos guardados hasta el punto de corte."
    except Exception as e:
        logger.error(f"❌ [GHOST SNIPER] Crash en el orquestador: {str(e)}")
        raise