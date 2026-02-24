import logging
import asyncio
import requests
import time
from typing import Dict, List, Any
from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from requests.exceptions import RequestException, Timeout

from .models import Institution
from .engine.serp_resolver import SERPResolverEngine
from .engine.recon_engine import _orchestrate

logger = logging.getLogger("CeleryEnterpriseWorkers")

# =========================================================
# MISIÓN 1: RADAR OPENSTREETMAP (DESCUBRIMIENTO MASIVO)
# =========================================================
@shared_task(
    bind=True, 
    queue='discovery_queue', 
    max_retries=3, 
    default_retry_delay=30, # Backoff base
    autoretry_for=(RequestException, Timeout), # Auto-sanación de red
    soft_time_limit=300, # 5 minutos max para evitar zombies
    time_limit=330
)
def task_run_osm_radar(self, country: str, city: str):
    """
    Motor de Extracción Geoespacial O(1) Database Memory.
    Utiliza inserciones en masa (Bulk Creates) para procesar miles de leads en milisegundos.
    """
    logger.info(f"🛰️ [OSM RADAR] Iniciando barrido satelital sobre: {city}, {country}")
    
    # Query optimizada para la API de Overpass
    query = f"""
    [out:json][timeout:90];
    area["name"="{city}"]->.searchArea;
    (
      node["amenity"="school"](area.searchArea);
      way["amenity"="school"](area.searchArea);
      relation["amenity"="school"](area.searchArea);
      node["amenity"="kindergarten"](area.searchArea);
      way["amenity"="kindergarten"](area.searchArea);
    );
    out center;
    """
    
    try:
        response = requests.post("http://overpass-api.de/api/interpreter", data={'data': query}, timeout=95)
        response.raise_for_status()
        elements = response.json().get('elements', [])
        
        if not elements:
            logger.warning(f"⚠️ [OSM RADAR] No se encontraron resultados para {city}.")
            return f"Cero resultados en {city}."

        institutions_to_create = []
        names_seen_in_batch = set()
        
        for el in elements:
            name = el.get('tags', {}).get('name')
            if not name or name.lower() in names_seen_in_batch: 
                continue
                
            names_seen_in_batch.add(name.lower())
            
            amenity = el.get('tags', {}).get('amenity', '')
            inst_type = 'kindergarten' if amenity == 'kindergarten' else 'school'

            institutions_to_create.append(
                Institution(
                    name=name,
                    city=city,
                    country=country,
                    institution_type=inst_type,
                    discovery_source='osm',
                    is_active=True
                )
            )

        # INYECCIÓN ATÓMICA EN POSTGRESQL/SQLITE (Bulk Insert)
        Institution.objects.bulk_create(
            institutions_to_create, 
            ignore_conflicts=True, 
            batch_size=1000
        )
        
        logger.info(f"✅ [OSM RADAR] Barrido exitoso. Lote de {len(institutions_to_create)} procesado.")
        return f"Éxito: Radar completó procesamiento de {len(institutions_to_create)} leads en {city}."

    except SoftTimeLimitExceeded:
        logger.error(f"⏳ [OSM RADAR] Timeout interno. La API de Overpass tardó demasiado.")
        raise
    except Exception as e:
        logger.error(f"❌ [OSM RADAR] Falla crítica: {str(e)}")
        raise 


# =========================================================
# MISIÓN 2: RESOLUCIÓN DE URLs (MOTOR SERP)
# =========================================================
@shared_task(
    bind=True, 
    queue='default',
    soft_time_limit=600, # 10 Minutos para evitar colgar el worker en proxies bloqueados
    time_limit=630
)
def task_run_serp_resolver(self, limit: int = 50):
    """Busca heurísticamente en Google/Bing la URL oficial de prospectos ciegos."""
    logger.info(f"🔍 [SERP RESOLVER] Iniciando resolución de {limit} prospectos ciegos.")
    try:
        engine = SERPResolverEngine(concurrency_limit=3)
        engine.resolve_missing_urls(limit=limit)
        return f"Resolución completada (Lote de {limit})."
        
    except SoftTimeLimitExceeded:
        logger.error(f"⏳ [SERP RESOLVER] Tarea interrumpida por TimeLimit.")
        return "Fallo: Soft Time Limit Exceeded"
    except Exception as e:
        logger.error(f"❌ [SERP RESOLVER] Falla estructural: {str(e)}")
        raise


# =========================================================
# MISIÓN 3: ENRIQUECIMIENTO PROFUNDO (THE GHOST SNIPER)
# =========================================================
@shared_task(
    bind=True, 
    queue='scraping_queue',
    soft_time_limit=1800, # 30 minutos máximos por lote
    time_limit=1830
)
def task_run_ghost_sniper(self, limit: int = 30):
    """
    Motor Forense Avanzado.
    Arquitectura Optimizada: Empaca el lote completo y se lo envía al Orquestador 
    Asíncrono de Playwright para reutilizar un único navegador.
    """
    logger.info(f"🕵️‍♂️ [GHOST SNIPER] Enganchando {limit} objetivos de alto valor...")
    
    qs = Institution.objects.filter(
        website__isnull=False, 
        last_scored_at__isnull=True, 
        is_active=True
    ).exclude(website='')[:limit]
    
    if not qs.exists():
        logger.info("🏆 [GHOST SNIPER] Inbox Zero: No hay objetivos pendientes de escaneo.")
        return "Inbox Zero: Sin objetivos."

    targets: List[Dict[str, Any]] = []
    for inst in qs:
        targets.append({
            'id': inst.id,
            'name': inst.name,
            'url': inst.website,
            'city': inst.city or "Global"
        })

    logger.info(f"🚀 [GHOST SNIPER] Inyectando {len(targets)} leads en el Motor Asíncrono...")
    
    try:
        # Abre Playwright UNA sola vez, procesa la lista entera y se apaga de forma segura.
        asyncio.run(_orchestrate(targets))
        
        logger.info(f"✅ [GHOST SNIPER] Misión completada para lote de {len(targets)}.")
        return f"Sniper finalizado: Lote de {len(targets)} procesado."
        
    except SoftTimeLimitExceeded:
        logger.error(f"⏳ [GHOST SNIPER] Aborto por Soft Timeout de Celery. Liberando memoria RAM...")
        raise
    except Exception as e:
        logger.error(f"❌ [GHOST SNIPER] Crash del motor durante el lote: {str(e)}")
        raise
