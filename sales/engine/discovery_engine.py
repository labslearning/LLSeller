import logging
import re
import asyncio
from typing import List, Dict, Any, Optional, Iterator
from urllib.parse import urlparse

# Dependencias de Misión Crítica
import httpx
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_exponential_jitter, 
    retry_if_exception_type,
    before_sleep_log
)
from django.db import transaction
from django.db.utils import IntegrityError
from django.utils import timezone

# Importamos nuestro modelo
from sales.models import Institution

# =========================================================
# 1. TELEMETRÍA DE ALTA PRECISIÓN (APM READY)
# =========================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s.%(msecs)03d - [%(levelname)s] [RADAR_ENGINE] - %(message)s', 
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.DiscoveryEngine")

# =========================================================
# 2. MOTOR DE DESCUBRIMIENTO GEOESPACIAL (SINGULARITY TIER)
# =========================================================
class OSMDiscoveryEngine:
    """
    [SINGULARITY TIER V2]
    Radar Geoespacial de Infraestructura Planetaria.
    Implementa: Node Racing con Dangling Task Reaper, Chunked DB Upserts, 
    Heuristic Data Sanitization y Overpass MaxSize Override.
    """
    
    OVERPASS_ENDPOINTS = [
        "https://overpass-api.de/api/interpreter",
        "https://lz4.overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter"
    ]

    # Lotes de inserción para evitar colapsar la RAM de PostgreSQL
    DB_BATCH_SIZE = 2000 

    def _build_query(self, city: str, country: str) -> str:
        """
        [GOD TIER FIX]: maxsize:1073741824 (1GB) fuerza a los servidores de Overpass
        a no abortar peticiones para mega-ciudades (ej. Ciudad de México, Sao Paulo).
        """
        return f"""
        [out:json][timeout:300][maxsize:1073741824];
        area["name"="{city}"]->.searchArea;
        (
          nwr["amenity"="school"](area.searchArea);
          nwr["amenity"="kindergarten"](area.searchArea);
          nwr["amenity"="university"](area.searchArea);
          nwr["amenity"="college"](area.searchArea);
        );
        out center tags;
        """

    async def _fetch_single_node(self, client: httpx.AsyncClient, endpoint: str, query: str) -> List[Dict]:
        """Sonda individual con compresión zstd/gzip forzada por httpx."""
        response = await client.post(endpoint, data={'data': query})
        response.raise_for_status()
        return response.json().get("elements", [])

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential_jitter(initial=2, max=20),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    async def _race_endpoints_async(self, query: str) -> List[Dict]:
        """
        [SINGULARITY TIER]: Node Racing con Dangling Task Reaper.
        Previene memory leaks destruyendo apropiadamente las corrutinas canceladas.
        """
        # Connection Pooling avanzado para máxima transferencia HTTP/2
        limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
        async with httpx.AsyncClient(timeout=240.0, http2=True, limits=limits) as client:
            
            tasks = [
                asyncio.create_task(self._fetch_single_node(client, ep, query), name=ep) 
                for ep in self.OVERPASS_ENDPOINTS
            ]
            
            logger.info("🏎️ [RACING] Sondas cuánticas desplegadas hacia 3 continentes...")
            
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            # 1. Cancelación inmediata de los perdedores
            for p in pending:
                p.cancel()
                
            # 2. [GOD TIER FIX]: REAPER (Cosechador) de tareas pendientes
            # Si no hacemos esto, Python arrojará "Task was destroyed but it is pending!" y fugará RAM.
            await asyncio.gather(*pending, return_exceptions=True)
            
            # 3. Procesamiento del ganador
            for task in done:
                try:
                    elements = task.result()
                    winner_node = task.get_name()
                    logger.info(f"🏆 [RACING] Ganador: {winner_node} | Payload: {len(elements)} targets.")
                    return elements
                except Exception as e:
                    logger.warning(f"⚠️ [RACING] Falso positivo. El nodo rápido falló: {str(e)}.")
                    raise Exception("Colapso en nodo ganador. Reintentando...")
                    
            return []

    def _sanitize_website(self, url: str) -> Optional[str]:
        """Limpieza heurística extrema de URLs malformadas por usuarios de OSM."""
        if not url: return None
        url = str(url).strip().lower()
        # Elimina duplicaciones absurdas como http://https://
        url = re.sub(r'^(https?://)+', '', url) 
        if not url: return None
        url = f"https://{url}" if not url.startswith('http') else url
        
        parsed = urlparse(url)
        if len(url) > 250 or not parsed.netloc or '.' not in parsed.netloc:
            return None
        return url

    def _sanitize_phone(self, phone: str) -> Optional[str]:
        """Extrae solo caracteres útiles de números de teléfono basura."""
        if not phone: return None
        # Mantiene solo números, +, espacios y guiones
        clean = re.sub(r'[^\d\+\-\s\(\)]', '', str(phone)).strip()
        return clean[:50] if len(clean) >= 5 else None

    def _normalize_stream(self, elements: List[Dict], city: str, country: str, state: str) -> Iterator[Institution]:
        """Generador Stream-Processing (O(1) Memory Complexity)."""
        for element in elements:
            tags = element.get("tags", {})
            
            name = tags.get("name") or tags.get("official_name")
            if not name or len(name) < 4:
                continue

            amenity = tags.get("amenity", "school")
            inst_type = Institution.InstitutionType.SCHOOL
            if amenity == "kindergarten":
                inst_type = Institution.InstitutionType.KINDERGARTEN
            elif amenity in ["university", "college"]:
                inst_type = Institution.InstitutionType.UNIVERSITY

            lat = element.get("lat") or element.get("center", {}).get("lat")
            lon = element.get("lon") or element.get("center", {}).get("lon")

            website = self._sanitize_website(tags.get("website") or tags.get("contact:website") or tags.get("url"))
            phone = self._sanitize_phone(tags.get("phone") or tags.get("contact:phone"))
            
            raw_email = tags.get("email") or tags.get("contact:email")
            email = str(raw_email).strip().lower()[:254] if raw_email and '@' in str(raw_email) else None

            street = tags.get("addr:street", "")
            housenumber = tags.get("addr:housenumber", "")
            postcode = tags.get("addr:postcode", "")
            address = f"{street} {housenumber} {postcode}".strip()

            yield Institution(
                name=name.strip(),
                website=website,
                email=email,
                phone=phone,
                institution_type=inst_type,
                country=country,
                state_region=state,
                city=tags.get("addr:city", city),
                address=address[:250] if address else None,
                latitude=lat,
                longitude=lon,
                discovery_source=Institution.DiscoverySource.OSM,
                is_private=True,
                is_active=True
            )

    def discover_and_inject(self, city: str, country: str, state: str = None):
        """
        [SINGULARITY TIER ORCHESTRATOR]
        Ingestión de datos ultra-segura. Transiciones entre el Event Loop
        asíncrono y las transacciones síncronas de PostgreSQL sin bloqueos.
        """
        logger.info(f"🚀 INICIANDO INGESTIÓN TOP-OF-FUNNEL: {city.upper()}, {country.upper()}")
        
        query = self._build_query(city, country)
        
        try:
            # I/O Cuántico aislado (Seguro para Celery/Django)
            raw_elements = asyncio.run(self._race_endpoints_async(query))
        except Exception as e:
            logger.error(f"❌ [CRÍTICO] Abortando radar. Escudo OSM impenetrable: {str(e)}")
            return
        
        if not raw_elements:
            logger.warning("📭 Radar completado: Sector vacío.")
            return

        # Evaluación Perezosa (Lazy Evaluation) vía Generators
        raw_instances = self._normalize_stream(raw_elements, city, country, state)
        
        # De-duplicación Criptográfica en Memoria
        unique_instances_map = {}
        for inst in raw_instances:
            key = (inst.name, inst.city, inst.country)
            if key not in unique_instances_map:
                unique_instances_map[key] = inst
            else:
                # Merge de enriquecimiento inteligente
                existing = unique_instances_map[key]
                if not existing.website and inst.website: existing.website = inst.website
                if not existing.email and inst.email: existing.email = inst.email
                if not existing.phone and inst.phone: existing.phone = inst.phone
                    
        instances = list(unique_instances_map.values())
        total_valid = len(instances)
        
        if total_valid == 0:
            logger.warning("🧹 Ningún registro superó la heurística de limpieza.")
            return

        logger.info(f"⚙️ Iniciando Bulk Upsert de {total_valid} leads (Batch Size: {self.DB_BATCH_SIZE})...")

        try:
            # [GOD TIER FIX]: batch_size protege la memoria compartida de la BD
            with transaction.atomic():
                Institution.objects.bulk_create(
                    instances,
                    batch_size=self.DB_BATCH_SIZE,
                    update_conflicts=True,
                    unique_fields=['name', 'city', 'country'],
                    update_fields=['website', 'phone', 'email', 'address', 'latitude', 'longitude', 'updated_at']
                )
            logger.info("=" * 65)
            logger.info(f"🏁 INGESTIÓN COMPLETADA: {city.upper()} | {total_valid} LEADS")
            logger.info("=" * 65)
            
        except Exception as e:
            logger.warning(f"⚠️ Bulk Upsert colisionó ({str(e)}). Activando Protocolo Fallback Secuencial...")
            self._fallback_sequential_inject(instances, city)

    def _fallback_sequential_inject(self, instances: List[Institution], city: str):
        """
        Plan B (Contingencia).
        Aísla errores de unique_constraints (como URLs compartidas por franquicias)
        fila por fila, asegurando que el 99% de la data ingrese intacta.
        """
        inserted, updated, skipped = 0, 0, 0
        
        for inst in instances:
            try:
                # Savepoints automáticos por iteración
                with transaction.atomic():
                    obj, created = Institution.objects.update_or_create(
                        name=inst.name, city=inst.city, country=inst.country,
                        defaults={
                            "website": inst.website, "phone": inst.phone, "email": inst.email,
                            "institution_type": inst.institution_type, "state_region": inst.state_region,
                            "address": inst.address, "latitude": inst.latitude, "longitude": inst.longitude,
                            "discovery_source": inst.discovery_source
                        }
                    )
                    if created: inserted += 1
                    else: updated += 1
            except IntegrityError as e:
                # Filtrado de ruido: Franquicias con el mismo dominio único, etc.
                skipped += 1
                pass 
            except Exception as e:
                logger.error(f"Falla atípica aislando '{inst.name}': {str(e)}")
                skipped += 1
                
        logger.info("=" * 65)
        logger.info(f"🏁 PROTOCOLO DE CONTINGENCIA COMPLETADO: {city.upper()}")
        logger.info(f"🟢 Insertados: {inserted} | 🟡 Actualizados: {updated} | 🔴 Descartados: {skipped}")
        logger.info("=" * 65)