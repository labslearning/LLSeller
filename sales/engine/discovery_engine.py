import logging
import re
import asyncio
import hashlib
import random
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
# 2. MOTOR DE DESCUBRIMIENTO GEOESPACIAL (GOD TIER V10)
# =========================================================
class OSMDiscoveryEngine:
    """
    [GOD TIER V10 - TRUE SWARM INTELLIGENCE]
    Radar Geoespacial Inmune a Nodos Suicidas.
    Implementa Búsqueda Radial Ultraligera y Autocuración de Enjambre.
    """
    
    # 🔥 Servidores saneados: Francia fue purgado por corrupción de DB.
    OVERPASS_ENDPOINTS = [
        "https://overpass-api.de/api/interpreter",          # Alemania (Principal)
        "https://lz4.overpass-api.de/api/interpreter",      # Alemania (Backup de alta velocidad)
        "https://overpass.kumi.systems/api/interpreter",    # Servidor Privado Kumi (Alta Fiabilidad)
        "https://overpass.openstreetmap.ru/cgi/interpreter" # Rusia (Backup inercial)
    ]

    DB_BATCH_SIZE = 2000 

    @staticmethod
    def _get_stealth_headers() -> Dict[str, str]:
        """Falsificación de identidades para evadir firewalls."""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ]
        return {
            "User-Agent": random.choice(user_agents),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-CO,es;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Connection": "keep-alive",
            "Referer": "https://www.openstreetmap.org/"
        }

    def _build_query(self, city: str, country: str) -> str:
        """
        [THE RADIAL QUERY]: Búsqueda Radial de Alta Velocidad.
        Evita los '504 Timeouts' y los 'open64 file errors' de OSM.
        """
        city_clean = city.strip()
        country_clean = country.strip().title()

        # Regex dinámico para ignorar tildes completamente
        replacements = {
            'a': '[aáAÁ]', 'á': '[aáAÁ]', 
            'e': '[eéEÉ]', 'é': '[eéEÉ]', 
            'i': '[iíIÍ]', 'í': '[iíIÍ]', 
            'o': '[oóOÓ]', 'ó': '[oóOÓ]', 
            'u': '[uúUÚ]', 'ú': '[uúUÚ]'
        }
        city_regex = "".join(replacements.get(c, c) for c in city_clean.lower())

        return f"""
        [out:json][timeout:200];
        
        // 1. ÁREA DEL PAÍS (Para delimitar la búsqueda)
        area["name"="{country_clean}"]["admin_level"="2"]->.country;
        
        // 2. NODO CENTRAL DE LA CIUDAD (Ultra ligero, no rompe la base de datos)
        node["place"~"city|town|village|municipality"]["name"~"{city_regex}", i](area.country)->.cityNode;

        // 3. BÚSQUEDA RADIAL Y TEXTUAL SIMULTÁNEA
        (
          nwr["amenity"~"school|kindergarten|university|college"](around.cityNode:20000);
          nwr["amenity"~"school|kindergarten|university|college"]["addr:city"~"{city_regex}", i](area.country);
        );
        
        out center tags;
        """

    async def _fetch_single_node(self, client: httpx.AsyncClient, endpoint: str, query: str) -> tuple:
        """Sonda individual. Devuelve una tupla (endpoint, elements) para identificar al ganador."""
        try:
            response = await client.post(endpoint, data={'data': query})
            response.raise_for_status()
            
            data = response.json()
            
            # Crash protection contra el error interno de bases de datos corruptas
            if "remark" in data and "runtime error" in data["remark"].lower():
                raise Exception(f"Overpass DB Crash: {data['remark']}")
                
            return endpoint, data.get("elements", [])
        except Exception as e:
            # Empaquetamos el error para saber qué nodo falló
            raise Exception(f"{str(e)}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=3, max=25),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    async def _race_endpoints_async(self, query: str) -> List[Dict]:
        """
        [TRUE SWARM LOGIC]: Tolerancia a fallos absoluta usando as_completed.
        """
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=40)
        timeout = httpx.Timeout(180.0, connect=15.0) 
        
        async with httpx.AsyncClient(timeout=timeout, http2=True, limits=limits, headers=self._get_stealth_headers()) as client:
            tasks = [
                asyncio.create_task(self._fetch_single_node(client, ep, query)) 
                for ep in self.OVERPASS_ENDPOINTS
            ]
            
            logger.info(f"🏎️ [SWARM] Desplegando enjambre hacia {len(self.OVERPASS_ENDPOINTS)} satélites mundiales...")
            
            # as_completed entrega las tareas a medida que van terminando (exitosas o fallidas)
            for coro in asyncio.as_completed(tasks):
                try:
                    winner_node, elements = await coro
                    
                    # ¡Tenemos el PRIMER ganador SANO! Destruimos el resto de las tareas para liberar memoria y red.
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                        
                    logger.info(f"🏆 [SWARM] Satélite exitoso: {winner_node} | Carga Útil: {len(elements)} Leads.")
                    return elements
                    
                except Exception as e:
                    # Este nodo falló. Lo reportamos y el bucle sigue esperando al siguiente nodo rápido.
                    logger.warning(f"⚠️ [SWARM] Nodo ignorado por corrupción o timeout: {str(e)}")
                    continue
            
            # Si el bucle termina y nadie retornó data, significa que todos fallaron.
            raise Exception("Todos los satélites del enjambre fallaron simultáneamente. Reiniciando...")

    def _sanitize_website(self, url: str) -> Optional[str]:
        if not url: return None
        url = str(url).strip().lower()
        url = re.sub(r'^(https?://)+', '', url) 
        if not url or len(url) < 4: return None
        url = f"https://{url}" if not url.startswith('http') else url
        
        try:
            parsed = urlparse(url)
            if len(url) > 250 or not parsed.netloc or '.' not in parsed.netloc:
                return None
            return url
        except Exception:
            return None

    def _sanitize_phone(self, phone: str) -> Optional[str]:
        if not phone: return None
        clean = re.sub(r'[^\d\+\-\s\(\)]', '', str(phone)).strip()
        if len(re.sub(r'\D', '', clean)) < 6:
            return None
        return clean[:50]

    def _generate_fingerprint(self, name: str, city: str, country: str) -> str:
        """Hashing ultra rápido en RAM para deduplicación (O(1))."""
        raw_string = f"{name.strip().lower()}|{city.strip().lower()}|{country.strip().lower()}"
        return hashlib.sha256(raw_string.encode('utf-8')).hexdigest()

    def _normalize_stream(self, elements: List[Dict], city: str, country: str, state: str) -> Iterator[Institution]:
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
                city=tags.get("addr:city", city), # Asignación de ciudad forzada al ancla
                address=address[:250] if address else None,
                latitude=lat,
                longitude=lon,
                discovery_source=Institution.DiscoverySource.OSM,
                is_private=True,
                is_active=True
            )

    def discover_and_inject(self, city: str, country: str, state: str = None):
        logger.info(f"🚀 INICIANDO INGESTIÓN TOP-OF-FUNNEL: {city.upper()}, {country.upper()}")
        
        query = self._build_query(city, country)
        
        try:
            raw_elements = asyncio.run(self._race_endpoints_async(query))
        except Exception as e:
            logger.error(f"❌ [CRÍTICO] Colapso total del Escudo OSM tras reintentos: {str(e)}")
            return
        
        if not raw_elements:
            logger.warning(f"📭 Escaneo Vectorial completado. No se detectaron instituciones en el radar para {city}.")
            return

        raw_instances = self._normalize_stream(raw_elements, city, country, state)
        
        unique_instances_map = {}
        for inst in raw_instances:
            fingerprint = self._generate_fingerprint(inst.name, inst.city, inst.country)
            
            if fingerprint not in unique_instances_map:
                unique_instances_map[fingerprint] = inst
            else:
                existing = unique_instances_map[fingerprint]
                if not existing.website and inst.website: existing.website = inst.website
                if not existing.email and inst.email: existing.email = inst.email
                if not existing.phone and inst.phone: existing.phone = inst.phone
                    
        instances = list(unique_instances_map.values())
        total_valid = len(instances)
        
        if total_valid == 0:
            logger.warning("🧹 Intersección estéril: Todos los registros fueron descartados.")
            return

        logger.info(f"⚙️ Abriendo compuertas transaccionales. Volcando {total_valid} Leads a la BD...")

        try:
            with transaction.atomic():
                Institution.objects.bulk_create(
                    instances,
                    batch_size=self.DB_BATCH_SIZE,
                    update_conflicts=True,
                    unique_fields=['name', 'city', 'country'],
                    update_fields=['website', 'phone', 'email', 'address', 'latitude', 'longitude', 'updated_at']
                )
            logger.info("=" * 70)
            logger.info(f"🏁 INGESTIÓN COMPLETADA CON ÉXITO: {city.upper()} | {total_valid} LEADS ASEGURADOS")
            logger.info("=" * 70)
            
        except Exception as e:
            logger.warning(f"⚠️ Caída del UPSERT Masivo ({str(e)}). Activando Protocolo Fallback Secuencial...")
            self._fallback_sequential_inject(instances, city)

    def _fallback_sequential_inject(self, instances: List[Institution], city: str):
        inserted, updated, skipped = 0, 0, 0
        
        for inst in instances:
            try:
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
            except IntegrityError:
                skipped += 1
                pass 
            except Exception as e:
                logger.error(f"Falla atípica aislando al objetivo '{inst.name}': {str(e)}")
                skipped += 1
                
        logger.info("=" * 70)
        logger.info(f"🏁 PROTOCOLO DE CONTINGENCIA COMPLETADO: {city.upper()}")
        logger.info(f"🟢 Nuevos: {inserted} | 🟡 Actualizados: {updated} | 🔴 Descartados: {skipped}")
        logger.info("=" * 70)