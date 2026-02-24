import logging
import re
from typing import List, Dict, Any, Optional, Iterator
from urllib.parse import urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from django.db import transaction
from django.db.utils import IntegrityError
from django.utils import timezone

# Importamos nuestro modelo
from sales.models import Institution

# Configuración de Logging de Alto Rendimiento
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("OSMDiscoveryEngine")

class OSMDiscoveryEngine:
    """
    Motor de Ingestión Top-of-Funnel (Tier God).
    Arquitectura optimizada para memoria (Generators), resiliencia de red (Exponential Backoff),
    y bases de datos masivas (Bulk Upserts con De-duplicación).
    """
    
    OVERPASS_ENDPOINTS = [
        "https://overpass-api.de/api/interpreter",
        "https://lz4.overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter"
    ]

    def __init__(self):
        self.session = self._build_resilient_session()

    def _build_resilient_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=5, 
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
            backoff_factor=2
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "User-Agent": "B2B_Sales_Recon_Engine/3.0 (Enterprise Data Mining)",
            "Accept-Encoding": "gzip, deflate"
        })
        return session

    def _build_query(self, city: str, country: str) -> str:
        return f"""
        [out:json][timeout:300];
        area["name"="{city}"]->.searchArea;
        (
          nwr["amenity"="school"](area.searchArea);
          nwr["amenity"="kindergarten"](area.searchArea);
          nwr["amenity"="university"](area.searchArea);
          nwr["amenity"="college"](area.searchArea);
        );
        out center tags;
        """

    def _fetch_data(self, query: str) -> List[Dict]:
        for endpoint in self.OVERPASS_ENDPOINTS:
            try:
                logger.info(f"📡 Transmitiendo consulta satelital vía {endpoint}...")
                response = self.session.post(endpoint, data={'data': query}, timeout=300)
                response.raise_for_status()
                
                data = response.json()
                elements = data.get("elements", [])
                logger.info(f"✅ Descarga completada: {len(elements)} nodos geoespaciales interceptados.")
                return elements
            
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️ Caída del nodo {endpoint}: {str(e)}. Saltando al siguiente...")
                continue
        
        logger.error("❌ Fallo crítico: Todos los servidores de OSM están inaccesibles.")
        return []

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

            website = tags.get("website") or tags.get("contact:website") or tags.get("url")
            phone = tags.get("phone") or tags.get("contact:phone")
            email = tags.get("email") or tags.get("contact:email")
            
            if website:
                website = website.strip().lower()
                if not website.startswith(("http://", "https://")):
                    website = f"https://{website}"
                if len(website) > 250 or not urlparse(website).netloc:
                    website = None

            street = tags.get("addr:street", "")
            housenumber = tags.get("addr:housenumber", "")
            postcode = tags.get("addr:postcode", "")
            address = f"{street} {housenumber} {postcode}".strip()

            yield Institution(
                name=name.strip(),
                website=website,
                email=email[:254] if email else None,
                phone=phone[:50] if phone else None,
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
        logger.info(f"🚀 INICIANDO INGESTIÓN TOP-OF-FUNNEL: {city.upper()}, {country.upper()}")
        
        query = self._build_query(city, country)
        raw_elements = self._fetch_data(query)
        
        if not raw_elements:
            return

        raw_instances = list(self._normalize_stream(raw_elements, city, country, state))
        
        # 1. DE-DUPLICACIÓN INTRA-BATCH (Anti-Error de Bulk Update)
        unique_instances_map = {}
        for inst in raw_instances:
            # Nuestra llave primaria lógica es: Nombre + Ciudad + País
            key = (inst.name, inst.city, inst.country)
            
            if key not in unique_instances_map:
                unique_instances_map[key] = inst
            else:
                # Si hay dos nodos con el mismo nombre, conservamos el que tenga más datos
                if (inst.website or inst.email) and not (unique_instances_map[key].website or unique_instances_map[key].email):
                    unique_instances_map[key] = inst
                    
        instances = list(unique_instances_map.values())
        total_valid = len(instances)
        
        if total_valid == 0:
            logger.warning("🧹 Ningún registro superó los filtros de calidad.")
            return

        logger.info(f"⚙️ Preparando Bulk Upsert para {total_valid} prospectos ÚNICOS...")

        try:
            with transaction.atomic():
                Institution.objects.bulk_create(
                    instances,
                    update_conflicts=True,
                    unique_fields=['name', 'city', 'country'],
                    update_fields=['website', 'phone', 'email', 'address', 'latitude', 'longitude', 'updated_at']
                )
            logger.info("=" * 50)
            logger.info(f"🏁 MISIÓN COMPLETADA CON ÉXITO")
            logger.info(f"🟢 {total_valid} Prospectos sincronizados al instante.")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.warning(f"⚠️ Bulk Upsert falló ({str(e)}). Cambiando a inyección secuencial de emergencia...")
            self._fallback_sequential_inject(instances)

    def _fallback_sequential_inject(self, instances: List[Institution]):
        """Plan B: Inyección secuencial blindada contra errores de Franquicias (Websites duplicados)."""
        inserted, updated = 0, 0
        
        for inst in instances:
            try:
                # El transaction.atomic() debe ir por registro, para que un fallo no cancele a los demás
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
                # Ignoramos silenciosamente colegios tipo Franquicia que comparten la misma URL
                logger.debug(f"Saltando {inst.name}: URL compartida con otra institución (Franquicia).")
                continue
            except Exception as e:
                logger.debug(f"Error inesperado al guardar {inst.name}: {str(e)}")
                continue
                
        logger.info("=" * 50)
        logger.info(f"🏁 FALLBACK COMPLETADO: {city.upper() if 'city' in locals() else 'CIUDAD'}")
        logger.info(f"🟢 Insertados: {inserted} | 🟡 Actualizados: {updated}")
        logger.info("=" * 50)