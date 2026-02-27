import logging
import asyncio
import random
import time
import re
import unicodedata
from urllib.parse import urlparse
from typing import List, Optional, Tuple, Set, Dict, Any

# Dependencias Nivel Omni-Singularity
import httpx
from duckduckgo_search import DDGS
from duckduckgo_search.exceptions import RatelimitException
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_exponential_jitter, 
    retry_if_exception_type,
    before_sleep_log
)
from django.db import transaction, IntegrityError
from django.utils import timezone

from sales.models import Institution

# =========================================================
# ⚙️ TELEMETRÍA MILITAR Y OBSERVABILIDAD
# =========================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s.%(msecs)03d - [%(levelname)s] [OSINT_RESOLVER] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.SingularityResolver")

# =========================================================
# 🛡️ MOTOR DE OSINT Y RESOLUCIÓN (TIER GOD - ZERO TRUST)
# =========================================================
class SERPResolverEngine:
    """
    [OMNI-SINGULARITY ENGINE: ZERO TRUST ARCHITECTURE]
    Motor de Inferencia de Identidad Digital. 
    Aplica NLP para coincidencia de dominios, evasión de WAF y HTTP/2 Multiplexing.
    Nadie entra a la base de datos sin superar la barrera heurística.
    """

    # La lista negra definitiva: Destruye agregadores SEO, foros y diccionarios al instante.
    DOMAIN_BLACKLIST = {
        'facebook', 'instagram', 'linkedin', 'twitter', 'x.com', 'youtube', 'tiktok',
        'wikipedia', 'paginasamarillas', 'infoisinfo', 'tripadvisor', 'foursquare', 'yelp',
        'scholastico', 'micolegio', 'buscacolegios', 'guia-colegios', 'educacionbogota',
        'mineducacion', 'civico', 'empresite', 'cylex', 'educaweb', 'scholaro',
        'top100colegios', 'micole', 'colegioscolombia', 'pymes', 'concepto.de', 
        'significados', 'baby-kingdom', 'plan.org', 'definicion', 'wiktionary',
        'orientacionandujar', 'scribd', 'issuu', 'pinterest', 'google', 'mapcarta',
        'zhihu', 'spanishdict', 'cybo', 'jardineriaon', 'valottery', 'forum', 
        'wordreference', 'brainly', 'prezi', 'coursehero', 'studocu', 'docsity',
        'computrabajo', 'elempleo', 'glassdoor', 'indeed', 'mercadolibre'
    }

    # Patrones de URL que indican que es un sub-producto y no el home oficial
    PATH_PENALTY = [
        'blog', 'portal', 'moodle', 'vle', 'canvas', 'login', 'wp-content', 
        'uploads', 'document', 'pdf', 'wiki', 'translate', 'question', 'foro',
        'article', 'news', 'noticias'
    ]

    USER_AGENT_POOL = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ]

    def __init__(self, concurrency_limit: int = 5):
        self.concurrency_limit = concurrency_limit
        self.seen_in_batch: Set[str] = set()
        # [APT TACTIC]: Multiplexación HTTP/2. Evita agotar sockets TCP locales.
        self.limits = httpx.Limits(max_keepalive_connections=30, max_connections=concurrency_limit * 3)

    def _get_stealth_headers(self) -> dict:
        return {
            "User-Agent": random.choice(self.USER_AGENT_POOL),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "es-CO,es;q=0.9,en-US;q=0.8",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1"
        }

    def _clean_url(self, url: str) -> str:
        """Sanitización canónica para evitar duplicados en DB."""
        url = url.lower().strip().split('?')[0].split('#')[0] 
        parsed = urlparse(url)
        netloc = parsed.netloc.replace('www.', '')
        path = parsed.path.rstrip('/')
        return f"{parsed.scheme}://{netloc}{path}"

    def _normalize_string(self, text: str) -> str:
        """[NLP CORE]: Elimina tildes, diéresis y caracteres especiales para matching perfecto."""
        if not text: return ""
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
        return re.sub(r'[^a-z0-9]', '', text.lower())

    def _calculate_url_relevance(self, url: str, inst_name: str, city: str) -> float:
        """
        [ZERO TRUST SCORING MODEL]
        Todo dominio es culpable hasta que se demuestre lo contrario. 
        Puntaje inicial: 0.0
        Umbral de aprobación: 45.0
        """
        score = 0.0
        parsed = urlparse(url.lower())
        domain = parsed.netloc.replace('www.', '')
        path = parsed.path

        # 1. Análisis de Capa Superior (TLD) - Afinidad Educativa / Colombia
        if domain.endswith('.edu.co'): score += 70.0
        elif domain.endswith('.edu'): score += 40.0
        elif domain.endswith('.com.co'): score += 30.0
        elif domain.endswith('.co'): score += 20.0
        elif domain.endswith('.org') or domain.endswith('.net'): score += 10.0

        # 2. Token Matching (Análisis Semántico del Nombre)
        ignore_words = {'colegio', 'institucion', 'educativa', 'escuela', 'liceo', 'gimnasio', 'fundacion', 'de', 'la', 'el', 'los', 'las', 'san', 'santa'}
        
        # Limpiamos el nombre original dividiéndolo en tokens vitales
        raw_tokens = [self._normalize_string(t) for t in re.split(r'\s+', inst_name)]
        vital_tokens = [t for t in raw_tokens if len(t) > 3 and t not in ignore_words]
        
        clean_city = self._normalize_string(city)
        domain_normalized = self._normalize_string(domain.split('.')[0]) # Solo la parte antes del primer punto
        
        tokens_found = 0
        for token in vital_tokens:
            if token in domain_normalized:
                tokens_found += 1
                score += 35.0  # Premio masivo por cada palabra clave que exista en el dominio

        if clean_city and len(clean_city) > 3 and clean_city in domain_normalized:
            score += 20.0

        # 3. Penalizaciones y Kill Switches
        if path and path not in ['/', '']: 
            score -= 25.0 # Si no es la raíz, es sospechoso
            if any(p in path for p in self.PATH_PENALTY):
                score -= 80.0 # Castigo mortal a directorios y subpáginas

        # [KILL SWITCH FATAL]: Si no hay coincidencias semánticas del nombre y no es un .edu.co oficial -> Es Basura.
        if tokens_found == 0 and not domain.endswith('.edu.co'):
            score -= 1000.0

        return score

    def _is_valid_candidate(self, url: str) -> bool:
        """Primer filtro en RAM. Si el dominio está en la Blacklist, ni siquiera lo procesamos."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if not domain or any(bad in domain for bad in self.DOMAIN_BLACKLIST):
                return False
            # Bloqueo de MIME Types binarios
            if parsed.path.lower().endswith(('.pdf', '.doc', '.docx', '.xls', '.jpg', '.png', '.zip', '.rar', '.txt')):
                return False
            return len(url) <= 120
        except Exception:
            return False

    async def _verify_url_live(self, client: httpx.AsyncClient, url: str) -> bool:
        """[FAST-FAIL SOCKET VERIFICATION]: Chequeo de pulso TLS ultrarrápido."""
        try:
            # HEAD request ahorra 90% de ancho de banda al no descargar el HTML
            response = await client.head(url, follow_redirects=True, timeout=6.0)
            if response.status_code < 400: return True
            
            # Si un firewall bloquea HEAD (común en IIS antiguo), disparamos GET como fallback
            response = await client.get(url, follow_redirects=True, timeout=9.0)
            return response.status_code < 400
        except Exception:
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=2, max=10),
        retry=retry_if_exception_type((RatelimitException, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def _search_provider(self, query: str) -> List[dict]:
        """Inyección de Query en DuckDuckGo con tolerancia a baneos temporales (Ratelimits)."""
        with DDGS(headers=self._get_stealth_headers()) as ddgs:
            return list(ddgs.text(query, max_results=5, backend="lite")) # Backend lite evade mejor

    async def _resolve_node(self, inst: Institution, client: httpx.AsyncClient, semaphore: asyncio.Semaphore) -> Tuple[Institution, Optional[str]]:
        """Unidad de trabajo atómica por Institución."""
        async with semaphore:
            # Micro-Jittering: Desincronización táctica para engañar heurísticas anti-bot
            await asyncio.sleep(random.uniform(0.5, 2.0))

            # [SMART QUERY]: Limpia y enfocada. "sitio web oficial" fue purgado.
            keyword = 'universidad' if inst.institution_type in ['university', 'college'] else 'colegio'
            search_query = f'"{inst.name}" {inst.city} {keyword}'
            logger.info(f"🛰️ Explorando Firma Digital: {inst.name[:35]}...")

            try:
                # Offloading a Thread para no congelar el Event Loop de Asyncio
                results = await asyncio.to_thread(self._search_provider, search_query)
                if not results: return inst, None

                candidates = []
                for r in results:
                    url = r.get('href', '')
                    if self._is_valid_candidate(url):
                        score = self._calculate_url_relevance(url, inst.name, inst.city)
                        candidates.append((url, score))
                
                # Clasificamos de mayor a menor puntuación heurística
                candidates.sort(key=lambda x: x[1], reverse=True)

                for candidate_url, score in candidates:
                    # [FILTRO MAESTRO]: Si no llega a 45 puntos, es basura SEO. Siguiente.
                    if score < 45.0: 
                        continue 
                    
                    clean_url = self._clean_url(candidate_url)
                    if clean_url in self.seen_in_batch: continue

                    is_alive = await self._verify_url_live(client, clean_url)
                    if is_alive:
                        self.seen_in_batch.add(clean_url)
                        logger.info(f"✅ Identidad Confirmada [Score:{score}]: {clean_url}")
                        return inst, clean_url
                                
            except Exception as e:
                logger.debug(f"⚠️ Perturbación de Red en Nodo {inst.id}: {str(e)[:50]}")
            
            return inst, None

    async def _orchestrate_osint(self, targets: List[Institution]) -> List[Institution]:
        """Arquitectura Swarm: Despliega trabajadores asíncronos en paralelo."""
        semaphore = asyncio.Semaphore(self.concurrency_limit)
        resolved_batch = []
        
        async with httpx.AsyncClient(
            http2=True, 
            limits=self.limits, 
            verify=False, # Ignora certificados caducados (Muy común en Latam)
            headers=self._get_stealth_headers(),
            timeout=httpx.Timeout(12.0)
        ) as client:
            tasks = [self._resolve_node(inst, client, semaphore) for inst in targets]
            
            # return_exceptions=True garantiza que un Crash no tumbe todo el clúster
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for res in results:
                if isinstance(res, tuple) and res[1]:
                    inst, found_url = res
                    inst.website = found_url
                    inst.updated_at = timezone.now()
                    resolved_batch.append(inst)
                elif isinstance(res, Exception):
                    logger.error(f"🔥 Falla en núcleo de worker: {str(res)}")

        return resolved_batch

    def resolve_missing_urls(self, limit: int = 50):
        """[ENTRY POINT ABSOLUTO]: Adaptador síncrono para Django/Celery."""
        targets = list(Institution.objects.filter(
            website__isnull=True,
            is_active=True
        ).order_by('-created_at')[:limit])

        if not targets:
            logger.info("✅ Bandeja Limpia: Pipeline de identidades sincronizado al 100%.")
            return

        logger.info(f"🚀 Encendiendo Singularity OSINT Engine | Objetivos: {len(targets)}")
        start_mark = time.perf_counter()
        self.seen_in_batch.clear()

        try:
            # Sandbox de ejecución del Event Loop
            resolved_instances = asyncio.run(self._orchestrate_osint(targets))
        except Exception as e:
            logger.error(f"❌ Kernel Panic en matriz de asincronismo: {str(e)}")
            return

        if resolved_instances:
            logger.info(f"💾 Inyectando {len(resolved_instances)} firmas digitales validadas a PostgreSQL...")
            try:
                with transaction.atomic():
                    # Bulk Update: Complejidad O(1) en DB, infinitamente más rápido que .save() en loop
                    Institution.objects.bulk_update(resolved_instances, ['website', 'updated_at'])
            except (IntegrityError, Exception) as e:
                logger.warning(f"⚠️ Colisión detectada en inyección Bulk: {str(e)}. Activando Escudo Secuencial.")
                self._fallback_safe_save(resolved_instances)

        latency = time.perf_counter() - start_mark
        logger.info("=" * 70)
        logger.info(f"🏁 CICLO TERMINADO: {latency:.2f}s | Precisión Quirúrgica: {len(resolved_instances)}/{len(targets)}")
        logger.info("=" * 70)

    def _fallback_safe_save(self, instances: List[Institution]):
        """[PROTOCOL FALLBACK]: Si el Bulk falla por duplicados en DB, guardamos quirúrgicamente 1 por 1."""
        count = 0
        for inst in instances:
            try:
                with transaction.atomic():
                    inst.save(update_fields=['website', 'updated_at'])
                    count += 1
            except IntegrityError:
                continue # Evade la colisión de UNIQUE constraint de PostgreSQL sin romper el sistema
            except Exception as e:
                logger.error(f"Error atípico consolidando '{inst.name}': {str(e)}")
        logger.info(f"🛡️ Escudo Secuencial Finalizado: {count} registros salvados exitosamente.")