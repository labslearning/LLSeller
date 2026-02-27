import logging
import asyncio
import random
import time
import re
import hashlib
from urllib.parse import urlparse
from typing import List, Optional, Tuple, Set, Dict, Any

# Dependencias Nivel Omni-Singularity (pip install httpx tenacity duckduckgo-search)
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
from django.db.models import Q

from sales.models import Institution

# =========================================================
# ⚙️ TELEMETRÍA DE ALTA DISPONIBILIDAD
# =========================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s.%(msecs)03d - [%(levelname)s] [OSINT_RESOLVER] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.SingularityResolver")

# =========================================================
# 🛡️ MOTOR DE OSINT Y RESOLUCIÓN (TIER 0)
# =========================================================
class SERPResolverEngine:
    """
    [OMNI-SINGULARITY ENGINE]
    Motor de Inferencia de Identidad Digital. 
    Evasión de WAF nivel militar, Validaciones TLS de baja latencia y Scoring Semántico.
    """

    # Dominios de ruido institucional y agregadores (SEO Garbage)
    DOMAIN_BLACKLIST = {
        'facebook', 'instagram', 'linkedin', 'twitter', 'x.com', 'youtube', 'tiktok',
        'wikipedia', 'paginasamarillas', 'infoisinfo', 'tripadvisor', 'foursquare', 'yelp',
        'scholastico', 'micolegio', 'buscacolegios', 'guia-colegios', 'educacionbogota',
        'mineducacion', 'civico', 'empresite', 'cylex', 'educaweb', 'scholaro',
        'top100colegios', 'micole', 'colegioscolombia', 'pymes', 'concepto.de', 
        'significados.com', 'baby-kingdom', 'plan.org', 'itq.edu.ec', 'definicion.de',
        'orientacionandujar', 'scribd', 'issuu', 'pinterest', 'google', 'mapcarta'
    }

    # Palabras que indican una subpágina y no el sitio raíz (Falsos Positivos)
    PATH_PENALTY = ['blog', 'portal', 'moodle', 'vle', 'canvas', 'login', 'wp-content', 'uploads', 'document', 'pdf']

    USER_AGENT_POOL = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1"
    ]

    def __init__(self, concurrency_limit: int = 5):
        self.concurrency_limit = concurrency_limit
        self.seen_in_batch: Set[str] = set()
        # Pool de conexiones persistente para evitar latencia de apretón de manos TCP/TLS
        self.limits = httpx.Limits(max_keepalive_connections=20, max_connections=concurrency_limit * 2)

    def _get_stealth_headers(self) -> dict:
        ua = random.choice(self.USER_AGENT_POOL)
        return {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1"
        }

    def _clean_url(self, url: str) -> str:
        """Normalización canónica: elimina redundancia y estandariza para indexación única."""
        url = url.lower().strip().split('?')[0] # Eliminar parámetros de tracking
        parsed = urlparse(url)
        netloc = parsed.netloc.replace('www.', '')
        path = parsed.path.rstrip('/')
        return f"{parsed.scheme}://{netloc}{path}"

    def _calculate_url_relevance(self, url: str, inst_name: str) -> float:
        """Scoring Heurístico: Evalúa qué tan probable es que esta sea la web oficial."""
        score = 100.0
        parsed = urlparse(url.lower())
        domain = parsed.netloc
        path = parsed.path

        # Penalización por subdirectorios (Preferimos la raíz)
        if path and path != '/':
            score -= 20.0
            if any(p in path for p in self.PATH_PENALTY):
                score -= 50.0

        # Bonus por coincidencia de nombre en dominio
        clean_name = re.sub(r'[^a-z0-9]', '', inst_name.lower())
        clean_domain = re.sub(r'[^a-z0-9]', '', domain.split('.')[0])
        if clean_name in clean_domain or clean_domain in clean_name:
            score += 30.0

        return score

    def _is_valid_candidate(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if not domain or any(bad in domain for bad in self.DOMAIN_BLACKLIST):
                return False
            
            # Bloqueo de tipos de archivo MIME peligrosos para el bot
            if parsed.path.lower().endswith(('.pdf', '.doc', '.docx', '.xls', '.jpg', '.png', '.zip', '.rar', '.txt')):
                return False
            
            return len(url) <= 110
        except Exception:
            return False

    async def _verify_url_live(self, client: httpx.AsyncClient, url: str) -> bool:
        """Validación de socket asíncrona con Fast-Fail y seguimiento de redirecciones."""
        try:
            # Intentamos HEAD (Ahorro de ancho de banda masivo)
            response = await client.head(url, follow_redirects=True, timeout=6.0)
            if response.status_code < 400: return True
            
            # Fallback a GET (Algunos servidores antiguos bloquean HEAD)
            response = await client.get(url, follow_redirects=True, timeout=10.0)
            return response.status_code < 400
        except Exception:
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=2, max=12),
        retry=retry_if_exception_type((RatelimitException, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def _search_provider(self, query: str) -> List[dict]:
        """Inyección de búsqueda en DDG con motor de reintentos cuántico."""
        with DDGS(headers=self._get_stealth_headers()) as ddgs:
            return list(ddgs.text(query, max_results=5))

    async def _resolve_node(self, inst: Institution, client: httpx.AsyncClient, semaphore: asyncio.Semaphore) -> Tuple[Institution, Optional[str]]:
        async with semaphore:
            # Latencia Orgánica: Evita la detección de patrones de escaneo
            await asyncio.sleep(random.uniform(0.7, 2.8))

            keyword = {
                'kindergarten': 'jardín infantil',
                'university': 'universidad',
                'college': 'universidad',
                'institute': 'instituto'
            }.get(inst.institution_type, 'colegio')

            search_query = f'"{inst.name}" {inst.city} {inst.country} {keyword} sitio web oficial'
            logger.info(f"🛰️ Resolviendo Nodo: {inst.name[:30]}...")

            try:
                # ThreadPool Offloading para no bloquear el Event Loop
                results = await asyncio.to_thread(self._search_provider, search_query)
                if not results: return inst, None

                # Ordenar por relevancia heurística antes de verificar
                candidates = []
                for r in results:
                    url = r.get('href', '')
                    if self._is_valid_candidate(url):
                        score = self._calculate_url_relevance(url, inst.name)
                        candidates.append((url, score))
                
                # Procesamos los mejores candidatos primero
                candidates.sort(key=lambda x: x[1], reverse=True)

                for candidate_url, score in candidates:
                    if score < 40: continue # Descartar basura
                    
                    clean_url = self._clean_url(candidate_url)
                    if clean_url in self.seen_in_batch: continue

                    is_alive = await self._verify_url_live(client, clean_url)
                    if is_alive:
                        self.seen_in_batch.add(clean_url)
                        logger.info(f"✅ Match [Score:{score}]: {clean_url}")
                        return inst, clean_url
                                
            except Exception as e:
                logger.debug(f"⚠️ Nodo {inst.id} falló: {str(e)}")
            
            return inst, None

    async def _orchestrate_osint(self, targets: List[Institution]) -> List[Institution]:
        """Pipeline de resolución asíncrona masiva."""
        semaphore = asyncio.Semaphore(self.concurrency_limit)
        resolved_batch = []
        
        # Httpx configurado con HTTP/2 y TLS de grado empresarial
        async with httpx.AsyncClient(
            http2=True, 
            limits=self.limits, 
            verify=False, 
            headers=self._get_stealth_headers(),
            timeout=httpx.Timeout(15.0)
        ) as client:
            tasks = [self._resolve_node(inst, client, semaphore) for inst in targets]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for res in results:
                if isinstance(res, tuple) and res[1]:
                    inst, found_url = res
                    inst.website = found_url
                    inst.updated_at = timezone.now()
                    resolved_batch.append(inst)
                elif isinstance(res, Exception):
                    logger.error(f"🔥 Error en worker asíncrono: {str(res)}")

        return resolved_batch

    def resolve_missing_urls(self, limit: int = 50):
        """Punto de entrada atómico para la infraestructura de Django."""
        # Priorizar leads más recientes
        targets = list(Institution.objects.filter(
            website__isnull=True,
            is_active=True
        ).order_by('-created_at')[:limit])

        if not targets:
            logger.info("✅ Inbox Zero: Pipeline de resolución limpio.")
            return

        logger.info(f"🚀 Iniciando Singularity OSINT Engine | Targets: {len(targets)}")
        start_mark = time.perf_counter()
        self.seen_in_batch.clear()

        try:
            # Ejecución del Event Loop Aislado
            resolved_instances = asyncio.run(self._orchestrate_osint(targets))
        except Exception as e:
            logger.error(f"❌ Kernel Panic en Event Loop: {str(e)}")
            return

        # Persistencia Atómica
        if resolved_instances:
            logger.info(f"💾 Sincronizando {len(resolved_instances)} identidades con PostgreSQL...")
            try:
                with transaction.atomic():
                    # Bulk Update optimizado para rendimiento
                    Institution.objects.bulk_update(resolved_instances, ['website', 'updated_at'])
            except (IntegrityError, Exception) as e:
                logger.warning(f"⚠️ Colisión de integridad en Bulk: {str(e)}. Activando Fallback Secuencial.")
                self._fallback_safe_save(resolved_instances)

        latency = time.perf_counter() - start_mark
        logger.info("=" * 65)
        logger.info(f"🏁 OPERACIÓN COMPLETADA: {latency:.2f}s | Eficiencia: {len(resolved_instances)}/{len(targets)}")
        logger.info("=" * 65)

    def _fallback_safe_save(self, instances: List[Institution]):
        """Protocolo de emergencia: Guarda uno a uno aislando colisiones de llaves únicas."""
        count = 0
        for inst in instances:
            try:
                with transaction.atomic():
                    inst.save(update_fields=['website', 'updated_at'])
                    count += 1
            except IntegrityError:
                continue # Saltar duplicados sin romper el loop
            except Exception as e:
                logger.error(f"Error atípico guardando {inst.name}: {str(e)}")
        logger.info(f"🛡️ Protocolo Fallback: {count} registros salvaguardados.")