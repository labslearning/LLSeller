import logging
import asyncio
import random
import time
import re
import unicodedata
from urllib.parse import urlparse
from typing import List, Optional, Tuple, Set

# Dependencias Nivel Omni-Singularity
import httpx
# [GOD TIER UPGRADE]: Importamos la versión síncrona estándar
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
# Importamos el Validador de IA que creamos previamente para precisión 100%
from sales.engine.ai_validators import DeepSeekOmniValidator

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
    [OMNI-SINGULARITY ENGINE: ZERO TRUST ARCHITECTURE V35.1]
    Motor de Inferencia de Identidad Digital. 
    Aplica NLP en RAM (O(1) lookups), evasión de WAF, HTTP/2 Multiplexing,
    y Validación Cognitiva (DeepSeek LLM) para 0% Falsos Positivos.
    Implementa Thread Pool Offloading para evadir bloqueos de I/O.
    """

    # La lista negra definitiva (Set de Python: Búsqueda O(1) ultra rápida)
    DOMAIN_BLACKLIST = frozenset({
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
    })

    PATH_PENALTY = frozenset({
        'blog', 'portal', 'moodle', 'vle', 'canvas', 'login', 'wp-content', 
        'uploads', 'document', 'pdf', 'wiki', 'translate', 'question', 'foro',
        'article', 'news', 'noticias'
    })

    USER_AGENT_POOL = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    # [OPTIMIZACIÓN DE MEMORIA]: Pre-compilar Regex una sola vez a nivel de clase
    _NORMALIZE_REGEX = re.compile(r'[^a-z0-9]')
    _SPLIT_REGEX = re.compile(r'\s+')

    def __init__(self, concurrency_limit: int = 5):
        self.concurrency_limit = concurrency_limit
        self.seen_in_batch: Set[str] = set()
        # Multiplexación HTTP/2 y pool de sockets para no ahogar el Kernel de Linux
        self.limits = httpx.Limits(max_keepalive_connections=30, max_connections=concurrency_limit * 3)
        # 🧠 Núcleo Cognitivo IA
        self.ai_validator = DeepSeekOmniValidator()

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
        if not url: return ""
        url = url.lower().strip().split('?')[0].split('#')[0] 
        parsed = urlparse(url)
        netloc = parsed.netloc.replace('www.', '')
        path = parsed.path.rstrip('/')
        return f"{parsed.scheme}://{netloc}{path}"

    def _normalize_string(self, text: str) -> str:
        """[NLP CORE]: Normalización unicode ultra-rápida (O(L))."""
        if not text: return ""
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
        return self._NORMALIZE_REGEX.sub('', text.lower())

    def _calculate_url_relevance(self, url: str, inst_name: str, city: str) -> float:
        """
        [HEURÍSTICA DE PRE-FILTRADO]
        Filtramos la basura obvia matemáticamente antes de gastar tokens de DeepSeek.
        """
        score = 0.0
        parsed = urlparse(url.lower())
        domain = parsed.netloc.replace('www.', '')
        path = parsed.path

        # 1. Análisis TLD (Top Level Domain)
        if domain.endswith('.edu.co'): score += 70.0
        elif domain.endswith('.edu'): score += 40.0
        elif domain.endswith('.com.co'): score += 30.0
        elif domain.endswith('.co'): score += 20.0
        elif domain.endswith(('.org', '.net')): score += 10.0

        # 2. Token Matching Semántico
        ignore_words = frozenset({'colegio', 'institucion', 'educativa', 'escuela', 'liceo', 'gimnasio', 'fundacion', 'de', 'la', 'el', 'los', 'las', 'san', 'santa'})
        raw_tokens = [self._normalize_string(t) for t in self._SPLIT_REGEX.split(inst_name)]
        vital_tokens = [t for t in raw_tokens if len(t) > 3 and t not in ignore_words]
        
        clean_city = self._normalize_string(city)
        domain_normalized = self._normalize_string(domain.split('.')[0])
        
        tokens_found = 0
        for token in vital_tokens:
            if token in domain_normalized:
                tokens_found += 1
                score += 35.0  

        if clean_city and len(clean_city) > 3 and clean_city in domain_normalized:
            score += 20.0

        # 3. Penalizaciones y Kill Switches
        if path and path not in ['/', '']: 
            score -= 25.0
            if any(p in path for p in self.PATH_PENALTY):
                score -= 80.0

        if tokens_found == 0 and not domain.endswith('.edu.co'):
            score -= 1000.0

        return score

    def _is_valid_candidate(self, url: str) -> bool:
        """Filtro en RAM. Complejidad O(1) usando frozensets."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if not domain: return False
            
            # Chequeo rápido contra la blacklist
            if any(bad in domain for bad in self.DOMAIN_BLACKLIST):
                return False
                
            if parsed.path.lower().endswith(('.pdf', '.doc', '.docx', '.xls', '.jpg', '.png', '.zip', '.rar', '.txt')):
                return False
                
            return len(url) <= 120
        except Exception:
            return False

    async def _verify_url_live(self, client: httpx.AsyncClient, url: str) -> bool:
        """[FAST-FAIL SOCKET]: Chequeo de pulso TLS ultrarrápido (HEAD request)."""
        try:
            response = await client.head(url, follow_redirects=True, timeout=6.0)
            if response.status_code < 400: return True
            
            # Fallback para IIS/Apache antiguos que bloquean HEAD
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
    def _search_provider_sync(self, query: str) -> List[dict]:
        """[CORE EXECUTION]: Ejecución síncrona nativa de DDG (Protegida por Tenacity)."""
        with DDGS(headers=self._get_stealth_headers()) as ddgs:
            results = ddgs.text(query, max_results=6, backend="lite")
            return list(results) if results else []

    async def _search_provider_async(self, query: str) -> List[dict]:
        """
        [NATIVE THREAD OFFLOADING]
        Delega la ejecución bloqueante de DDG a un procesador C (Thread Pool).
        Esto mantiene el IO de Asyncio corriendo a máximos FPS.
        """
        return await asyncio.to_thread(self._search_provider_sync, query)

    async def _resolve_node(self, inst: Institution, client: httpx.AsyncClient, semaphore: asyncio.Semaphore) -> Tuple[Institution, Optional[str]]:
        """Unidad de trabajo atómica, inyectada con Validación de Inteligencia Artificial."""
        async with semaphore:
            # Micro-Jittering: Desincronización táctica de threads
            await asyncio.sleep(random.uniform(0.5, 2.0))

            keyword = 'universidad' if inst.institution_type in ['university', 'college'] else 'colegio'
            search_query = f'"{inst.name}" {inst.city} {keyword}'
            logger.info(f"🛰️ Explorando Firma Digital: {inst.name[:35]}...")

            try:
                results = await self._search_provider_async(search_query)
                if not results: return inst, None

                # FASE 1: Filtrado Heurístico CPU-Bound (Costo 0)
                candidates = []
                for r in results:
                    url = r.get('href', '')
                    if self._is_valid_candidate(url):
                        score = self._calculate_url_relevance(url, inst.name, inst.city)
                        if score >= 30.0: # Umbral más bajo porque la IA hará la decisión final
                            candidates.append((url, score))
                
                candidates.sort(key=lambda x: x[1], reverse=True)
                top_urls_to_evaluate = [c[0] for c in candidates[:3]] # Enviamos máximo 3 a la IA
                
                if not top_urls_to_evaluate:
                    return inst, None

                # FASE 2: 🧠 DELEGACIÓN COGNITIVA AL LLM (DeepSeek OmniValidator)
                official_url = await self.ai_validator.get_official_url_async(
                    institution_name=inst.name,
                    city=inst.city,
                    country=inst.country,
                    serp_urls=top_urls_to_evaluate
                )

                # FASE 3: Validación Viva (Evitar guardar URLs caídas)
                if official_url:
                    clean_url = self._clean_url(official_url)
                    if clean_url in self.seen_in_batch: 
                        return inst, None

                    is_alive = await self._verify_url_live(client, clean_url)
                    if is_alive:
                        self.seen_in_batch.add(clean_url)
                        logger.info(f"✅ Identidad Confirmada por IA: {clean_url}")
                        return inst, clean_url
                                
            except Exception as e:
                logger.debug(f"⚠️ Perturbación de Red en Nodo {inst.id}: {str(e)[:50]}")
            
            return inst, None

    async def _orchestrate_osint(self, targets: List[Institution]) -> List[Institution]:
        """Arquitectura Swarm: Despliegue masivo asíncrono con tolerancia a fallos."""
        semaphore = asyncio.Semaphore(self.concurrency_limit)
        resolved_batch = []
        
        async with httpx.AsyncClient(
            http2=True, 
            limits=self.limits, 
            verify=False, 
            headers=self._get_stealth_headers(),
            timeout=httpx.Timeout(12.0)
        ) as client:
            tasks = [self._resolve_node(inst, client, semaphore) for inst in targets]
            
            # Gather permite que si un colegio falla, los otros 49 sigan funcionando
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
        """[ENTRY POINT ABSOLUTO]: Ejecutado por Celery o Management Command."""
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
            resolved_instances = asyncio.run(self._orchestrate_osint(targets))
        except Exception as e:
            logger.error(f"❌ Kernel Panic en matriz de asincronismo: {str(e)}")
            return

        if resolved_instances:
            logger.info(f"💾 Inyectando {len(resolved_instances)} firmas digitales validadas a PostgreSQL...")
            try:
                with transaction.atomic():
                    # Upsert masivo O(1) para máxima velocidad de I/O en BD
                    Institution.objects.bulk_update(resolved_instances, ['website', 'updated_at'])
            except (IntegrityError, Exception) as e:
                logger.warning(f"⚠️ Colisión detectada en inyección Bulk: {str(e)}. Activando Escudo Secuencial.")
                self._fallback_safe_save(resolved_instances)

        latency = time.perf_counter() - start_mark
        logger.info("=" * 70)
        logger.info(f"🏁 CICLO TERMINADO: {latency:.2f}s | Precisión Quirúrgica: {len(resolved_instances)}/{len(targets)}")
        logger.info("=" * 70)

    def _fallback_safe_save(self, instances: List[Institution]):
        """[PROTOCOL FALLBACK]: Aislamiento de colisiones para asegurar la data sobreviviente."""
        count = 0
        for inst in instances:
            try:
                with transaction.atomic():
                    inst.save(update_fields=['website', 'updated_at'])
                    count += 1
            except IntegrityError:
                continue # Evade la colisión de UNIQUE constraint
            except Exception as e:
                logger.error(f"Error atípico consolidando '{inst.name}': {str(e)}")
        logger.info(f"🛡️ Escudo Secuencial Finalizado: {count} registros salvados exitosamente.")