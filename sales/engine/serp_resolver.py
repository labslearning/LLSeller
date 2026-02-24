import logging
import asyncio
import aiohttp
import random
import time
from urllib.parse import urlparse
from typing import List, Optional, Tuple, Set
from duckduckgo_search import DDGS
from django.db import transaction, IntegrityError
from django.utils import timezone

from sales.models import Institution

# Configuración de Logging Forense (Estilo Silicon Valley)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("SERP_AI_Resolver")

class SERPResolverEngine:
    """
    Enterprise-Grade URL Resolution Engine (Event Loop Resilient).
    Features: 
    - Async Concurrency with dynamic Semaphores.
    - Live HTTP Probing (HEAD/GET validation).
    - Intra-batch De-duplication.
    - Atomic Transaction Fallback for DB safety.
    """

    # 🚫 Lista Negra: Dominios que ensucian tu pipeline de ventas
    DOMAIN_BLACKLIST = {
        'facebook', 'instagram', 'linkedin', 'twitter', 'x.com', 'youtube', 'tiktok',
        'wikipedia', 'paginasamarillas', 'infoisinfo', 'tripadvisor', 'foursquare', 'yelp',
        'scholastico', 'micolegio', 'buscacolegios', 'guia-colegios', 'educacionbogota',
        'mineducacion', 'civico', 'empresite', 'cylex', 'educaweb', 'scholaro',
        'top100colegios', 'micole', 'colegioscolombia', 'pymes', 'concepto.de', 
        'significados.com', 'baby-kingdom', 'plan.org', 'itq.edu.ec', 'definicion.de',
        'orientacionandujar', 'scribd', 'issuu'
    }

    def __init__(self, concurrency_limit: int = 3):
        self.concurrency_limit = concurrency_limit
        self.seen_in_batch: Set[str] = set()
        # Nota: No inicializamos el semáforo aquí para evitar errores de hilos/loops cruzados.

    def _is_valid_candidate(self, url: str) -> bool:
        """Filtro heurístico: Separa webs oficiales de ruido o documentos."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if not domain or any(bad in domain for bad in self.DOMAIN_BLACKLIST):
                return False
            
            # Evitar descargas directas que bloquean el bot
            if parsed.path.lower().endswith(('.pdf', '.doc', '.docx', '.xls', '.jpg', '.png')):
                return False
            
            # URLs excesivamente largas suelen ser hilos de foros, no webs oficiales
            return len(url) <= 125
        except Exception:
            return False

    async def _verify_url_live(self, session: aiohttp.ClientSession, url: str) -> bool:
        """Prueba de Vida: Asegura que el sitio no sea un 404 o esté caído."""
        try:
            # HEAD es más rápido porque no descarga el contenido del sitio
            async with session.head(url, timeout=7, allow_redirects=True) as response:
                return response.status < 400
        except Exception:
            try:
                # Fallback a GET si el servidor bloquea peticiones HEAD
                async with session.get(url, timeout=7, allow_redirects=True) as response:
                    return response.status < 400
            except Exception:
                return False

    def _sync_ddg_search(self, query: str) -> List[dict]:
        """Búsqueda síncrona aislada para evitar conflictos con el event loop."""
        try:
            with DDGS() as ddgs:
                return list(ddgs.text(query, max_results=5))
        except Exception as e:
            logger.debug(f"DDG Search Error: {str(e)}")
            return []

    async def _resolve_single_institution(self, inst: Institution, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> Tuple[Institution, Optional[str]]:
        """Busca, filtra y valida la URL de un solo lead."""
        async with semaphore:
            # Jitter: Pausa aleatoria para no disparar sistemas anti-bot
            await asyncio.sleep(random.uniform(1.1, 3.5))

            keyword = "colegio"
            if inst.institution_type == 'kindergarten': keyword = "jardín infantil"
            elif inst.institution_type == 'university': keyword = "universidad"

            query = f'"{inst.name}" {inst.city} {inst.country} {keyword} sitio web oficial'
            logger.info(f"🔎 Resolving: {inst.name[:35]}...")

            try:
                # Ejecutamos la búsqueda en un hilo separado
                results = await asyncio.to_thread(self._sync_ddg_search, query)
                if not results:
                    return inst, None

                for r in results:
                    candidate_url = r.get('href', '')
                    
                    if self._is_valid_candidate(candidate_url) and candidate_url not in self.seen_in_batch:
                        if await self._verify_url_live(session, candidate_url):
                            logger.info(f"  👉 Match Found: {candidate_url}")
                            self.seen_in_batch.add(candidate_url)
                            return inst, candidate_url
            except Exception:
                pass
            
            return inst, None

    async def _run_pipeline(self, targets: List[Institution]) -> List[Institution]:
        """Orquesta la ejecución asíncrona por lotes."""
        # El semáforo se crea AQUÍ para vivir dentro del Event Loop activo
        semaphore = asyncio.Semaphore(self.concurrency_limit)
        resolved = []
        timeout = aiohttp.ClientTimeout(total=15)
        
        async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}) as session:
            tasks = [self._resolve_single_institution(inst, session, semaphore) for inst in targets]
            results = await asyncio.gather(*tasks)
            
            for inst, found_url in results:
                if found_url:
                    inst.website = found_url
                    inst.updated_at = timezone.now()
                    resolved.append(inst)
        return resolved

    def resolve_missing_urls(self, limit: int = 50):
        """Punto de entrada principal para el comando de Django."""
        targets = list(Institution.objects.filter(
            website__isnull=True,
            is_active=True
        ).order_by('-created_at')[:limit])

        if not targets:
            logger.info("✅ Inbox Zero: No hay URLs pendientes.")
            return

        logger.info(f"🚀 Iniciando SERP AI Resolver para {len(targets)} leads...")
        start_time = time.time()
        
        self.seen_in_batch.clear()

        # Ejecutamos el pipeline asíncrono
        resolved_instances = asyncio.run(self._run_pipeline(targets))

        # 💾 GUARDADO SEGURO
        if resolved_instances:
            logger.info(f"💾 Guardando {len(resolved_instances)} URLs verificadas...")
            try:
                with transaction.atomic():
                    # Intento de actualización masiva (Rápido)
                    Institution.objects.bulk_update(resolved_instances, ['website', 'updated_at'])
            except IntegrityError:
                # Si falla por duplicados en la base de datos, guardamos uno a uno (Seguro)
                logger.warning("⚠️ Colisión en Bulk Update. Cambiando a guardado secuencial atómico.")
                self._safe_sequential_save(resolved_instances)

        elapsed = time.time() - start_time
        logger.info("=" * 50)
        logger.info(f"🏁 FINALIZADO EN {elapsed:.2f}s | Éxito: {len(resolved_instances)}/{len(targets)}")
        logger.info("=" * 50)

    def _safe_sequential_save(self, instances: List[Institution]):
        """Guarda registros uno por uno ignorando duplicados existentes."""
        count = 0
        for inst in instances:
            try:
                with transaction.atomic():
                    inst.save(update_fields=['website', 'updated_at'])
                    count += 1
            except IntegrityError:
                logger.debug(f"URL duplicada omitida para: {inst.name}")
                continue
        logger.info(f"✅ Se recuperaron {count} registros exitosamente.")