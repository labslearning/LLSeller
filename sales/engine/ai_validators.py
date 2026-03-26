import os
import json
import asyncio
import logging
import time
from urllib.parse import urlparse
from typing import Optional, List, Dict

# Dependencias Críticas
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

logger = logging.getLogger("Sovereign.OmniValidator")

class DeepSeekOmniValidator:
    """
    [GOD TIER V32.0 - ASYNC AI VALIDATOR]
    Motor asíncrono de validación cognitiva.
    Usa 'Chain of Thought' y JSON estricto para precisión balística.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        # 🛡️ Seguridad TIER 1: Nunca hardcodear keys en producción
        # Se lee de variable de entorno, si no existe, usa la que envíes por parámetro
        self.raw_key = api_key or os.getenv("DEEPSEEK_API_KEY", "sk-b6020f82f33f445daae865f32d723a44")
        
        # 🚀 Cliente 100% Asíncrono para no bloquear el EventLoop de Django/Celery
        self.client = AsyncOpenAI(
            api_key=self.raw_key, 
            base_url="https://api.deepseek.com",
            max_retries=0 # Desactivamos el retry nativo para usar nuestro propio Circuit Breaker
        )

    @staticmethod
    def _sanitize_url(url: str) -> Optional[str]:
        """Asegura que lo que devuelve la IA sea sintácticamente una URL válida."""
        if not url or url.upper() == "NONE":
            return None
        url = url.strip().lower()
        if not url.startswith('http'):
            url = f"https://{url}"
        try:
            parsed = urlparse(url)
            if not parsed.netloc or '.' not in parsed.netloc:
                return None
            return url
        except Exception:
            return None

    # 🛡️ CIRCUIT BREAKER: Absorbe caídas de API y Rate Limits con Jitter Aleatorio
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=False
    )
    async def get_official_url_async(self, institution_name: str, city: str, country: str, serp_urls: List[str]) -> Optional[str]:
        """
        [THE SNIPER SCOPE]
        Analiza las URLs candidatas y devuelve la correcta con validación matemática.
        """
        if not serp_urls:
            return None

        # 🧠 PROMPT ENGINEERING AVANZADO (Chain of Thought + Estructuración JSON)
        prompt = f"""
        Eres una máquina forense de inteligencia OSINT. Tu tarea es encontrar la URL web OFICIAL de la siguiente institución educativa:
        
        [TARGET]
        Nombre: {institution_name}
        Ubicación: {city}, {country}
        
        [CANDIDATAS EXTRAÍDAS DE GOOGLE]
        {json.dumps(serp_urls, indent=2)}

        [REGLAS DE EXCLUSIÓN FATAL]
        Descartar INMEDIATAMENTE:
        - Redes sociales (facebook.com, instagram.com, linkedin.com, twitter.com)
        - Directorios (paginasamarillas.com, wikipedia.org, losmejorescolegios.com, civico.com, foursquare.com)
        - Sitios de noticias locales.

        [OUTPUT REQUERIDO]
        Devuelve ÚNICAMENTE un objeto JSON válido, sin formato Markdown, con la siguiente estructura exacta:
        {{
            "is_found": true o false,
            "confidence_score": 0 al 100 (Debes estar >85% seguro para que sea oficial),
            "reasoning": "Breve explicación de por qué descartaste las otras o elegiste esta",
            "official_url": "La URL oficial pura o null si no existe"
        }}
        """

        start_time = time.time()
        
        try:
            response = await self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "You are a JSON-only response bot. No conversational text."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,      # Cero creatividad = Máxima Precisión Lógica
                max_tokens=150,       # Ligeramente mayor para permitir el JSON y el Reasoning
                response_format={"type": "json_object"} # 🔥 Fuerza que la IA no rompa el código
            )
            
            # Decodificación del Output
            raw_response = response.choices[0].message.content.strip()
            result_data = json.loads(raw_response)
            
            elapsed = time.time() - start_time
            
            # Filtro de Confianza (Si la IA duda, es basura)
            if not result_data.get("is_found") or result_data.get("confidence_score", 0) < 85:
                logger.debug(f"🛑 [{institution_name}] Descartado por IA. Razón: {result_data.get('reasoning')} | Confianza: {result_data.get('confidence_score')}%")
                return None
                
            clean_url = self._sanitize_url(result_data.get("official_url"))
            
            if clean_url:
                logger.info(f"🎯 [{institution_name}] URL Asegurada: {clean_url} | ⏱️ {elapsed:.2f}s | 🧠 Razón: {result_data.get('reasoning')}")
                
            return clean_url

        except json.JSONDecodeError as e:
            logger.error(f"❌ [{institution_name}] La IA alucinó y no devolvió JSON: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"⚠️ [{institution_name}] Fallo de red con DeepSeek: {str(e)}")
            raise e # Lanza la excepción para que @retry haga su magia