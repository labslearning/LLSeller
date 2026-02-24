import os
import logging
import asyncio
import time
from typing import Optional
from openai import AsyncOpenAI, RateLimitError, APITimeoutError, APIError
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# 1. Configuración de Logging Estructurado y Telemetría
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

# [NIVEL DIOS 1]: Cliente Asíncrono con Pool de Conexiones, Retries y Timeouts
api_key = os.getenv("DEEPSEEK_API_KEY")
client = AsyncOpenAI(
    api_key=api_key,
    base_url="https://api.deepseek.com",
    max_retries=3,  # Resiliencia: Si DeepSeek falla por red, reintenta 3 veces automáticamente
    timeout=30.0    # Evita que el hilo se quede colgado para siempre ahorrando RAM
) if api_key else None

# [NIVEL DIOS 2]: Ingeniería de Prompts Dinámica (Role-Based Routing)
def _get_dynamic_pain_point(role: str) -> str:
    """Asigna un dolor corporativo específico según el cargo del tomador de decisiones."""
    if not role:
        return "los procesos educativos manuales que quitan tiempo valioso"
        
    role_lower = str(role).lower()
    if "rector" in role_lower or "director" in role_lower or "gerente" in role_lower:
        return "la pérdida de competitividad frente a otros colegios y la falta de métricas consolidadas de rendimiento"
    elif "coordinador" in role_lower or "académico" in role_lower:
        return "la excesiva carga operativa de los profesores al evaluar y la falta de interactividad en clase"
    elif "tecnología" in role_lower or "sistemas" in role_lower or "it" in role_lower:
        return "la fragmentación de plataformas, altos costos de servidores y los problemas de integración de datos"
    
    return "los procesos educativos manuales que quitan tiempo valioso"

async def generate_b2b_email(contact_name: str, contact_role: str, institution_name: str) -> Optional[str]:
    """
    Generador IA Asíncrono de Nivel Empresarial.
    Implementa Role-Based Prompting, Telemetría de Tokens y Manejo Granular de Excepciones.
    """
    if not client:
        logger.error("[FATAL] DEEPSEEK_API_KEY no detectada. Abortando motor de IA.")
        return None

    # Detectar el dolor exacto para disparar la conversión
    pain_point = _get_dynamic_pain_point(contact_role)
    
    # [NIVEL DIOS 3]: Prompt Engineering de Alto Rendimiento (Preparación A/B Testing)
    prompt = f"""
    Actúa como el SDR (Sales Development Representative) B2B con mayor tasa de conversión de Silicon Valley.
    Tu prospecto es {contact_name}, quien tiene el cargo de '{contact_role}' en {institution_name}.
    
    OBJETIVO: Agendar una llamada de 10 minutos para presentar "Learning Labs".
    
    REGLAS DE ORO:
    1. Hook: Empieza abordando su principal reto directamente: {pain_point}.
    2. Solución: Menciona cómo Learning Labs elimina ese dolor específico con tecnología educativa.
    3. Fricción Cero: Cierra con un CTA de muy bajo compromiso (Ej: "¿Estás abierto a explorar esto la próxima semana?").
    4. Formato: Máximo 90 palabras. Nada de "Espero que estés bien". Directo a la yugular del valor.
    """

    start_time = time.time()
    
    try:
        logger.info(f"🧠 [IA ENGINE] Procesando pitch asíncrono para {contact_name} en {institution_name}...")
        
        # [NIVEL DIOS 4]: I/O No Bloqueante (Async) con Separación de Roles System/User
        response = await client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "Eres un experto en ventas B2B que escribe correos extremadamente concisos y persuasivos."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.6, # 0.6 es ideal para B2B: balance perfecto entre creatividad y lógica
        )
        
        email_content = response.choices[0].message.content.strip()
        
        # [NIVEL DIOS 5]: Telemetría de Costos y Latencia (Revenue Intelligence)
        latency = round(time.time() - start_time, 2)
        tokens = response.usage.total_tokens
        logger.info(f"[✓] Pitch generado en {latency}s | Costo: {tokens} tokens | Target: {institution_name}")
        
        return email_content
        
    except RateLimitError:
        logger.error(f"[X] [RATE LIMIT] DeepSeek saturado. (Se requiere backoff para {institution_name})")
    except APITimeoutError:
        logger.error(f"[X] [TIMEOUT] DeepSeek tardó más de 30s. Petición abortada para {institution_name}.")
    except APIError as e:
        logger.error(f"[X] [API ERROR] Fallo en el proveedor: {e}")
    except Exception as e:
        logger.error(f"[X] [ERROR CRÍTICO] {str(e)}")
        
    return None
