"""
================================================================================
[TRANSCENDENT GOD TIER ARCHITECTURE: OMEGA QUANTUM LEVIATHAN CLASS ∞]
MODULE: COSMIC INTELLIGENCE ANALYZER ULTRA - VERSION 99.9.9.9.9.OMEGA
MODE: HYPER-AGGRESSIVE EXTRACTION - ZERO DATA LEFT BEHIND
STANDARD: SILICON VALLEY / TEL AVIV / WADI / SHANGHAI / TOKYO / DUBLIN / LONDON
================================================================================

Este analizador extrae ABSOLUTAMENTE TODA la información de una institución:
- Datos de contacto (emails, teléfonos, WhatsApp, redes sociales)
- LMS y tecnología (Moodle, Canvas, Phidias, Schoolnet, etc.)
- Certificaciones (IB, Cambridge, Oxford, ISO, EFQM, etc.)
- Programas académicos (STEM, Robótica, Programación, Idiomas)
- Infraestructura (laboratorios, instalaciones, transporte)
- Extracurriculares (deportes, artes, clubes, campamentos)
- Convenios (universidades, empresas, ONGs)
- Rendimiento (ICFES, premios, ranking)
- MISIÓN, VISIÓN, VALORES, FILOSOFÍA EDUCATIVA
- TODO lo que aparezca en la página web
"""

import os
import json
import logging
import asyncio
import hashlib
import time
import re
import uuid
import sqlite3
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Set
from dataclasses import dataclass, field, asdict
from datetime import datetime
from collections import defaultdict
import traceback

try:
    from openai import AsyncOpenAI, RateLimitError, APITimeoutError, APIError
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# =========================================================
# TELEMETRÍA
# =========================================================
logger = logging.getLogger("Sovereign.CosmicAnalyzerUltra")

# =========================================================
# CONFIGURACIÓN GOD TIER
# =========================================================
MAX_TEXT_LENGTH = 100000  # 100KB - Capturar TODO el texto
MAX_HTML_LENGTH = 50000   # 50KB de HTML
TEMPERATURE = 0.01        # Casi cero - máxima precisión
MAX_TOKENS = 12000        # Respuestas muy largas
CACHE_TTL = 86400 * 7     # 7 días de cache
RETRY_ATTEMPTS = 5
TIMEOUT = 90               # 90 segundos para páginas grandes

# =========================================================
# CACHE PERSISTENTE (GOD TIER)
# =========================================================
CACHE_DIR = Path("/tmp/cosmic_ultra_cache")
CACHE_DIR.mkdir(exist_ok=True)
DB_PATH = CACHE_DIR / "cosmic_ultra_cache.db"

class QuantumUltraCache:
    """Cache persistente con SQLite y memoria RAM - GOD TIER OMEGA"""
    
    def __init__(self):
        self._memory_cache: Dict[str, Tuple[float, Any]] = {}
        self._init_db()
    
    def _init_db(self):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ultra_cache (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    timestamp REAL,
                    ttl INTEGER,
                    access_count INTEGER DEFAULT 0
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ultra_timestamp ON ultra_cache(timestamp)")
    
    def get(self, key: str) -> Optional[Any]:
        if key in self._memory_cache:
            timestamp, value = self._memory_cache[key]
            if time.time() - timestamp < CACHE_TTL:
                return value
            del self._memory_cache[key]
        
        try:
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.execute(
                    "SELECT value, timestamp FROM ultra_cache WHERE key = ?",
                    (key,)
                )
                row = cursor.fetchone()
                if row:
                    value, timestamp = row
                    if time.time() - timestamp < CACHE_TTL:
                        self._memory_cache[key] = (timestamp, json.loads(value))
                        conn.execute(
                            "UPDATE ultra_cache SET access_count = access_count + 1 WHERE key = ?",
                            (key,)
                        )
                        conn.commit()
                        return json.loads(value)
        except Exception as e:
            logger.debug(f"Ultra cache read error: {e}")
        return None
    
    def set(self, key: str, value: Any):
        try:
            serialized = json.dumps(value, ensure_ascii=False)
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO ultra_cache (key, value, timestamp, ttl) VALUES (?, ?, ?, ?)",
                    (key, serialized, time.time(), CACHE_TTL)
                )
                conn.commit()
            self._memory_cache[key] = (time.time(), value)
            if len(self._memory_cache) > 500:
                oldest = min(self._memory_cache.keys(), key=lambda k: self._memory_cache[k][0])
                del self._memory_cache[oldest]
        except Exception as e:
            logger.debug(f"Ultra cache write error: {e}")

ULTRA_CACHE = QuantumUltraCache()

# =========================================================
# DATA STRUCTURE ULTRA COMPLETA - TODOS LOS CAMPOS
# =========================================================
@dataclass
class InstitutionProfileUltra:
    """Perfil ultra-completo de la institución - NO DEJA NADA ATRÁS"""
    
    # ========== IDENTIFICACIÓN ==========
    name: str = ""
    city: str = ""
    country: str = ""
    website: str = ""
    foundation_year: str = ""
    
    # ========== CONTACTOS (TODOS) ==========
    emails: List[str] = field(default_factory=list)
    phones: List[str] = field(default_factory=list)
    whatsapp: List[str] = field(default_factory=list)
    social_media: Dict[str, str] = field(default_factory=dict)
    address: str = ""
    location_map: str = ""
    
    # ========== MISIÓN, VISIÓN, VALORES ==========
    mission: str = ""
    vision: str = ""
    values: List[str] = field(default_factory=list)
    educational_philosophy: str = ""
    institutional_horizon: str = ""  # Horizonte institucional
    educational_principles: List[str] = field(default_factory=list)
    
    # ========== ACADÉMICO ==========
    calendar: str = ""
    levels_offered: List[str] = field(default_factory=list)
    pedagogical_model: str = ""
    academic_emphasis: str = ""
    languages_taught: List[str] = field(default_factory=list)
    is_bilingual: bool = False
    is_trilingual: bool = False
    language_levels: Dict[str, str] = field(default_factory=dict)
    international_programs: List[str] = field(default_factory=list)
    
    # ========== TECNOLOGÍA (TODA) ==========
    lms_provider: str = ""
    lms_version: str = ""
    lms_confidence: float = 0.0
    tech_stack: Dict[str, Any] = field(default_factory=dict)
    digital_platforms: List[str] = field(default_factory=list)
    has_robotics: bool = False
    robotics_details: Dict[str, Any] = field(default_factory=dict)
    has_programming: bool = False
    programming_details: Dict[str, Any] = field(default_factory=dict)
    has_stem: bool = False
    stem_details: Dict[str, Any] = field(default_factory=dict)
    laboratories: List[str] = field(default_factory=list)
    classroom_tech: List[str] = field(default_factory=list)
    wifi_available: bool = False
    virtual_platform: str = ""
    
    # ========== CERTIFICACIONES (TODAS) ==========
    ib: Dict[str, Any] = field(default_factory=lambda: {
        "has_ib": False,
        "programs": [],
        "since": "",
        "coordinator": "",
        "authorization_date": ""
    })
    cambridge: Dict[str, Any] = field(default_factory=lambda: {
        "has_cambridge": False,
        "exams": [],
        "preparation_center": False,
        "since": "",
        "center_number": ""
    })
    oxford: Dict[str, Any] = field(default_factory=lambda: {"has_oxford": False})
    toefl: Dict[str, Any] = field(default_factory=lambda: {"has_toefl": False})
    ielts: Dict[str, Any] = field(default_factory=lambda: {"has_ielts": False})
    
    # Certificaciones de calidad
    iso_9001: bool = False
    iso_14001: bool = False
    efqm: bool = False
    great_place_to_study: bool = False
    other_certifications: List[str] = field(default_factory=list)
    
    # Certificaciones Colombianas
    men_resolution: str = ""
    icfes_registration: str = ""
    high_quality_accreditation: bool = False
    
    # ========== CONVENIOS Y ALIANZAS ==========
    university_agreements: List[str] = field(default_factory=list)
    corporate_agreements: List[str] = field(default_factory=list)
    international_agreements: List[str] = field(default_factory=list)
    ngo_agreements: List[str] = field(default_factory=list)
    government_programs: List[str] = field(default_factory=list)
    
    # ========== RENDIMIENTO ACADÉMICO ==========
    icfes_results: Dict[str, Any] = field(default_factory=dict)
    icfes_score: str = ""
    icfes_category: str = ""
    icfes_ranking: str = ""
    icfes_year: str = ""
    awards: List[str] = field(default_factory=list)
    recognitions: List[str] = field(default_factory=list)
    notable_alumni: List[str] = field(default_factory=list)
    university_admission_rate: str = ""
    top_universities: List[str] = field(default_factory=list)
    
    # ========== INFRAESTRUCTURA ==========
    campus_size: str = ""
    campus_locations: List[str] = field(default_factory=list)
    buildings: List[str] = field(default_factory=list)
    classrooms: int = 0
    laboratories_list: List[str] = field(default_factory=list)
    sports_facilities: List[str] = field(default_factory=list)
    library: Dict[str, Any] = field(default_factory=dict)
    dining: Dict[str, Any] = field(default_factory=dict)
    transportation: Dict[str, Any] = field(default_factory=dict)
    green_areas: bool = False
    accessibility: List[str] = field(default_factory=list)
    security_measures: List[str] = field(default_factory=list)
    
    # ========== EXTRACURRICULARES ==========
    sports: List[str] = field(default_factory=list)
    arts: List[str] = field(default_factory=list)
    clubs: List[str] = field(default_factory=list)
    camps: List[str] = field(default_factory=list)
    competitions: List[str] = field(default_factory=list)
    community_service: bool = False
    volunteer_programs: List[str] = field(default_factory=list)
    student_government: bool = False
    publications: List[str] = field(default_factory=list)
    
    # ========== PROYECTOS ESPECIALES ==========
    special_projects: List[str] = field(default_factory=list)
    innovation_initiatives: List[str] = field(default_factory=list)
    sustainability_programs: List[str] = field(default_factory=list)
    inclusion_programs: List[str] = field(default_factory=list)
    
    # ========== DATOS DEMOGRÁFICOS ==========
    student_count: int = 0
    teacher_count: int = 0
    student_teacher_ratio: str = ""
    average_class_size: int = 0
    
    # ========== ADMISIONES ==========
    admission_requirements: List[str] = field(default_factory=list)
    admission_process: str = ""
    scholarships: List[str] = field(default_factory=list)
    tuition_range: str = ""
    
    # ========== ANÁLISIS ESTRATÉGICO ==========
    swot_analysis: Dict[str, List[str]] = field(default_factory=dict)
    pain_points: List[str] = field(default_factory=list)
    sales_triggers: List[str] = field(default_factory=list)
    opportunities: List[str] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)
    ideal_contact: str = ""
    recommended_approach: str = ""
    sales_priority: str = "Medium"
    estimated_revenue_potential: str = ""
    
    # ========== METADATOS ==========
    executive_summary: str = ""
    confidence_score: float = 0.0
    extraction_completeness: float = 0.0
    analysis_timestamp: float = 0.0
    trace_id: str = ""
    raw_text_length: int = 0
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def to_markdown(self) -> str:
        """Genera reporte Markdown ultra-detallado"""
        
        md = f"""
# 🌌 COSMIC INTELLIGENCE REPORT - ULTRA EDITION
## {self.name}
### {self.city}, {self.country}

---

## 🎯 EXECUTIVE SUMMARY
{self.executive_summary or "No summary available."}

---

## 📞 CONTACT INFORMATION

### 📧 Emails
{self._format_list(self.emails)}

### 📞 Phones
{self._format_list(self.phones)}

### 💬 WhatsApp
{self._format_list(self.whatsapp)}

### 🌐 Social Media
{self._format_dict(self.social_media)}

### 📍 Address
{self.address or "Not specified"}

---

## 🏛️ INSTITUTIONAL IDENTITY

### Mission
{self.mission or "Not specified"}

### Vision
{self.vision or "Not specified"}

### Values
{self._format_list(self.values)}

### Educational Philosophy
{self.educational_philosophy or "Not specified"}

### Foundation Year
{self.foundation_year or "Not specified"}

---

## 📚 ACADEMIC PROFILE

| Field | Value |
|-------|-------|
| **Calendar** | {self.calendar or "Not specified"} |
| **Levels Offered** | {', '.join(self.levels_offered) or "Not specified"} |
| **Pedagogical Model** | {self.pedagogical_model or "Not specified"} |
| **Academic Emphasis** | {self.academic_emphasis or "Not specified"} |
| **Languages Taught** | {', '.join(self.languages_taught) or "Not specified"} |
| **Bilingual** | {'✅' if self.is_bilingual else '❌'} |
| **Trilingual** | {'✅' if self.is_trilingual else '❌'} |

---

## 🏆 CERTIFICATIONS & ACCREDITATIONS

### 🌍 International Certifications

| Certification | Status | Details |
|---------------|--------|---------|
| **IB** | {'✅' if self.ib.get('has_ib') else '❌'} | {', '.join(self.ib.get('programs', []))} |
| **Cambridge** | {'✅' if self.cambridge.get('has_cambridge') else '❌'} | {', '.join(self.cambridge.get('exams', []))} |
| **Oxford** | {'✅' if self.oxford.get('has_oxford') else '❌'} | - |
| **TOEFL** | {'✅' if self.toefl.get('has_toefl') else '❌'} | - |
| **IELTS** | {'✅' if self.ielts.get('has_ielts') else '❌'} | - |

### 🏅 Quality Certifications
- **ISO 9001**: {'✅' if self.iso_9001 else '❌'}
- **ISO 14001**: {'✅' if self.iso_14001 else '❌'}
- **EFQM**: {'✅' if self.efqm else '❌'}
- **Great Place to Study**: {'✅' if self.great_place_to_study else '❌'}

### 🇨🇴 Colombian Certifications
- **MEN Resolution**: {self.men_resolution or "Not specified"}
- **ICFES Registration**: {self.icfes_registration or "Not specified"}
- **High Quality Accreditation**: {'✅' if self.high_quality_accreditation else '❌'}

### Other Certifications
{self._format_list(self.other_certifications)}

---

## 🤖 TECHNOLOGY & INNOVATION

### LMS & Digital Platforms
| Aspect | Details |
|--------|---------|
| **LMS Provider** | {self.lms_provider or "Not detected"} |
| **LMS Version** | {self.lms_version or "Not specified"} |
| **Confidence** | {self.lms_confidence:.1%} |
| **Digital Platforms** | {', '.join(self.digital_platforms) or "None"} |

### Robotics
- **Status**: {'✅' if self.has_robotics else '❌'}
- **Details**: {json.dumps(self.robotics_details, indent=2) if self.robotics_details else "Not specified"}

### Programming
- **Status**: {'✅' if self.has_programming else '❌'}
- **Details**: {json.dumps(self.programming_details, indent=2) if self.programming_details else "Not specified"}

### STEM
- **Status**: {'✅' if self.has_stem else '❌'}
- **Details**: {json.dumps(self.stem_details, indent=2) if self.stem_details else "Not specified"}

### Laboratories
{self._format_list(self.laboratories)}

### Classroom Technology
{self._format_list(self.classroom_tech)}

### WiFi Available
{'✅' if self.wifi_available else '❌'}

### Virtual Platform
{self.virtual_platform or "Not specified"}

---

## 📊 PERFORMANCE & ACHIEVEMENTS

### ICFES Results
| Metric | Value |
|--------|-------|
| **Score** | {self.icfes_score or "Not available"} |
| **Category** | {self.icfes_category or "Not available"} |
| **Ranking** | {self.icfes_ranking or "Not available"} |
| **Year** | {self.icfes_year or "Not available"} |

### Awards & Recognitions
{self._format_list(self.awards)}

### Notable Alumni
{self._format_list(self.notable_alumni)}

### University Admission Rate
{self.university_admission_rate or "Not specified"}

### Top Universities (Graduation Destinations)
{self._format_list(self.top_universities)}

---

## 🏛️ INFRASTRUCTURE

| Aspect | Details |
|--------|---------|
| **Campus Size** | {self.campus_size or "Not specified"} |
| **Campus Locations** | {', '.join(self.campus_locations) or "Not specified"} |
| **Buildings** | {', '.join(self.buildings) or "Not specified"} |
| **Classrooms** | {self.classrooms or "Not specified"} |
| **Laboratories** | {', '.join(self.laboratories_list) or "Not specified"} |
| **Sports Facilities** | {', '.join(self.sports_facilities) or "Not specified"} |
| **Green Areas** | {'✅' if self.green_areas else '❌'} |

### Library
{self._format_dict(self.library)}

### Dining
{self._format_dict(self.dining)}

### Transportation
{self._format_dict(self.transportation)}

### Accessibility
{self._format_list(self.accessibility)}

### Security Measures
{self._format_list(self.security_measures)}

---

## 🎪 EXTRACURRICULAR ACTIVITIES

| Category | Activities |
|----------|------------|
| **Sports** | {', '.join(self.sports) or "None"} |
| **Arts** | {', '.join(self.arts) or "None"} |
| **Clubs** | {', '.join(self.clubs) or "None"} |
| **Camps** | {', '.join(self.camps) or "None"} |
| **Competitions** | {', '.join(self.competitions) or "None"} |

### Community Service
{'✅' if self.community_service else '❌'}

### Volunteer Programs
{self._format_list(self.volunteer_programs)}

### Student Government
{'✅' if self.student_government else '❌'}

### Publications
{self._format_list(self.publications)}

---

## 🤝 AGREEMENTS & PARTNERSHIPS

### University Agreements
{self._format_list(self.university_agreements)}

### Corporate Agreements
{self._format_list(self.corporate_agreements)}

### International Agreements
{self._format_list(self.international_agreements)}

### NGO Agreements
{self._format_list(self.ngo_agreements)}

### Government Programs
{self._format_list(self.government_programs)}

---

## 💼 SALES INTELLIGENCE

### 🔴 Pain Points
{self._format_list(self.pain_points)}

### 🟢 Sales Triggers
{self._format_list(self.sales_triggers)}

### 🚀 Opportunities
{self._format_list(self.opportunities)}

### ⚠️ Risks
{self._format_list(self.risks)}

### 👤 Ideal Contact
{self.ideal_contact or "Not specified"}

### 💰 Estimated Revenue Potential
{self.estimated_revenue_potential or "Not specified"}

### 🎯 Recommended Approach
{self.recommended_approach or "Not specified"}

### 📊 Sales Priority
{self.sales_priority}

---

## 📈 METADATA
- **Confidence Score**: {self.confidence_score:.1%}
- **Extraction Completeness**: {self.extraction_completeness:.1%}
- **Raw Text Length**: {self.raw_text_length:,} characters
- **Analysis Date**: {datetime.fromtimestamp(self.analysis_timestamp).strftime('%Y-%m-%d %H:%M:%S') if self.analysis_timestamp else "N/A"}
- **Trace ID**: {self.trace_id}

---

*Report generated by Cosmic Intelligence Engine ULTRA v99.9.9.9.9.OMEGA*
*Powered by DeepSeek AI - Hyper-Aggressive Extraction Mode*
"""
        return md
    
    def _format_list(self, items: List[str]) -> str:
        if not items:
            return "None"
        return "\n".join([f"- {item}" for item in items])
    
    def _format_dict(self, data: Dict) -> str:
        if not data:
            return "None"
        return "\n".join([f"- **{k}**: {v}" for k, v in data.items()])


# =========================================================
# EL ANALIZADOR ULTRA - CORAZÓN DEL SISTEMA
# =========================================================
class CosmicAnalyzerUltra:
    """
    Analizador ultra-agresivo que extrae ABSOLUTAMENTE TODO de la página web.
    Utiliza DeepSeek con un prompt masivo que busca cada detalle.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        
        self.api_key = os.getenv("DEEPSEEK_API_KEY")
        self.client = None
        
        if self.api_key and OPENAI_AVAILABLE:
            self.client = AsyncOpenAI(
                api_key=self.api_key,
                base_url="https://api.deepseek.com",
                timeout=TIMEOUT,
                max_retries=0
            )
            logger.info("✅ Cosmic Analyzer Ultra initialized with DeepSeek")
        else:
            logger.warning("⚠️ No API key found. AI analysis disabled.")
    
    def _generate_cache_key(self, name: str, city: str, country: str, text_hash: str) -> str:
        raw = f"ultra_{name.lower()}_{city.lower()}_{country.lower()}_{text_hash}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]
    
    async def analyze(
        self,
        name: str,
        city: str,
        country: str,
        webpage_text: str,
        raw_html: Optional[str] = None,
        extracted_data: Optional[Dict] = None
    ) -> InstitutionProfileUltra:
        """
        Análisis ultra-agresivo - Extrae TODO lo que encuentra
        """
        trace_id = uuid.uuid4().hex[:12]
        start_time = time.perf_counter()
        
        profile = InstitutionProfileUltra(
            name=name,
            city=city,
            country=country,
            analysis_timestamp=time.time(),
            trace_id=trace_id,
            raw_text_length=len(webpage_text or "")
        )
        
        logger.info(f"🌌 ULTRA [{trace_id}] Starting cosmic analysis: {name} | {city}, {country}")
        logger.info(f"📄 Text length: {len(webpage_text or 0):,} chars")
        
        if not self.client:
            profile.executive_summary = "⚠️ AI analysis unavailable (API not configured)"
            profile.confidence_score = 0.0
            return profile
        
        if not webpage_text or len(webpage_text.strip()) < 200:
            profile.executive_summary = f"⚠️ Insufficient text for analysis ({len(webpage_text or 0)} chars)"
            profile.confidence_score = 0.1
            return profile
        
        # Generar cache key
        text_hash = hashlib.md5(webpage_text[:10000].encode()).hexdigest()
        cache_key = self._generate_cache_key(name, city, country, text_hash)
        
        # Verificar cache
        cached = ULTRA_CACHE.get(cache_key)
        if cached:
            logger.info(f"⚡ ULTRA [{trace_id}] Cache HIT for {name}")
            for key, value in cached.items():
                if hasattr(profile, key):
                    setattr(profile, key, value)
            elapsed = (time.perf_counter() - start_time) * 1000
            logger.info(f"✅ ULTRA [{trace_id}] Analysis from cache in {elapsed:.0f}ms")
            return profile
        
        # Preparar texto
        safe_text = webpage_text[:MAX_TEXT_LENGTH]
        safe_html = (raw_html or "")[:MAX_HTML_LENGTH]
        
        # Construir prompt ultra-detallado
        prompt = self._build_ultra_prompt(name, city, country, safe_text, safe_html, extracted_data)
        
        try:
            logger.info(f"🧠 ULTRA [{trace_id}] Sending request to DeepSeek API...")
            
            async def api_call():
                return await self.client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {"role": "system", "content": self._get_system_prompt()},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=TEMPERATURE,
                    max_tokens=MAX_TOKENS,
                    response_format={"type": "json_object"}
                )
            
            # Retry con backoff
            response = None
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    response = await api_call()
                    break
                except Exception as e:
                    if attempt == RETRY_ATTEMPTS - 1:
                        raise
                    wait_time = 2 ** attempt
                    logger.warning(f"Retry {attempt + 1}/{RETRY_ATTEMPTS} after {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
            
            raw_response = response.choices[0].message.content.strip()
            
            # Limpiar posibles marcadores
            if raw_response.startswith('```json'):
                raw_response = raw_response[7:]
            if raw_response.endswith('```'):
                raw_response = raw_response[:-3]
            
            data = json.loads(raw_response)
            
            # Poblar el perfil
            self._populate_profile(profile, data)
            
            # Calcular métricas
            profile.confidence_score = self._calculate_confidence(data, len(webpage_text))
            profile.extraction_completeness = self._calculate_completeness(data)
            
            # Guardar en cache
            ULTRA_CACHE.set(cache_key, profile.to_dict())
            
            elapsed = (time.perf_counter() - start_time) * 1000
            logger.info(f"✅ ULTRA [{trace_id}] Analysis complete | Confidence: {profile.confidence_score:.1%} | Completeness: {profile.extraction_completeness:.1%} | {elapsed:.0f}ms")
            
        except Exception as e:
            logger.error(f"❌ ULTRA [{trace_id}] Analysis failed: {e}")
            traceback.print_exc()
            profile.executive_summary = f"❌ Analysis failed: {str(e)[:200]}"
            profile.confidence_score = 0.0
        
        return profile
    
    def _get_system_prompt(self) -> str:
        return """You are the world's most advanced educational intelligence analyst - ULTRA EDITION.
Your mission: Extract EVERY piece of information from the institution's website.
Be OBSESSIVELY DETAILED. Leave NO stone unturned.
Output ONLY valid JSON. No markdown, no explanatory text.
Use empty arrays [] for missing data, empty strings "" for missing text.
Be AGGRESSIVE in extraction - if something is mentioned, capture it."""

    def _build_ultra_prompt(self, name: str, city: str, country: str, text: str, html: str, extracted: Dict) -> str:
        """Construye el prompt más detallado del mundo"""
        
        extracted_section = ""
        if extracted:
            extracted_section = f"""
PRE-VERIFIED DATA (ALREADY EXTRACTED - USE THIS):
- LMS: {extracted.get('lms_provider', 'Not detected')}
- Emails: {', '.join(extracted.get('emails', [])[:10])}
- Phones: {', '.join(extracted.get('phones', [])[:10])}
- WhatsApp: {', '.join(extracted.get('whatsapp', [])[:10])}
"""
        
        return f"""
TARGET INSTITUTION: {name}
LOCATION: {city}, {country}

{extracted_section}

==================== RAW WEBPAGE TEXT ====================
{text[:45000]}

==================== HTML CONTEXT (if available) ====================
{html[:15000]}

==================== YOUR MISSION ====================
Extract EVERY SINGLE piece of information from this institution's website.
Be OBSESSIVELY DETAILED. If something is mentioned, capture it.

=== WHAT TO EXTRACT (COMPLETE LIST) ===

1. CONTACT INFORMATION (ALL):
   - ALL email addresses (include every single email found)
   - ALL phone numbers (include every number)
   - ALL WhatsApp numbers
   - ALL social media links (Facebook, Instagram, LinkedIn, Twitter/X, YouTube, TikTok)
   - Complete physical address
   - Google Maps links or coordinates

2. INSTITUTIONAL IDENTITY:
   - Mission statement (complete text)
   - Vision statement (complete text)
   - Values (list all values mentioned)
   - Educational philosophy
   - Foundation year
   - Institutional horizon

3. ACADEMIC PROFILE:
   - Calendar type (A or B)
   - ALL levels offered (Preescolar, Primaria, Bachillerato, etc.)
   - Pedagogical model
   - Academic emphasis/approach
   - Languages taught (which languages, what levels)
   - Bilingual status (yes/no, which languages)
   - Trilingual status (yes/no, which languages)
   - International programs

4. TECHNOLOGY & LMS (COMPLETE):
   - LMS provider (Moodle, Canvas, Phidias, Schoolnet, etc.)
   - LMS version if visible
   - Other digital platforms used
   - Robotics program (details: type, platforms, competitions, achievements)
   - Programming program (languages taught, frameworks, grade levels)
   - STEM program (details, projects)
   - Laboratories (list all types: robotics, science, computers, etc.)
   - Classroom technology (smartboards, tablets, etc.)
   - WiFi availability
   - Virtual learning platforms

5. CERTIFICATIONS & ACCREDITATIONS (ALL):
   - IB (International Baccalaureate): has_ib, programs (PYP/MYP/DP), since, coordinator
   - Cambridge: has_cambridge, exams (PET, FCE, CAE, CPE, KET), preparation_center, center_number
   - Oxford: has_oxford
   - TOEFL: has_toefl
   - IELTS: has_ielts
   - ISO 9001
   - ISO 14001
   - EFQM
   - Great Place to Study
   - MEN resolution number
   - ICFES registration
   - High Quality Accreditation
   - Any other certifications

6. AGREEMENTS & PARTNERSHIPS:
   - University agreements (list all universities mentioned)
   - Corporate agreements (list all companies)
   - International agreements
   - NGO agreements
   - Government programs

7. PERFORMANCE & RESULTS:
   - ICFES scores (numeric value, category, ranking, year)
   - Awards and recognitions
   - Notable alumni
   - University admission rate
   - Top universities where graduates go

8. INFRASTRUCTURE:
   - Campus size
   - Number of campuses/locations
   - Buildings
   - Number of classrooms
   - Laboratories (detailed list)
   - Sports facilities (fields, courts, gym, pool, etc.)
   - Library (size, resources, digital access)
   - Dining (cafeteria, restaurant, meal plans)
   - Transportation (school bus, routes)
   - Green areas
   - Accessibility features
   - Security measures

9. EXTRACURRICULAR ACTIVITIES:
   - Sports (list all sports)
   - Arts (music, theater, dance, painting, etc.)
   - Clubs (robotics, chess, debate, etc.)
   - Camps (summer, winter, language camps)
   - Competitions participated/won
   - Community service programs
   - Volunteer opportunities
   - Student government
   - Publications (newspaper, magazine, yearbook)

10. DEMOGRAPHICS:
    - Number of students
    - Number of teachers
    - Student-teacher ratio
    - Average class size

11. ADMISSIONS:
    - Admission requirements
    - Admission process
    - Scholarships available
    - Tuition range

12. SALES INTELLIGENCE (Strategic Analysis):
    - Pain points (problems the institution might have that our solution can solve)
    - Sales triggers (what indicates they might need our solution)
    - Opportunities (where we can add value)
    - Risks (potential obstacles)
    - Ideal contact person (role/title)
    - Recommended sales approach
    - Estimated revenue potential (Low/Medium/High)

13. EXECUTIVE SUMMARY:
    - 2-3 sentence summary for the sales team

=== OUTPUT FORMAT (STRICT JSON) ===
{{
    "emails": ["email1", "email2"],
    "phones": ["phone1", "phone2"],
    "whatsapp": ["wa1", "wa2"],
    "social_media": {{"facebook": "url", "instagram": "url", "linkedin": "url"}},
    "address": "full address",
    "mission": "complete mission statement",
    "vision": "complete vision statement",
    "values": ["value1", "value2"],
    "educational_philosophy": "philosophy text",
    "foundation_year": "year",
    "calendar": "A or B",
    "levels_offered": ["Preescolar", "Primaria", "Bachillerato"],
    "pedagogical_model": "model name",
    "academic_emphasis": "emphasis description",
    "languages_taught": ["Español", "Inglés", "Francés"],
    "is_bilingual": true/false,
    "is_trilingual": true/false,
    "lms_provider": "Moodle/Canvas/Phidias/etc",
    "lms_version": "version if found",
    "lms_confidence": 0.0,
    "digital_platforms": ["Google Classroom", "Microsoft Teams"],
    "has_robotics": true/false,
    "robotics_details": {{"type": "LEGO Education", "platforms": ["LEGO Mindstorms"], "competitions": ["FIRST LEGO League"], "achievements": ["Campeones 2023"]}},
    "has_programming": true/false,
    "programming_details": {{"languages": ["Python", "Scratch"], "grade_levels": ["5° a 11°"]}},
    "has_stem": true/false,
    "stem_details": {{"programs": ["STEM Lab", "Science Fair"]}},
    "laboratories": ["Robótica", "Ciencias", "Computación"],
    "classroom_tech": ["Smartboards", "Tablets"],
    "wifi_available": true/false,
    "ib": {{"has_ib": true/false, "programs": ["PYP", "MYP", "DP"], "since": "2020"}},
    "cambridge": {{"has_cambridge": true/false, "exams": ["PET", "FCE"], "preparation_center": true, "center_number": "CO123"}},
    "oxford": {{"has_oxford": true/false}},
    "toefl": {{"has_toefl": true/false}},
    "ielts": {{"has_ielts": true/false}},
    "iso_9001": true/false,
    "iso_14001": true/false,
    "efqm": true/false,
    "great_place_to_study": true/false,
    "other_certifications": ["cert1", "cert2"],
    "men_resolution": "resolution number",
    "icfes_registration": "registration number",
    "high_quality_accreditation": true/false,
    "university_agreements": ["Universidad de los Andes", "Universidad Javeriana"],
    "corporate_agreements": ["Microsoft", "LEGO Education"],
    "international_agreements": ["Exchange with Spain"],
    "icfes_score": "78",
    "icfes_category": "A+",
    "icfes_ranking": "Top 10%",
    "icfes_year": "2023",
    "awards": ["Premio a la Excelencia"],
    "notable_alumni": ["Person Name"],
    "university_admission_rate": "85%",
    "top_universities": ["Universidad de los Andes"],
    "campus_size": "20,000 m²",
    "campus_locations": ["Sede principal", "Sede norte"],
    "classrooms": 50,
    "sports_facilities": ["Cancha de fútbol", "Gimnasio", "Piscina"],
    "library": {{"size": "5,000 volumes", "digital_access": true}},
    "dining": {{"has_cafeteria": true, "meal_plans": true}},
    "transportation": {{"has_school_bus": true, "routes": ["Norte", "Sur"]}},
    "green_areas": true,
    "sports": ["Fútbol", "Baloncesto", "Natación"],
    "arts": ["Música", "Teatro", "Danza"],
    "clubs": ["Ajedrez", "Robótica", "Debate"],
    "camps": ["Campamento de inglés", "Campamento de verano"],
    "competitions": ["FIRST LEGO League", "Olimpiadas de Matemáticas"],
    "community_service": true,
    "student_government": true,
    "student_count": 1200,
    "teacher_count": 80,
    "student_teacher_ratio": "15:1",
    "average_class_size": 25,
    "admission_requirements": ["Entrevista", "Prueba de admisión"],
    "scholarships": ["Excelencia académica", "Becas deportivas"],
    "tuition_range": "$5,000,000 - $8,000,000 COP",
    "pain_points": ["Falta de plataforma digital unificada", "Sistemas obsoletos"],
    "sales_triggers": ["Expansión de campus", "Certificación IB reciente"],
    "opportunities": ["Migración a LMS moderno", "Implementación de robótica"],
    "risks": ["Presupuesto limitado", "Competencia local"],
    "ideal_contact": "Rector o Director de Tecnología",
    "recommended_approach": "Demostración técnica seguida de propuesta económica",
    "estimated_revenue_potential": "Alto",
    "sales_priority": "High/Medium/Low",
    "executive_summary": "Resumen ejecutivo para el equipo de ventas"
}}

EXTRACT EVERYTHING YOU FIND. BE THOROUGH. LEAVE NOTHING OUT.
"""

    def _populate_profile(self, profile: InstitutionProfileUltra, data: Dict):
        """Pobla el perfil con los datos extraídos"""
        
        # Contactos
        profile.emails = data.get('emails', [])
        profile.phones = data.get('phones', [])
        profile.whatsapp = data.get('whatsapp', [])
        profile.social_media = data.get('social_media', {})
        profile.address = data.get('address', '')
        
        # Identidad
        profile.mission = data.get('mission', '')
        profile.vision = data.get('vision', '')
        profile.values = data.get('values', [])
        profile.educational_philosophy = data.get('educational_philosophy', '')
        profile.foundation_year = data.get('foundation_year', '')
        
        # Académico
        profile.calendar = data.get('calendar', '')
        profile.levels_offered = data.get('levels_offered', [])
        profile.pedagogical_model = data.get('pedagogical_model', '')
        profile.academic_emphasis = data.get('academic_emphasis', '')
        profile.languages_taught = data.get('languages_taught', [])
        profile.is_bilingual = data.get('is_bilingual', False)
        profile.is_trilingual = data.get('is_trilingual', False)
        
        # Tecnología
        profile.lms_provider = data.get('lms_provider', '')
        profile.lms_version = data.get('lms_version', '')
        profile.lms_confidence = data.get('lms_confidence', 0.0)
        profile.digital_platforms = data.get('digital_platforms', [])
        profile.has_robotics = data.get('has_robotics', False)
        profile.robotics_details = data.get('robotics_details', {})
        profile.has_programming = data.get('has_programming', False)
        profile.programming_details = data.get('programming_details', {})
        profile.has_stem = data.get('has_stem', False)
        profile.stem_details = data.get('stem_details', {})
        profile.laboratories = data.get('laboratories', [])
        profile.classroom_tech = data.get('classroom_tech', [])
        profile.wifi_available = data.get('wifi_available', False)
        
        # Certificaciones
        profile.ib = data.get('ib', profile.ib)
        profile.cambridge = data.get('cambridge', profile.cambridge)
        profile.oxford = data.get('oxford', profile.oxford)
        profile.toefl = data.get('toefl', profile.toefl)
        profile.ielts = data.get('ielts', profile.ielts)
        profile.iso_9001 = data.get('iso_9001', False)
        profile.iso_14001 = data.get('iso_14001', False)
        profile.efqm = data.get('efqm', False)
        profile.great_place_to_study = data.get('great_place_to_study', False)
        profile.other_certifications = data.get('other_certifications', [])
        profile.men_resolution = data.get('men_resolution', '')
        profile.icfes_registration = data.get('icfes_registration', '')
        profile.high_quality_accreditation = data.get('high_quality_accreditation', False)
        
        # Convenios
        profile.university_agreements = data.get('university_agreements', [])
        profile.corporate_agreements = data.get('corporate_agreements', [])
        profile.international_agreements = data.get('international_agreements', [])
        
        # Rendimiento
        profile.icfes_score = data.get('icfes_score', '')
        profile.icfes_category = data.get('icfes_category', '')
        profile.icfes_ranking = data.get('icfes_ranking', '')
        profile.icfes_year = data.get('icfes_year', '')
        profile.awards = data.get('awards', [])
        profile.notable_alumni = data.get('notable_alumni', [])
        profile.university_admission_rate = data.get('university_admission_rate', '')
        profile.top_universities = data.get('top_universities', [])
        
        # Infraestructura
        profile.campus_size = data.get('campus_size', '')
        profile.campus_locations = data.get('campus_locations', [])
        profile.classrooms = data.get('classrooms', 0)
        profile.sports_facilities = data.get('sports_facilities', [])
        profile.laboratories_list = data.get('laboratories', [])
        profile.library = data.get('library', {})
        profile.dining = data.get('dining', {})
        profile.transportation = data.get('transportation', {})
        profile.green_areas = data.get('green_areas', False)
        
        # Extracurriculares
        profile.sports = data.get('sports', [])
        profile.arts = data.get('arts', [])
        profile.clubs = data.get('clubs', [])
        profile.camps = data.get('camps', [])
        profile.competitions = data.get('competitions', [])
        profile.community_service = data.get('community_service', False)
        profile.student_government = data.get('student_government', False)
        
        # Demografía
        profile.student_count = data.get('student_count', 0)
        profile.teacher_count = data.get('teacher_count', 0)
        profile.student_teacher_ratio = data.get('student_teacher_ratio', '')
        profile.average_class_size = data.get('average_class_size', 0)
        
        # Admisiones
        profile.admission_requirements = data.get('admission_requirements', [])
        profile.scholarships = data.get('scholarships', [])
        profile.tuition_range = data.get('tuition_range', '')
        
        # Inteligencia de ventas
        profile.pain_points = data.get('pain_points', [])
        profile.sales_triggers = data.get('sales_triggers', [])
        profile.opportunities = data.get('opportunities', [])
        profile.risks = data.get('risks', [])
        profile.ideal_contact = data.get('ideal_contact', '')
        profile.recommended_approach = data.get('recommended_approach', '')
        profile.estimated_revenue_potential = data.get('estimated_revenue_potential', '')
        profile.sales_priority = data.get('sales_priority', 'Medium')
        
        # Resumen ejecutivo
        profile.executive_summary = data.get('executive_summary', '')
    
    def _calculate_confidence(self, data: Dict, text_length: int) -> float:
        """Calcula la confianza del análisis"""
        if not data:
            return 0.0
        
        total = 0
        filled = 0
        
        def count_fields(obj, depth=0):
            nonlocal total, filled
            if depth > 3:
                return
            if isinstance(obj, dict):
                for k, v in obj.items():
                    total += 1
                    if v:
                        if isinstance(v, (list, dict)):
                            if v:
                                filled += 1
                        elif v not in (None, "", False):
                            filled += 1
                    if isinstance(v, (dict, list)):
                        count_fields(v, depth + 1)
        
        count_fields(data)
        field_score = filled / max(total, 1) if total > 0 else 0
        text_factor = min(1.0, text_length / 10000)
        
        return (field_score * 0.6) + (text_factor * 0.4)
    
    def _calculate_completeness(self, data: Dict) -> float:
        """Calcula la completitud de la extracción"""
        if not data:
            return 0.0
        
        critical_fields = [
            "emails", "phones", "mission", "vision", "levels_offered",
            "lms_provider", "has_robotics", "icfes_score", "executive_summary"
        ]
        
        completed = 0
        for field in critical_fields:
            value = data.get(field)
            if value:
                if isinstance(value, list) and value:
                    completed += 1
                elif value not in (None, "", False):
                    completed += 1
        
        return completed / len(critical_fields)


# =================================================================================
# EXPORT FUNCTIONS
# =================================================================================
_analyzer_ultra = None

def get_analyzer_ultra() -> CosmicAnalyzerUltra:
    global _analyzer_ultra
    if _analyzer_ultra is None:
        _analyzer_ultra = CosmicAnalyzerUltra()
    return _analyzer_ultra

async def analyze_institution_ultra(
    name: str,
    city: str,
    country: str,
    webpage_text: str,
    raw_html: Optional[str] = None,
    extracted_data: Optional[Dict] = None
) -> InstitutionProfileUltra:
    analyzer = get_analyzer_ultra()
    return await analyzer.analyze(name, city, country, webpage_text, raw_html, extracted_data)

def analyze_institution_ultra_sync(
    name: str,
    city: str,
    country: str,
    webpage_text: str,
    raw_html: Optional[str] = None,
    extracted_data: Optional[Dict] = None
) -> InstitutionProfileUltra:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(
            analyze_institution_ultra(name, city, country, webpage_text, raw_html, extracted_data)
        )
    finally:
        loop.close()


# =================================================================================
# SELF-TEST
# =================================================================================
async def self_test():
    logger.info("🧪 Running Ultra self-test...")
    
    test_text = """
    Colegio Gimnasio Los Arrayanes
    Fundado en 1995
    Misión: Formar líderes integrales con excelencia académica y valores humanos.
    Visión: Ser reconocidos como la mejor institución educativa de la región para 2030.
    Valores: Respeto, Responsabilidad, Honestidad, Solidaridad.
    
    Ofrecemos educación desde preescolar hasta bachillerato.
    Calendario A (enero a noviembre).
    Somos un colegio bilingüe español-inglés con certificación Cambridge.
    Contamos con programa de robótica LEGO Education y participamos en FIRST LEGO League.
    En 2023 obtuvimos puntaje ICFES de 82 en categoría A+.
    
    Infraestructura: campus de 15,000 m² con laboratorios de ciencias, robótica y computación.
    Cancha de fútbol, gimnasio cubierto, piscina semiolímpica.
    Biblioteca con 8,000 volúmenes y acceso digital.
    Transporte escolar con 5 rutas.
    
    Extracurriculares: fútbol, baloncesto, natación, música, teatro, ajedrez, debate.
    Convenios con Universidad de los Andes y Universidad Javeriana.
    Convenios empresariales con Microsoft y LEGO Education.
    
    Contacto: info@losarrayanes.edu.co, 601-1234567, WhatsApp: 3131234567
    Facebook: /losarrayanes, Instagram: @losarrayanes
    """
    
    analyzer = get_analyzer_ultra()
    profile = await analyzer.analyze(
        name="Colegio Gimnasio Los Arrayanes",
        city="Bogotá",
        country="Colombia",
        webpage_text=test_text
    )
    
    print("\n" + "="*80)
    print("📊 ULTRA SELF-TEST RESULTS")
    print("="*80)
    print(f"Name: {profile.name}")
    print(f"Confidence: {profile.confidence_score:.1%}")
    print(f"Completeness: {profile.extraction_completeness:.1%}")
    print(f"Emails: {profile.emails}")
    print(f"Phones: {profile.phones}")
    print(f"WhatsApp: {profile.whatsapp}")
    print(f"Mission: {profile.mission[:100]}...")
    print(f"Vision: {profile.vision[:100]}...")
    print(f"Values: {profile.values}")
    print(f"Levels: {profile.levels_offered}")
    print(f"Bilingual: {profile.is_bilingual}")
    print(f"Cambridge: {profile.cambridge.get('has_cambridge')}")
    print(f"Robotics: {profile.has_robotics}")
    print(f"ICFES: {profile.icfes_score} - {profile.icfes_category}")
    print(f"Sports: {profile.sports}")
    print(f"Arts: {profile.arts}")
    print(f"University Agreements: {profile.university_agreements}")
    print("\n" + "="*80)
    print("✅ Ultra self-test completed")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(self_test())