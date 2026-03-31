# sales/engine/ai_analyzer.py
"""
================================================================================
[GOD TIER ARCHITECTURE: OMEGA QUANTUM LEVIATHAN CLASS ∞]
MODULE: COSMIC INSTITUTION INTELLIGENCE EXTRACTOR - THE FINAL FORM
VERSION: 99.9.9.9.9
ENGINEERING: QUANTUM CACHING, ADAPTIVE RETRY, TELEMETRY, TRACEABILITY
================================================================================
"""

import os
import sys
import json
import logging
import asyncio
import hashlib
import time
import re
import uuid
import sqlite3
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime
from functools import wraps
import traceback

try:
    from openai import AsyncOpenAI, RateLimitError, APITimeoutError, APIError
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Configuración de logging con formato GOD TIER
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] [🌌 COSMIC] %(message)s',
    datefmt='%H:%M:%S'
)

logger = logging.getLogger("Sovereign.CosmicAnalyzer")

# =================================================================================
# CONSTANTES GLOBALES
# =================================================================================

CACHE_DIR = Path("/tmp/cosmic_cache")
CACHE_DIR.mkdir(exist_ok=True)
DB_PATH = CACHE_DIR / "cosmic_cache.db"

# =================================================================================
# CACHE PERSISTENTE EN DISCO
# =================================================================================

class PersistentDiskCache:
    """Cache persistente en disco usando SQLite - GOD TIER"""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Inicializa la base de datos SQLite"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    timestamp REAL,
                    ttl INTEGER
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON cache(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ttl ON cache(ttl)")

    def get(self, key: str, ttl: int = 86400) -> Optional[Dict]:
        """Obtiene un valor del cache si no ha expirado"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT value, timestamp FROM cache WHERE key = ?",
                    (key,)
                )
                row = cursor.fetchone()
                if row:
                    value, timestamp = row
                    if time.time() - timestamp < ttl:
                        return json.loads(value)
        except Exception as e:
            logger.debug(f"Disk cache read error: {e}")
        return None

    def set(self, key: str, value: Dict, ttl: int = 86400):
        """Guarda un valor en el cache"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO cache (key, value, timestamp, ttl) VALUES (?, ?, ?, ?)",
                    (key, json.dumps(value), time.time(), ttl)
                )
                conn.commit()
        except Exception as e:
            logger.debug(f"Disk cache write error: {e}")

    def clear_expired(self):
        """Limpia entradas expiradas"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM cache WHERE timestamp + ttl < ?", (time.time(),))
                conn.commit()
        except Exception as e:
            logger.debug(f"Disk cache cleanup error: {e}")

    def clear_all(self):
        """Limpia todo el cache"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM cache")
                conn.commit()
        except Exception as e:
            logger.debug(f"Disk cache clear error: {e}")


# =================================================================================
# DATA STRUCTURES - VERSIÓN MEJORADA CON TODOS LOS CAMPOS REQUERIDOS
# =================================================================================

@dataclass
class AnalysisConfig:
    """Configuración de análisis con valores optimizados - GOD TIER"""
    max_text_length: int = 15000
    max_html_length: int = 5000
    temperature: float = 0.1
    max_tokens: int = 4000
    timeout_seconds: float = 45.0
    retry_attempts: int = 5
    cache_ttl_seconds: int = 86400
    enable_disk_cache: bool = True
    enable_memory_cache: bool = True
    adaptive_retry: bool = True
    max_concurrent: int = 5


@dataclass
class InstitutionProfile:
    """Perfil completo de la institución - VERSIÓN MEJORADA CON TODOS LOS CAMPOS"""
    
    # Core
    name: str = ""
    city: str = ""
    country: str = ""
    website: str = ""
    
    # Academic
    calendar: str = ""
    levels_offered: List[str] = field(default_factory=list)
    pedagogical_emphasis: str = ""
    institution_essence: str = ""
    foundation_year: str = ""
    accreditation_level: str = ""
    
    # Certifications
    ib: Dict[str, Any] = field(default_factory=lambda: {"has_ib": False, "programs": [], "since": ""})
    cambridge: Dict[str, Any] = field(default_factory=lambda: {"has_cambridge": False, "exams": []})
    oxford: Dict[str, Any] = field(default_factory=lambda: {"has_oxford": False})
    quality: Dict[str, Any] = field(default_factory=lambda: {"has_iso": False, "has_efqm": False, "others": []})
    
    # International
    double_degree: Dict[str, Any] = field(default_factory=lambda: {"has_double_degree": False, "partners": [], "countries": []})
    exchanges: Dict[str, Any] = field(default_factory=lambda: {"has_exchanges": False, "countries": [], "universities": []})
    language_immersion: Dict[str, Any] = field(default_factory=lambda: {"has_immersion": False, "destinations": [], "duration": ""})
    international_agreements: List[str] = field(default_factory=list)
    
    # Technology
    stem: Dict[str, Any] = field(default_factory=lambda: {"has_stem": False, "programs": [], "grade_levels": []})
    robotics: Dict[str, Any] = field(default_factory=lambda: {
        "has_robotics": False,
        "type": "",
        "platforms": [],
        "competitions": [],
        "levels": [],
        "achievements": []
    })
    programming: Dict[str, Any] = field(default_factory=lambda: {
        "has_programming": False,
        "languages": [],
        "frameworks": [],
        "grade_levels": []
    })
    laboratories: List[str] = field(default_factory=list)
    classroom_tech: List[str] = field(default_factory=list)
    digital_platforms: List[str] = field(default_factory=list)
    ai_initiatives: List[str] = field(default_factory=list)
    
    # Performance
    icfes: Dict[str, Any] = field(default_factory=lambda: {
        "score": "",
        "category": "",
        "ranking": "",
        "percentile": "",
        "year": "",
        "trend": ""
    })
    awards: List[str] = field(default_factory=list)
    university_admission_rate: str = ""
    top_universities: List[str] = field(default_factory=list)
    notable_alumni: List[str] = field(default_factory=list)
    
    # Extracurricular
    sports: List[str] = field(default_factory=list)
    arts: List[str] = field(default_factory=list)
    clubs: List[str] = field(default_factory=list)
    camps: List[str] = field(default_factory=list)
    community_service: bool = False
    competitions_won: List[str] = field(default_factory=list)
    
    # Infrastructure
    campus: Dict[str, Any] = field(default_factory=lambda: {"size": "", "locations": [], "facilities": []})
    green_areas: bool = False
    sports_facilities: List[str] = field(default_factory=list)
    library: str = ""
    transport: bool = False
    dining: str = ""
    capacity: int = 0
    
    # Agreements
    university_agreements: List[str] = field(default_factory=list)
    corporate_agreements: List[str] = field(default_factory=list)
    ngo_agreements: List[str] = field(default_factory=list)
    government_programs: List[str] = field(default_factory=list)
    
    # Sales Intelligence
    pain_points: List[str] = field(default_factory=list)
    sales_triggers: List[str] = field(default_factory=list)
    opportunities: List[str] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)
    ideal_contact: str = ""
    budget_indication: str = ""
    decision_timeline: str = ""
    recommended_approach: str = ""
    estimated_revenue_potential: str = ""
    sales_priority: str = "Medium"
    
    # Metadata
    executive_summary: str = ""
    confidence_score: float = 0.0
    extraction_completeness: float = 0.0
    analysis_timestamp: float = 0.0
    trace_id: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)

    def to_markdown(self) -> str:
        md = f"""
# 🌌 COSMIC INTELLIGENCE REPORT

## {self.name}
### {self.city}, {self.country}

---

## 🎯 EXECUTIVE SUMMARY
{self.executive_summary or "No summary available."}

---

## 📚 ACADEMIC PROFILE

| Field | Value |
|-------|-------|
| **Calendar** | {self.calendar or "Not specified"} |
| **Levels Offered** | {', '.join(self.levels_offered) or "Not specified"} |
| **Pedagogical Emphasis** | {self.pedagogical_emphasis or "Not specified"} |
| **Institution Essence** | {self.institution_essence or "Not specified"} |
| **Foundation Year** | {self.foundation_year or "Not specified"} |

---

## 🏆 CERTIFICATIONS

### 🌍 International
- **IB**: {'✅' if self.ib.get('has_ib') else '❌'}
- **Cambridge**: {'✅' if self.cambridge.get('has_cambridge') else '❌'}
- **Oxford**: {'✅' if self.oxford.get('has_oxford') else '❌'}

### 🏅 Quality
- **ISO 9001**: {'✅' if self.quality.get('has_iso') else '❌'}
- **EFQM**: {'✅' if self.quality.get('has_efqm') else '❌'}

---

## 🌎 INTERNATIONAL PROGRAMS

| Program | Status | Details |
|---------|--------|---------|
| **Double Degree** | {'✅' if self.double_degree.get('has_double_degree') else '❌'} | {', '.join(self.double_degree.get('partners', []))} |
| **Exchanges** | {'✅' if self.exchanges.get('has_exchanges') else '❌'} | {', '.join(self.exchanges.get('countries', []))} |
| **Immersion** | {'✅' if self.language_immersion.get('has_immersion') else '❌'} | {', '.join(self.language_immersion.get('destinations', []))} |

---

## 🤖 TECHNOLOGY & STEM

### Robotics
- **Status**: {'✅' if self.robotics.get('has_robotics') else '❌'}
- **Type**: {self.robotics.get('type', 'N/A')}
- **Competitions**: {', '.join(self.robotics.get('competitions', []))}

### Programming
- **Status**: {'✅' if self.programming.get('has_programming') else '❌'}
- **Languages**: {', '.join(self.programming.get('languages', []))}

### Laboratories
{self._format_list(self.laboratories)}

---

## 📊 PERFORMANCE

| Metric | Value |
|--------|-------|
| **ICFES Score** | {self.icfes.get('score', 'N/A')} |
| **Category** | {self.icfes.get('category', 'N/A')} |
| **Ranking** | {self.icfes.get('ranking', 'N/A')} |

---

## 🎪 EXTRACURRICULAR

| Category | Activities |
|----------|------------|
| **Sports** | {', '.join(self.sports) or "None"} |
| **Arts** | {', '.join(self.arts) or "None"} |
| **Clubs** | {', '.join(self.clubs) or "None"} |

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

---

## 📈 METADATA
- **Confidence Score**: {self.confidence_score:.1%}
- **Analysis Date**: {datetime.fromtimestamp(self.analysis_timestamp).strftime('%Y-%m-%d %H:%M:%S') if self.analysis_timestamp else "N/A"}

---

*Report generated by Cosmic Intelligence Engine*
"""
        return md

    def _format_list(self, items: List[str]) -> str:
        if not items:
            return "None"
        return "\n".join([f"- {item}" for item in items])


# =================================================================================
# THE COSMIC ANALYZER - EL NÚCLEO COGNITIVO SUPREMO
# =================================================================================

class CosmicInstitutionAnalyzer:
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, api_key: str = None, config: AnalysisConfig = None):
        if self._initialized:
            return
        self._initialized = True

        self.config = config or AnalysisConfig()
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self.client = None
        self._memory_cache = {}
        self._disk_cache = PersistentDiskCache() if self.config.enable_disk_cache else None
        self._stats = {
            "total_analyses": 0,
            "successful": 0,
            "failed": 0,
            "cache_hits": 0,
            "total_tokens": 0,
            "avg_latency_ms": 0
        }

        if self.api_key and OPENAI_AVAILABLE:
            base_url = "https://api.deepseek.com"
            self.client = AsyncOpenAI(
                api_key=self.api_key,
                base_url=base_url,
                timeout=self.config.timeout_seconds,
                max_retries=0
            )
            logger.info("✅ Cosmic Analyzer initialized with DeepSeek")
        else:
            logger.warning("⚠️ No API key found. AI analysis disabled.")

    def _generate_cache_key(self, name: str, city: str, country: str, text_hash: str) -> str:
        raw = f"{name.lower()}_{city.lower()}_{country.lower()}_{text_hash}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    async def _get_from_cache(self, key: str) -> Optional[Dict]:
        if self.config.enable_memory_cache and key in self._memory_cache:
            entry = self._memory_cache[key]
            if time.time() - entry["timestamp"] < self.config.cache_ttl_seconds:
                self._stats["cache_hits"] += 1
                return entry["profile"]

        if self._disk_cache:
            data = self._disk_cache.get(key, self.config.cache_ttl_seconds)
            if data:
                if self.config.enable_memory_cache:
                    self._memory_cache[key] = {
                        "profile": data,
                        "timestamp": time.time()
                    }
                self._stats["cache_hits"] += 1
                return data
        return None

    async def _set_in_cache(self, key: str, profile: Dict):
        if self.config.enable_memory_cache:
            self._memory_cache[key] = {
                "profile": profile,
                "timestamp": time.time()
            }
            if len(self._memory_cache) > 200:
                oldest = min(self._memory_cache.keys(), key=lambda k: self._memory_cache[k]["timestamp"])
                del self._memory_cache[oldest]

        if self._disk_cache:
            self._disk_cache.set(key, profile, self.config.cache_ttl_seconds)

    async def _retry_with_backoff(self, coro, max_retries: int = 5):
        last_exception = None
        for attempt in range(max_retries):
            try:
                return await coro
            except RateLimitError as e:
                wait_time = 2 ** attempt + (attempt * 0.5)
                logger.warning(f"Rate limit hit (attempt {attempt + 1}/{max_retries}), waiting {wait_time}s")
                await asyncio.sleep(wait_time)
                last_exception = e
            except APITimeoutError as e:
                wait_time = 1 ** attempt
                logger.warning(f"Timeout (attempt {attempt + 1}/{max_retries}), waiting {wait_time}s")
                await asyncio.sleep(wait_time)
                last_exception = e
            except APIError as e:
                wait_time = 2 ** attempt
                logger.warning(f"API error (attempt {attempt + 1}/{max_retries}): {e}")
                await asyncio.sleep(wait_time)
                last_exception = e
        raise last_exception or Exception("Max retries exceeded")

    async def analyze(
        self,
        name: str,
        city: str,
        country: str,
        webpage_text: str,
        raw_html: Optional[str] = None,
        extracted_data: Optional[Dict] = None
    ) -> InstitutionProfile:
        trace_id = uuid.uuid4().hex[:8]
        start_time = time.perf_counter()

        profile = InstitutionProfile(
            name=name,
            city=city,
            country=country,
            analysis_timestamp=time.time(),
            trace_id=trace_id
        )

        self._stats["total_analyses"] += 1
        logger.info(f"🌌 [{trace_id}] Starting cosmic analysis: {name} | {city}, {country}")

        if not self.client:
            profile.executive_summary = "⚠️ AI analysis unavailable (API not configured)"
            profile.confidence_score = 0.0
            self._stats["failed"] += 1
            return profile

        if not webpage_text or len(webpage_text.strip()) < 100:
            profile.executive_summary = f"⚠️ Insufficient text for analysis ({len(webpage_text or '')} chars)"
            profile.confidence_score = 0.1
            self._stats["failed"] += 1
            return profile

        text_hash = hashlib.md5(webpage_text[:5000].encode()).hexdigest()
        cache_key = self._generate_cache_key(name, city, country, text_hash)

        cached = await self._get_from_cache(cache_key)
        if cached:
            logger.info(f"⚡ [{trace_id}] Cache HIT for {name}")
            self._populate_profile(profile, cached)
            profile.analysis_timestamp = time.time()
            profile.trace_id = trace_id
            elapsed = (time.perf_counter() - start_time) * 1000
            self._stats["avg_latency_ms"] = (self._stats["avg_latency_ms"] * (self._stats["total_analyses"] - 1) + elapsed) / self._stats["total_analyses"]
            self._stats["successful"] += 1
            return profile

        safe_text = webpage_text[:self.config.max_text_length]
        safe_html = (raw_html or "")[:self.config.max_html_length]

        prompt = self._build_prompt(name, city, country, safe_text, safe_html, extracted_data)

        try:
            logger.info(f"🧠 [{trace_id}] Sending request to DeepSeek API...")

            async def api_call():
                return await self.client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {"role": "system", "content": self._get_system_prompt()},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=self.config.temperature,
                    max_tokens=self.config.max_tokens,
                    response_format={"type": "json_object"}
                )

            response = await self._retry_with_backoff(api_call(), self.config.retry_attempts)
            raw_response = response.choices[0].message.content.strip()
            data = json.loads(raw_response)

            if hasattr(response, 'usage'):
                self._stats["total_tokens"] += response.usage.total_tokens
                logger.info(f"📊 [{trace_id}] Tokens: {response.usage.total_tokens}")

            self._populate_profile(profile, data)
            profile.confidence_score = self._calculate_confidence(data, len(webpage_text))
            profile.extraction_completeness = self._calculate_completeness(data)

            await self._set_in_cache(cache_key, data)

            elapsed = (time.perf_counter() - start_time) * 1000
            self._stats["avg_latency_ms"] = (self._stats["avg_latency_ms"] * (self._stats["total_analyses"] - 1) + elapsed) / self._stats["total_analyses"]
            self._stats["successful"] += 1

            logger.info(f"✅ [{trace_id}] Analysis complete | Confidence: {profile.confidence_score:.1%} | Completeness: {profile.extraction_completeness:.1%} | {elapsed:.0f}ms")

        except Exception as e:
            logger.error(f"❌ [{trace_id}] Analysis failed: {e}")
            traceback.print_exc()
            profile.executive_summary = f"❌ Analysis failed: {str(e)[:200]}"
            profile.confidence_score = 0.0
            self._stats["failed"] += 1

        return profile

    def _get_system_prompt(self) -> str:
        return """You are the world's most advanced educational intelligence analyst.
Extract complete institution data. Output ONLY valid JSON. No markdown, no explanatory text.
Be detailed and precise. Use empty arrays [] for missing data, empty strings "" for missing text."""

    def _build_prompt(self, name: str, city: str, country: str, text: str, html: str, extracted: Dict) -> str:
        extracted_section = ""
        if extracted:
            extracted_section = f"""
PRE-VERIFIED DATA:
- LMS: {extracted.get('lms_provider', 'Not detected')}
- Emails: {', '.join(extracted.get('emails', [])[:5])}
- Phones: {', '.join(extracted.get('phones', [])[:5])}
"""

        return f"""
TARGET: {name} | {city}, {country}

{extracted_section}

TEXT CONTENT:
{text[:10000]}

HTML: {html[:3000]}

EXTRACT THE FOLLOWING IN JSON FORMAT:

{{
    "academic_profile": {{
        "calendar": "A or B",
        "levels_offered": ["Preescolar", "Primaria", "Bachillerato"],
        "pedagogical_emphasis": "",
        "institution_essence": "",
        "foundation_year": "",
        "accreditation_level": ""
    }},
    "certifications": {{
        "ib": {{"has_ib": false, "programs": [], "since": ""}},
        "cambridge": {{"has_cambridge": false, "exams": []}},
        "oxford": {{"has_oxford": false}},
        "quality": {{"has_iso": false, "has_efqm": false, "others": []}}
    }},
    "international_programs": {{
        "double_degree": {{"has_double_degree": false, "partners": [], "countries": []}},
        "exchanges": {{"has_exchanges": false, "countries": [], "universities": []}},
        "language_immersion": {{"has_immersion": false, "destinations": [], "duration": ""}},
        "international_agreements": []
    }},
    "technology": {{
        "stem": {{"has_stem": false, "programs": [], "grade_levels": []}},
        "robotics": {{
            "has_robotics": false,
            "type": "",
            "platforms": [],
            "competitions": [],
            "levels": [],
            "achievements": []
        }},
        "programming": {{
            "has_programming": false,
            "languages": [],
            "frameworks": [],
            "grade_levels": []
        }},
        "laboratories": [],
        "classroom_tech": [],
        "digital_platforms": [],
        "ai_initiatives": []
    }},
    "performance": {{
        "icfes": {{"score": "", "category": "", "ranking": "", "percentile": "", "year": "", "trend": ""}},
        "awards": [],
        "university_admission_rate": "",
        "top_universities": [],
        "notable_alumni": []
    }},
    "extracurricular": {{
        "sports": [],
        "arts": [],
        "clubs": [],
        "camps": [],
        "community_service": false,
        "competitions_won": []
    }},
    "infrastructure": {{
        "campus": {{"size": "", "locations": [], "facilities": []}},
        "green_areas": false,
        "sports_facilities": [],
        "library": "",
        "transport": false,
        "dining": "",
        "capacity": 0
    }},
    "agreements": {{
        "university_agreements": [],
        "corporate_agreements": [],
        "ngo_agreements": [],
        "government_programs": []
    }},
    "sales_intelligence": {{
        "pain_points": [],
        "sales_triggers": [],
        "opportunities": [],
        "risks": [],
        "ideal_contact": "",
        "budget_indication": "",
        "decision_timeline": "",
        "recommended_approach": "",
        "estimated_revenue_potential": "",
        "sales_priority": "Medium"
    }},
    "executive_summary": ""
}}
"""

    def _populate_profile(self, profile: InstitutionProfile, data: Dict):
        # Academic
        acad = data.get("academic_profile", {})
        profile.calendar = acad.get("calendar", "")
        profile.levels_offered = acad.get("levels_offered", [])
        profile.pedagogical_emphasis = acad.get("pedagogical_emphasis", "")
        profile.institution_essence = acad.get("institution_essence", "")
        profile.foundation_year = acad.get("foundation_year", "")
        profile.accreditation_level = acad.get("accreditation_level", "")

        # Certifications
        certs = data.get("certifications", {})
        profile.ib = certs.get("ib", profile.ib)
        profile.cambridge = certs.get("cambridge", profile.cambridge)
        profile.oxford = certs.get("oxford", profile.oxford)
        profile.quality = certs.get("quality", profile.quality)

        # International
        intl = data.get("international_programs", {})
        profile.double_degree = intl.get("double_degree", profile.double_degree)
        profile.exchanges = intl.get("exchanges", profile.exchanges)
        profile.language_immersion = intl.get("language_immersion", profile.language_immersion)
        profile.international_agreements = intl.get("international_agreements", [])

        # Technology
        tech = data.get("technology", {})
        profile.stem = tech.get("stem", profile.stem)
        profile.robotics = tech.get("robotics", profile.robotics)
        profile.programming = tech.get("programming", profile.programming)
        profile.laboratories = tech.get("laboratories", [])
        profile.classroom_tech = tech.get("classroom_tech", [])
        profile.digital_platforms = tech.get("digital_platforms", [])
        profile.ai_initiatives = tech.get("ai_initiatives", [])

        # Performance
        perf = data.get("performance", {})
        profile.icfes = perf.get("icfes", profile.icfes)
        profile.awards = perf.get("awards", [])
        profile.university_admission_rate = perf.get("university_admission_rate", "")
        profile.top_universities = perf.get("top_universities", [])
        profile.notable_alumni = perf.get("notable_alumni", [])

        # Extracurricular
        extra = data.get("extracurricular", {})
        profile.sports = extra.get("sports", [])
        profile.arts = extra.get("arts", [])
        profile.clubs = extra.get("clubs", [])
        profile.camps = extra.get("camps", [])
        profile.community_service = extra.get("community_service", False)
        profile.competitions_won = extra.get("competitions_won", [])

        # Infrastructure
        infra = data.get("infrastructure", {})
        profile.campus = infra.get("campus", profile.campus)
        profile.green_areas = infra.get("green_areas", False)
        profile.sports_facilities = infra.get("sports_facilities", [])
        profile.library = infra.get("library", "")
        profile.transport = infra.get("transport", False)
        profile.dining = infra.get("dining", "")
        profile.capacity = infra.get("capacity", 0)

        # Agreements
        agree = data.get("agreements", {})
        profile.university_agreements = agree.get("university_agreements", [])
        profile.corporate_agreements = agree.get("corporate_agreements", [])
        profile.ngo_agreements = agree.get("ngo_agreements", [])
        profile.government_programs = agree.get("government_programs", [])

        # Sales Intelligence
        sales = data.get("sales_intelligence", {})
        profile.pain_points = sales.get("pain_points", [])
        profile.sales_triggers = sales.get("sales_triggers", [])
        profile.opportunities = sales.get("opportunities", [])
        profile.risks = sales.get("risks", [])
        profile.ideal_contact = sales.get("ideal_contact", "")
        profile.budget_indication = sales.get("budget_indication", "")
        profile.decision_timeline = sales.get("decision_timeline", "")
        profile.recommended_approach = sales.get("recommended_approach", "")
        profile.estimated_revenue_potential = sales.get("estimated_revenue_potential", "")
        profile.sales_priority = sales.get("sales_priority", "Medium")

        # Executive Summary
        profile.executive_summary = data.get("executive_summary", "")

    def _calculate_confidence(self, data: Dict, text_length: int) -> float:
        if not data:
            return 0.0

        total = 0
        filled = 0

        def count(obj, depth=0):
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
                        count(v, depth + 1)

        count(data)
        field_score = filled / max(total, 1) if total > 0 else 0
        text_factor = min(1.0, text_length / 5000)
        return (field_score * 0.7) + (text_factor * 0.3)

    def _calculate_completeness(self, data: Dict) -> float:
        if not data:
            return 0.0

        key_fields = [
            "academic_profile.calendar",
            "academic_profile.levels_offered",
            "academic_profile.pedagogical_emphasis",
            "certifications.ib.has_ib",
            "certifications.cambridge.has_cambridge",
            "technology.robotics.has_robotics",
            "technology.programming.has_programming",
            "performance.icfes.score",
            "sales_intelligence.pain_points",
            "executive_summary"
        ]

        def get_nested(obj, path):
            parts = path.split(".")
            for part in parts:
                if not isinstance(obj, dict):
                    return None
                obj = obj.get(part)
                if obj is None:
                    return None
            return obj

        completed = 0
        for field in key_fields:
            value = get_nested(data, field)
            if value:
                if isinstance(value, list) and value:
                    completed += 1
                elif value not in (None, "", False):
                    completed += 1
        return completed / len(key_fields)

    def get_stats(self) -> Dict:
        return {
            **self._stats,
            "cache_memory_size": len(self._memory_cache),
            "success_rate": self._stats["successful"] / max(1, self._stats["total_analyses"])
        }

    def clear_cache(self):
        self._memory_cache.clear()
        if self._disk_cache:
            self._disk_cache.clear_all()
        logger.info("🗑️ Cache cleared")


# =================================================================================
# EXPORT FUNCTIONS
# =================================================================================

_analyzer = None

def get_analyzer() -> CosmicInstitutionAnalyzer:
    global _analyzer
    if _analyzer is None:
        _analyzer = CosmicInstitutionAnalyzer()
    return _analyzer

async def analyze_institution(
    name: str,
    city: str,
    country: str,
    webpage_text: str,
    raw_html: Optional[str] = None,
    extracted_data: Optional[Dict] = None
) -> InstitutionProfile:
    analyzer = get_analyzer()
    return await analyzer.analyze(name, city, country, webpage_text, raw_html, extracted_data)

def analyze_institution_sync(
    name: str,
    city: str,
    country: str,
    webpage_text: str,
    raw_html: Optional[str] = None,
    extracted_data: Optional[Dict] = None
) -> InstitutionProfile:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(
            analyze_institution(name, city, country, webpage_text, raw_html, extracted_data)
        )
    finally:
        loop.close()


# =================================================================================
# SELF-TEST
# =================================================================================

async def self_test():
    logger.info("🧪 Running self-test...")
    test_text = """
Colegio Bilingüe San José es una institución educativa en Bogotá, Colombia.
Ofrece educación desde preescolar hasta bachillerato.
Tienen certificación Cambridge con exámenes PET, FCE y CAE.
Cuentan con programa de robótica LEGO Education y participan en FIRST LEGO League.
En 2023 obtuvieron puntaje ICFES de 78 en categoría A+.
Su filosofía es la formación integral con énfasis en valores y tecnología.
"""

    analyzer = get_analyzer()
    profile = await analyzer.analyze(
        name="Colegio Bilingüe San José",
        city="Bogotá",
        country="Colombia",
        webpage_text=test_text
    )

    print("\n" + "="*80)
    print("📊 SELF-TEST RESULTS")
    print("="*80)
    print(f"Name: {profile.name}")
    print(f"Confidence: {profile.confidence_score:.1%}")
    print(f"Completeness: {profile.extraction_completeness:.1%}")
    print(f"Cambridge: {profile.cambridge.get('has_cambridge', False)}")
    print(f"Robotics: {profile.robotics.get('has_robotics', False)}")
    print(f"ICFES: {profile.icfes.get('score', 'N/A')}")
    print("\n" + "="*80)
    print("✅ Self-test completed")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(self_test())