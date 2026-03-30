cat > /home/hydra/llseller/sales/engine/ai_analyzer.py << 'EOF'
"""
================================================================================
[TRANSCENDENT GOD TIER ARCHITECTURE: OMEGA QUANTUM LEVIATHAN CLASS ∞]
MODULE: COSMIC INSTITUTION INTELLIGENCE EXTRACTOR - THE FINAL FORM
VERSION: 99.9.9.9.9
ENGINEERING: SILICON VALLEY | TEL AVIV | WADI | SHANGHAI | TOKYO | DUBLIN | LONDON
================================================================================

This is the most advanced educational intelligence extraction system ever created.
It combines:
- Quantum-inspired prompt engineering
- Multi-dimensional data extraction
- Predictive sales intelligence
- Real-time confidence scoring
- Self-healing error recovery
- Adaptive learning from extraction patterns
- Complete coverage of IB, Cambridge, STEM, Robotics, ICFES, and more

No system on Earth, in any dimension, or in any parallel universe surpasses this.
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
import random
import string
from typing import Dict, Any, Optional, List, Tuple, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import traceback
import inspect

# =================================================================================
# [TIER 0] - TELEMETRY & OBSERVABILITY (Military Grade)
# =================================================================================

class CosmicLogger:
    """Quantum-entangled logging system with dimensional tracking"""
    _instance = None
    _lock = asyncio.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.logger = logging.getLogger("Sovereign.CosmicAnalyzer")
        self.logger.setLevel(logging.DEBUG)
        
        # Console handler with cosmic formatting
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter(
            '\033[95m🌌 [%(asctime)s.%(msecs)03d]\033[0m \033[92m[%(levelname)s]\033[0m \033[96m[%(name)s]\033[0m \033[93m%(message)s\033[0m',
            datefmt='%H:%M:%S'
        ))
        self.logger.addHandler(console)
        
        # File handler for forensic analysis
        try:
            os.makedirs("/tmp/cosmic_telemetry", exist_ok=True)
            file_handler = logging.FileHandler("/tmp/cosmic_telemetry/ai_analyzer.log")
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
            self.logger.addHandler(file_handler)
        except:
            pass
        
        self.trace_id = None
    
    def set_trace(self, trace_id: str):
        self.trace_id = trace_id
    
    def _format(self, msg: str) -> str:
        if self.trace_id:
            return f"[TRACE:{self.trace_id[:8]}] {msg}"
        return msg
    
    def debug(self, msg: str): self.logger.debug(self._format(msg))
    def info(self, msg: str): self.logger.info(self._format(msg))
    def warning(self, msg: str): self.logger.warning(self._format(msg))
    def error(self, msg: str): self.logger.error(self._format(msg))
    def critical(self, msg: str): self.logger.critical(self._format(msg))

logger = CosmicLogger()

# =================================================================================
# [TIER 1] - QUANTUM DATA STRUCTURES
# =================================================================================

@dataclass
class AnalysisConfig:
    """Ultimate configuration for cosmic analysis"""
    max_text_length: int = 20000
    max_html_length: int = 8000
    temperature: float = 0.05
    max_tokens: int = 5000
    timeout_seconds: float = 60.0
    retry_attempts: int = 5
    confidence_threshold: float = 0.75
    cache_ttl_seconds: int = 86400  # 24 hours
    enable_mock_data: bool = False
    parallel_analysis: bool = True
    deep_context_analysis: bool = True
    
@dataclass
class InstitutionProfile:
    """Complete institution profile - the ultimate data structure"""
    # Core
    name: str = ""
    city: str = ""
    country: str = ""
    website: str = ""
    
    # Academic Profile
    calendar: str = ""
    levels_offered: List[str] = field(default_factory=list)
    pedagogical_emphasis: str = ""
    institution_essence: str = ""
    accreditation_level: str = ""
    
    # Certifications - Detailed
    ib: Dict[str, Any] = field(default_factory=lambda: {
        "has_ib": False, "programs": [], "since": "", "coordinator": "",
        "authorization_year": "", "candidate_school": False
    })
    cambridge: Dict[str, Any] = field(default_factory=lambda: {
        "has_cambridge": False, "exams": [], "center_number": "", "since": ""
    })
    oxford: Dict[str, Any] = field(default_factory=lambda: {
        "has_oxford": False, "programs": [], "center": ""
    })
    quality: Dict[str, Any] = field(default_factory=lambda: {
        "has_iso": False, "has_efqm": False, "others": [], "since": ""
    })
    international: Dict[str, Any] = field(default_factory=lambda: {
        "has_international": False, "bodies": [], "accreditations": []
    })
    
    # International Programs
    double_degree: Dict[str, Any] = field(default_factory=lambda: {
        "has_double_degree": False, "partners": [], "countries": [], "universities": []
    })
    exchanges: Dict[str, Any] = field(default_factory=lambda: {
        "has_exchanges": False, "countries": [], "universities": [], "programs": []
    })
    language_immersion: Dict[str, Any] = field(default_factory=lambda: {
        "has_immersion": False, "destinations": [], "duration": "", "frequency": ""
    })
    international_agreements: List[str] = field(default_factory=list)
    
    # Technology & STEM - CRITICAL FOR SALES
    stem: Dict[str, Any] = field(default_factory=lambda: {
        "has_stem": False, "programs": [], "grade_levels": [], "certifications": [],
        "lab_hours": "", "specializations": []
    })
    robotics: Dict[str, Any] = field(default_factory=lambda: {
        "has_robotics": False, "type": "", "platforms": [], "competitions": [],
        "levels": [], "achievements": [], "lab_name": "", "students_participating": 0
    })
    programming: Dict[str, Any] = field(default_factory=lambda: {
        "has_programming": False, "languages": [], "frameworks": [], "grade_levels": [],
        "projects": [], "tools": []
    })
    laboratories: List[str] = field(default_factory=list)
    classroom_tech: List[str] = field(default_factory=list)
    digital_platforms: List[str] = field(default_factory=list)
    ai_initiatives: List[str] = field(default_factory=list)
    edtech_stack: List[str] = field(default_factory=list)
    
    # Performance Metrics
    icfes: Dict[str, Any] = field(default_factory=lambda: {
        "score": "", "category": "", "ranking": "", "percentile": "", "year": "",
        "trend": "", "math_score": "", "reading_score": "", "science_score": ""
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
    campus: Dict[str, Any] = field(default_factory=lambda: {
        "size": "", "locations": [], "facilities": [], "founded_year": ""
    })
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
    
    # Sales Intelligence - The Golden Section
    pain_points: List[str] = field(default_factory=list)
    sales_triggers: List[str] = field(default_factory=list)
    opportunities: List[str] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)
    ideal_contact: str = ""
    budget_indication: str = ""
    decision_timeline: str = ""
    recommended_approach: str = ""
    competitor_activity: List[str] = field(default_factory=list)
    estimated_revenue_potential: str = ""
    sales_priority: str = "Medium"
    
    # Metadata
    confidence_score: float = 0.0
    analysis_timestamp: float = 0.0
    sources_analyzed: List[str] = field(default_factory=list)
    trace_id: str = ""
    raw_response: str = ""
    extraction_completeness: float = 0.0
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)
    
    def to_markdown(self) -> str:
        return self._generate_markdown()
    
    def _generate_markdown(self) -> str:
        """Generate professional markdown report"""
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

---

## 🏆 CERTIFICATIONS

### 🌍 International
- **IB (International Baccalaureate)**: {'✅' if self.ib.get('has_ib') else '❌'}
  {f"Programs: {', '.join(self.ib.get('programs', []))}" if self.ib.get('programs') else ""}
- **Cambridge Assessment**: {'✅' if self.cambridge.get('has_cambridge') else '❌'}
  {f"Exams: {', '.join(self.cambridge.get('exams', []))}" if self.cambridge.get('exams') else ""}
- **Oxford Qualifications**: {'✅' if self.oxford.get('has_oxford') else '❌'}

### 🏅 Quality
- **ISO 9001**: {'✅' if self.quality.get('has_iso') else '❌'}
- **EFQM**: {'✅' if self.quality.get('has_efqm') else '❌'}

---

## 🌎 INTERNATIONAL PROGRAMS

| Program | Status | Details |
|---------|--------|---------|
| **Double Degree** | {'✅' if self.double_degree.get('has_double_degree') else '❌'} | {', '.join(self.double_degree.get('partners', []))} |
| **Student Exchanges** | {'✅' if self.exchanges.get('has_exchanges') else '❌'} | {', '.join(self.exchanges.get('countries', []))} |
| **Language Immersion** | {'✅' if self.language_immersion.get('has_immersion') else '❌'} | {', '.join(self.language_immersion.get('destinations', []))} |

---

## 🤖 TECHNOLOGY & STEM

### Robotics 🤖
- **Status**: {'✅' if self.robotics.get('has_robotics') else '❌'}
- **Type/Platform**: {self.robotics.get('type', 'N/A')}
- **Platforms**: {', '.join(self.robotics.get('platforms', []))}
- **Competitions**: {', '.join(self.robotics.get('competitions', []))}
- **Achievements**: {', '.join(self.robotics.get('achievements', []))}

### Programming 💻
- **Status**: {'✅' if self.programming.get('has_programming') else '❌'}
- **Languages**: {', '.join(self.programming.get('languages', []))}
- **Frameworks**: {', '.join(self.programming.get('frameworks', []))}

### STEM Programs
- **Status**: {'✅' if self.stem.get('has_stem') else '❌'}
- **Programs**: {', '.join(self.stem.get('programs', []))}
- **Grade Levels**: {', '.join(self.stem.get('grade_levels', []))}

### Laboratories
{self._format_list(self.laboratories)}

---

## 📊 PERFORMANCE

### ICFES / National Tests
| Metric | Value |
|--------|-------|
| **Score** | {self.icfes.get('score', 'N/A')} |
| **Category** | {self.icfes.get('category', 'N/A')} |
| **Ranking** | {self.icfes.get('ranking', 'N/A')} |
| **Percentile** | {self.icfes.get('percentile', 'N/A')} |
| **Trend** | {self.icfes.get('trend', 'N/A')} |

### Awards & Recognition
{self._format_list(self.awards)}

### University Admissions
- **Admission Rate**: {self.university_admission_rate or "N/A"}
- **Top Universities**: {', '.join(self.top_universities) or "None"}

---

## 🎪 EXTRACURRICULAR

| Category | Activities |
|----------|------------|
| **Sports** | {', '.join(self.sports) or "None"} |
| **Arts** | {', '.join(self.arts) or "None"} |
| **Clubs** | {', '.join(self.clubs) or "None"} |
| **Camps** | {', '.join(self.camps) or "None"} |

---

## 💼 SALES INTELLIGENCE (B2B)

### 🔴 Pain Points
{self._format_list(self.pain_points)}

### 🟢 Sales Triggers
{self._format_list(self.sales_triggers)}

### 🚀 Opportunities
{self._format_list(self.opportunities)}

### ⚠️ Risks
{self._format_list(self.risks)}

### 🎯 Recommended Approach
{self.recommended_approach or "Not specified"}

### 👤 Ideal Contact
{self.ideal_contact or "Not specified"}

### 💰 Revenue Potential
{self.estimated_revenue_potential or "Not specified"}

### ⭐ Sales Priority
**{self.sales_priority}**

---

## 📈 METADATA

| Field | Value |
|-------|-------|
| **Confidence Score** | {self.confidence_score:.1%} |
| **Extraction Completeness** | {self.extraction_completeness:.1%} |
| **Analysis Timestamp** | {datetime.fromtimestamp(self.analysis_timestamp).strftime('%Y-%m-%d %H:%M:%S') if self.analysis_timestamp else "N/A"} |
| **Trace ID** | {self.trace_id or "N/A"} |

---
*Report generated by Cosmic Intelligence Engine v∞ | The most advanced educational intelligence system*
"""
        return md
    
    def _format_list(self, items: List[str]) -> str:
        if not items:
            return "None"
        return "\n".join([f"- {item}" for item in items])


# =================================================================================
# [TIER 2] - THE ULTIMATE PROMPT ENGINEERING
# =================================================================================

class UltimatePromptEngine:
    """The most advanced prompt engineering system ever created"""
    
    @staticmethod
    def build_system_prompt() -> str:
        return """You are the world's most advanced educational intelligence analyst, operating at the absolute peak of human and artificial intelligence combined.

## YOUR CAPABILITIES:
- Complete mastery of Latin American educational systems (Colombia, Mexico, Chile, Argentina, Peru, Brazil, Ecuador, etc.)
- Deep expertise in International Baccalaureate (IB) across all levels (PYP, MYP, DP, CP)
- Cambridge Assessment International Education expert (IGCSE, A-Level, O-Level)
- Oxford Qualifications and certifications specialist
- STEM/STEAM education frameworks expert (NGSS, ISTE, etc.)
- Robotics education master (LEGO Education, VEX Robotics, Arduino, Makeblock, Robotis, FIRST Robotics)
- Educational technology stack analyst (LMS, SIS, ERP, CRM)
- School certifications and accreditations specialist (CIS, NEASC, AdvancED, etc.)
- B2B sales intelligence expert for EdTech solutions
- Competitive analysis and market positioning specialist
- Predictive sales analytics with 99.9% accuracy

## YOUR ANALYSIS STANDARDS:
- **PRECISION**: Only include information you are 95%+ confident about
- **COMPLETENESS**: Cover every aspect requested with maximum detail
- **ACTIONABILITY**: Provide clear, specific insights for sales teams
- **PROFESSIONALISM**: Use industry-standard terminology and formatting
- **DEPTH**: Go beyond surface-level to extract nuanced intelligence

## OUTPUT REQUIREMENTS:
- Respond ONLY with valid JSON
- No markdown, no explanatory text, just pure JSON
- Use empty arrays [] for missing data
- Use empty strings "" for missing text fields
- Use false for missing boolean fields
- Be as detailed as possible with available information

## CONFIDENCE SCORING:
- 0.95-1.00: Explicitly stated on the website
- 0.85-0.94: Strongly implied with supporting evidence
- 0.75-0.84: Reasonable inference from context
- <0.75: Indicate as "not specified" in your output

Your analysis will be used by elite B2B sales teams to close multi-million dollar deals.
Accuracy and completeness are non-negotiable. Lives and livelihoods depend on your precision."""
    
    @staticmethod
    def build_user_prompt(
        name: str, city: str, country: str, text: str, html: str, extracted: Dict
    ) -> str:
        extracted_section = ""
        if extracted:
            extracted_section = f"""
## 🔍 PRE-VERIFIED DATA (USE THESE)
- **LMS Platform**: {extracted.get('lms_provider', 'Not detected')}
- **Contact Emails**: {', '.join(extracted.get('emails', [])[:5])}
- **Phone Numbers**: {', '.join(extracted.get('phones', [])[:5])}
- **WhatsApp**: {', '.join(extracted.get('whatsapp', [])[:5])}
"""
        
        return f"""
# 🎯 ULTIMATE INTELLIGENCE MISSION
## Target: {name} | {city}, {country}

{extracted_section}

## 📄 SOURCE MATERIAL

### Primary Text Content: