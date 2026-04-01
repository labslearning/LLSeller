"""
================================================================================
[GOD TIER OMEGA ARCHITECTURE: TRANSCENDENT QUANTUM LEVIATHAN CLASS]
PROJECT: GHOST SWARM - COSMIC INTELLIGENCE HARVESTER
VERSION: 99.9.9.9.9.OMEGA
STANDARD: SURPASSING ALL HUMAN ACHIEVEMENT - SILICON VALLEY / TEL AVIV / WADI / SHANGHAI / TOKYO / DUBLIN / LONDON
================================================================================
"""

import uuid
import re
import secrets
import hashlib
from typing import Optional, List, Dict, Any
from datetime import timedelta
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator, RegexValidator
from django.db.models import Count, Q, Avg, CheckConstraint, F, Case, When, Value, IntegerField, FloatField
from django.db.models.functions import Coalesce, Concat
from django.utils.translation import gettext_lazy as _
from django.utils import timezone
from django.core.exceptions import ValidationError

# ======================================================================
# 0. CORE: ABSTRACT BASE (AUDIT TRAIL & DDD) - OMEGA ENHANCED
# ======================================================================

class TimeStampedModel(models.Model):
    """
    [GOD TIER OMEGA AUDIT TRAIL]
    Capa de inmutabilidad base con trazabilidad cuántica.
    Implementa índices estratégicos para consultas temporales O(1)
    y TTL policies para limpieza automática de registros obsoletos.
    """
    created_at = models.DateTimeField(
        auto_now_add=True, 
        db_index=True,
        db_comment="Timestamp inmutable de creación (UTC). Usado para TTL y auditoría forense."
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        db_comment="Timestamp dinámico de última mutación (UTC). Usado para caché y scoring."
    )
    
    # Campo de expiración para TTL automático (pruning de datos viejos)
    expires_at = models.DateTimeField(
        blank=True, 
        null=True, 
        db_index=True,
        db_comment="Fecha de expiración para purga automática de datos obsoletos."
    )

    class Meta:
        abstract = True
        
    def is_expired(self) -> bool:
        """Verifica si el registro ha expirado según su TTL."""
        if not self.expires_at:
            return False
        return timezone.now() > self.expires_at
    
    def set_ttl(self, days: int = 365):
        """Configura TTL para purga automática."""
        self.expires_at = timezone.now() + timedelta(days=days)


# ======================================================================
# 1. TIER 0: MASTER NODE (INSTITUTION & STATE MACHINE) - OMEGA TIER
# ======================================================================

class Institution(TimeStampedModel):
    """
    [OMEGA C2 Master Node & Quantum State Machine]
    Controla el flujo de trabajo (Radar -> Sniper -> AI Outreach) con 
    trazabilidad completa, anti-duplicación cuántica y scoring predictivo.
    """
    id = models.UUIDField(
        primary_key=True, 
        default=uuid.uuid4, 
        editable=False,
        db_comment="UUIDv4 criptográfico para blindar el acceso y prevenir enumeración."
    )
    
    # =========================================================
    # 🛡️ TRAZABILIDAD Y CONTROL DE MISIONES
    # =========================================================
    mission_id = models.UUIDField(
        null=True, blank=True, db_index=True, 
        verbose_name="ID de Misión/Batch",
        db_comment="Agrupador lógico para campañas Celery (Swarm ID). Permite trazabilidad end-to-end."
    )
    
    # 🔥 GOD TIER OMEGA: ANTI-DUPLICACIÓN CUÁNTICA
    search_hash = models.CharField(
        max_length=64,
        blank=True,
        null=True,
        db_index=True,
        verbose_name="Hash de Búsqueda Cuántico",
        help_text="Hash SHA-256 de la sesión de búsqueda. Cambia cada 5 minutos para evitar duplicados en el mismo lote.",
        db_comment="Anti-duplicación por lote: mismo hash = misma sesión de búsqueda."
    )
    
    # Fingerprint criptográfico único de la institución (nombre+ciudad+país hasheado)
    cryptographic_fingerprint = models.CharField(
        max_length=64,
        unique=True,
        blank=True,
        null=True,
        db_index=True,
        verbose_name="Fingerprint Criptográfico",
        help_text="Hash SHA-256 de nombre+ciudad+país para deduplicación global."
    )
    
    # =========================================================
    # 🏛️ CLASIFICACIÓN DE INSTITUCIÓN
    # =========================================================
    class InstitutionType(models.TextChoices):
        KINDERGARTEN = 'kindergarten', _('🏫 Jardín Infantil / Preescolar')
        SCHOOL = 'school', _('📚 Colegio (Básica/Media)')
        UNIVERSITY = 'university', _('🎓 Universidad / Educación Superior')
        INSTITUTE = 'institute', _('⚙️ Instituto Técnico / Tecnológico')
        OTHER = 'other', _('📖 Otro')

    class DiscoverySource(models.TextChoices):
        OSM = 'osm', _('🛰️ OpenStreetMap (GeoRadar)')
        GOV_DATA = 'gov_data', _('🏛️ Directorio Gubernamental')
        SERP = 'serp', _('🔍 SERP Engine (Web Scraping)')
        MANUAL = 'manual', _('✍️ CRM Inbound / Manual')
        GHOST = 'Ghost_Omega_V99', _('👻 Ghost Sniper Omega V99 (Autonomous)')
        COSMIC_AI = 'cosmic_ai', _('🌌 Cosmic AI Discovery')

    # =========================================================
    # 🚦 STATE MACHINE (Pipeline de Procesamiento)
    # =========================================================
    class ProcessingStatus(models.TextChoices):
        RAW_RADAR = 'RAW', _('🌍 Crudo (Solo Mapa/Radar)')
        SNIPER_LOCKED = 'LOCKED', _('🎯 Bloqueado (Sniper Extrayendo)')
        ENRICHED = 'ENRICHED', _('✨ Enriquecido (Listo para Ventas)')
        DISCARDED = 'DISCARDED', _('🗑️ Descartado (Falso Positivo/Caído)')
        AI_PROCESSING = 'AI_PROCESSING', _('🧠 Procesando con IA Cósmica')
        COSMIC_COMPLETE = 'COSMIC', _('🌌 Reporte Cósmico Generado')

    processing_status = models.CharField(
        max_length=20, choices=ProcessingStatus.choices,
        default=ProcessingStatus.RAW_RADAR, db_index=True,
        verbose_name="Estado en Pipeline"
    )

    # =========================================================
    # 📇 IDENTIDAD Y CONTACTO (OMNI-CHANNEL READY)
    # =========================================================
    name = models.CharField(
        max_length=255, 
        verbose_name="Nombre de la Institución",
        db_index=True,
        help_text="Nombre oficial de la institución educativa."
    )
    
    website = models.URLField(
        max_length=255, unique=True, null=True, blank=True, 
        verbose_name="Sitio Web Oficial",
        db_comment="URL canónica oficial. Única para evitar duplicados."
    )
    
    url_trust_score = models.FloatField(
        default=0.0, 
        validators=[MinValueValidator(0.0), MaxValueValidator(100.0)],
        help_text="Heurística de Levenshtein (Qué tan real es la URL)",
        db_comment="Filtro matemático para descartar Falsos Positivos de URLs antes del Scrape."
    )
    
    email = models.EmailField(
        blank=True, null=True, 
        verbose_name="Email Principal (Sanitizado)",
        db_index=True,
        help_text="Email principal validado y sanitizado."
    )
    
    phone = models.CharField(
        max_length=120, blank=True, null=True, 
        verbose_name="Omni-Canal (WA/TEL)",
        help_text="Soporta formato Ghost Sniper (Ej: W:3001234567 T:6011234567)",
        db_comment="Formato unificado: W: para WhatsApp, T: para teléfono fijo."
    )
    
    # Campos de contacto adicionales (extracción avanzada)
    whatsapp_numbers = models.JSONField(
        default=list, blank=True,
        verbose_name="Números de WhatsApp",
        help_text="Lista de números de WhatsApp detectados.",
        db_comment="Extracción masiva de números WhatsApp del sitio web."
    )
    
    social_media = models.JSONField(
        default=dict, blank=True,
        verbose_name="Redes Sociales",
        help_text="Diccionario con URLs de redes sociales (facebook, instagram, linkedin, etc.)",
        db_comment="Extracción completa de perfiles sociales."
    )

    # =========================================================
    # 🎯 CLASIFICACIÓN DE NEGOCIO (B2B TARGETING)
    # =========================================================
    institution_type = models.CharField(
        max_length=20, choices=InstitutionType.choices, 
        default=InstitutionType.SCHOOL, db_index=True
    )
    is_private = models.BooleanField(default=True, db_index=True)
    student_count = models.PositiveIntegerField(null=True, blank=True)
    teacher_count = models.PositiveIntegerField(null=True, blank=True)
    
    # Métricas de rendimiento (extraídas por IA)
    icfes_score = models.CharField(max_length=20, blank=True, null=True, db_index=True)
    icfes_category = models.CharField(max_length=5, blank=True, null=True)
    icfes_ranking = models.CharField(max_length=50, blank=True, null=True)

    # =========================================================
    # 🌍 GEOLOCALIZACIÓN Y TERRITORIOS
    # =========================================================
    country = models.CharField(max_length=100, db_index=True, default="Colombia")
    state_region = models.CharField(max_length=100, blank=True, null=True, db_index=True)
    city = models.CharField(max_length=100, db_index=True)
    address = models.CharField(max_length=255, blank=True, null=True)
    latitude = models.DecimalField(max_digits=11, decimal_places=8, blank=True, null=True)
    longitude = models.DecimalField(max_digits=11, decimal_places=8, blank=True, null=True)
    
    # Google Maps y coordenadas enriquecidas
    google_maps_url = models.URLField(max_length=500, blank=True, null=True)
    place_id = models.CharField(max_length=100, blank=True, null=True)

    # =========================================================
    # 📊 TRAZABILIDAD Y CRM ROUTING
    # =========================================================
    discovery_source = models.CharField(
        max_length=20, 
        choices=DiscoverySource.choices, 
        default=DiscoverySource.MANUAL
    )
    is_active = models.BooleanField(default=True, db_index=True)
    last_scored_at = models.DateTimeField(blank=True, null=True, db_index=True)
    
    lead_score = models.IntegerField(
        default=0, db_index=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        db_comment="Termómetro de Venta Predictivo (0-100). Actualizado por ML semanalmente."
    )
    
    contacted = models.BooleanField(default=False, db_index=True)
    
    # Scoring de confianza (0-1)
    confidence_score = models.FloatField(
        default=0.0,
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text="Confianza del análisis (0-1). Mayor a 0.85 es confiable."
    )
    
    extraction_completeness = models.FloatField(
        default=0.0,
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text="Completitud de la extracción de datos (0-1)."
    )

    class Meta:
        verbose_name = "🌌 Institución Educativa (Cosmic)"
        verbose_name_plural = "🌌 Instituciones Educativas (Cosmic)"
        ordering = ['-lead_score', '-updated_at', '-confidence_score']
        
        indexes = [
            # Índices existentes mejorados
            models.Index(fields=['processing_status', 'city'], name='idx_processing_city'),
            models.Index(fields=['-lead_score', 'contacted', 'is_active'], name='idx_score_contacted_active'),
            models.Index(fields=['country', 'state_region', 'city'], name='idx_geo_location'),
            models.Index(
                fields=['processing_status', 'website'], 
                name='idx_sniper_queue',
                condition=Q(is_active=True) & Q(website__isnull=False) & Q(processing_status='RAW')
            ),
            # 🔥 NUEVOS ÍNDICES OMEGA
            models.Index(fields=['search_hash', 'city', 'name'], name='idx_search_hash_city_name'),
            models.Index(fields=['cryptographic_fingerprint'], name='idx_crypto_fingerprint'),
            models.Index(fields=['lead_score', 'confidence_score'], name='idx_score_confidence'),
            models.Index(fields=['icfes_score', 'icfes_category'], name='idx_icfes_performance'),
            models.Index(fields=['institution_type', 'is_private'], name='idx_type_private'),
            models.Index(fields=['city', 'lead_score'], name='idx_city_score'),
        ]
        
        constraints = [
            models.CheckConstraint(
                check=models.Q(lead_score__gte=0) & models.Q(lead_score__lte=100),
                name='lead_score_range_0_to_100'
            ),
            models.CheckConstraint(
                check=models.Q(confidence_score__gte=0) & models.Q(confidence_score__lte=1),
                name='confidence_score_range'
            ),
            models.CheckConstraint(
                check=models.Q(extraction_completeness__gte=0) & models.Q(extraction_completeness__lte=1),
                name='completeness_range'
            ),
            models.UniqueConstraint(
                fields=['name', 'city', 'country'],
                name='unique_institution_per_city_country'
            ),
            # 🔥 UNIQUE CONSTRAINT CON FINGERPRINT
            models.UniqueConstraint(
                fields=['cryptographic_fingerprint'],
                condition=Q(cryptographic_fingerprint__isnull=False),
                name='unique_crypto_fingerprint'
            ),
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._original_name = self.name
        self._original_city = self.city
        self._original_country = self.country

    def save(self, *args, **kwargs):
        """[OMEGA] Auto-genera fingerprint criptográfico antes de guardar."""
        # Generar fingerprint si no existe o si los datos cambiaron
        if (self.name and self.city and self.country and 
            (not self.cryptographic_fingerprint or 
             self.name != self._original_name or 
             self.city != self._original_city or 
             self.country != self._original_country)):
            fingerprint_data = f"{self.name.lower().strip()}|{self.city.lower().strip()}|{self.country.lower().strip()}"
            self.cryptographic_fingerprint = hashlib.sha256(fingerprint_data.encode()).hexdigest()
        
        super().save(*args, **kwargs)
    
    def __str__(self):
        status_icon = {
            'RAW': '🌍',
            'LOCKED': '🎯',
            'ENRICHED': '✨',
            'AI_PROCESSING': '🧠',
            'COSMIC': '🌌',
            'DISCARDED': '🗑️'
        }.get(self.processing_status, '📌')
        score_color = "🔥" if self.lead_score >= 75 else "⚡" if self.lead_score >= 50 else "❄️"
        return f"{status_icon} {self.name} | {score_color} {self.lead_score} pts | {self.city}"

    def lock_for_sniper(self):
        """[DDD] Bloquea el registro para que ningún otro worker lo procese concurrente."""
        self.processing_status = self.ProcessingStatus.SNIPER_LOCKED
        self.save(update_fields=['processing_status', 'updated_at'])

    def escalate_lead(self, points: int):
        """Escala el lead score con límite máximo de 100."""
        self.lead_score = min(self.lead_score + points, 100)
        self.save(update_fields=['lead_score', 'updated_at'])
    
    def mark_as_cosmic_complete(self):
        """Marca la institución como completada con reporte cósmico."""
        self.processing_status = self.ProcessingStatus.COSMIC_COMPLETE
        self.save(update_fields=['processing_status', 'updated_at'])
    
    @property
    def is_high_value(self) -> bool:
        """Propiedad derivada para leads de alto valor."""
        return self.lead_score >= 75
    
    @property
    def is_cosmic_ready(self) -> bool:
        """Verifica si tiene reporte cósmico generado."""
        return hasattr(self, 'forensic_profile') and self.forensic_profile and bool(self.forensic_profile.ai_comprehensive_report)
    
    @classmethod
    def generate_search_hash(cls, country: str, city: str) -> str:
        """Genera hash de búsqueda para anti-duplicación por lote."""
        import time
        time_window = int(time.time() // 300)  # Cambia cada 5 minutos
        identifier = f"{country}_{city}_{time_window}"
        return hashlib.sha256(identifier.encode()).hexdigest()[:32]


# ======================================================================
# 2. TIER 1 & 2: OMNI-RECON PROFILES - OMEGA ENHANCED
# ======================================================================

class TechProfile(TimeStampedModel):
    """
    [Tier 1: Tech Stack Recon - OMEGA]
    Vector de ataque tecnológico con detección de múltiples LMS y tecnologías.
    """
    institution = models.OneToOneField(
        Institution, on_delete=models.CASCADE, 
        related_name='tech_profile',
        related_query_name='tech_profile'
    )
    
    # LMS Detection
    has_lms = models.BooleanField(default=False, db_index=True)
    lms_provider = models.CharField(max_length=100, blank=True, null=True, db_index=True)
    lms_version = models.CharField(max_length=50, blank=True, null=True)
    lms_confidence = models.FloatField(default=0.0, validators=[MinValueValidator(0.0), MaxValueValidator(1.0)])
    
    # Multiple LMS detection (para instituciones con más de una plataforma)
    detected_lms_list = models.JSONField(default=list, blank=True, help_text="Lista de todos los LMS detectados")
    
    # CMS Detection
    is_wordpress = models.BooleanField(default=False)
    wordpress_version = models.CharField(max_length=20, blank=True, null=True)
    has_custom_cms = models.BooleanField(default=False)
    cms_type = models.CharField(max_length=50, blank=True, null=True)
    
    # Analytics
    has_analytics = models.BooleanField(default=False)
    analytics_providers = models.JSONField(default=list, blank=True)
    
    # Security & Infrastructure
    has_cloudflare = models.BooleanField(default=False)
    has_ssl = models.BooleanField(default=False)
    hosting_provider = models.CharField(max_length=100, blank=True, null=True)
    
    # Frontend Framework
    frontend_framework = models.CharField(max_length=50, blank=True, null=True)
    has_modern_frontend = models.BooleanField(default=False)
    
    # E-commerce / Payments
    has_ecommerce = models.BooleanField(default=False)
    payment_providers = models.JSONField(default=list, blank=True)
    
    # SEO y Metadatos
    seo_framework = models.CharField(max_length=50, blank=True, null=True)
    has_schema_org = models.BooleanField(default=False)
    
    last_scanned = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Perfil Tecnológico (Omega)"
        verbose_name_plural = "Perfiles Tecnológicos (Omega)"
        indexes = [
            models.Index(fields=['lms_provider', 'has_lms']),
            models.Index(fields=['is_wordpress', 'has_analytics']),
        ]

    def __str__(self):
        if self.has_lms and self.lms_provider:
            return f"🔧 {self.institution.name} | LMS: {self.lms_provider}"
        return f"🔧 {self.institution.name} | Sin LMS detectado"


class DeepForensicProfile(TimeStampedModel):
    """
    ======================================================================
    [GOD TIER OMEGA ARCHITECTURE: THE UNIFIED CORE]
    [Tier 2: AI Deep Recon & Neural Semantics - COSMIC EDITION]
    Despliegue de variables explícitas y reportes de IA para filtrado B2B 
    avanzado de Élite. Combina extracción de DOM y análisis semántico LLM.
    ======================================================================
    """
    institution = models.OneToOneField(
        Institution, on_delete=models.CASCADE, 
        related_name='forensic_profile',
        related_query_name='forensic_profile'
    )
    
    # =========================================================
    # 🎓 VARIABLES DE CALIFICACIÓN EDUCATIVA TOP TIER
    # =========================================================
    is_bilingual = models.BooleanField(default=False, db_index=True, verbose_name="Es Bilingüe")
    is_trilingual = models.BooleanField(default=False, db_index=True, verbose_name="Es Trilingüe")
    is_multilingual = models.BooleanField(default=False, db_index=True, verbose_name="Es Multilingüe")
    
    # Idiomas detectados con niveles
    languages_detected = models.JSONField(default=list, blank=True, verbose_name="Lista de Idiomas (Ej: ['Inglés', 'Francés'])")
    language_levels = models.JSONField(default=dict, blank=True, verbose_name="Niveles por idioma")
    
    # =========================================================
    # 🏆 CERTIFICACIONES DE PESO PESADO
    # =========================================================
    has_ib_cert = models.BooleanField(default=False, db_index=True, verbose_name="Certificación IB (Bachillerato Internacional)")
    ib_programs = models.JSONField(default=list, blank=True, verbose_name="Programas IB (PYP/MYP/DP)")
    ib_since = models.CharField(max_length=10, blank=True, null=True)
    
    has_cambridge_cert = models.BooleanField(default=False, db_index=True, verbose_name="Certificación Cambridge")
    cambridge_exams = models.JSONField(default=list, blank=True, verbose_name="Exámenes Cambridge ofrecidos")
    cambridge_center_number = models.CharField(max_length=20, blank=True, null=True)
    
    has_oxford_cert = models.BooleanField(default=False, db_index=True, verbose_name="Certificación Oxford")
    has_toefl_cert = models.BooleanField(default=False, verbose_name="Certificación TOEFL")
    has_ielts_cert = models.BooleanField(default=False, verbose_name="Certificación IELTS")
    
    # Certificaciones de calidad
    iso_9001 = models.BooleanField(default=False, verbose_name="ISO 9001")
    iso_14001 = models.BooleanField(default=False, verbose_name="ISO 14001")
    efqm = models.BooleanField(default=False, verbose_name="EFQM")
    great_place_to_study = models.BooleanField(default=False, verbose_name="Great Place to Study")
    
    other_certifications = models.JSONField(default=list, blank=True, verbose_name="Otras certificaciones")
    
    # =========================================================
    # 🎨 ÉNFASIS PEDAGÓGICO
    # =========================================================
    pedagogical_emphasis = models.CharField(max_length=150, blank=True, null=True, db_index=True, verbose_name="Énfasis (Ej: Montessori, STEAM)")
    pedagogical_model = models.CharField(max_length=100, blank=True, null=True, verbose_name="Modelo Pedagógico")
    academic_emphasis = models.CharField(max_length=100, blank=True, null=True, verbose_name="Énfasis Académico")
    
    # =========================================================
    # 🤖 TECNOLOGÍA EDUCATIVA AVANZADA
    # =========================================================
    has_robotics = models.BooleanField(default=False, db_index=True, verbose_name="Programa de Robótica")
    robotics_details = models.JSONField(default=dict, blank=True, verbose_name="Detalles de Robótica")
    
    has_stem = models.BooleanField(default=False, db_index=True, verbose_name="Programa STEM")
    stem_details = models.JSONField(default=dict, blank=True, verbose_name="Detalles de STEM")
    
    has_programming = models.BooleanField(default=False, db_index=True, verbose_name="Programación")
    programming_details = models.JSONField(default=dict, blank=True, verbose_name="Detalles de Programación")
    
    has_ai_initiatives = models.BooleanField(default=False, verbose_name="Iniciativas de IA")
    
    # =========================================================
    # 🏛️ INFRAESTRUCTURA Y EXTRACURRICULARES
    # =========================================================
    campus_size = models.CharField(max_length=50, blank=True, null=True)
    sports_facilities = models.JSONField(default=list, blank=True)
    has_library = models.BooleanField(default=False)
    has_transport = models.BooleanField(default=False)
    has_dining = models.BooleanField(default=False)
    
    sports = models.JSONField(default=list, blank=True)
    arts = models.JSONField(default=list, blank=True)
    clubs = models.JSONField(default=list, blank=True)
    
    # =========================================================
    # 🤝 CONVENIOS Y ALIANZAS
    # =========================================================
    university_agreements = models.JSONField(default=list, blank=True)
    corporate_agreements = models.JSONField(default=list, blank=True)
    international_agreements = models.JSONField(default=list, blank=True)
    
    # =========================================================
    # 💾 PAYLOAD NATIVO PARA IA
    # =========================================================
    extracted_data = models.JSONField(
        default=dict, blank=True, 
        verbose_name="Data Profunda Cruda (DOM Completo JSON)",
        help_text="HTML procesado y datos extraídos para análisis."
    )
    
    # =========================================================
    # 🧠 NEURAL ENGINE UNIFIED FIELDS - OMEGA
    # =========================================================
    ai_comprehensive_report = models.TextField(
        "🌌 Reporte Estratégico IA (Cosmic Edition)", 
        blank=True, 
        null=True,
        help_text="Reporte Markdown generado por el LLM (DeepSeek/OpenAI) con análisis cualitativo B2B completo."
    )
    
    ai_structured_data = models.JSONField(
        "📊 Data Estructurada (JSON) - Cosmic Intelligence",
        blank=True, 
        null=True,
        help_text="Diccionario JSON estricto forzado por Prompt Engineering para consultas avanzadas O(1)."
    )
    
    # =========================================================
    # 📈 ANÁLISIS DE VENTAS (SALES INTELLIGENCE)
    # =========================================================
    pain_points = models.JSONField(default=list, blank=True, verbose_name="Pain Points Detectados")
    sales_triggers = models.JSONField(default=list, blank=True, verbose_name="Sales Triggers")
    opportunities = models.JSONField(default=list, blank=True, verbose_name="Oportunidades Comerciales")
    ideal_contact_role = models.CharField(max_length=100, blank=True, null=True, verbose_name="Rol de Contacto Ideal")
    recommended_approach = models.TextField(blank=True, null=True, verbose_name="Enfoque de Venta Recomendado")
    estimated_revenue_potential = models.CharField(max_length=50, blank=True, null=True, verbose_name="Potencial de Ingreso Estimado")
    sales_priority = models.CharField(max_length=20, default="Medium", choices=[
        ('High', 'Alta'),
        ('Medium', 'Media'),
        ('Low', 'Baja')
    ])
    
    # =========================================================
    # 📊 MÉTRICAS DE EXTRACCIÓN
    # =========================================================
    ai_classification = models.CharField(max_length=100, blank=True, null=True, db_index=True)
    executive_summary = models.TextField(blank=True, null=True)
    sales_playbook = models.JSONField(default=list, blank=True)
    predictive_copy = models.TextField(blank=True, null=True)
    
    estimated_budget = models.CharField(max_length=100, blank=True, null=True)
    ai_confidence_score = models.FloatField(default=0.0)
    
    last_scanned = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "🌌 Perfil Forense IA (Cosmic)"
        verbose_name_plural = "🌌 Perfiles Forenses IA (Cosmic)"
        
        indexes = [
            models.Index(fields=['is_bilingual', 'is_trilingual'], name='idx_bilingual_trilingual'),
            models.Index(fields=['has_ib_cert', 'has_cambridge_cert'], name='idx_ib_cambridge'),
            models.Index(fields=['has_robotics', 'has_stem', 'has_programming'], name='idx_tech_programs'),
            models.Index(fields=['sales_priority', 'ai_confidence_score'], name='idx_sales_priority'),
            models.Index(fields=['pedagogical_emphasis'], name='idx_pedagogical_emphasis'),
        ]

    def __str__(self):
        badges = []
        if self.is_bilingual:
            badges.append("🗣️")
        if self.has_ib_cert:
            badges.append("🏆")
        if self.has_robotics:
            badges.append("🤖")
        if self.has_stem:
            badges.append("🔬")
        badge_str = " ".join(badges) if badges else "📊"
        return f"{badge_str} {self.institution.name} | Score: {self.ai_confidence_score:.1%}"


# ======================================================================
# 3. CRM & OUTREACH: TARGETS & INTERACTIONS (AI MEMORY ENGINE) - OMEGA
# ======================================================================

class Contact(TimeStampedModel):
    """[OMEGA] Contacto humano con scoring y trazabilidad completa."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    institution = models.ForeignKey(Institution, on_delete=models.CASCADE, related_name='contacts')
    
    name = models.CharField(max_length=255)
    role = models.CharField(max_length=255, blank=True, null=True, db_index=True)
    email = models.EmailField(blank=True, null=True, unique=True)
    linkedin = models.URLField(blank=True, null=True)
    phone = models.CharField(max_length=50, blank=True, null=True)
    
    # Scoring del contacto
    contact_score = models.IntegerField(default=0, validators=[MinValueValidator(0), MaxValueValidator(100)])
    is_decision_maker = models.BooleanField(default=False, db_index=True)
    is_technical_contact = models.BooleanField(default=False)
    
    # Metadata de contacto
    is_valid_email = models.BooleanField(default=True)
    email_validation_date = models.DateTimeField(blank=True, null=True)
    last_contacted_at = models.DateTimeField(blank=True, null=True)
    
    # Preferencias de contacto
    preferred_channel = models.CharField(max_length=20, choices=[
        ('email', 'Email'),
        ('whatsapp', 'WhatsApp'),
        ('phone', 'Teléfono'),
        ('linkedin', 'LinkedIn')
    ], default='email')
    
    class Meta:
        verbose_name = "Contacto (Omega)"
        verbose_name_plural = "Contactos (Omega)"
        indexes = [
            models.Index(fields=['institution', 'role'], name='idx_inst_role'),
            models.Index(fields=['is_decision_maker', 'contact_score'], name='idx_dm_score'),
            models.Index(fields=['email'], name='idx_contact_email'),
        ]

    def __str__(self):
        dm_icon = "👑" if self.is_decision_maker else "👤"
        return f"{dm_icon} {self.name} | {self.role} | {self.email or 'No email'}"


class Interaction(TimeStampedModel):
    """
    [OMEGA AI MEMORY & QUANTUM STATE MACHINE]
    Posee un hilo conversacional (Thread ID) y un programador de acciones
    para que la IA sepa exactamente cuándo y cómo re-contactar.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # [MEMORIA DE LA IA] Agrupa correos y WhatsApps en una sola línea temporal
    thread_id = models.CharField(
        max_length=255, blank=True, null=True, db_index=True,
        verbose_name="ID de Hilo (Contexto IA)",
        help_text="UUID que agrupa toda la conversación."
    )
    
    class Status(models.TextChoices):
        NEW = 'NEW', _('📝 Nuevo (Pendiente)')
        QUEUED = 'QUEUED', _('⏳ En Cola (Worker)') 
        SENT = 'SENT', _('📨 Enviado (Entregado)')
        OPENED = 'OPENED', _('👁️ Abierto (Pixel Tracking)')
        REPLIED = 'REPLIED', _('💬 Respondido (Inbound)')
        MEETING = 'MEETING', _('📅 Reunión Agendada (Success)')
        BOUNCED = 'BOUNCED', _('⚠️ Rebotado (Fallo de Red/Email)')
        CLOSED = 'CLOSED', _('🔒 Cerrado (Perdido/Sin Interés)')

    class Channel(models.TextChoices):
        EMAIL = 'EMAIL', _('📧 Email')
        WHATSAPP = 'WHATSAPP', _('💬 WhatsApp')
        LINKEDIN = 'LINKEDIN', _('🔗 LinkedIn')
        CALL = 'CALL', _('📞 Llamada')

    institution = models.ForeignKey(
        Institution, on_delete=models.CASCADE, 
        related_name='interactions', 
        db_index=True
    )
    contact = models.ForeignKey(
        Contact, on_delete=models.SET_NULL, 
        null=True, blank=True, 
        related_name='interactions', 
        db_index=True
    )

    channel = models.CharField(max_length=20, choices=Channel.choices, default=Channel.EMAIL, db_index=True)
    
    subject = models.CharField(max_length=255, blank=True, null=True)
    message_sent = models.TextField(blank=True, null=True)
    message_received = models.TextField(blank=True, null=True)
    
    # Analítica de Sentimiento inyectada por IA
    ai_sentiment = models.CharField(
        max_length=50, blank=True, null=True, db_index=True, 
        verbose_name="Sentimiento (Positivo/Negativo/Objeción)"
    )
    sentiment_score = models.FloatField(default=0.0, validators=[MinValueValidator(-1.0), MaxValueValidator(1.0)])
    
    # Telemetría completa
    telemetry_data = models.JSONField(default=dict, blank=True)
    
    # Métricas de engagement
    opened_count = models.IntegerField(default=0)
    clicked_count = models.IntegerField(default=0)
    replied = models.BooleanField(default=False, db_index=True)
    
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.NEW, db_index=True)
    
    # Meeting data
    meeting_date = models.DateTimeField(blank=True, null=True)
    meeting_link = models.URLField(blank=True, null=True)
    
    # [IA AUTÓNOMA] Fecha en la que la IA debe leer este registro y enviar el siguiente mensaje
    next_action_date = models.DateTimeField(blank=True, null=True, db_index=True, verbose_name="Próxima Acción (IA Follow-up)")
    
    # Follow-up tracking
    follow_up_count = models.IntegerField(default=0)
    last_follow_up_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = "🌌 Interacción B2B (Cosmic)"
        verbose_name_plural = "🌌 Interacciones B2B (Cosmic)"
        
        indexes = [
            models.Index(fields=['institution', 'status'], name='idx_inst_status'),
            models.Index(fields=['status', 'replied'], name='idx_status_replied'),
            models.Index(fields=['thread_id', 'created_at'], name='idx_thread_created'),
            models.Index(fields=['next_action_date'], name='idx_next_action'),
            models.Index(fields=['channel', 'status'], name='idx_channel_status'),
            models.Index(fields=['contact', 'created_at'], name='idx_contact_created'),
        ]

    def __str__(self) -> str:
        status_icon = {
            'NEW': '📝',
            'SENT': '📨',
            'OPENED': '👁️',
            'REPLIED': '💬',
            'MEETING': '📅',
            'BOUNCED': '⚠️',
            'CLOSED': '🔒'
        }.get(self.status, '⚡')
        return f"{status_icon} [{self.get_channel_display()}] Thread: {self.thread_id[:8] if self.thread_id else 'N/A'} -> {self.get_status_display()}"

    def register_open(self, ip_address: str = "Unknown", user_agent: str = "Unknown") -> None:
        """Registra apertura de correo con telemetría completa."""
        self.opened_count += 1
        if self.status in [self.Status.NEW, self.Status.QUEUED, self.Status.SENT]:
            self.status = self.Status.OPENED
            self.institution.escalate_lead(10)
        
        opens_log = self.telemetry_data.get('opens', [])
        opens_log.append({
            'timestamp': timezone.now().isoformat(), 
            'ip': ip_address, 
            'user_agent': user_agent,
            'event_number': self.opened_count
        })
        self.telemetry_data['opens'] = opens_log
        self.save(update_fields=['opened_count', 'status', 'telemetry_data', 'updated_at'])

    def register_inbound_reply(self, raw_payload: str, intent: str = "NEUTRAL", sentiment_score: float = 0.0) -> None:
        """Registra respuesta entrante con análisis de sentimiento IA."""
        self.message_received = raw_payload
        self.replied = True
        self.ai_sentiment = intent
        self.sentiment_score = sentiment_score
        
        if self.status not in [self.Status.MEETING, self.Status.CLOSED]:
            self.status = self.Status.REPLIED
            self.institution.escalate_lead(30)
            
        self.telemetry_data['nlp_engine'] = {
            'intent': intent, 
            'sentiment_score': sentiment_score, 
            'processed_at': timezone.now().isoformat(),
            'raw_length': len(raw_payload)
        }
        
        # Desactivamos el seguimiento automático porque el humano ya respondió
        self.next_action_date = None
        
        self.save(update_fields=[
            'message_received', 'replied', 'status', 'telemetry_data', 
            'updated_at', 'ai_sentiment', 'sentiment_score', 'next_action_date'
        ])
    
    def schedule_follow_up(self, days: int = 3):
        """Programa follow-up para una fecha futura."""
        self.next_action_date = timezone.now() + timedelta(days=days)
        self.save(update_fields=['next_action_date', 'updated_at'])


# ======================================================================
# 4. HIGH-PERFORMANCE DATA WAREHOUSE (BI LAYER) - OMEGA
# ======================================================================

class CommandCenterQuerySet(models.QuerySet):
    """[OMEGA] QuerySet optimizado para el centro de comando con caching."""
    
    def get_funnel_metrics(self) -> dict:
        """Obtiene métricas del embudo de ventas con una sola query."""
        return self.aggregate(
            total_leads=Count('id'),
            blind_leads=Count('id', filter=Q(website__isnull=True) | Q(website='')),
            ready_to_scan=Count('id', filter=Q(is_active=True, website__isnull=False, processing_status='RAW') & ~Q(website='')),
            enriched_leads=Count('id', filter=Q(processing_status='ENRICHED')),
            cosmic_leads=Count('id', filter=Q(processing_status='COSMIC')),
            avg_score=Avg('lead_score'),
            avg_confidence=Avg('confidence_score'),
            hot_leads=Count('id', filter=Q(lead_score__gte=75)),
            warm_leads=Count('id', filter=Q(lead_score__gte=50, lead_score__lt=75)),
            cold_leads=Count('id', filter=Q(lead_score__lt=50)),
            bilingual_leads=Count('id', filter=Q(forensic_profile__is_bilingual=True)),
            ib_leads=Count('id', filter=Q(forensic_profile__has_ib_cert=True)),
            robotics_leads=Count('id', filter=Q(forensic_profile__has_robotics=True)),
        )
    
    def with_cosmic_reports(self):
        """Filtra instituciones con reporte cósmico."""
        return self.filter(forensic_profile__ai_comprehensive_report__isnull=False)
    
    def high_value_targets(self, min_score: int = 75):
        """Obtiene objetivos de alto valor."""
        return self.filter(lead_score__gte=min_score, is_active=True)


class CommandCenterManager(models.Manager):
    """[OMEGA] Manager con métodos optimizados para el centro de comando."""
    
    def get_queryset(self): 
        return CommandCenterQuerySet(self.model, using=self._db)
    
    def get_dashboard_stats(self): 
        return self.get_queryset().get_funnel_metrics()
    
    def get_recent_cosmic_reports(self, limit: int = 10):
        """Obtiene reportes cósmicos recientes."""
        return self.get_queryset().filter(
            forensic_profile__ai_comprehensive_report__isnull=False
        ).select_related('forensic_profile').order_by('-updated_at')[:limit]
    
    def get_leads_by_city(self, city: str, min_score: int = 0):
        """Obtiene leads filtrados por ciudad."""
        return self.get_queryset().filter(
            city__iexact=city, 
            lead_score__gte=min_score,
            is_active=True
        )


# ======================================================================
# 5. THE FACADE PATTERN (ADMIN PROXY ROUTERS) - OMEGA ENHANCED
# ======================================================================

class CommandCenter(Institution):
    """Proxy para el Centro de Comando con permisos específicos."""
    objects = CommandCenterManager()
    
    class Meta:
        proxy = True
        app_label = 'sales'
        verbose_name = _('🚀 Sovereign Command Center (Omega)')
        verbose_name_plural = _('🚀 Sovereign Command Center (Omega)')
        permissions = [
            ("can_execute_osm_radar", _("Security: Can launch OSM Satellite Discovery")),
            ("can_execute_serp_resolver", _("Security: Can launch SERP URL Resolver")),
            ("can_execute_ghost_sniper", _("Security: Can launch the Ghost Sniper Engine")),
            ("view_executive_dashboard", _("Analytics: Can view C-Level Pipeline Metrics")),
            ("can_generate_cosmic_reports", _("AI: Can generate Cosmic AI Reports")),
            ("view_cosmic_intelligence", _("AI: Can view Cosmic Intelligence Reports")),
        ]


class GlobalPipeline(Institution):
    """Proxy para el Pipeline Global de Ventas."""
    class Meta: 
        proxy = True
        app_label = 'sales'
        verbose_name = "1. 🌐 Global Database (Cosmic)"
        verbose_name_plural = "1. 🌐 Global Database (Cosmic)"


class SniperConsole(Institution):
    """Proxy para la Consola de Sniper."""
    class Meta: 
        proxy = True
        app_label = 'sales'
        verbose_name = "2. 🎯 Sniper Console (Omega)"
        verbose_name_plural = "2. 🎯 Sniper Console (Omega)"


class GeoRadarWorkspace(Institution):
    """Proxy para el Radar Geoespacial con soporte para modo extremo."""
    class Meta: 
        proxy = True
        app_label = 'sales'
        verbose_name = "3. 🛰️ Geospatial Radar (Omega)"
        verbose_name_plural = "3. 🛰️ Geospatial Radar (Omega)"