import uuid
import re
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator, RegexValidator
from django.db.models import Count, Q, Avg, CheckConstraint, F
from django.utils.translation import gettext_lazy as _
from django.utils import timezone
#from .base_models import TimeStampedModel # <-- Ajusta este import a tu arquitectura
#from model_utils.models import TimeStampedModel
# ======================================================================
# 0. CORE: ABSTRACT BASE (AUDIT TRAIL & DDD)
# ======================================================================

class TimeStampedModel(models.Model):
    """
    [God Tier Audit Trail]
    Capa de inmutabilidad base. Utiliza db_index estratégico para
    consultas temporales y limpieza de registros (TTL Policies).
    """
    created_at = models.DateTimeField(
        auto_now_add=True, 
        db_index=True,
        db_comment="Timestamp inmutable de creación (UTC)."
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        db_comment="Timestamp dinámico de última mutación (UTC)."
    )

    class Meta:
        abstract = True


# ======================================================================
# 1. TIER 0: MASTER NODE (INSTITUTION & STATE MACHINE)
# ======================================================================

class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True

class Institution(TimeStampedModel):
    """
    [C2 Master Node & State Machine]
    Controla el flujo de trabajo (Radar -> Sniper -> AI Outreach).
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    mission_id = models.UUIDField(
        null=True, blank=True, db_index=True, 
        verbose_name="ID de Misión/Batch",
        db_comment="Agrupador lógico para campañas Celery (Swarm ID)."
    )
    
    class InstitutionType(models.TextChoices):
        KINDERGARTEN = 'kindergarten', _('Jardín Infantil / Preescolar')
        SCHOOL = 'school', _('Colegio (Básica/Media)')
        UNIVERSITY = 'university', _('Universidad / Educación Superior')
        INSTITUTE = 'institute', _('Instituto Técnico / Tecnológico')
        OTHER = 'other', _('Otro')

    class DiscoverySource(models.TextChoices):
        OSM = 'osm', _('OpenStreetMap (GeoRadar)')
        GOV_DATA = 'gov_data', _('Directorio Gubernamental')
        SERP = 'serp', _('SERP Engine (Web Scraping)')
        MANUAL = 'manual', _('CRM Inbound / Manual')
        GHOST = 'Ghost_V23', _('Ghost Sniper V23 (Autonomous)') # Actualizado a V23

    # [GOD TIER: STATE MACHINE] Control de flujo anti-duplicación
    class ProcessingStatus(models.TextChoices):
        RAW_RADAR = 'RAW', _('Crudo (Solo Mapa/Radar)')
        SNIPER_LOCKED = 'LOCKED', _('Bloqueado (Sniper Extrayendo)')
        ENRICHED = 'ENRICHED', _('Enriquecido (Listo para Ventas)')
        DISCARDED = 'DISCARDED', _('Descartado (Falso Positivo/Caído)')

    processing_status = models.CharField(
        max_length=20, choices=ProcessingStatus.choices,
        default=ProcessingStatus.RAW_RADAR, db_index=True,
        verbose_name="Estado en Pipeline"
    )

    # --- IDENTIDAD Y CONTACTO BÁSICO ---
    name = models.CharField(max_length=255, verbose_name="Nombre de la Institución")
    
    website = models.URLField(
        max_length=255, unique=True, null=True, blank=True, 
        verbose_name="Sitio Web Oficial"
    )
    
    # Índice de confianza de URL (Evita atacar URLs basura como directorios)
    url_trust_score = models.FloatField(
        default=0.0, validators=[MinValueValidator(0.0), MaxValueValidator(100.0)],
        help_text="Heurística de Levenshtein (Qué tan real es la URL)",
        db_comment="Filtro matemático para descartar Falsos Positivos de URLs antes del Scrape."
    )
    
    email = models.EmailField(blank=True, null=True, verbose_name="Email Principal (Sanitizado)")
    
    # [GOD TIER FIX]: Campo de teléfono agnóstico.
    # 1. Eliminamos el RegexValidator restrictivo para permitir la sintaxis "W:300... T:601...".
    # 2. Aumentamos el max_length a 120 para soportar múltiples canales de comunicación sin Truncation.
    phone = models.CharField(
        max_length=120, blank=True, null=True, 
        verbose_name="Omni-Canal (WA/TEL)",
        help_text="Soporta formato Ghost Sniper (Ej: W:3001234567 T:6011234567)"
    )
    
    # --- CLASIFICACIÓN DE NEGOCIO (B2B TARGETING) ---
    institution_type = models.CharField(
        max_length=20, choices=InstitutionType.choices, 
        default=InstitutionType.SCHOOL, db_index=True
    )
    is_private = models.BooleanField(default=True, db_index=True)
    student_count = models.PositiveIntegerField(null=True, blank=True)

    # --- GEOLOCALIZACIÓN Y TERRITORIOS ---
    country = models.CharField(max_length=100, db_index=True, default="Colombia")
    state_region = models.CharField(max_length=100, blank=True, null=True, db_index=True)
    city = models.CharField(max_length=100, db_index=True)
    address = models.CharField(max_length=255, blank=True, null=True)
    latitude = models.DecimalField(max_digits=11, decimal_places=8, blank=True, null=True)
    longitude = models.DecimalField(max_digits=11, decimal_places=8, blank=True, null=True)

    # --- TRAZABILIDAD Y CRM ROUTING ---
    discovery_source = models.CharField(max_length=20, choices=DiscoverySource.choices, default=DiscoverySource.MANUAL)
    is_active = models.BooleanField(default=True, db_index=True)
    last_scored_at = models.DateTimeField(blank=True, null=True, db_index=True)
    
    lead_score = models.IntegerField(
        default=0, db_index=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        db_comment="Termómetro de Venta Predictivo (0-100)."
    )
    
    contacted = models.BooleanField(default=False, db_index=True)

    class Meta:
        verbose_name = "Institución Educativa"
        verbose_name_plural = "Instituciones Educativas"
        ordering = ['-lead_score', '-updated_at']
        
        indexes = [
            models.Index(fields=['processing_status', 'city']), # Acelera la consulta "Dame 500 de Cajicá crudos"
            models.Index(fields=['-lead_score', 'contacted', 'is_active']),
            models.Index(fields=['country', 'state_region', 'city']),
            models.Index(
                fields=['processing_status', 'website'], 
                name='idx_sniper_queue',
                condition=Q(is_active=True) & Q(website__isnull=False) & Q(processing_status='RAW')
            ),
        ]
        
        constraints = [
            models.CheckConstraint(
                check=models.Q(lead_score__gte=0) & models.Q(lead_score__lte=100),
                name='lead_score_range_0_to_100'
            ),
            models.UniqueConstraint(
                fields=['name', 'city', 'country'],
                name='unique_institution_per_city_country'
            )
        ]

    def __str__(self):
        return f"{self.name} | Score: {self.lead_score} | {self.city}"

    def lock_for_sniper(self):
        """[DDD] Bloquea el registro para que ningún otro worker lo procese concurrente."""
        self.processing_status = self.ProcessingStatus.SNIPER_LOCKED
        self.save(update_fields=['processing_status', 'updated_at'])

    def escalate_lead(self, points: int):
        self.lead_score = min(self.lead_score + points, 100)
        self.save(update_fields=['lead_score', 'updated_at'])


# ======================================================================
# 2. TIER 1 & 2: OMNI-RECON PROFILES
# ======================================================================

class TechProfile(TimeStampedModel):
    """
    [Tier 1: Tech Stack Recon]
    Vector de ataque tecnológico (LMS, CMS, etc).
    """
    institution = models.OneToOneField(
        Institution, on_delete=models.CASCADE, 
        related_name='tech_profile',
        related_query_name='tech_profile'
    )
    
    has_lms = models.BooleanField(default=False, db_index=True)
    lms_provider = models.CharField(max_length=100, blank=True, null=True, db_index=True)
    
    is_wordpress = models.BooleanField(default=False)
    has_analytics = models.BooleanField(default=False)
    
    last_scanned = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Perfil Tecnológico"
        verbose_name_plural = "Perfiles Tecnológicos"


class DeepForensicProfile(TimeStampedModel):
    """
    ======================================================================
    [GOD TIER ARCHITECTURE: THE UNIFIED CORE]
    [Tier 2: AI Deep Recon & Neural Semantics]
    Despliegue de variables explícitas y reportes de IA para filtrado B2B 
    avanzado de Élite. Combina extracción de DOM y análisis semántico LLM.
    ======================================================================
    """
    institution = models.OneToOneField(
        Institution, on_delete=models.CASCADE, 
        related_name='forensic_profile',
        related_query_name='forensic_profile'
    )
    
    # --- VARIABLES DE CALIFICACIÓN EDUCATIVA TOP TIER ---
    is_bilingual = models.BooleanField(default=False, db_index=True, verbose_name="Es Bilingüe")
    is_trilingual = models.BooleanField(default=False, db_index=True, verbose_name="Es Trilingüe")
    
    # Certificaciones de Peso Pesado
    has_ib_cert = models.BooleanField(default=False, db_index=True, verbose_name="Certificación IB (Bachillerato Internacional)")
    has_cambridge_cert = models.BooleanField(default=False, db_index=True, verbose_name="Certificación Cambridge")
    
    languages_detected = models.JSONField(default=list, blank=True, verbose_name="Lista de Idiomas (Ej: ['Inglés', 'Francés'])")
    pedagogical_emphasis = models.CharField(max_length=150, blank=True, null=True, db_index=True, verbose_name="Énfasis (Ej: Montessori, STEAM)")
    
    # Payload nativo para la IA
    extracted_data = models.JSONField(
        default=dict, blank=True, 
        verbose_name="Data Profunda Cruda (DOM Completo JSON)"
    )
    
    # --- LEGACY AI FIELDS (Se mantienen para compatibilidad estructural) ---
    ai_classification = models.CharField(max_length=100, blank=True, null=True, db_index=True)
    executive_summary = models.TextField(blank=True, null=True)
    sales_playbook = models.JSONField(default=list, blank=True)
    predictive_copy = models.TextField(blank=True, null=True)
    
    estimated_budget = models.CharField(max_length=100, blank=True, null=True)
    ai_confidence_score = models.FloatField(default=0.0)

    # =========================================================
    # [GOD TIER V44.0]: NEURAL ENGINE UNIFIED FIELDS
    # =========================================================
    ai_comprehensive_report = models.TextField(
        "Reporte Estratégico IA", 
        blank=True, 
        null=True,
        help_text="Reporte Markdown generado por el LLM (DeepSeek/OpenAI) con análisis cualitativo B2B."
    )
    ai_structured_data = models.JSONField(
        "Data Estructurada (JSON)", 
        blank=True, 
        null=True,
        help_text="Diccionario JSON estricto forzado por Prompt Engineering para consultas avanzadas (O(1))."
    )

    last_scanned = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Perfil Forense IA"
        verbose_name_plural = "Perfiles Forenses IA"
        
        indexes = [
            models.Index(fields=['is_bilingual', 'is_trilingual']),
            models.Index(fields=['has_ib_cert', 'has_cambridge_cert']),
        ]

    def __str__(self):
        return f"Deep Forensic: {self.institution.name}"


# ======================================================================
# 3. CRM & OUTREACH: TARGETS & INTERACTIONS (AI MEMORY ENGINE)
# ======================================================================

class Contact(TimeStampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    institution = models.ForeignKey(Institution, on_delete=models.CASCADE, related_name='contacts')
    
    name = models.CharField(max_length=255)
    role = models.CharField(max_length=255, blank=True, null=True, db_index=True)
    email = models.EmailField(blank=True, null=True, unique=True)
    linkedin = models.URLField(blank=True, null=True)
    phone = models.CharField(max_length=50, blank=True, null=True)
    
    is_valid_email = models.BooleanField(default=True)

    class Meta:
        indexes = [models.Index(fields=['institution', 'role'])]


class Interaction(TimeStampedModel):
    """
    [AI MEMORY & STATE MACHINE]
    Posee un hilo conversacional (Thread ID) y un programador de acciones
    para que la IA sepa exactamente cuándo y cómo re-contactar.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # [MEMORIA DE LA IA] Agrupa correos y WhatsApps en una sola línea temporal
    thread_id = models.CharField(
        max_length=255, blank=True, null=True, db_index=True,
        verbose_name="ID de Hilo (Contexto IA)"
    )
    
    class Status(models.TextChoices):
        NEW = 'NEW', _('Nuevo (Pendiente)')
        QUEUED = 'QUEUED', _('En Cola (Worker)') 
        SENT = 'SENT', _('Enviado (Entregado)')
        OPENED = 'OPENED', _('Abierto (Pixel Tracking)')
        REPLIED = 'REPLIED', _('Respondido (Inbound)')
        MEETING = 'MEETING', _('Reunión Agendada (Success)')
        BOUNCED = 'BOUNCED', _('Rebotado (Fallo de Red/Email)')
        CLOSED = 'CLOSED', _('Cerrado (Perdido/Sin Interés)')

    class Channel(models.TextChoices):
        EMAIL = 'EMAIL', _('Email')
        WHATSAPP = 'WHATSAPP', _('WhatsApp')
        LINKEDIN = 'LINKEDIN', _('LinkedIn')
        CALL = 'CALL', _('Llamada')

    institution = models.ForeignKey(Institution, on_delete=models.CASCADE, related_name='interactions', db_index=True)
    contact = models.ForeignKey(Contact, on_delete=models.SET_NULL, null=True, blank=True, related_name='interactions', db_index=True)

    channel = models.CharField(max_length=20, choices=Channel.choices, default=Channel.EMAIL, db_index=True)
    
    subject = models.CharField(max_length=255, blank=True, null=True)
    message_sent = models.TextField(blank=True, null=True)
    message_received = models.TextField(blank=True, null=True)
    
    # Analítica de Sentimiento inyectada por IA
    ai_sentiment = models.CharField(max_length=50, blank=True, null=True, db_index=True, verbose_name="Sentimiento (Positivo/Negativo/Objeción)")
    
    telemetry_data = models.JSONField(default=dict, blank=True)
    
    opened_count = models.IntegerField(default=0)
    clicked_count = models.IntegerField(default=0)
    replied = models.BooleanField(default=False, db_index=True)
    
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.NEW, db_index=True)
    
    meeting_date = models.DateTimeField(blank=True, null=True)
    
    # [IA AUTÓNOMA] Fecha en la que la IA debe leer este registro y enviar el siguiente mensaje
    next_action_date = models.DateTimeField(blank=True, null=True, db_index=True, verbose_name="Próxima Acción (IA Follow-up)")

    class Meta:
        ordering = ['-created_at']
        verbose_name = "Interacción B2B"
        verbose_name_plural = "Interacciones B2B"
        
        indexes = [
            models.Index(fields=['institution', 'status']),
            models.Index(fields=['status', 'replied']),
            models.Index(fields=['thread_id', 'created_at']), # Acelera la reconstrucción de memoria de la IA
            models.Index(fields=['next_action_date']), # Permite al Cron Job encontrar rápidamente a quién escribirle hoy
        ]

    def __str__(self) -> str:
        return f"[{self.get_channel_display()}] Thread: {self.thread_id} -> {self.get_status_display()}"

    def register_open(self, ip_address: str = "Unknown", user_agent: str = "Unknown") -> None:
        self.opened_count += 1
        if self.status in [self.Status.NEW, self.Status.QUEUED, self.Status.SENT]:
            self.status = self.Status.OPENED
            self.institution.escalate_lead(10)
        
        opens_log = self.telemetry_data.get('opens', [])
        opens_log.append({'timestamp': timezone.now().isoformat(), 'ip': ip_address, 'user_agent': user_agent})
        self.telemetry_data['opens'] = opens_log
        self.save(update_fields=['opened_count', 'status', 'telemetry_data', 'updated_at'])

    def register_inbound_reply(self, raw_payload: str, intent: str = "NEUTRAL", sentiment_score: float = 0.0) -> None:
        self.message_received = raw_payload
        self.replied = True
        self.ai_sentiment = intent
        
        if self.status not in [self.Status.MEETING, self.Status.CLOSED]:
            self.status = self.Status.REPLIED
            self.institution.escalate_lead(30)
            
        self.telemetry_data['nlp_engine'] = {
            'intent': intent, 'sentiment_score': sentiment_score, 'processed_at': timezone.now().isoformat()
        }
        
        # Desactivamos el seguimiento automático porque el humano ya respondió (La IA debe evaluarlo primero)
        self.next_action_date = None
        
        self.save(update_fields=['message_received', 'replied', 'status', 'telemetry_data', 'updated_at', 'ai_sentiment', 'next_action_date'])


# ======================================================================
# 4. HIGH-PERFORMANCE DATA WAREHOUSE (BI LAYER)
# ======================================================================

class CommandCenterQuerySet(models.QuerySet):
    def get_funnel_metrics(self) -> dict:
        return self.aggregate(
            total_leads=Count('id'),
            blind_leads=Count('id', filter=Q(website__isnull=True) | Q(website='')),
            ready_to_scan=Count('id', filter=Q(is_active=True, website__isnull=False, processing_status='RAW') & ~Q(website='')),
            enriched_leads=Count('id', filter=Q(processing_status='ENRICHED')),
            avg_score=Avg('lead_score'),
            hot_leads=Count('id', filter=Q(lead_score__gte=75))
        )

class CommandCenterManager(models.Manager):
    def get_queryset(self): return CommandCenterQuerySet(self.model, using=self._db)
    def get_dashboard_stats(self): return self.get_queryset().get_funnel_metrics()


# ======================================================================
# 5. THE FACADE PATTERN (ADMIN PROXY ROUTERS)
# ======================================================================

class CommandCenter(Institution):
    objects = CommandCenterManager()
    class Meta:
        proxy = True
        app_label = 'sales'
        verbose_name = _('🚀 Sovereign Command Center')
        verbose_name_plural = _('🚀 Sovereign Command Center')
        permissions = [
            ("can_execute_osm_radar", _("Security: Can launch OSM Satellite Discovery")),
            ("can_execute_serp_resolver", _("Security: Can launch SERP URL Resolver")),
            ("can_execute_ghost_sniper", _("Security: Can launch the Ghost Sniper Engine")),
            ("view_executive_dashboard", _("Analytics: Can view C-Level Pipeline Metrics")),
        ]

class GlobalPipeline(Institution):
    class Meta: proxy = True; app_label = 'sales'; verbose_name = "1. 🌐 Global Database"; verbose_name_plural = "1. 🌐 Global Database"

class SniperConsole(Institution):
    class Meta: proxy = True; app_label = 'sales'; verbose_name = "2. 🎯 Sniper Console"; verbose_name_plural = "2. 🎯 Sniper Console"

class GeoRadarWorkspace(Institution):
    class Meta: proxy = True; app_label = 'sales'; verbose_name = "3. 🛰️ Geospatial Radar"; verbose_name_plural = "3. 🛰️ Geospatial Radar"



'''
class NeuralForensicReport(models.Model):
    """
    ===========================================================================
    [GOD TIER ARCHITECTURE: LEVIATHAN CLASS]
    Modelo Satélite de Análisis Cognitivo y Extracción Semántica (LLM Data Lake).
    
    Diseñado para aislar la carga computacional de la tabla `Institution`.
    Implementa telemetría FinOps (Tokens/Latencia), Manejo de Estado para 
    operaciones asíncronas (Celery) y almacenamiento estructurado (JSON) para 
    consultas complejas de Inteligencia de Negocios (B2B).
    ===========================================================================
    """

    class AnalysisStatus(models.TextChoices):
        PENDING = 'PENDING', _('En Espera / Encolado')
        PROCESSING = 'PROCESSING', _('Procesando (Inferencia IA)')
        COMPLETED = 'COMPLETED', _('Completado Exitosamente')
        FAILED = 'FAILED', _('Fallo de Inferencia / Timeout')
        PARTIAL = 'PARTIAL', _('Completado con Alucinaciones/Incompleto')

    class AIProviderMode(models.TextChoices):
        DEEPSEEK_CHAT = 'deepseek-chat', _('DeepSeek V3 / Coder')
        OPENAI_GPT4O_MINI = 'gpt-4o-mini', _('OpenAI GPT-4 Omni Mini')
        OPENAI_GPT4O = 'gpt-4o', _('OpenAI GPT-4 Omni')
        CLAUDE_3_5 = 'claude-3-5-sonnet', _('Anthropic Claude 3.5 Sonnet')
        LOCAL_LLAMA = 'llama-3-8b', _('Local LLaMA 3 (Fallback)')
        UNKNOWN = 'unknown', _('Desconocido / Legacy')

    # 1. IDENTIFICACIÓN Y RELACIÓN CRÍTICA
    id = models.UUIDField(
        primary_key=True, 
        default=uuid.uuid4, 
        editable=False,
        help_text="UUIDv4 para blindar el acceso a los reportes de inteligencia (Anti-Scraping ID)."
    )
    institution = models.OneToOneField(
        'Institution', # Cambia por la referencia exacta si difiere
        on_delete=models.CASCADE, 
        related_name='neural_report',
        db_index=True
    )

    # 2. CONTROL DE ESTADO (STATE MACHINE PARA CELERY/WORKERS)
    status = models.CharField(
        "Estado del Análisis",
        max_length=20,
        choices=AnalysisStatus.choices,
        default=AnalysisStatus.PENDING,
        db_index=True
    )

    # 3. RESULTADOS DE LA INFERENCIA (HUMAN & MACHINE READABLE)
    comprehensive_report = models.TextField(
        "Reporte Estratégico B2B (Human Readable)", 
        blank=True, 
        null=True,
        help_text="El output en crudo (Markdown/Texto) generado por el LLM diseñado para el equipo de ventas."
    )
    
    structured_intel = models.JSONField(
        "Inteligencia Estructurada (Machine Readable)",
        blank=True,
        null=True,
        help_text="Diccionario JSON extraído por la IA (Ej: {'has_stem': true, 'is_ib': false, 'icfes_score': 'A+'}). Permite consultas complejas en BD."
    )

    # 4. TELEMETRÍA Y FINOPS (AUDITORÍA DE COSTOS Y RENDIMIENTO)
    ai_model_version = models.CharField(
        "Versión del Modelo LLM",
        max_length=50,
        choices=AIProviderMode.choices,
        default=AIProviderMode.UNKNOWN,
        help_text="Identifica el motor cognitivo que produjo este resultado para auditoría de calidad."
    )
    raw_corpus_size = models.IntegerField(
        "Tamaño del Corpus Web (Caracteres)", 
        default=0,
        validators=[MinValueValidator(0)],
        help_text="Cantidad de caracteres del HTML limpiado enviados al LLM en el prompt."
    )
    prompt_tokens = models.PositiveIntegerField(
        "Tokens de Entrada (Prompt)",
        default=0,
        help_text="Para cálculo de costos de Ingesta (FinOps)."
    )
    completion_tokens = models.PositiveIntegerField(
        "Tokens de Salida (Completion)",
        default=0,
        help_text="Para cálculo de costos de Generación (FinOps)."
    )
    processing_latency_ms = models.PositiveIntegerField(
        "Latencia de Inferencia (ms)",
        default=0,
        help_text="Milisegundos que tardó el proveedor de IA en responder la petición HTTP."
    )
    
    # 5. DIAGNÓSTICO DE ERRORES (DEBUGGING)
    error_traceback = models.TextField(
        "Traza de Error (Debug)",
        blank=True,
        null=True,
        help_text="Almacena el stacktrace si la llamada a la IA o el parseo JSON falla."
    )

    # 6. TIMESTAMPS (AUDITORÍA TEMPORAL)
    created_at = models.DateTimeField("Creado el", auto_now_add=True)
    updated_at = models.DateTimeField("Última actualización general", auto_now=True)
    analyzed_at = models.DateTimeField(
        "Momento exacto del Análisis IA", 
        blank=True, 
        null=True,
        db_index=True
    )

    class Meta:
        verbose_name = "Reporte Neural de IA"
        verbose_name_plural = "Reportes Neurales de IA"
        db_table = "sales_neural_forensic_report" # Evita nombres generados automáticamente por Django
        ordering = ['-analyzed_at', '-created_at']
        # Índices compuestos para máxima velocidad (O(1) / O(log N)) al generar dashboards o colas de re-procesamiento
        indexes = [
            models.Index(fields=['status', 'ai_model_version']),
            models.Index(fields=['-analyzed_at', 'status']),
        ]

    def __str__(self):
        return f"Neural Intel [{self.status}]: {self.institution.name}"

    # =========================================================
    # [MÉTODOS NATIVOS] LOGICA DE NEGOCIO ENCAPSULADA
    # =========================================================
    @property
    def total_tokens_used(self) -> int:
        """Calcula el consumo total de tokens para esta operación."""
        return self.prompt_tokens + self.completion_tokens

    @property
    def is_stale(self) -> bool:
        """
        Determina si el reporte cognitivo es demasiado antiguo (Ej: más de 90 días) 
        y requiere que la flota Ghost Swarm vuelva a escanear el sitio web.
        """
        if not self.analyzed_at:
            return True
        stale_threshold = timezone.now() - timezone.timedelta(days=90)
        return self.analyzed_at < stale_threshold

    def mark_as_processing(self, provider: str):
        """Manejo de estado seguro pre-inferencia."""
        self.status = self.AnalysisStatus.PROCESSING
        self.ai_model_version = provider
        self.save(update_fields=['status', 'ai_model_version', 'updated_at'])

    def mark_as_failed(self, error_message: str):
        """Manejo de estado seguro ante caídas de la API (Evita dejar tareas en limbo)."""
        self.status = self.AnalysisStatus.FAILED
        self.error_traceback = str(error_message)
        self.save(update_fields=['status', 'error_traceback', 'updated_at'])

    def register_success(self, text_report: str, latency: int, p_tokens: int = 0, c_tokens: int = 0, structured_json: dict = None):
        """Commit transaccional del reporte exitoso."""
        self.status = self.AnalysisStatus.COMPLETED
        self.comprehensive_report = text_report
        self.processing_latency_ms = latency
        self.prompt_tokens = p_tokens
        self.completion_tokens = c_tokens
        self.analyzed_at = timezone.now()
        
        if structured_json:
            self.structured_intel = structured_json
            
        self.error_traceback = None # Limpiamos cualquier error previo
        self.save()'''