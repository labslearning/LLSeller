import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db.models import Count, Q, Avg
from django.utils.translation import gettext_lazy as _

# ==========================================
# 0. CORE: CLASE BASE (DRY)
# ==========================================

class TimeStampedModel(models.Model):
    """
    Clase base abstracta. Otorga trazabilidad cronológica (Audit Trail)
    a nivel de base de datos para cada registro del sistema.
    """
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


# ==========================================
# 1. TIER 0: MASTER ENTITY (INSTITUTION)
# ==========================================

class Institution(TimeStampedModel):
    """
    [Master Node]
    Representa a la empresa o colegio prospecto. Mantiene solo la información 
    de enrutamiento básico (Identity & Routing). Los datos pesados se delegan a los perfiles OneToOne.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    class InstitutionType(models.TextChoices):
        KINDERGARTEN = 'kindergarten', 'Jardín Infantil / Preescolar'
        SCHOOL = 'school', 'Colegio (Básica/Media)'
        UNIVERSITY = 'university', 'Universidad / Educación Superior'
        INSTITUTE = 'institute', 'Instituto Técnico / Tecnológico'
        OTHER = 'other', 'Otro'

    class DiscoverySource(models.TextChoices):
        OSM = 'osm', 'OpenStreetMap'
        GOV_DATA = 'gov_data', 'Directorio Gubernamental'
        SERP = 'serp', 'Buscador (Web Scraping)'
        MANUAL = 'manual', 'Ingreso Manual / CRM'

    # --- IDENTIDAD Y CONTACTO BÁSICO ---
    name = models.CharField(max_length=255, verbose_name="Nombre de la Institución")
    website = models.URLField(max_length=255, unique=True, null=True, blank=True, verbose_name="Sitio Web")
    email = models.EmailField(blank=True, null=True, verbose_name="Email Principal")
    phone = models.CharField(max_length=50, blank=True, null=True, verbose_name="Teléfono Principal")
    
    # --- CLASIFICACIÓN DE NEGOCIO (B2B TARGETING) ---
    institution_type = models.CharField(
        max_length=20, 
        choices=InstitutionType.choices, 
        default=InstitutionType.SCHOOL, 
        verbose_name="Nivel Educativo"
    )
    is_private = models.BooleanField(default=True, db_index=True, verbose_name="Es Privada")
    student_count = models.PositiveIntegerField(null=True, blank=True, verbose_name="Estudiantes Estimados")

    # --- GEOLOCALIZACIÓN Y TERRITORIOS ---
    country = models.CharField(max_length=100, db_index=True, default="Colombia", verbose_name="País")
    state_region = models.CharField(max_length=100, blank=True, null=True, db_index=True, verbose_name="Estado / Región")
    city = models.CharField(max_length=100, db_index=True, verbose_name="Ciudad / Municipio")
    address = models.CharField(max_length=255, blank=True, null=True, verbose_name="Dirección Física")
    latitude = models.DecimalField(max_digits=11, decimal_places=8, blank=True, null=True, verbose_name="Latitud")
    longitude = models.DecimalField(max_digits=11, decimal_places=8, blank=True, null=True, verbose_name="Longitud")

    # --- TRAZABILIDAD (CRM ROUTING) ---
    discovery_source = models.CharField(
        max_length=20, 
        choices=DiscoverySource.choices, 
        default=DiscoverySource.MANUAL, 
        verbose_name="Origen del Dato"
    )
    is_active = models.BooleanField(default=True, db_index=True, verbose_name="Activo en CRM")
    contacted = models.BooleanField(default=False, db_index=True, verbose_name="Contactado")
    
    # Conservamos el Score y Last Scanned aquí porque se usan agresivamente para Order By y Filtros Rápidos
    last_scored_at = models.DateTimeField(blank=True, null=True, verbose_name="Último Escaneo")
    lead_score = models.IntegerField(
        default=0, 
        db_index=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        verbose_name="Score de Venta (0-100)"
    )

    class Meta:
        verbose_name = "Institución Educativa"
        verbose_name_plural = "Instituciones Educativas"
        ordering = ['-lead_score']
        
        indexes = [
            # Índice compuesto táctico para el Dashboard
            models.Index(fields=['-lead_score', 'contacted', 'is_active']),
            # Índice principal para el Buscador Geográfico Mundial
            models.Index(fields=['country', 'state_region', 'city']),
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
        return f"{self.name} ({self.lead_score} pts) - {self.city}"


# ==========================================
# 2. TIER 1 & 2: MODULAR INTELLIGENCE PROFILES
# ==========================================

class TechProfile(TimeStampedModel):
    """
    [Tier 1: Fast Recon]
    Almacena los resultados del escaneo ligero de infraestructura tecnológica (LMS, CMS, Analytics).
    Separado de la institución para no saturar la tabla principal.
    """
    institution = models.OneToOneField(Institution, on_delete=models.CASCADE, related_name='tech_profile')
    
    has_lms = models.BooleanField(default=False, db_index=True)
    lms_provider = models.CharField(max_length=100, blank=True, null=True, db_index=True, verbose_name="Proveedor LMS (Ej: Moodle)")
    is_wordpress = models.BooleanField(default=False, verbose_name="Usa WordPress")
    has_analytics = models.BooleanField(default=False, verbose_name="Tiene Google Analytics/Tag Manager")
    
    last_scanned = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Perfil Tecnológico"
        verbose_name_plural = "Perfiles Tecnológicos"

    def __str__(self):
        return f"Tech Stack: {self.institution.name}"


class DeepForensicProfile(TimeStampedModel):
    """
    [Tier 2: Deep AI Recon]
    Almacena los resultados del motor de IA (DeepSeek/OpenAI), estrategias de ventas,
    y datos estructurados que toman más tiempo en procesarse.
    """
    institution = models.OneToOneField(Institution, on_delete=models.CASCADE, related_name='forensic_profile')
    
    ai_classification = models.CharField(max_length=100, blank=True, null=True, db_index=True, verbose_name="Clasificación IA")
    executive_summary = models.TextField(blank=True, null=True, verbose_name="Resumen Ejecutivo")
    
    # Array/JSON de tácticas de ventas generadas por la IA
    sales_playbook = models.JSONField(default=list, blank=True, verbose_name="Recomendaciones Tácticas (Lista)")
    predictive_copy = models.TextField(blank=True, null=True, verbose_name="Draft de Cold Email")
    
    estimated_budget = models.CharField(max_length=100, blank=True, null=True, verbose_name="Presupuesto Estimado")
    
    last_scanned = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Perfil Forense IA"
        verbose_name_plural = "Perfiles Forenses IA"

    def __str__(self):
        return f"AI Intelligence: {self.institution.name}"


# ==========================================
# 3. CRM & OUTREACH: CONTACTOS E INTERACCIONES
# ==========================================

class Contact(TimeStampedModel):
    """Representa al tomador de decisiones (Rector, Director IT) dentro de la institución."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    institution = models.ForeignKey(Institution, on_delete=models.CASCADE, related_name='contacts')
    
    name = models.CharField(max_length=255)
    role = models.CharField(max_length=255, blank=True, null=True)
    email = models.EmailField(blank=True, null=True, unique=True)
    linkedin = models.URLField(blank=True, null=True)
    phone = models.CharField(max_length=50, blank=True, null=True)

    def __str__(self):
        return f"{self.name} - {self.role or 'Sin Cargo'}"


class Interaction(TimeStampedModel):
    """Registra cada punto de contacto (email, llamada) con el prospecto."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    class Status(models.TextChoices):
        NEW = 'NEW', 'Nuevo'
        SENT = 'SENT', 'Enviado'
        OPENED = 'OPENED', 'Abierto'
        REPLIED = 'REPLIED', 'Respondido'
        MEETING = 'MEETING', 'Reunión Agendada'
        CLOSED = 'CLOSED', 'Cerrado'

    institution = models.ForeignKey(Institution, on_delete=models.CASCADE, related_name='interactions')
    contact = models.ForeignKey(Contact, on_delete=models.CASCADE, null=True, blank=True, related_name='interactions')
    
    subject = models.CharField(max_length=255, blank=True, null=True)
    message_sent = models.TextField(blank=True, null=True)
    
    opened_count = models.IntegerField(default=0)
    replied = models.BooleanField(default=False)
    
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.NEW, db_index=True)
    meeting_date = models.DateTimeField(blank=True, null=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        contact_name = self.contact.name if self.contact else "Sin Contacto"
        return f"Interacción: {contact_name} | Estado: {self.get_status_display()}"


# ==========================================
# 4. DATA WAREHOUSE & DASHBOARD MANAGER
# ==========================================

class CommandCenterQuerySet(models.QuerySet):
    """
    Capa de acceso a datos de Alto Rendimiento. Resuelve métricas complejas en la DB.
    """
    def get_funnel_metrics(self) -> dict:
        return self.aggregate(
            total_leads=Count('id'),
            blind_leads=Count('id', filter=Q(website__isnull=True) | Q(website='')),
            ready_to_scan=Count('id', filter=Q(website__isnull=False, last_scored_at__isnull=True) & ~Q(website='')),
            enriched_leads=Count('id', filter=Q(last_scored_at__isnull=False)),
            avg_score=Avg('lead_score')
        )

class CommandCenterManager(models.Manager):
    """
    Aisla la lógica de Business Intelligence (BI) de las consultas normales del ORM.
    """
    def get_queryset(self):
        return CommandCenterQuerySet(self.model, using=self._db)
        
    def get_dashboard_stats(self):
        return self.get_queryset().get_funnel_metrics()


# ==========================================
# 5. THE FACADE PATTERN (PROXY MODEL)
# ==========================================

class CommandCenter(Institution):
    """
    [Architecture Pattern: Proxy Facade]
    Controlador central del B2B Growth Engine.
    Actúa como un Gateway de seguridad y analítica para las operaciones de Celery.
    """
    objects = CommandCenterManager()

    class Meta:
        proxy = True
        app_label = 'sales'
        verbose_name = _('🚀 Sovereign Command Center')
        verbose_name_plural = _('🚀 Sovereign Command Center')
        
        # 🛡️ Role-Based Access Control (RBAC)
        permissions = [
            ("can_execute_osm_radar", _("Security: Can launch OSM Satellite Discovery")),
            ("can_execute_serp_resolver", _("Security: Can launch SERP URL Resolver")),
            ("can_execute_ghost_sniper", _("Security: Can launch the Ghost Sniper Engine")),
            ("view_executive_dashboard", _("Analytics: Can view C-Level Pipeline Metrics")),
        ]

    def __str__(self):
        return "B2B Growth Engine Operations"