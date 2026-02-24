import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.contrib.postgres.indexes import GinIndex

from django.db.models import Count, Q, Avg
from django.utils.translation import gettext_lazy as _


# ==========================================
# 1. MODELO BASE (HERENCIA DRY)
# ==========================================

class TimeStampedModel(models.Model):
    """
    Clase base abstracta. Al heredar de esta clase, todos nuestros modelos 
    tendrán registro exacto de creación y de su última actualización automáticamente.
    """
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True

# ==========================================
# 2. MODELO CORE: INSTITUCIÓN EDUCATIVA
# ==========================================

class Institution(TimeStampedModel):
    """
    Representa a la empresa o colegio que vamos a prospectar.
    Optimizado para Búsqueda Global, Geolocalización y OSINT.
    """
    # ID inquebrantable (Estándar de seguridad militar)
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # --- ENUMS (Listas de Opciones Validadas) ---
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
    # website DEBE permitir nulos, porque OSM encontrará colegios de los que aún no sabemos su URL
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
    is_private = models.BooleanField(
        default=True, 
        db_index=True, 
        verbose_name="Es Privada",
        help_text="Las privadas suelen tener mayor presupuesto para software."
    )
    student_count = models.PositiveIntegerField(null=True, blank=True, verbose_name="Estudiantes Estimados")

    # --- GEOLOCALIZACIÓN GLOBAL ---
    country = models.CharField(max_length=100, db_index=True, default="Colombia", verbose_name="País")
    state_region = models.CharField(max_length=100, blank=True, null=True, db_index=True, verbose_name="Estado / Región")
    city = models.CharField(max_length=100, db_index=True, verbose_name="Ciudad / Municipio")
    address = models.CharField(max_length=255, blank=True, null=True, verbose_name="Dirección Física")
    latitude = models.DecimalField(max_digits=11, decimal_places=8, blank=True, null=True, verbose_name="Latitud")
    longitude = models.DecimalField(max_digits=11, decimal_places=8, blank=True, null=True, verbose_name="Longitud")

    # --- TRAZABILIDAD Y ESTADO DEL LEAD ---
    discovery_source = models.CharField(
        max_length=20, 
        choices=DiscoverySource.choices, 
        default=DiscoverySource.MANUAL, 
        verbose_name="Origen del Dato"
    )
    is_active = models.BooleanField(default=True, db_index=True, verbose_name="Activo en CRM")
    contacted = models.BooleanField(default=False, db_index=True, verbose_name="Contactado")
    
    # --- INTELIGENCIA FORENSE Y SCORING ---
    last_scored_at = models.DateTimeField(blank=True, null=True, verbose_name="Último Escaneo Forense")
    tech_stack = models.JSONField(default=dict, blank=True, verbose_name="Inteligencia Estructurada (JSON)")
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
        
        # 🚀 ÍNDICES DE SILICON VALLEY
        indexes = [
            # Índice compuesto táctico para el Dashboard
            models.Index(fields=['-lead_score', 'contacted', 'is_active']),
            # Índice principal para el Buscador Geográfico Mundial
            models.Index(fields=['country', 'state_region', 'city']),
            # Índice GIN para búsquedas a la velocidad de la luz dentro del JSON
            GinIndex(fields=['tech_stack'], name='tech_stack_gin_idx'),
        ]
        
        constraints = [
            # Garantiza que el score nunca salga del rango 0-100 a nivel de hardware
            models.CheckConstraint(
                check=models.Q(lead_score__gte=0) & models.Q(lead_score__lte=100),
                name='lead_score_range_0_to_100'
            ),
            # Evita que el motor de descubrimiento inserte el mismo colegio duplicado
            models.UniqueConstraint(
                fields=['name', 'city', 'country'],
                name='unique_institution_per_city_country'
            )
        ]

    def __str__(self):
        return f"{self.name} ({self.lead_score} pts) - {self.city}"


# ==========================================
# 3. MODELOS DE VENTAS: CONTACTOS E INTERACCIONES
# ==========================================

class Contact(TimeStampedModel):
    """Representa al tomador de decisiones dentro de la institución."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    institution = models.ForeignKey(Institution, on_delete=models.CASCADE, related_name='contacts')
    
    name = models.CharField(max_length=255)
    role = models.CharField(max_length=255, blank=True, null=True)
    email = models.EmailField(blank=True, null=True, unique=True) # Evita contactos duplicados
    linkedin = models.URLField(blank=True, null=True)
    phone = models.CharField(max_length=50, blank=True, null=True)

    def __str__(self):
        return f"{self.name} - {self.role or 'Sin Cargo'}"


class Interaction(TimeStampedModel):
    """Registra cada correo, apertura y respuesta con exactitud militar y máxima seguridad."""
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
    
    status = models.CharField(
        max_length=20, 
        choices=Status.choices, 
        default=Status.NEW, 
        db_index=True
    )
    meeting_date = models.DateTimeField(blank=True, null=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        contact_name = self.contact.name if self.contact else "Sin Contacto"
        return f"Interacción: {contact_name} | Estado: {self.get_status_display()} | Aperturas: {self.opened_count}"



# ==========================================
# 1. ENTERPRISE QUERYSET & DATA ACCESS LAYER
# ==========================================
class CommandCenterQuerySet(models.QuerySet):
    """
    Capa de acceso a datos optimizada para el Dashboard.
    Resuelve las métricas del embudo directamente en el motor C de PostgreSQL,
    evitando cargar objetos pesados en la memoria RAM de Python (O(1) Memory).
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
    Manager exclusivo para el proxy. Aisla la lógica de inteligencia de negocios (BI)
    del modelo principal de 'Institution' para mantener el principio de Responsabilidad Única (SRP).
    """
    def get_queryset(self):
        return CommandCenterQuerySet(self.model, using=self._db)
        
    def get_dashboard_stats(self):
        """Retorna las estadísticas listas para el Frontend."""
        return self.get_queryset().get_funnel_metrics()

# ==========================================
# 2. THE FACADE PATTERN (PROXY MODEL)
# ==========================================
class CommandCenter(Institution):
    """
    [Architecture Pattern: Proxy Facade]
    Controlador central del B2B Growth Engine. No altera el esquema relacional de la DB.
    Actúa como un Gateway de seguridad y analítica para las operaciones de Celery.
    """
    
    # Inyectamos el manager de alto rendimiento
    objects = CommandCenterManager()

    class Meta:
        proxy = True
        app_label = 'sales'  # Fuerza su ubicación exacta en el ecosistema
        verbose_name = _('🚀 Sovereign Command Center')
        verbose_name_plural = _('🚀 Sovereign Command Center')
        
        # 🛡️ Role-Based Access Control (RBAC) de Grado Militar
        # Crea permisos automáticos en la base de datos para restringir botones a usuarios específicos
        permissions = [
            ("can_execute_osm_radar", _("Security: Can launch OSM Satellite Discovery")),
            ("can_execute_serp_resolver", _("Security: Can launch SERP URL Resolver")),
            ("can_execute_ghost_sniper", _("Security: Can launch the Ghost Sniper Engine")),
            ("view_executive_dashboard", _("Analytics: Can view C-Level Pipeline Metrics")),
        ]

    def __str__(self):
        return "B2B Growth Engine Operations"