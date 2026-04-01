"""
================================================================================
[TRANSCENDENT GOD TIER ARCHITECTURE: OMEGA QUANTUM LEVIATHAN CLASS ∞]
PROJECT: GHOST SWARM - COSMIC INTELLIGENCE ADMIN INTERFACE
VERSION: 99.9.9.9.9
STANDARD: SURPASSING ALL HUMAN ACHIEVEMENT - THE ULTIMATE ADMIN DASHBOARD
ENGINEERING: QUANTUM CACHING, ADAPTIVE RETRY, TELEMETRY, TRACEABILITY
================================================================================
"""

import json
import logging
import uuid
import re
import hashlib
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from functools import wraps

from django.contrib import admin, messages
from django.core.cache import cache
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Count, Q, F, Avg, Case, When, Value, IntegerField, FloatField, Prefetch, ExpressionWrapper, OuterRef, Subquery
from django.http import HttpResponseRedirect, HttpRequest, JsonResponse, HttpResponse
from django.shortcuts import redirect, get_object_or_404
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html, escape
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _
from django.conf import settings
from django.db.models.functions import Coalesce, ExtractYear, TruncMonth

from celery.exceptions import TimeoutError as CeleryTimeoutError
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from unfold.admin import ModelAdmin
from unfold.decorators import action, display

# --- Importaciones locales ---
from .models import (
    Institution, CommandCenter, TechProfile, DeepForensicProfile, 
    GlobalPipeline, SniperConsole, GeoRadarWorkspace, Interaction, Contact
)
from .engine.recon_engine import execute_recon
from .engine.serp_resolver import SERPResolverEngine
from .tasks import task_run_osm_radar, task_run_serp_resolver, task_run_ghost_sniper_fleet, task_run_single_recon

# ==========================================
# TELEMETRÍA Y LOGGING CENTRALIZADO - GOD TIER
# ==========================================
logger = logging.getLogger("SovereignAdminGateway")

# [GOD TIER OPTIMIZATION]: Pre-compilación O(1) para el renderizado del listado
WA_REGEX = re.compile(r'W:\s*([\d\+\s]+)')
TEL_REGEX = re.compile(r'T:\s*([\d\+\s]+)')

# ==========================================
# DECORADORES DE ALTO RENDIMIENTO
# ==========================================
def cache_result(ttl: int = 300):
    """Decorador para cachear resultados de métodos con timeout configurable"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, obj, *args, **kwargs):
            cache_key = f"admin_{func.__name__}_{obj.id}_{hashlib.md5(str(args).encode()).hexdigest()[:8]}"
            cached = cache.get(cache_key)
            if cached is not None:
                return cached
            result = func(self, obj, *args, **kwargs)
            cache.set(cache_key, result, ttl)
            return result
        return wrapper
    return decorator

def async_bulk_action(concurrency: int = 5):
    """Decorador para ejecutar acciones masivas de forma asíncrona"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, request, queryset):
            from concurrent.futures import ThreadPoolExecutor
            import threading
            
            results = {'success': 0, 'failed': 0, 'skipped': 0}
            lock = threading.Lock()
            
            def process_item(item):
                try:
                    result = func(self, request, item)
                    with lock:
                        if result:
                            results['success'] += 1
                        else:
                            results['failed'] += 1
                except Exception as e:
                    with lock:
                        results['failed'] += 1
                    logger.error(f"Error processing {item}: {e}")
            
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                executor.map(process_item, queryset)
            
            self.message_user(request, f"✅ Procesado: {results['success']} exitosos, {results['failed']} fallidos, {results['skipped']} omitidos.")
            return results
        return wrapper
    return decorator

# ==========================================
# 1. FILTROS ESTRATÉGICOS (DATA WAREHOUSE) - MEJORADOS
# ==========================================
class StrategicIntentFilter(admin.SimpleListFilter):
    title = '🎯 Prioridad de Ejecución'
    parameter_name = 'intent_priority'

    def lookups(self, request, model_admin):
        return (
            ('strike', '🎯 Ready to Strike (Score > 75 + Email)'),
            ('hunt', '🏹 Needs Hunting (High Score + No Email)'),
            ('stale', '🧊 Leads Estancados (+15 días sin scan)'),
            ('cosmic_ready', '🌌 Cosmic Report Ready'),
            ('cosmic_missing', '📭 Missing Cosmic Report'),
        )

    def queryset(self, request, queryset):
        val = self.value()
        if val == 'strike':
            return queryset.filter(lead_score__gte=75).exclude(email__isnull=True).exclude(email='')
        if val == 'hunt':
            return queryset.filter(lead_score__gte=60, email__isnull=True)
        if val == 'stale':
            threshold = timezone.now() - timezone.timedelta(days=15)
            return queryset.filter(last_scored_at__lt=threshold)
        if val == 'cosmic_ready':
            return queryset.filter(forensic_profile__ai_comprehensive_report__isnull=False)
        if val == 'cosmic_missing':
            return queryset.filter(Q(forensic_profile__ai_comprehensive_report__isnull=True) | Q(forensic_profile__isnull=True))
        return queryset

class EnterpriseTechFilter(admin.SimpleListFilter):
    title = '🛠️ Arquitectura Digital'
    parameter_name = 'tech_stack_granular'

    def lookups(self, request, model_admin):
        return (
            ('premium', '💎 Premium Stack (Phidias/Schoolnet/Canvas)'),
            ('open_source', '🟢 Open Source (Moodle/Chamilo)'),
            ('blue_ocean', '🌊 Blue Ocean (Sin LMS)'),
            ('legacy', '🏚️ Legacy Systems'),
            ('cloud_native', '☁️ Cloud Native'),
        )

    def queryset(self, request, queryset):
        val = self.value()
        if val == 'premium':
            return queryset.filter(tech_profile__lms_provider__in=['phidias', 'schoolnet', 'canvas', 'cibercolegios', 'blackboard'])
        if val == 'open_source':
            return queryset.filter(tech_profile__lms_provider__in=['moodle', 'chamilo', 'sakai', 'ilias'])
        if val == 'blue_ocean':
            return queryset.filter(Q(tech_profile__has_lms=False) | Q(tech_profile__isnull=True))
        if val == 'legacy':
            return queryset.filter(tech_profile__lms_provider__in=['blackboard', 'webct', 'angel'])
        if val == 'cloud_native':
            return queryset.filter(tech_profile__lms_provider__in=['canvas', 'google classroom', 'microsoft teams'])
        return queryset

class AcademicCertificationFilter(admin.SimpleListFilter):
    """[GOD TIER]: Filtro específico para encontrar los colegios Bilingües y con IB."""
    title = '🎓 Nivel Académico (Sniper Data)'
    parameter_name = 'academic_level'

    def lookups(self, request, model_admin):
        return (
            ('bilingual', '🗣️ Bilingües / Trilingües'),
            ('ib_cert', '🏆 Certificación IB'),
            ('cambridge', '🇬🇧 Cambridge / Oxford'),
            ('robotics', '🤖 Con Robótica / STEM'),
            ('high_icfes', '📊 ICFES Alto (A+ / A)'),
            ('international', '🌍 International Programs'),
            ('double_degree', '🎓 Double Degree'),
        )

    def queryset(self, request, queryset):
        val = self.value()
        if val == 'bilingual':
            return queryset.filter(Q(forensic_profile__is_bilingual=True) | Q(forensic_profile__is_trilingual=True))
        if val == 'ib_cert':
            return queryset.filter(forensic_profile__has_ib_cert=True)
        if val == 'cambridge':
            return queryset.filter(forensic_profile__has_cambridge_cert=True)
        if val == 'robotics':
            return queryset.filter(
                Q(forensic_profile__ai_structured_data__technology__robotics__has_robotics=True) |
                Q(forensic_profile__ai_structured_data__has_robotics=True) |
                Q(forensic_profile__ai_structured_data__technology__stem__has_stem=True)
            )
        if val == 'high_icfes':
            return queryset.filter(
                Q(forensic_profile__ai_structured_data__performance__icfes__category__in=['A+', 'A']) |
                Q(forensic_profile__ai_structured_data__icfes_category__in=['A+', 'A'])
            )
        if val == 'international':
            return queryset.filter(
                Q(forensic_profile__ai_structured_data__international_programs__exchanges__has_exchanges=True) |
                Q(forensic_profile__ai_structured_data__international_programs__double_degree__has_double_degree=True)
            )
        if val == 'double_degree':
            return queryset.filter(forensic_profile__ai_structured_data__international_programs__double_degree__has_double_degree=True)
        return queryset

class AdvancedPerformanceFilter(admin.SimpleListFilter):
    """[GOD TIER]: Filtro avanzado de rendimiento académico"""
    title = '📈 Rendimiento Académico'
    parameter_name = 'performance_level'

    def lookups(self, request, model_admin):
        return (
            ('top_icfes', '🏆 ICFES Top 10%'),
            ('high_icfes', '⭐ ICFES Alto (A+/A)'),
            ('university_admission', '🎓 Alta Admisión Universitaria'),
            ('awards', '🏅 Con Premios/Reconocimientos'),
            ('notable_alumni', '👥 Alumni Notables'),
        )

    def queryset(self, request, queryset):
        val = self.value()
        if val == 'top_icfes':
            return queryset.filter(
                Q(forensic_profile__ai_structured_data__performance__icfes__score__gte=85) |
                Q(forensic_profile__ai_structured_data__icfes_score__gte=85)
            )
        if val == 'high_icfes':
            return queryset.filter(
                Q(forensic_profile__ai_structured_data__performance__icfes__category__in=['A+', 'A']) |
                Q(forensic_profile__ai_structured_data__icfes_category__in=['A+', 'A'])
            )
        if val == 'university_admission':
            return queryset.filter(
                Q(forensic_profile__ai_structured_data__performance__university_admission_rate__gte=80) |
                Q(forensic_profile__ai_structured_data__university_admission_rate__gte=80)
            )
        if val == 'awards':
            return queryset.filter(
                Q(forensic_profile__ai_structured_data__performance__awards__isnull=False) &
                ~Q(forensic_profile__ai_structured_data__performance__awards=[])
            )
        if val == 'notable_alumni':
            return queryset.filter(
                Q(forensic_profile__ai_structured_data__performance__notable_alumni__isnull=False) &
                ~Q(forensic_profile__ai_structured_data__performance__notable_alumni=[])
            )
        return queryset

# ==========================================
# 2. EL CENTRO DE MANDO B2B (THE GRID) - VERSIÓN OMEGA
# ==========================================
try:
    admin.site.unregister(Institution)
except admin.sites.NotRegistered:
    pass

@admin.register(GlobalPipeline)
class GlobalPipelineAdmin(ModelAdmin):
    """
    SDR Intelligence Interface - GOD TIER OMEGA VERSION.
    Visualización completa de datos de inteligencia cósmica con renderizado optimizado.
    """
    
    list_display = (
        'display_institution_identity',
        'data_density_badge',
        'advanced_recon_trigger',
        'display_intelligence_radar',
        'display_performance_score',
        'display_contact_card',
        'display_cosmic_readiness',
        'display_sync_metrics'
    )

    list_filter = (
        StrategicIntentFilter,
        EnterpriseTechFilter,
        AcademicCertificationFilter,
        AdvancedPerformanceFilter,
        'processing_status',
        'country',
        'city',
        'institution_type',
        'is_private',
    )

    search_fields = ('name', 'website', 'email', 'city', 'country', 'phone', 'address')
    search_help_text = _("Búsqueda avanzada: Nombre, URL, Email, Teléfono, Ciudad, País")
    
    list_select_related = ('tech_profile', 'forensic_profile')
    
    readonly_fields = (
        'id', 'last_scored_at', 'display_performance_score',
        'display_cosmic_full_report',
        'display_cosmic_executive_summary',
        'display_cosmic_certifications',
        'display_cosmic_technology',
        'display_cosmic_sales_intelligence',
        'display_cosmic_extracurricular',
        'display_cosmic_agreements',
        'display_cosmic_infrastructure',
    )

    list_per_page = 50
    list_max_show_all = 200
    list_filter_submit = True
    show_full_result_count = True
    save_as = True
    save_on_top = True

    class Media:
        js = (
            'https://unpkg.com/htmx.org@1.9.10',
            'https://unpkg.com/alpinejs@3.13.3/dist/cdn.min.js',
        )
        css = {
            'all': (
                'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap',
                'https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&display=swap',
            )
        }

    def get_ordering(self, request):
        return ('-lead_score', '-last_scored_at', '-created_at')
    
    def get_queryset(self, request):
        """Optimización de queryset con anotaciones avanzadas"""
        return super().get_queryset(request).select_related(
            'tech_profile', 'forensic_profile'
        ).annotate(
            data_density=ExpressionWrapper(
                Case(When(Q(email__isnull=False) & ~Q(email=''), then=Value(20)), default=Value(0)) +
                Case(
                    When(
                        Q(tech_profile__has_lms=True) & 
                        Q(tech_profile__lms_provider__isnull=False) & 
                        ~Q(tech_profile__lms_provider=''), 
                        then=Value(35)
                    ), 
                    default=Value(0)
                ) +
                Case(When(Q(phone__isnull=False) & ~Q(phone=''), then=Value(10)), default=Value(0)) +
                Case(When(Q(website__isnull=False) & ~Q(website=''), then=Value(10)), default=Value(0)) +
                Case(
                    When(forensic_profile__is_trilingual=True, then=Value(15)),
                    When(forensic_profile__is_bilingual=True, then=Value(10)),
                    default=Value(0)
                ) +
                Case(When(forensic_profile__has_ib_cert=True, then=Value(5)), default=Value(0)) +
                Case(When(forensic_profile__has_cambridge_cert=True, then=Value(5)), default=Value(0)) +
                Case(
                    When(forensic_profile__ai_comprehensive_report__isnull=False, then=Value(25)),
                    default=Value(0)
                ) +
                Case(
                    When(processing_status='ENRICHED', then=Value(10)), 
                    When(processing_status='DISCARDED', then=Value(-100)), 
                    default=Value(0)
                ),
                output_field=IntegerField()
            ),
            cosmic_completeness=Case(
                When(forensic_profile__ai_structured_data__isnull=False, then=Value(1)),
                default=Value(0),
                output_field=IntegerField()
            )
        ).order_by('-data_density', '-lead_score', '-updated_at')
    
    @display(description="Data Density (Intel)", ordering='-data_density')
    def data_density_badge(self, obj):
        score = getattr(obj, 'data_density', 0)
        
        if score >= 100:
            color = "bg-gradient-to-r from-emerald-900/80 to-teal-900/80 text-emerald-300 border-emerald-400/50"
            shadow = "shadow-[0_0_15px_rgba(16,185,129,0.4)]"
            icon = "verified_user"
            label = "OMEGA"
        elif score >= 80:
            color = "bg-emerald-900/50 text-emerald-400 border-emerald-500/50"
            shadow = "shadow-[0_0_10px_rgba(16,185,129,0.3)]"
            icon = "verified_user"
            label = "COMPLETE"
        elif score >= 50:
            color = "bg-amber-900/40 text-amber-400 border-amber-500/50"
            shadow = ""
            icon = "warning"
            label = "PARTIAL"
        elif score > 0:
            color = "bg-blue-900/40 text-blue-400 border-blue-500/50"
            shadow = ""
            icon = "troubleshoot"
            label = "BASIC"
        else:
            color = "bg-slate-900/80 text-slate-500 border-slate-700"
            shadow = ""
            icon = "visibility_off"
            label = "EMPTY"

        return format_html(
            f'<div class="flex items-center gap-1.5 w-fit px-2 py-1 rounded border {color} {shadow}">'
            f'  <span class="material-symbols-outlined text-[13px]">{icon}</span>'
            f'  <span class="text-[10px] font-black tracking-widest uppercase">{score} PTS</span>'
            f'  <span class="text-[8px] font-mono opacity-70">{label}</span>'
            f'</div>'
        )

    def changelist_view(self, request, extra_context=None):
        """
        Dashboard con KPIs en tiempo real
        [GOD TIER FIX] - Corregido error de serialización de datetime
        """
        qs = self.get_queryset(request)
        
        # Métricas agregadas con caché
        metrics = cache.get('admin_global_metrics')
        if not metrics:
            metrics = qs.aggregate(
                total=Count('id'),
                hot=Count('id', filter=Q(lead_score__gte=75)),
                warm=Count('id', filter=Q(lead_score__gte=50, lead_score__lt=75)),
                cold=Count('id', filter=Q(lead_score__lt=50)),
                avg_score=Avg('lead_score', output_field=FloatField()),
                enriched=Count('id', filter=Q(tech_profile__isnull=False)),
                with_cosmic_report=Count('id', filter=Q(forensic_profile__ai_comprehensive_report__isnull=False)),
                bilingual=Count('id', filter=Q(forensic_profile__is_bilingual=True)),
                ib_certified=Count('id', filter=Q(forensic_profile__has_ib_cert=True)),
                cambridge=Count('id', filter=Q(forensic_profile__has_cambridge_cert=True)),
                robotics=Count('id', filter=Q(forensic_profile__ai_structured_data__has_robotics=True)),
            )
            cache.set('admin_global_metrics', metrics, 300)
        
        # Series temporales para gráficos - CORREGIDO
        timeline = list(Institution.objects
            .annotate(month=TruncMonth('created_at'))
            .values('month')
            .annotate(count=Count('id'))
            .order_by('-month')[:12]
        )
        
        # 🔥 CORRECCIÓN CRÍTICA: Convertir datetime a string para JSON serialization
        timeline_serializable = []
        for item in timeline:
            timeline_serializable.append({
                'month': item['month'].isoformat() if item['month'] else None,
                'count': item['count']
            })
        
        # Distribución de LMS
        lms_distribution = list(Institution.objects.filter(tech_profile__isnull=False)
            .annotate(
                lms_clean=Case(
                    When(tech_profile__lms_provider__isnull=True, then=Value('Ninguno/In-House')),
                    When(tech_profile__lms_provider='', then=Value('Ninguno/In-House')),
                    default=F('tech_profile__lms_provider')
                )
            )
            .values('lms_clean')
            .annotate(total=Count('id'))
            .order_by('-total')[:8]
        )
        
        # Preparar datos para el template
        extra_context = extra_context or {}
        extra_context.update({
            "kpi": [
                {"title": "Total Leads Pipeline", "metric": metrics['total'], "footer": "Leads capturados globalmente", "color": "blue", "icon": "public"},
                {"title": "🎯 Ready to Strike", "metric": metrics['hot'], "footer": "Score > 75 pts", "color": "emerald", "icon": "whatshot"},
                {"title": "🧠 Data Coverage", "metric": f"{(metrics['enriched']/metrics['total']*100 if metrics['total'] > 0 else 0):.1f}%", "footer": "Prospectos con Tech Stack", "color": "purple", "icon": "memory"},
                {"title": "🌌 Cosmic Reports", "metric": metrics['with_cosmic_report'], "footer": "Reportes IA completos", "color": "indigo", "icon": "psychology"},
                {"title": "📈 Calidad Promedio", "metric": f"{metrics['avg_score'] or 0:.1f}", "footer": "Nivel de madurez del pipeline", "color": "amber", "icon": "trending_up"},
                {"title": "🗣️ Bilingües", "metric": metrics['bilingual'], "footer": "Colegios bilingües", "color": "blue", "icon": "translate"},
                {"title": "🏆 IB Certified", "metric": metrics['ib_certified'], "footer": "Bachillerato Internacional", "color": "emerald", "icon": "school"},
                {"title": "🤖 Robótica/STEM", "metric": metrics['robotics'], "footer": "Con programas tecnológicos", "color": "cyan", "icon": "biotech"},
            ],
            "timeline": json.dumps(timeline_serializable),
            "lms_labels": json.dumps([str(item['lms_clean']).upper() for item in lms_distribution]),
            "lms_data": json.dumps([item['total'] for item in lms_distribution]),
        })
        
        return super().changelist_view(request, extra_context=extra_context)

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('auto-sniper/<str:inst_id>/', self.admin_site.admin_view(self.run_auto_sniper), name='sales_globalpipeline_auto_sniper'),
            path('resolve-url/<str:inst_id>/', self.admin_site.admin_view(self.run_resolve_url), name='sales_globalpipeline_resolve_url'),
            path('scan-lms/<str:inst_id>/', self.admin_site.admin_view(self.run_scan_lms), name='sales_globalpipeline_scan_lms'),
            path('scan-deep/<str:inst_id>/', self.admin_site.admin_view(self.run_scan_deep), name='sales_globalpipeline_scan_deep'),
            path('check-scan/<str:inst_id>/', self.admin_site.admin_view(self.check_scan_status), name='sales_globalpipeline_check_scan'),
            path('cosmic-report/<str:inst_id>/', self.admin_site.admin_view(self.view_cosmic_report), name='sales_globalpipeline_cosmic_report'),
            path('cosmic-report-export/<str:inst_id>/', self.admin_site.admin_view(self.export_cosmic_report), name='sales_globalpipeline_cosmic_report_export'),
            path('ws/status/<str:inst_id>/', self.admin_site.admin_view(self.ws_status), name='ws_status'),
            path('bulk-cosmic-generate/', self.admin_site.admin_view(self.bulk_generate_cosmic), name='sales_globalpipeline_bulk_cosmic'),
        ]   
        return custom_urls + urls

    '''def _get_polling_html(self, inst_id):
        poll_url = reverse('admin:sales_globalpipeline_check_scan', args=[inst_id])
        return format_html(
            '<div id="recon-panel-{}" class="whitespace-nowrap min-w-[120px]" '
            'hx-get="{}" hx-trigger="every 2s" hx-swap="outerHTML">'
            '  <div class="flex items-center gap-2 px-3 py-1 rounded bg-amber-500/20 border border-amber-500/30">'
            '    <div class="w-2 h-2 rounded-full bg-amber-500 animate-pulse"></div>'
            '    <span class="text-[10px] font-bold text-amber-400 uppercase tracking-wider">ANALIZANDO...</span>'
            '  </div>'
            '</div>', inst_id, poll_url
        )'''
        
    def _get_polling_html(self, inst_id: str, elapsed: str = "") -> str:
        poll_url = reverse('admin:sales_globalpipeline_check_scan', args=[inst_id])
    
        return format_html(
        '''<div id="recon-panel-{}" class="whitespace-nowrap min-w-[180px] transition-all duration-300">
            <div class="relative overflow-hidden rounded-xl bg-gradient-to-r from-amber-950/30 to-red-950/30 border border-amber-500/30 p-2 shadow-lg backdrop-blur-sm">
                <div class="absolute bottom-0 left-0 h-0.5 bg-gradient-to-r from-amber-500 via-red-500 to-purple-500 animate-[progress_2s_ease-in-out_infinite]" style="width: 60%"></div>
                
                <div class="flex flex-col gap-1.5 relative z-10">
                    <div class="flex items-center gap-2">
                        <div class="relative">
                            <div class="w-2.5 h-2.5 rounded-full bg-amber-500 animate-pulse shadow-[0_0_8px_rgba(245,158,11,0.8)]"></div>
                            <div class="absolute inset-0 w-2.5 h-2.5 rounded-full bg-amber-500 animate-ping opacity-75"></div>
                        </div>
                        <span class="text-[10px] font-black text-amber-400 uppercase tracking-wider">🎯 GHOST SNIPER ACTIVE</span>
                    </div>
                    
                    <div class="flex items-center gap-2 text-[9px] font-mono text-amber-300/80">
                        <svg class="animate-spin h-3 w-3 text-amber-400" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                        </svg>
                        <span>EXTRACTANDO DATOS{}</span>
                    </div>
                    
                    <div class="grid grid-cols-2 gap-1 mt-1 text-[7px] font-mono">
                        <div class="flex items-center gap-1 text-emerald-400">
                            <span class="material-symbols-outlined text-[10px]">check_circle</span>
                            <span>LMS</span>
                        </div>
                        <div class="flex items-center gap-1 text-amber-400 animate-pulse">
                            <span class="material-symbols-outlined text-[10px]">sync</span>
                            <span>EMAILS</span>
                        </div>
                        <div class="flex items-center gap-1 text-amber-400 animate-pulse">
                            <span class="material-symbols-outlined text-[10px]">sync</span>
                            <span>PHONES</span>
                        </div>
                        <div class="flex items-center gap-1 text-amber-400 animate-pulse">
                            <span class="material-symbols-outlined text-[10px]">sync</span>
                            <span>IA</span>
                        </div>
                    </div>
                    
                    <div class="w-full bg-slate-800/50 rounded-full h-1 mt-1 overflow-hidden">
                        <div class="h-full bg-gradient-to-r from-amber-500 via-red-500 to-purple-500 rounded-full animate-[progress_1.5s_ease-in-out_infinite]" style="width: 65%"></div>
                    </div>
                    
                    <div class="text-[7px] text-slate-500 font-mono text-center mt-1">
                        ⏳ Tiempo transcurrido: {} | Polling cada 2s
                    </div>
                </div>
            </div>
            <div hx-get="{}" 
                 hx-trigger="every 2s" 
                 hx-swap="outerHTML" 
                 hx-target="#recon-panel-{}"
                 hx-select="#recon-panel-{}"
                 class="hidden"></div>
        </div>''',
        inst_id,
        elapsed,
        elapsed.strip("() ") if elapsed else "0s",
        poll_url,
        inst_id,
        inst_id
    )


    def run_resolve_url(self, request, inst_id):
        try:
            inst = Institution.objects.get(pk=inst_id)
            if not inst.website:
                resolver = SERPResolverEngine(concurrency_limit=1)
                resolver.resolve_missing_urls(limit=1)
                inst.refresh_from_db()
                if inst.website:
                    messages.success(request, f"🌐 ¡URL encontrada! {inst.website}. Ya puedes ejecutar el escaneo LMS.")
                    self._send_ws_update(inst_id, "URL resuelta")
                else:
                    messages.warning(request, f"⚠️ Imposible encontrar una URL oficial confiable para {inst.name}.")
            else:
                messages.info(request, "Este lead ya posee una URL asignada.")
        except Exception as e:
            messages.error(request, f"❌ Error en resolución: {str(e)}")
        return redirect('admin:sales_globalpipeline_changelist')

    def run_scan_lms(self, request, inst_id):
        cache.set(f"scan_in_progress_{inst_id}", True, timeout=600)
        task_run_single_recon.delay(inst_id)
        return HttpResponse(self._get_polling_html(inst_id))

    def run_scan_deep(self, request, inst_id):
        cache.set(f"scan_in_progress_{inst_id}", True, timeout=600)
        task_run_single_recon.delay(inst_id)
        return HttpResponse(self._get_polling_html(inst_id))

    '''def check_scan_status(self, request, inst_id):
        is_scanning = cache.get(f"scan_in_progress_{inst_id}")
        
        if is_scanning:
            return HttpResponse(self._get_polling_html(inst_id))
            
        inst = Institution.objects.select_related('tech_profile', 'forensic_profile').get(pk=inst_id)
        
        btn_html = self.advanced_recon_trigger(inst)
        
        tech_html = self.display_intelligence_radar(inst)
        tech_oob = tech_html.replace(f'id="tech-radar-{inst.pk}"', f'id="tech-radar-{inst.pk}" hx-swap-oob="true"')
        
        score_html = self.display_performance_score(inst)
        score_oob = score_html.replace(f'id="score-panel-{inst.pk}"', f'id="score-panel-{inst.pk}" hx-swap-oob="true"')
        
        cosmic_html = self.display_cosmic_readiness(inst)
        cosmic_oob = cosmic_html.replace(f'id="cosmic-panel-{inst.pk}"', f'id="cosmic-panel-{inst.pk}" hx-swap-oob="true"') if hasattr(inst, 'id') else ""

        return HttpResponse(f"{btn_html}\n{tech_oob}\n{score_oob}\n{cosmic_oob}")'''

    
    def check_scan_status(self, request, inst_id: str) -> HttpResponse:
        """
        [GOD TIER OMEGA] Verifica el estado del escaneo y actualiza la UI en tiempo real.
        """
        import logging
        import time
        logger = logging.getLogger("Sovereign.Admin.ScanStatus")
        
        # =========================================================
        # 1. VERIFICAR ESTADO DEL ESCANEO
        # =========================================================
        is_scanning = cache.get(f"scan_in_progress_{inst_id}")
        scan_start_time = cache.get(f"scan_start_time_{inst_id}")
        
        # =========================================================
        # 2. SI AÚN ESTÁ ESCANEANDO, MOSTRAR SPINNER CON TIEMPO ESTIMADO
        # =========================================================
        if is_scanning:
            # Calcular tiempo transcurrido para mostrar al usuario
            elapsed = ""
            if scan_start_time:
                elapsed_seconds = int(time.time() - scan_start_time)
                if elapsed_seconds < 60:
                    elapsed = f" ({elapsed_seconds}s)"
                else:
                    elapsed = f" ({elapsed_seconds//60}m {elapsed_seconds%60}s)"
            
            # Devolver HTML de polling con tiempo estimado
            return HttpResponse(self._get_polling_html(inst_id, elapsed))
        
        # =========================================================
        # 3. ESCANEO COMPLETADO - OBTENER DATOS ACTUALIZADOS
        # =========================================================
        try:
            # Obtener institución con todas las relaciones necesarias
            inst = Institution.objects.select_related(
                'tech_profile', 
                'forensic_profile'
            ).prefetch_related(
                'contacts'
            ).get(pk=inst_id)
            
            logger.info(f"✅ [SCAN COMPLETE] {inst.name} | Score: {inst.lead_score}")
            
        except Institution.DoesNotExist:
            logger.error(f"❌ [SCAN ERROR] Institución {inst_id} no encontrada")
            return HttpResponse(f'<div id="recon-panel-{inst_id}" class="text-red-500 text-xs">Error: Institución no encontrada</div>')
        
        # =========================================================
        # 4. GENERAR HTML DE TODOS LOS COMPONENTES ACTUALIZADOS
        # =========================================================
        
        # 4.1 Botones de acción (vuelven a estado normal)
        btn_html = self.advanced_recon_trigger(inst)
        
        # 4.2 Tecnología detectada (LMS, CMS, Analytics)
        tech_html = self.display_intelligence_radar(inst)
        tech_oob = tech_html.replace(
            f'id="tech-radar-{inst.pk}"', 
            f'id="tech-radar-{inst.pk}" hx-swap-oob="true"'
        )
        
        # 4.3 Score predictivo (0-100)
        score_html = self.display_performance_score(inst)
        score_oob = score_html.replace(
            f'id="score-panel-{inst.pk}"', 
            f'id="score-panel-{inst.pk}" hx-swap-oob="true"'
        )
        
        # 4.4 Tarjeta de contacto (emails, teléfonos, WhatsApp)
        contact_html = self.display_contact_card(inst)
        contact_oob = contact_html.replace(
            f'id="contact-card-{inst.pk}"',
            f'id="contact-card-{inst.pk}" hx-swap-oob="true"'
        )
        
        # 4.5 Reporte cósmico (IA)
        cosmic_html = self.display_cosmic_readiness(inst) if hasattr(self, 'display_cosmic_readiness') else ""
        cosmic_oob = ""
        if cosmic_html and hasattr(inst, 'id'):
            cosmic_oob = cosmic_html.replace(
                f'id="cosmic-panel-{inst.pk}"', 
                f'id="cosmic-panel-{inst.pk}" hx-swap-oob="true"'
            )
        
        # =========================================================
        # 5. LIMPIAR CACHÉ Y ESTADOS TEMPORALES
        # =========================================================
        cache.delete(f"scan_in_progress_{inst_id}")
        cache.delete(f"scan_start_time_{inst_id}")
        cache.delete(f"telemetry_{inst_id}")
        
        # =========================================================
        # 6. CONSTRUIR RESPUESTA CON MÚLTIPLES ACTUALIZACIONES OOB
        # =========================================================
        response_parts = [btn_html, tech_oob, score_oob, contact_oob]
        if cosmic_oob:
            response_parts.append(cosmic_oob)
        
        return HttpResponse("\n".join(response_parts))

    def view_cosmic_report(self, request, inst_id):
        """Vista para ver el reporte cósmico completo con formato profesional"""
        inst = get_object_or_404(Institution.objects.select_related('forensic_profile'), pk=inst_id)
        
        if hasattr(inst, 'forensic_profile') and inst.forensic_profile and inst.forensic_profile.ai_comprehensive_report:
            report = inst.forensic_profile.ai_comprehensive_report
            structured = inst.forensic_profile.ai_structured_data or {}
            
            # Extraer métricas para el header
            confidence = structured.get('confidence_score', 0.85) * 100
            completeness = structured.get('extraction_completeness', 0.75) * 100
            lms = structured.get('lms_provider', inst.tech_profile.lms_provider if hasattr(inst, 'tech_profile') else 'Desconocido')
            
            # Construir badges
            badges = []
            if structured.get('is_bilingual') or (hasattr(inst.forensic_profile, 'is_bilingual') and inst.forensic_profile.is_bilingual):
                badges.append('<span class="bg-blue-500/20 text-blue-300 px-2 py-1 rounded-full text-xs font-bold">🗣️ BILINGÜE</span>')
            if structured.get('is_trilingual') or (hasattr(inst.forensic_profile, 'is_trilingual') and inst.forensic_profile.is_trilingual):
                badges.append('<span class="bg-purple-500/20 text-purple-300 px-2 py-1 rounded-full text-xs font-bold">🌍 TRILINGÜE</span>')
            if structured.get('has_ib') or (hasattr(inst.forensic_profile, 'has_ib_cert') and inst.forensic_profile.has_ib_cert):
                badges.append('<span class="bg-emerald-500/20 text-emerald-300 px-2 py-1 rounded-full text-xs font-bold">🏆 IB</span>')
            if structured.get('has_cambridge') or (hasattr(inst.forensic_profile, 'has_cambridge_cert') and inst.forensic_profile.has_cambridge_cert):
                badges.append('<span class="bg-red-500/20 text-red-300 px-2 py-1 rounded-full text-xs font-bold">🇬🇧 CAMBRIDGE</span>')
            if structured.get('has_robotics'):
                badges.append('<span class="bg-cyan-500/20 text-cyan-300 px-2 py-1 rounded-full text-xs font-bold">🤖 ROBÓTICA</span>')
            if structured.get('has_stem'):
                badges.append('<span class="bg-green-500/20 text-green-300 px-2 py-1 rounded-full text-xs font-bold">🔬 STEM</span>')
            
            badges_html = '<div class="flex flex-wrap gap-2 mb-4">' + ''.join(badges) + '</div>' if badges else ''
            
            # CORRECCIÓN: Usar una variable separada para el reporte con reemplazo
            report_with_br = report.replace('\n', '<br/>')
            
            return HttpResponse(f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>🌌 Cosmic Report - {inst.name}</title>
                <script src="https://cdn.tailwindcss.com"></script>
                <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
                <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0,1" rel="stylesheet" />
                <style>
                    body {{ font-family: 'Inter', sans-serif; background: linear-gradient(135deg, #030303 0%, #0a0a1a 100%); }}
                    .font-mono {{ font-family: 'JetBrains Mono', monospace; }}
                    .prose-cosmic {{ max-width: 1200px; margin: 0 auto; }}
                    .prose-cosmic h1 {{ color: #c084fc; font-size: 2rem; font-weight: 800; border-bottom: 2px solid #c084fc30; padding-bottom: 1rem; }}
                    .prose-cosmic h2 {{ color: #a78bfa; font-size: 1.5rem; font-weight: 700; margin-top: 1.5rem; border-left: 3px solid #c084fc; padding-left: 1rem; }}
                    .prose-cosmic h3 {{ color: #94a3b8; font-size: 1.2rem; font-weight: 600; }}
                    .prose-cosmic table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; background: #111; border-radius: 12px; overflow: hidden; }}
                    .prose-cosmic th {{ background: #1e1a3a; padding: 0.75rem; text-align: left; color: #c084fc; font-weight: 600; }}
                    .prose-cosmic td {{ padding: 0.75rem; border-bottom: 1px solid #2d2d4a; }}
                    .prose-cosmic ul {{ list-style-type: none; padding-left: 0; }}
                    .prose-cosmic li {{ padding: 0.25rem 0; position: relative; padding-left: 1.5rem; }}
                    .prose-cosmic li:before {{ content: "▹"; position: absolute; left: 0; color: #c084fc; }}
                    ::-webkit-scrollbar {{ width: 8px; }}
                    ::-webkit-scrollbar-track {{ background: #0a0a0a; }}
                    ::-webkit-scrollbar-thumb {{ background: #c084fc; border-radius: 4px; }}
                    .glow-text {{ text-shadow: 0 0 10px rgba(192,132,252,0.5); }}
                </style>
            </head>
            <body class="text-slate-200">
                <div class="min-h-screen p-8">
                    <div class="max-w-6xl mx-auto">
                        <div class="flex justify-between items-center mb-6 border-b border-purple-500/30 pb-4">
                            <div>
                                <h1 class="text-3xl font-black bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent glow-text">🌌 COSMIC INTELLIGENCE REPORT</h1>
                                <p class="text-slate-400 font-mono text-sm mt-2">{inst.name} | {inst.city}, {inst.country}</p>
                            </div>
                            <div class="flex gap-3">
                                <a href="{reverse('admin:sales_globalpipeline_cosmic_report_export', args=[inst_id])}" class="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 rounded-xl text-white font-bold transition-all flex items-center gap-2">
                                    <span class="material-symbols-outlined text-[18px]">download</span> EXPORTAR
                                </a>
                                <button onclick="window.close()" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded-xl text-white font-bold transition-all flex items-center gap-2">
                                    <span class="material-symbols-outlined text-[18px]">close</span> CERRAR
                                </button>
                            </div>
                        </div>
                        
                        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                            <div class="bg-gradient-to-r from-purple-950/30 to-indigo-950/30 rounded-xl p-4 border border-purple-500/30">
                                <div class="text-[10px] text-purple-400 uppercase tracking-wider">Confianza del Análisis</div>
                                <div class="text-2xl font-bold text-white">{confidence:.0f}%</div>
                                <div class="w-full bg-slate-700 h-1 mt-2 rounded-full overflow-hidden">
                                    <div class="bg-purple-500 h-full rounded-full" style="width: {confidence}%"></div>
                                </div>
                            </div>
                            <div class="bg-gradient-to-r from-emerald-950/30 to-teal-950/30 rounded-xl p-4 border border-emerald-500/30">
                                <div class="text-[10px] text-emerald-400 uppercase tracking-wider">Completitud de Datos</div>
                                <div class="text-2xl font-bold text-white">{completeness:.0f}%</div>
                                <div class="w-full bg-slate-700 h-1 mt-2 rounded-full overflow-hidden">
                                    <div class="bg-emerald-500 h-full rounded-full" style="width: {completeness}%"></div>
                                </div>
                            </div>
                            <div class="bg-gradient-to-r from-blue-950/30 to-cyan-950/30 rounded-xl p-4 border border-blue-500/30">
                                <div class="text-[10px] text-blue-400 uppercase tracking-wider">LMS Detectado</div>
                                <div class="text-xl font-bold text-white font-mono">{lms}</div>
                            </div>
                        </div>
                        
                        {badges_html}
                        
                        <div class="prose-cosmic">
                            {mark_safe(report_with_br)}
                        </div>
                        
                        <div class="mt-8 pt-4 border-t border-purple-500/20 text-center text-slate-500 text-xs font-mono">
                            Reporte generado por Cosmic Intelligence Engine v99.9.9.9.9 | Powered by DeepSeek AI
                        </div>
                    </div>
                </div>
            </body>
            </html>
            """)
        else:
            return HttpResponse(f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Reporte no disponible</title>
                <script src="https://cdn.tailwindcss.com"></script>
                <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0,1" rel="stylesheet" />
            </head>
            <body class="bg-black text-white flex items-center justify-center min-h-screen">
                <div class="text-center p-8 bg-slate-900/50 rounded-2xl border border-red-500/30 max-w-md">
                    <span class="text-6xl mb-4 block">📭</span>
                    <h2 class="text-2xl font-bold text-red-400 mb-4">No hay reporte disponible</h2>
                    <p class="text-slate-400 mb-6">Ejecuta un escaneo profundo para generar el reporte cósmico con toda la inteligencia de la institución.</p>
                    <div class="flex gap-3 justify-center">
                        <a href="{reverse('admin:sales_globalpipeline_auto_sniper', args=[inst_id])}" class="px-6 py-2 bg-gradient-to-r from-red-600 to-purple-700 hover:from-red-500 hover:to-purple-600 rounded-xl text-white font-bold transition-all flex items-center gap-2">
                            <span class="material-symbols-outlined text-[18px]">my_location</span> EJECUTAR SNIPER
                        </a>
                        <button onclick="window.close()" class="px-6 py-2 bg-slate-800 hover:bg-slate-700 rounded-xl text-white font-bold transition-all">CERRAR</button>
                    </div>
                </div>
            </body>
            </html>
            """)

    def export_cosmic_report(self, request, inst_id):
        """Exporta el reporte cósmico en formato Markdown/Texto"""
        inst = get_object_or_404(Institution.objects.select_related('forensic_profile'), pk=inst_id)
        
        if hasattr(inst, 'forensic_profile') and inst.forensic_profile and inst.forensic_profile.ai_comprehensive_report:
            report = inst.forensic_profile.ai_comprehensive_report
            response = HttpResponse(report, content_type='text/markdown')
            response['Content-Disposition'] = f'attachment; filename="cosmic_report_{inst.name.replace(" ", "_")}.md"'
            return response
        
        messages.error(request, "No hay reporte disponible para exportar")
        return redirect('admin:sales_globalpipeline_changelist')

    def bulk_generate_cosmic(self, request):
        """Acción masiva para generar reportes cósmicos en lote"""
        if request.method == 'POST':
            limit = int(request.POST.get('limit', 50))
            city = request.POST.get('city', '')
            
            queryset = Institution.objects.filter(
                is_active=True,
                website__isnull=False
            ).exclude(website='')
            
            if city:
                queryset = queryset.filter(city__icontains=city)
            
            targets = list(queryset.values_list('id', flat=True)[:limit])
            
            for inst_id in targets:
                task_run_single_recon.delay(str(inst_id))
            
            messages.success(request, f"🌌 Generación masiva iniciada para {len(targets)} instituciones.")
            return redirect('admin:sales_globalpipeline_changelist')
        
        return HttpResponse("""
        <div class="p-6">
            <h2 class="text-xl font-bold mb-4">🌌 Generación Masiva de Reportes Cósmicos</h2>
            <form method="post">
                <input type="hidden" name="csrfmiddlewaretoken" value="{}">
                <div class="mb-4">
                    <label class="block text-sm font-medium mb-2">Límite de instituciones</label>
                    <input type="number" name="limit" value="50" class="border rounded px-3 py-2 w-32">
                </div>
                <div class="mb-4">
                    <label class="block text-sm font-medium mb-2">Ciudad (opcional)</label>
                    <input type="text" name="city" class="border rounded px-3 py-2 w-64">
                </div>
                <button type="submit" class="bg-purple-600 text-white px-4 py-2 rounded hover:bg-purple-700">Iniciar Generación</button>
            </form>
        </div>
        """.format(request.COOKIES.get('csrftoken', '')))

    def ws_status(self, request, inst_id):
        return JsonResponse({"status": "ok", "message": f"Canal abierto para {inst_id}", "timestamp": time.time()})

    def _send_ws_update(self, inst_id, message):
        try:
            channel_layer = get_channel_layer()
            async_to_sync(channel_layer.group_send)(
                f"institution_{inst_id}",
                {"type": "recon.update", "message": message, "timestamp": time.time()}
            )
        except Exception as e:
            logger.warning(f"WebSocket no disponible: {e}")

    # ==========================================
    # CELDAS ESTÁTICAS - VERSIÓN OMEGA
    # ==========================================

    @display(description='Identidad', ordering='name')
    def display_institution_identity(self, obj):
        url = obj.website or ""
        clean_url = url.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0] if url else "URL Pendiente"
        icon = "🏫" if obj.institution_type == "school" else "🏢" if obj.institution_type == "university" else "📚"
        flag = "🇨🇴" if "colombia" in (obj.country or "").lower() else "🇪🇸" if "españa" in (obj.country or "").lower() else "🌎"
        city = obj.city or "Global"
        
        cosmic_badge = ""
        if hasattr(obj, 'forensic_profile') and obj.forensic_profile and obj.forensic_profile.ai_comprehensive_report:
            cosmic_badge = '<span class="ml-2 text-[8px] bg-gradient-to-r from-purple-500/30 to-indigo-500/30 text-purple-300 px-1.5 py-0.5 rounded-full border border-purple-500/30">🌌 COSMIC</span>'
        
        url_html = format_html('<a href="{}" target="_blank" class="text-blue-600 dark:text-blue-400 text-xs font-mono hover:underline flex items-center gap-1"><span class="material-symbols-outlined text-[12px]">link</span>{}</a>', url, clean_url[:25]) if url else format_html('<span class="text-red-500 text-xs font-mono">URL Pendiente</span>')

        return format_html(
            '<div class="whitespace-nowrap min-w-[260px]">'
            '  <div class="flex items-center gap-1.5">'
            '    <span class="text-lg">{}</span>'
            '    <strong class="text-sm text-gray-900 dark:text-white font-semibold">{}</strong>'
            '    {}'
            '  </div>'
            '  <div class="flex items-center gap-2 mt-1 text-[11px] text-slate-500">'
            '    <span>{}</span>'
            '    <span class="text-slate-400">|</span>'
            '    <span>{}</span>'
            '    <span class="text-slate-400">|</span>'
            '    {}'
            '  </div>'
            '</div>',
            icon, obj.name[:40], mark_safe(cosmic_badge), flag, city, url_html
        )

    @display(description="Mando")
    def advanced_recon_trigger(self, obj) -> str:
        url_sniper = reverse('admin:sales_globalpipeline_auto_sniper', args=[obj.pk])
        url_report = reverse('admin:sales_globalpipeline_cosmic_report', args=[obj.pk])
        
        btn_base = (
            "group relative inline-flex w-full items-center justify-center gap-2 px-3 py-1.5 mb-1.5 "
            "text-[10px] font-black uppercase tracking-[0.15em] rounded shadow-sm transition-all "
            "duration-300 overflow-hidden disabled:opacity-50 disabled:cursor-not-allowed disabled:grayscale"
        )

        sniper_btn = format_html(
            '<button type="button" hx-get="{url}" hx-target="#recon-panel-{pk}" hx-swap="outerHTML" '
            'hx-disabled-elt="this" '
            'class="{classes} text-white bg-gradient-to-r from-red-600 via-red-500 to-purple-700 '
            'hover:from-red-500 hover:to-purple-500 shadow-[0_0_15px_rgba(220,38,38,0.4)] ring-1 ring-white/10">'
            '<span class="absolute inset-0 w-full h-full bg-gradient-to-r from-transparent via-white/20 to-transparent '
            '-translate-x-full group-hover:animate-[shimmer_1.5s_infinite] pointer-events-none"></span>'
            '<span class="material-symbols-outlined text-[13px] group-active:scale-90 transition-transform '
            'drop-shadow-md pointer-events-none">my_location</span>'
            '<span class="relative z-10 drop-shadow-md pointer-events-none">FULL SNIPER</span>'
            '</button>',
            url=url_sniper,
            pk=obj.pk,
            classes=btn_base
        )

        report_btn = format_html(
            '<a href="{url_report}" target="_blank" class="{classes} bg-purple-600 text-white hover:bg-purple-500 '
            'shadow-[0_0_15px_rgba(168,85,247,0.3)] ring-1 ring-purple-500/50 flex items-center justify-center">'
            '<span class="material-symbols-outlined text-[13px]">psychology</span>'
            '<span class="pointer-events-none">COSMIC REPORT</span>'
            '</a>',
            url_report=url_report,
            classes=btn_base
        )

        secondary_btns = ""
        if obj.website:
            url_lms = reverse('admin:sales_globalpipeline_scan_lms', args=[obj.pk])
            url_deep = reverse('admin:sales_globalpipeline_scan_deep', args=[obj.pk])
            
            secondary_btns = format_html(
                '<div class="flex gap-1">'
                '<button type="button" hx-get="{url_lms}" hx-target="#recon-panel-{pk}" hx-swap="outerHTML" '
                'hx-disabled-elt="this" '
                'class="{classes} bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-white '
                'ring-1 ring-slate-700/50 dark:bg-slate-200 dark:text-slate-800 dark:hover:bg-white dark:ring-slate-300">'
                '<span class="material-symbols-outlined text-[13px] pointer-events-none">radar</span> '
                '<span class="pointer-events-none">SCAN LMS</span>'
                '</button>'
                '<button type="button" hx-get="{url_deep}" hx-target="#recon-panel-{pk}" hx-swap="outerHTML" '
                'hx-disabled-elt="this" '
                'class="{classes} bg-[#050505] text-slate-500 hover:bg-[#111] hover:text-emerald-400 ring-1 ring-white/5">'
                '<span class="material-symbols-outlined text-[13px] pointer-events-none">memory</span> '
                '<span class="pointer-events-none">DEEP RECON</span>'
                '</button>'
                '</div>',
                url_lms=url_lms,
                url_deep=url_deep,
                pk=obj.pk,
                classes=btn_base
            )

        return format_html(
            '<div id="recon-panel-{pk}" class="whitespace-nowrap min-w-[180px] flex flex-col gap-1 '
            'animate-in fade-in zoom-in-95 duration-300 ease-out">'
            '<div class="flex gap-1">{sniper_btn}{report_btn}</div>'
            '{secondary_btns}'
            '</div>',
            pk=obj.pk,
            sniper_btn=sniper_btn,
            report_btn=report_btn,
            secondary_btns=secondary_btns
        )
        
    @display(description='Tecnología / Inteligencia')
    @cache_result(ttl=60)
    def display_intelligence_radar(self, obj):
        if not hasattr(obj, 'tech_profile') or not obj.tech_profile:
            return format_html('<div id="tech-radar-{}" class="whitespace-nowrap min-w-[100px]"><span class="text-xs text-gray-400 italic">Sin escanear</span></div>', obj.pk)

        tech = obj.tech_profile
        forensic = getattr(obj, 'forensic_profile', None)
        badges = []
        b_class = "inline-block px-2 py-0.5 rounded text-[9px] font-bold uppercase text-white mb-1 shadow-sm"

        if tech.has_lms and tech.lms_provider:
            lms = str(tech.lms_provider).upper()
            if "SCHOOLNET" in lms:
                color = "bg-blue-600"
            elif "PHIDIAS" in lms:
                color = "bg-purple-600"
            elif "CIBER" in lms:
                color = "bg-cyan-600"
            elif "MOODLE" in lms:
                color = "bg-orange-600"
            elif "CANVAS" in lms:
                color = "bg-red-600"
            else:
                color = "bg-gray-700"
            badges.append(format_html('<span class="{} {}">{}</span><br>', b_class, color, lms[:20]))
        elif obj.last_scored_at:
            badges.append(format_html('<span class="inline-block px-2 py-0.5 rounded text-[9px] font-bold uppercase bg-gray-200 text-gray-600 dark:bg-gray-800 dark:text-gray-300 mb-1 border border-slate-700">SIN LMS</span><br>'))

        if forensic:
            if forensic.is_trilingual:
                badges.append(format_html('<span class="inline-block px-2 py-0.5 mr-1 rounded text-[9px] font-bold uppercase bg-indigo-100 text-indigo-700 dark:bg-indigo-900/50 dark:text-indigo-300 border border-indigo-700/30">🌍 TRILINGÜE</span>'))
            elif forensic.is_bilingual:
                badges.append(format_html('<span class="inline-block px-2 py-0.5 mr-1 rounded text-[9px] font-bold uppercase bg-blue-100 text-blue-700 dark:bg-blue-900/50 dark:text-blue-300 border border-blue-700/30">🗣️ BILINGÜE</span>'))
            
            if forensic.has_ib_cert:
                badges.append(format_html('<span class="inline-block px-2 py-0.5 mr-1 rounded text-[9px] font-bold uppercase bg-emerald-100 text-emerald-700 dark:bg-emerald-900/50 dark:text-emerald-300 border border-emerald-700/30">🏆 IB</span>'))
            if forensic.has_cambridge_cert:
                badges.append(format_html('<span class="inline-block px-2 py-0.5 mr-1 rounded text-[9px] font-bold uppercase bg-yellow-100 text-yellow-700 dark:bg-yellow-900/50 dark:text-yellow-300 border border-yellow-700/30">🇬🇧 CAMBRIDGE</span>'))

        if not badges:
            return format_html('<div id="tech-radar-{}" class="whitespace-nowrap min-w-[100px]"><span class="text-xs text-gray-400 italic">-</span></div>', obj.pk)

        return format_html('<div id="tech-radar-{}" class="whitespace-nowrap min-w-[100px] leading-tight">{}</div>', obj.pk, format_html("".join(badges)))

    @display(description='Score', ordering='lead_score')
    def display_performance_score(self, obj):
        score = obj.lead_score or 0
        
        if score >= 90:
            color = "text-emerald-500"
            bg = "bg-emerald-500/20"
            icon = "🔥"
            label = "ELITE"
        elif score >= 75:
            color = "text-emerald-400"
            bg = "bg-emerald-500/10"
            icon = "⚡"
            label = "HOT"
        elif score >= 50:
            color = "text-amber-400"
            bg = "bg-amber-500/10"
            icon = "🌤️"
            label = "WARM"
        elif score >= 25:
            color = "text-orange-400"
            bg = "bg-orange-500/10"
            icon = "❄️"
            label = "COLD"
        else:
            color = "text-red-400"
            bg = "bg-red-500/10"
            icon = "💀"
            label = "FROZEN"
        
        return format_html(
            '<div id="score-panel-{}" class="whitespace-nowrap min-w-[100px]">'
            '  <div class="flex items-center gap-2 px-2 py-1 rounded-lg {} border border-white/10">'
            '    <span class="text-sm">{}</span>'
            '    <div class="flex flex-col">'
            '      <strong class="text-base {} font-black">{} PTS</strong>'
            '      <span class="text-[8px] text-slate-500 uppercase tracking-wider">{}</span>'
            '    </div>'
            '  </div>'
            '</div>', obj.pk, bg, icon, color, score, label
        )

    # =========================================================
    # [GOD TIER OMEGA]: CONTACT CARD DISPLAY CON VALIDACIÓN AVANZADA
    # =========================================================
    @display(description='Contacto / Vectores')
    @cache_result(ttl=120)
    def display_contact_card(self, obj):
        """Muestra la tarjeta de contacto con emails y teléfonos validados"""
        html = '<div class="flex flex-col gap-1.5 min-w-[180px]">'

        if obj.email:
            email_validated = "✅" if "@" in obj.email and "." in obj.email.split("@")[-1] else "⚠️"
            html += f'''
            <div class="flex items-center gap-1.5 group">
                <span class="material-symbols-outlined text-[12px] text-emerald-500">mail</span>
                <a href="mailto:{obj.email}" class="text-[10px] font-mono text-slate-800 dark:text-slate-300 hover:text-emerald-600 dark:hover:text-emerald-400 transition-colors truncate max-w-[150px] group-hover:underline" title="{obj.email}">
                    {obj.email[:35]}{'...' if len(obj.email) > 35 else ''}
                </a>
                <span class="text-[8px] opacity-50">{email_validated}</span>
            </div>
            '''
        else:
            html += '<div class="text-[9px] font-mono text-slate-500 dark:text-slate-600">✉️ NO EMAIL</div>'

        if obj.phone:
            phones_html = ""
            raw_segments = obj.phone.split()
            for segment in raw_segments:
                if segment.startswith('W:'):
                    nums = segment.replace('W:', '').split(',')
                    for n in nums:
                        if n and len(n) >= 7:
                            phones_html += f'<a href="https://wa.me/{n}" target="_blank" class="px-1.5 py-0.5 mb-1 mr-1 bg-emerald-100 dark:bg-emerald-950/50 text-emerald-700 dark:text-emerald-400 border border-emerald-300 dark:border-emerald-500/30 rounded font-mono text-[9px] font-bold hover:bg-emerald-200 dark:hover:bg-emerald-900 transition-colors inline-flex items-center gap-1 shadow-sm"><span class="material-symbols-outlined text-[10px]">forum</span> {n}</a>'
                elif segment.startswith('T:'):
                    nums = segment.replace('T:', '').split(',')
                    for n in nums:
                        if n and len(n) >= 6:
                            phones_html += f'<a href="tel:{n}" class="px-1.5 py-0.5 mb-1 mr-1 bg-blue-100 dark:bg-blue-950/50 text-blue-700 dark:text-blue-400 border border-blue-300 dark:border-blue-500/30 rounded font-mono text-[9px] font-bold hover:bg-blue-200 dark:hover:bg-blue-900 transition-colors inline-flex items-center gap-1 shadow-sm"><span class="material-symbols-outlined text-[10px]">call</span> {n}</a>'
                else:
                    clean_seg = re.sub(r'[^0-9+]', '', segment)
                    if clean_seg and len(clean_seg) >= 6:
                        phones_html += f'<a href="tel:{clean_seg}" class="px-1.5 py-0.5 mb-1 mr-1 bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-400 border border-slate-300 dark:border-slate-600 rounded font-mono text-[9px] inline-flex shadow-sm hover:bg-slate-300 dark:hover:bg-slate-700 transition-colors">📞 {clean_seg[:15]}</a>'
            
            if phones_html:
                html += f'<div class="flex flex-wrap mt-1 border-t border-slate-200 dark:border-slate-800/50 pt-1.5 max-h-[120px] overflow-y-auto custom-scrollbar">{phones_html}</div>'
            else:
                html += '<div class="text-[9px] font-mono text-slate-500 dark:text-slate-600 mt-1">📞 NÚMERO INVÁLIDO</div>'
        else:
            html += '<div class="text-[9px] font-mono text-slate-500 dark:text-slate-600 mt-1">📞 NO PHONE</div>'

        html += '</div>'
        return format_html(html)

    # =========================================================
    # [GOD TIER OMEGA]: COSMIC INTELLIGENCE DISPLAY COMPLETO
    # =========================================================
    @display(description='🌌 Cosmic Intelligence')
    @cache_result(ttl=180)
    def display_cosmic_readiness(self, obj):
        if hasattr(obj, 'forensic_profile') and obj.forensic_profile and obj.forensic_profile.ai_comprehensive_report:
            structured = obj.forensic_profile.ai_structured_data or {}
            
            # Extraer todos los indicadores
            has_ib = structured.get('certifications', {}).get('ib', {}).get('has_ib', False) or obj.forensic_profile.has_ib_cert
            has_cambridge = structured.get('certifications', {}).get('cambridge', {}).get('has_cambridge', False) or obj.forensic_profile.has_cambridge_cert
            has_oxford = structured.get('certifications', {}).get('oxford', {}).get('has_oxford', False)
            has_robotics = structured.get('technology', {}).get('robotics', {}).get('has_robotics', False) or structured.get('has_robotics', False)
            has_stem = structured.get('technology', {}).get('stem', {}).get('has_stem', False) or structured.get('has_stem', False)
            has_programming = structured.get('technology', {}).get('programming', {}).get('has_programming', False) or structured.get('has_programming', False)
            is_bilingual = structured.get('is_bilingual', False) or obj.forensic_profile.is_bilingual
            is_trilingual = structured.get('is_trilingual', False) or obj.forensic_profile.is_trilingual
            
            icfes = structured.get('performance', {}).get('icfes', {}) if structured.get('performance') else {}
            icfes_score = icfes.get('score', '') or structured.get('icfes_score', '')
            icfes_category = icfes.get('category', '') or structured.get('icfes_category', '')
            
            # Construir badges
            badges = []
            
            if is_bilingual:
                badges.append('<span class="bg-blue-900/60 text-blue-300 border border-blue-500/50 px-2 py-0.5 rounded text-[9px] font-bold">🗣️ BILINGÜE</span>')
            if is_trilingual:
                badges.append('<span class="bg-purple-900/60 text-purple-300 border border-purple-500/50 px-2 py-0.5 rounded text-[9px] font-bold">🌍 TRILINGÜE</span>')
            if has_ib:
                badges.append('<span class="bg-emerald-900/60 text-emerald-300 border border-emerald-500/50 px-2 py-0.5 rounded text-[9px] font-bold">🏆 IB</span>')
            if has_cambridge:
                badges.append('<span class="bg-red-900/60 text-red-300 border border-red-500/50 px-2 py-0.5 rounded text-[9px] font-bold">🇬🇧 CAMBRIDGE</span>')
            if has_oxford:
                badges.append('<span class="bg-orange-900/60 text-orange-300 border border-orange-500/50 px-2 py-0.5 rounded text-[9px] font-bold">📚 OXFORD</span>')
            if has_robotics:
                badges.append('<span class="bg-purple-900/60 text-purple-300 border border-purple-500/50 px-2 py-0.5 rounded text-[9px] font-bold">🤖 ROBÓTICA</span>')
            if has_stem:
                badges.append('<span class="bg-cyan-900/60 text-cyan-300 border border-cyan-500/50 px-2 py-0.5 rounded text-[9px] font-bold">🔬 STEM</span>')
            if has_programming:
                badges.append('<span class="bg-green-900/60 text-green-300 border border-green-500/50 px-2 py-0.5 rounded text-[9px] font-bold">💻 PROGRAMACIÓN</span>')
            if icfes_score:
                badges.append(f'<span class="bg-amber-900/60 text-amber-300 border border-amber-500/50 px-2 py-0.5 rounded text-[9px] font-bold">📊 ICFES: {icfes_score} {icfes_category}</span>')
            
            # Extraer resumen ejecutivo
            executive_summary = structured.get('executive_summary', '')[:150]
            
            badges_html = '<div class="flex flex-wrap gap-1.5 mb-2">' + ''.join(badges) + '</div>' if badges else '<div class="text-slate-500 text-[9px] mb-2">📊 Datos básicos disponibles</div>'
            
            # Botón para ver reporte completo
            view_url = reverse('admin:sales_globalpipeline_cosmic_report', args=[obj.id])
            
            return format_html(
                '<div id="cosmic-panel-{}" class="min-w-[280px] max-w-[400px] bg-gradient-to-br from-black/80 to-purple-950/40 rounded-xl border border-purple-500/30 p-2 shadow-lg">'
                '<div class="flex items-center justify-between border-b border-purple-500/30 pb-1 mb-2">'
                '<span class="text-[9px] font-mono text-purple-400 uppercase tracking-wider flex items-center gap-1">'
                '<span class="material-symbols-outlined text-[12px]">psychology</span> COSMIC INTELLIGENCE'
                '</span>'
                '<a href="{}" target="_blank" class="text-[8px] bg-purple-600/40 hover:bg-purple-600/60 text-purple-300 px-2 py-0.5 rounded transition-all flex items-center gap-1">'
                '<span class="material-symbols-outlined text-[10px]">open_in_new</span> VER'
                '</a>'
                '</div>'
                '{}'
                '<div class="text-[9px] font-mono text-slate-400 leading-relaxed max-h-[80px] overflow-hidden">'
                '<span class="text-emerald-400">📋</span> {}{}'
                '</div>'
                '<div class="text-right mt-1">'
                '<span class="text-[7px] text-purple-500/50">🤖 DeepSeek AI</span>'
                '</div>'
                '</div>',
                obj.id, view_url, mark_safe(badges_html), executive_summary[:120], '...' if len(executive_summary) > 120 else ''
            )
        
        return format_html(
            '<div id="cosmic-panel-{}" class="min-w-[180px] bg-slate-900/40 rounded-xl border border-slate-700/50 p-2 text-center">'
            '<span class="material-symbols-outlined text-[20px] text-slate-500">psychology_off</span>'
            '<div class="text-[9px] text-slate-500 mt-1">Sin reporte IA</div>'
            '<a href="{}" class="text-[8px] text-purple-400 hover:text-purple-300 mt-1 inline-block">Ejecutar escaneo</a>'
            '</div>',
            obj.id, reverse('admin:sales_globalpipeline_auto_sniper', args=[obj.id])
        )

    @display(description='Cosmic Full Report')
    def display_cosmic_full_report(self, obj):
        """Muestra el reporte completo del Cosmic Analyzer con formato profesional"""
        if not hasattr(obj, 'forensic_profile') or not obj.forensic_profile:
            return format_html('<div class="text-sm text-gray-500 italic p-4 bg-gray-50 dark:bg-gray-800/50 border border-dashed rounded-lg">⚠️ No hay datos forenses. Ejecuta un escaneo profundo primero.</div>')
        
        profile = obj.forensic_profile
        
        if not profile.ai_comprehensive_report:
            return format_html(
                '<div class="p-6 bg-gradient-to-r from-purple-900/20 to-indigo-900/20 rounded-xl border border-purple-500/30 text-center">'
                '  <span class="text-4xl mb-3 block">🌌</span>'
                '  <h4 class="text-white font-bold mb-2">Reporte Cósmico no disponible</h4>'
                '  <p class="text-slate-400 text-sm mb-4">Ejecuta un escaneo profundo para generar inteligencia completa.</p>'
                '  <a href="{}" class="inline-block px-4 py-2 bg-purple-600 rounded-lg text-white text-sm hover:bg-purple-500">Ejecutar Sniper</a>'
                '</div>',
                reverse('admin:sales_globalpipeline_auto_sniper', args=[obj.id])
            )
        
        # CORRECCIÓN: Usar una variable separada para el reporte con reemplazo
        report_with_br = profile.ai_comprehensive_report.replace('\n', '<br/>')
        
        return format_html(
            '<div class="p-6 bg-gradient-to-br from-black/90 to-purple-950/30 rounded-xl border border-purple-500/30 shadow-2xl max-h-[600px] overflow-y-auto">'
            '  <div class="prose prose-invert prose-sm max-w-none">'
            '    {}'
            '  </div>'
            '</div>',
            mark_safe(report_with_br)
        )

    @display(description="Executive Summary")
    @cache_result(ttl=300)
    def display_cosmic_executive_summary(self, obj):
        if not hasattr(obj, 'forensic_profile') or not obj.forensic_profile:
            return "-"
        
        structured = obj.forensic_profile.ai_structured_data or {}
        summary = structured.get('executive_summary', '')
        
        if summary:
            return format_html(
                '<div class="p-3 bg-emerald-950/30 border-l-4 border-emerald-500 rounded-r-lg">'
                '  <div class="text-[11px] text-emerald-300 leading-relaxed">{}</div>'
                '</div>',
                summary[:350] + ("..." if len(summary) > 350 else "")
            )
        return "-"

    @display(description="Certifications")
    @cache_result(ttl=300)
    def display_cosmic_certifications(self, obj):
        if not hasattr(obj, 'forensic_profile') or not obj.forensic_profile:
            return "-"
        
        structured = obj.forensic_profile.ai_structured_data or {}
        certs = structured.get('certifications', {})
        
        badges = []
        
        ib = certs.get('ib', {})
        if ib.get('has_ib') or obj.forensic_profile.has_ib_cert:
            badges.append(f'<span class="inline-block px-2 py-1 bg-emerald-900/50 text-emerald-400 rounded text-[10px] font-bold">🏆 IB {", ".join(ib.get("programs", []))}</span>')
        
        cambridge = certs.get('cambridge', {})
        if cambridge.get('has_cambridge') or obj.forensic_profile.has_cambridge_cert:
            badges.append(f'<span class="inline-block px-2 py-1 bg-blue-900/50 text-blue-400 rounded text-[10px] font-bold">🇬🇧 Cambridge {", ".join(cambridge.get("exams", []))}</span>')
        
        oxford = certs.get('oxford', {})
        if oxford.get('has_oxford'):
            badges.append('<span class="inline-block px-2 py-1 bg-yellow-900/50 text-yellow-400 rounded text-[10px] font-bold">📚 Oxford</span>')
        
        quality = certs.get('quality', {})
        if quality.get('has_iso'):
            badges.append('<span class="inline-block px-2 py-1 bg-slate-700 text-slate-300 rounded text-[10px] font-bold">✅ ISO 9001</span>')
        if quality.get('has_efqm'):
            badges.append('<span class="inline-block px-2 py-1 bg-slate-700 text-slate-300 rounded text-[10px] font-bold">🏅 EFQM</span>')
        
        if not badges:
            return '<span class="text-slate-500 text-[10px] italic">No especificado</span>'
        
        return format_html('<div class="flex flex-wrap gap-1.5">{}</div>', mark_safe(" ".join(badges)))

    @display(description="Technology & STEM")
    @cache_result(ttl=300)
    def display_cosmic_technology(self, obj):
        if not hasattr(obj, 'forensic_profile') or not obj.forensic_profile:
            return "-"
        
        structured = obj.forensic_profile.ai_structured_data or {}
        tech = structured.get('technology', {})
        
        badges = []
        
        robotics = tech.get('robotics', {})
        if robotics.get('has_robotics') or structured.get('has_robotics', False):
            badges.append(f'<span class="inline-block px-2 py-1 bg-purple-900/50 text-purple-400 rounded text-[10px] font-bold">🤖 {robotics.get("type", "Robótica")}</span>')
        
        programming = tech.get('programming', {})
        if programming.get('has_programming') or structured.get('has_programming', False):
            langs = ", ".join(programming.get('languages', [])[:3])
            badges.append(f'<span class="inline-block px-2 py-1 bg-cyan-900/50 text-cyan-400 rounded text-[10px] font-bold">💻 {langs or "Programación"}</span>')
        
        stem = tech.get('stem', {})
        if stem.get('has_stem') or structured.get('has_stem', False):
            badges.append('<span class="inline-block px-2 py-1 bg-emerald-900/50 text-emerald-400 rounded text-[10px] font-bold">🔬 STEM</span>')
        
        labs = tech.get('laboratories', [])
        if labs:
            badges.append(f'<span class="inline-block px-2 py-1 bg-slate-700 text-slate-300 rounded text-[10px] font-bold">🔧 {len(labs)} laboratorios</span>')
        
        if not badges:
            return '<span class="text-slate-500 text-[10px] italic">No especificado</span>'
        
        return format_html('<div class="flex flex-wrap gap-1.5">{}</div>', mark_safe(" ".join(badges)))

    @display(description="Extracurricular")
    @cache_result(ttl=300)
    def display_cosmic_extracurricular(self, obj):
        if not hasattr(obj, 'forensic_profile') or not obj.forensic_profile:
            return "-"
        
        structured = obj.forensic_profile.ai_structured_data or {}
        extra = structured.get('extracurricular', {})
        
        items = []
        
        if extra.get('sports'):
            items.append(f'<span class="text-indigo-400 text-[9px] font-bold">⚽ Deportes:</span> <span class="text-slate-300 text-[9px]">{", ".join(extra["sports"][:3])}</span>')
        if extra.get('arts'):
            items.append(f'<span class="text-pink-400 text-[9px] font-bold">🎨 Artes:</span> <span class="text-slate-300 text-[9px]">{", ".join(extra["arts"][:3])}</span>')
        if extra.get('clubs'):
            items.append(f'<span class="text-purple-400 text-[9px] font-bold">🎯 Clubs:</span> <span class="text-slate-300 text-[9px]">{", ".join(extra["clubs"][:3])}</span>')
        if extra.get('camps'):
            items.append(f'<span class="text-orange-400 text-[9px] font-bold">🏕️ Campamentos:</span> <span class="text-slate-300 text-[9px]">{", ".join(extra["camps"][:2])}</span>')
        
        if not items:
            return '<span class="text-slate-500 text-[10px] italic">No especificado</span>'
        
        return format_html('<div class="space-y-1 max-w-[250px]">{}</div>', mark_safe("".join([f'<div>{i}</div>' for i in items])))

    @display(description="Agreements")
    @cache_result(ttl=300)
    def display_cosmic_agreements(self, obj):
        if not hasattr(obj, 'forensic_profile') or not obj.forensic_profile:
            return "-"
        
        structured = obj.forensic_profile.ai_structured_data or {}
        agreements = structured.get('agreements', {})
        
        items = []
        
        uni_agreements = agreements.get('university_agreements', [])
        if uni_agreements:
            items.append(f'<span class="text-blue-400 text-[9px] font-bold">🏛️ Universidades:</span> <span class="text-slate-300 text-[9px]">{", ".join(uni_agreements[:3])}</span>')
        
        corp_agreements = agreements.get('corporate_agreements', [])
        if corp_agreements:
            items.append(f'<span class="text-emerald-400 text-[9px] font-bold">🤝 Empresas:</span> <span class="text-slate-300 text-[9px]">{", ".join(corp_agreements[:3])}</span>')
        
        if not items:
            return '<span class="text-slate-500 text-[10px] italic">No especificado</span>'
        
        return format_html('<div class="space-y-1 max-w-[250px]">{}</div>', mark_safe("".join([f'<div>{i}</div>' for i in items])))

    @display(description="Infrastructure")
    @cache_result(ttl=300)
    def display_cosmic_infrastructure(self, obj):
        if not hasattr(obj, 'forensic_profile') or not obj.forensic_profile:
            return "-"
        
        structured = obj.forensic_profile.ai_structured_data or {}
        infra = structured.get('infrastructure', {})
        
        items = []
        
        campus = infra.get('campus', {})
        if campus.get('size'):
            items.append(f'📐 {campus["size"]}')
        
        if infra.get('sports_facilities'):
            items.append(f'⚽ {len(infra["sports_facilities"])} instalaciones deportivas')
        
        if infra.get('library'):
            items.append(f'📚 Biblioteca')
        
        if infra.get('transport'):
            items.append(f'🚌 Transporte escolar')
        
        if not items:
            return '<span class="text-slate-500 text-[10px] italic">No especificado</span>'
        
        return format_html('<div class="text-[10px] text-slate-300">{}</div>', ", ".join(items[:4]))

    @display(description="Sales Intelligence")
    @cache_result(ttl=300)
    def display_cosmic_sales_intelligence(self, obj):
        if not hasattr(obj, 'forensic_profile') or not obj.forensic_profile:
            return "-"
        
        structured = obj.forensic_profile.ai_structured_data or {}
        sales = structured.get('sales_intelligence', {})
        
        items = []
        
        pain_points = sales.get('pain_points', [])[:2]
        if pain_points:
            items.append(f'<div><span class="text-red-400 text-[9px] font-bold">🔴 Dolores:</span> <span class="text-slate-300 text-[9px]">{", ".join(pain_points)}</span></div>')
        
        triggers = sales.get('sales_triggers', [])[:2]
        if triggers:
            items.append(f'<div><span class="text-green-400 text-[9px] font-bold">🟢 Triggers:</span> <span class="text-slate-300 text-[9px]">{", ".join(triggers)}</span></div>')
        
        opportunities = sales.get('opportunities', [])[:2]
        if opportunities:
            items.append(f'<div><span class="text-blue-400 text-[9px] font-bold">🚀 Oportunidades:</span> <span class="text-slate-300 text-[9px]">{", ".join(opportunities)}</span></div>')
        
        ideal_contact = sales.get('ideal_contact', '')
        if ideal_contact:
            items.append(f'<div><span class="text-purple-400 text-[9px] font-bold">👤 Contacto ideal:</span> <span class="text-slate-300 text-[9px]">{ideal_contact}</span></div>')
        
        if not items:
            return '<span class="text-slate-500 text-[10px] italic">No especificado</span>'
        
        return format_html('<div class="space-y-1 max-w-[250px]">{}</div>', mark_safe("".join(items)))

    @display(description='Último Scan')
    def display_sync_metrics(self, obj):
        if not obj.last_scored_at:
            return format_html('<div class="whitespace-nowrap min-w-[70px]"><span class="text-xs text-gray-400 italic">-</span></div>')
        
        days_ago = (timezone.now() - obj.last_scored_at).days
        if days_ago == 0:
            color = "text-emerald-400"
            label = "hoy"
            bg = "bg-emerald-500/10"
        elif days_ago < 7:
            color = "text-amber-400"
            label = f"{days_ago}d"
            bg = "bg-amber-500/10"
        elif days_ago < 30:
            color = "text-orange-400"
            label = f"{days_ago}d"
            bg = "bg-orange-500/10"
        else:
            color = "text-red-400"
            label = f"{days_ago}d"
            bg = "bg-red-500/10"
        
        return format_html(
            '<div class="whitespace-nowrap min-w-[80px]">'
            '  <div class="flex flex-col items-end">'
            '    <span class="font-mono text-[10px] {} font-bold px-2 py-0.5 rounded {}">{}</span>'
            '    <span class="text-[8px] text-slate-500 mt-0.5">{}</span>'
            '  </div>'
            '</div>',
            color, bg, label, obj.last_scored_at.strftime("%d/%m/%y")
        )

    # --- ACCIONES MASIVAS MEJORADAS ---
    actions = ['trigger_deep_recon', 'generate_cosmic_reports', 'export_cosmic_csv']

    @action(description="🎯 Lote: Desplegar Misión Ghost Sniper (Deep Recon)")
    def trigger_deep_recon(self, request, queryset):
        success, skipped, failed = 0, 0, 0
        for inst in queryset:
            if inst.website:
                try:
                    task_run_single_recon.delay(str(inst.id))
                    success += 1
                except Exception as e:
                    logger.error(f"Fallo en bulk recon {inst.name}: {e}")
                    failed += 1
            else:
                skipped += 1
        self.message_user(request, f"🚀 Misión masiva completada: {success} encolados, {failed} fallos, {skipped} omitidos (Sin URL).")

    @action(description="🌌 Generar Reportes Cósmicos (IA)")
    def generate_cosmic_reports(self, request, queryset):
        success = 0
        failed = 0
        for inst in queryset:
            if inst.website:
                try:
                    task_run_single_recon.delay(str(inst.id))
                    success += 1
                except Exception as e:
                    logger.error(f"Fallo generando reporte para {inst.name}: {e}")
                    failed += 1
            else:
                failed += 1
        self.message_user(request, f"🌌 Generación de reportes iniciada: {success} encolados, {failed} fallos.")

    @action(description="📊 Exportar a CSV (Cosmic Data)")
    def export_cosmic_csv(self, request, queryset):
        import csv
        from django.http import HttpResponse
        
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="cosmic_export.csv"'
        
        writer = csv.writer(response)
        writer.writerow(['ID', 'Nombre', 'Ciudad', 'País', 'LMS', 'Email', 'Teléfono', 'Lead Score', 'Bilingüe', 'IB', 'Cambridge', 'Robótica', 'STEM', 'ICFES', 'Último Escaneo'])
        
        for inst in queryset.select_related('tech_profile', 'forensic_profile'):
            structured = inst.forensic_profile.ai_structured_data if hasattr(inst, 'forensic_profile') else {}
            
            writer.writerow([
                inst.id,
                inst.name,
                inst.city,
                inst.country,
                inst.tech_profile.lms_provider if hasattr(inst, 'tech_profile') else '',
                inst.email,
                inst.phone,
                inst.lead_score,
                inst.forensic_profile.is_bilingual if hasattr(inst, 'forensic_profile') else '',
                inst.forensic_profile.has_ib_cert if hasattr(inst, 'forensic_profile') else '',
                inst.forensic_profile.has_cambridge_cert if hasattr(inst, 'forensic_profile') else '',
                structured.get('has_robotics', ''),
                structured.get('has_stem', ''),
                structured.get('icfes_score', ''),
                inst.last_scored_at.strftime('%Y-%m-%d') if inst.last_scored_at else '',
            ])
        
        self.message_user(request, f"📊 Exportación completada para {queryset.count()} instituciones.")
        return response

    fieldsets = (
        ('Identidad Estratégica', {'classes': ('tab',), 'fields': (('name', 'institution_type', 'processing_status'), ('country', 'state_region', 'city'), ('address',), ('website', 'email', 'phone'),),}),
        ('🌌 Inteligencia Cósmica (IA Avanzada)', {'classes': ('tab',), 'fields': ('display_cosmic_full_report',)}),
        ('📊 Resumen Ejecutivo', {'classes': ('tab',), 'fields': ('display_cosmic_executive_summary',)}),
        ('🏆 Certificaciones', {'classes': ('tab',), 'fields': ('display_cosmic_certifications',)}),
        ('🤖 Tecnología y STEM', {'classes': ('tab',), 'fields': ('display_cosmic_technology',)}),
        ('🎪 Extracurriculares', {'classes': ('tab',), 'fields': ('display_cosmic_extracurricular',)}),
        ('🤝 Convenios y Alianzas', {'classes': ('tab',), 'fields': ('display_cosmic_agreements',)}),
        ('🏛️ Infraestructura', {'classes': ('tab',), 'fields': ('display_cosmic_infrastructure',)}),
        ('💼 Inteligencia de Ventas', {'classes': ('tab',), 'fields': ('display_cosmic_sales_intelligence',)}),
        ('🔬 Analítica Base', {'classes': ('tab',), 'fields': (('lead_score', 'last_scored_at', 'discovery_source'),),}),
    )

    '''def run_auto_sniper(self, request, inst_id):
        cache.set(f"scan_in_progress_{inst_id}", True, timeout=600)
        task_run_single_recon.delay(inst_id)
        return HttpResponse(self._get_polling_html(inst_id))'''

    def run_auto_sniper(self, request, inst_id):
        cache.set(f"scan_in_progress_{inst_id}", True, timeout=600)
    
    # 🔥 GOD TIER: Usar la tarea de Celery
        from sales.tasks import task_run_single_recon
        task_run_single_recon.delay(str(inst_id))
    
    # Devolver el HTML de polling para actualización en tiempo real
        return HttpResponse(self._get_polling_html(inst_id))

# =========================================================
# [GOD TIER OMEGA] - NUEVO BOTÓN PARA REPORTE EXTERNO
# =========================================================

def view_cosmic_report_external(self, obj) -> str:
    """Botón para ver el reporte cósmico en la vista externa profesional"""
    from django.urls import reverse
    url = reverse('sales:cosmic_report', args=[obj.id])
    return format_html(
        '<a href="{}" target="_blank" class="inline-flex items-center gap-1 px-2 py-1 bg-gradient-to-r from-purple-600 to-indigo-600 hover:from-purple-500 hover:to-indigo-500 text-white text-[9px] font-black uppercase tracking-wider rounded transition-all shadow-[0_0_8px_rgba(168,85,247,0.5)]">'
        '<span class="material-symbols-outlined text-[12px]">psychology</span>'
        'REPORTE COMPLETO'
        '</a>',
        url
    )

# Luego, MODIFICA advanced_recon_trigger para incluir este botón:
'''@display(description="Mando")
def advanced_recon_trigger(self, obj) -> str:
    url_sniper = reverse('admin:sales_globalpipeline_auto_sniper', args=[obj.pk])
    url_report = reverse('admin:sales_globalpipeline_cosmic_report', args=[obj.pk])
    url_external = reverse('sales:cosmic_report', args=[obj.pk])
    
    btn_base = (
        "group relative inline-flex w-full items-center justify-center gap-2 px-3 py-1.5 mb-1.5 "
        "text-[10px] font-black uppercase tracking-[0.15em] rounded shadow-sm transition-all "
        "duration-300 overflow-hidden disabled:opacity-50 disabled:cursor-not-allowed disabled:grayscale"
    )

    sniper_btn = format_html(
        '<button type="button" hx-get="{url}" hx-target="#recon-panel-{pk}" hx-swap="outerHTML" '
        'hx-disabled-elt="this" '
        'class="{classes} text-white bg-gradient-to-r from-red-600 via-red-500 to-purple-700 '
        'hover:from-red-500 hover:to-purple-500 shadow-[0_0_15px_rgba(220,38,38,0.4)] ring-1 ring-white/10">'
        '<span class="absolute inset-0 w-full h-full bg-gradient-to-r from-transparent via-white/20 to-transparent '
        '-translate-x-full group-hover:animate-[shimmer_1.5s_infinite] pointer-events-none"></span>'
        '<span class="material-symbols-outlined text-[13px] group-active:scale-90 transition-transform '
        'drop-shadow-md pointer-events-none">my_location</span>'
        '<span class="relative z-10 drop-shadow-md pointer-events-none">FULL SNIPER</span>'
        '</button>',
        url=url_sniper,
        pk=obj.pk,
        classes=btn_base
    )

    report_btn = format_html(
        '<a href="{url_report}" target="_blank" class="{classes} bg-purple-600 text-white hover:bg-purple-500 '
        'shadow-[0_0_15px_rgba(168,85,247,0.3)] ring-1 ring-purple-500/50 flex items-center justify-center">'
        '<span class="material-symbols-outlined text-[13px]">psychology</span>'
        '<span class="pointer-events-none">COSMIC</span>'
        '</a>',
        url_report=url_report,
        classes=btn_base
    )
    
    # 🔥 NUEVO BOTÓN PARA REPORTE EXTERNO PROFESIONAL
    external_btn = format_html(
        '<a href="{url_external}" target="_blank" class="{classes} bg-gradient-to-r from-purple-700 to-indigo-700 hover:from-purple-600 hover:to-indigo-600 '
        'shadow-[0_0_15px_rgba(168,85,247,0.3)] ring-1 ring-purple-500/50 flex items-center justify-center">'
        '<span class="material-symbols-outlined text-[13px]">open_in_new</span>'
        '<span class="pointer-events-none">FULL</span>'
        '</a>',
        url_external=url_external,
        classes=btn_base
    )

    secondary_btns = ""
    if obj.website:
        url_lms = reverse('admin:sales_globalpipeline_scan_lms', args=[obj.pk])
        url_deep = reverse('admin:sales_globalpipeline_scan_deep', args=[obj.pk])
        
        secondary_btns = format_html(
            '<div class="flex gap-1">'
            '<button type="button" hx-get="{url_lms}" hx-target="#recon-panel-{pk}" hx-swap="outerHTML" '
            'hx-disabled-elt="this" '
            'class="{classes} bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-white '
            'ring-1 ring-slate-700/50 dark:bg-slate-200 dark:text-slate-800 dark:hover:bg-white dark:ring-slate-300">'
            '<span class="material-symbols-outlined text-[13px] pointer-events-none">radar</span> '
            '<span class="pointer-events-none">SCAN LMS</span>'
            '</button>'
            '<button type="button" hx-get="{url_deep}" hx-target="#recon-panel-{pk}" hx-swap="outerHTML" '
            'hx-disabled-elt="this" '
            'class="{classes} bg-[#050505] text-slate-500 hover:bg-[#111] hover:text-emerald-400 ring-1 ring-white/5">'
            '<span class="material-symbols-outlined text-[13px] pointer-events-none">memory</span> '
            '<span class="pointer-events-none">DEEP RECON</span>'
            '</button>'
            '</div>',
            url_lms=url_lms,
            url_deep=url_deep,
            pk=obj.pk,
            classes=btn_base
        )

    return format_html(
        '<div id="recon-panel-{pk}" class="whitespace-nowrap min-w-[220px] flex flex-col gap-1 '
        'animate-in fade-in zoom-in-95 duration-300 ease-out">'
        '<div class="flex gap-1">{sniper_btn}{report_btn}{external_btn}</div>'
        '{secondary_btns}'
        '</div>',
        pk=obj.pk,
        sniper_btn=sniper_btn,
        report_btn=report_btn,
        external_btn=external_btn,
        secondary_btns=secondary_btns
    )'''

@display(description="🎯 Comando de Combate | God Tier Omega")
def advanced_recon_trigger(self, obj) -> str:
    """
    [GOD TIER OMEGA] Panel de control de combate con 5 modos de operación:
    - FULL SNIPER: Escaneo completo con Ghost Sniper (HTMX + polling en tiempo real)
    - COSMIC: Reporte cósmico completo con IA
    - FULL: Vista externa profesional del reporte
    - SCAN LMS: Escaneo rápido de LMS
    - DEEP RECON: Reconocimiento profundo con Playwright
    """
    # URLs de combate
    url_sniper = reverse('admin:sales_globalpipeline_auto_sniper', args=[obj.pk])
    url_report = reverse('admin:sales_globalpipeline_cosmic_report', args=[obj.pk])
    url_external = reverse('sales:cosmic_report', args=[obj.pk])
    
    # Base de estilos GOD TIER con animaciones y efectos
    btn_base = (
        "group relative inline-flex items-center justify-center gap-2 px-3 py-1.5 "
        "text-[10px] font-black uppercase tracking-[0.15em] rounded-xl shadow-lg "
        "transition-all duration-300 overflow-hidden disabled:opacity-50 "
        "disabled:cursor-not-allowed disabled:grayscale "
        "hover:scale-[1.02] active:scale-[0.98]"
    )
    
    # =========================================================
    # 🔥 BOTÓN PRINCIPAL: FULL SNIPER (Con polling y actualización en tiempo real)
    # =========================================================
    sniper_btn = format_html(
        '''<button type="button" 
            hx-get="{url}" 
            hx-target="#recon-panel-{pk}" 
            hx-swap="outerHTML"
            hx-disabled-elt="this"
            hx-indicator="#sniper-spinner-{pk}"
            class="{classes} bg-gradient-to-r from-red-600 via-red-500 to-purple-700 
                   hover:from-red-500 hover:to-purple-500 
                   shadow-[0_0_20px_rgba(220,38,38,0.5)] hover:shadow-[0_0_30px_rgba(220,38,38,0.8)]
                   ring-1 ring-white/20 hover:ring-white/40
                   border border-white/10"
            title="Ejecuta el Ghost Sniper para extraer emails, teléfonos, LMS y generar reporte cósmico">
            <div class="absolute inset-0 w-full h-full bg-gradient-to-r from-transparent via-white/30 to-transparent 
                        -translate-x-full group-hover:translate-x-full transition-transform duration-1000 ease-out"></div>
            <span class="material-symbols-outlined text-[14px] group-active:scale-90 transition-transform 
                         drop-shadow-md pointer-events-none">my_location</span>
            <span class="relative z-10 drop-shadow-md pointer-events-none">FULL SNIPER</span>
            <span id="sniper-spinner-{pk}" class="htmx-indicator ml-1">
                <svg class="animate-spin h-3 w-3 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                    <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
            </span>
        </button>''',
        url=url_sniper,
        pk=obj.pk,
        classes=btn_base
    )
    
    # =========================================================
    # 📊 BOTÓN COSMIC: Reporte cósmico completo (vista admin)
    # =========================================================
    report_btn = format_html(
        '''<a href="{url_report}" 
            target="_blank" 
            class="{classes} bg-gradient-to-r from-purple-600 to-indigo-600 
                   hover:from-purple-500 hover:to-indigo-500
                   shadow-[0_0_20px_rgba(168,85,247,0.4)] hover:shadow-[0_0_30px_rgba(168,85,247,0.6)]
                   ring-1 ring-purple-500/30 hover:ring-purple-400/50
                   border border-white/10"
            title="Ver reporte cósmico completo con análisis de IA (IB, Cambridge, STEM, Robótica, ICFES)">
            <span class="material-symbols-outlined text-[14px] drop-shadow-md">psychology</span>
            <span class="pointer-events-none">COSMIC</span>
            <span class="absolute -top-1 -right-1 flex h-2 w-2">
                <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-purple-400 opacity-75"></span>
                <span class="relative inline-flex rounded-full h-2 w-2 bg-purple-500"></span>
            </span>
        </a>''',
        url_report=url_report,
        classes=btn_base
    )
    
    # =========================================================
    # 🌌 BOTÓN FULL: Reporte externo profesional (nueva ventana)
    # =========================================================
    external_btn = format_html(
        '''<a href="{url_external}" 
            target="_blank" 
            class="{classes} bg-gradient-to-r from-indigo-600 to-purple-700 
                   hover:from-indigo-500 hover:to-purple-600
                   shadow-[0_0_20px_rgba(99,102,241,0.4)] hover:shadow-[0_0_30px_rgba(99,102,241,0.6)]
                   ring-1 ring-indigo-500/30 hover:ring-indigo-400/50
                   border border-white/10"
            title="Abrir reporte cósmico en ventana externa con formato profesional">
            <span class="material-symbols-outlined text-[14px] drop-shadow-md">open_in_new</span>
            <span class="pointer-events-none">FULL</span>
        </a>''',
        url_external=url_external,
        classes=btn_base
    )
    
    # =========================================================
    # 🛠️ BOTONES SECUNDARIOS: SCAN LMS y DEEP RECON
    # =========================================================
    secondary_btns = ""
    if obj.website:
        url_lms = reverse('admin:sales_globalpipeline_scan_lms', args=[obj.pk])
        url_deep = reverse('admin:sales_globalpipeline_scan_deep', args=[obj.pk])
        
        secondary_btns = format_html(
            '''<div class="flex gap-1 mt-1">
                <button type="button" 
                    hx-get="{url_lms}" 
                    hx-target="#recon-panel-{pk}" 
                    hx-swap="outerHTML"
                    hx-disabled-elt="this"
                    class="{classes} bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-white 
                           hover:shadow-[0_0_15px_rgba(100,116,139,0.4)]
                           ring-1 ring-slate-700/50 dark:bg-slate-200 dark:text-slate-800 
                           dark:hover:bg-white dark:ring-slate-300 transition-all duration-300"
                    title="Escaneo rápido de LMS (Moodle, Canvas, Phidias, Schoolnet)">
                    <span class="material-symbols-outlined text-[13px] pointer-events-none">radar</span>
                    <span class="pointer-events-none">SCAN LMS</span>
                </button>
                <button type="button" 
                    hx-get="{url_deep}" 
                    hx-target="#recon-panel-{pk}" 
                    hx-swap="outerHTML"
                    hx-disabled-elt="this"
                    class="{classes} bg-[#050505] text-slate-500 hover:bg-[#111] hover:text-emerald-400 
                           hover:shadow-[0_0_15px_rgba(16,185,129,0.3)]
                           ring-1 ring-white/5 transition-all duration-300"
                    title="Reconocimiento profundo con Playwright (extrae emails, teléfonos, redes sociales)">
                    <span class="material-symbols-outlined text-[13px] pointer-events-none">memory</span>
                    <span class="pointer-events-none">DEEP RECON</span>
                </button>
            </div>''',
            url_lms=url_lms,
            url_deep=url_deep,
            pk=obj.pk,
            classes=btn_base
        )
    
    # =========================================================
    # 🎨 RETORNO DEL PANEL COMPLETO CON ANIMACIONES
    # =========================================================
    return format_html(
        '''<div id="recon-panel-{pk}" 
                class="whitespace-nowrap min-w-[260px] flex flex-col gap-1 
                       animate-in fade-in zoom-in-95 duration-300 ease-out
                       transition-all hover:scale-[1.02]">
            <div class="flex gap-1 bg-black/20 p-1 rounded-lg backdrop-blur-sm">
                {sniper_btn}{report_btn}{external_btn}
            </div>
            {secondary_btns}
            <div class="text-[8px] text-slate-500 font-mono text-center mt-1 opacity-60">
                ⚡ Sniper completo | 🌌 Cosmic IA | 🔍 Deep recon
            </div>
        </div>''',
        pk=obj.pk,
        sniper_btn=sniper_btn,
        report_btn=report_btn,
        external_btn=external_btn,
        secondary_btns=secondary_btns
    )


# ==========================================
# 3. DASHBOARD CENTRAL (COMMAND CENTER) - VERSIÓN OMEGA
# ==========================================
@admin.register(CommandCenter)
class CommandCenterAdmin(ModelAdmin):
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('', self.admin_site.admin_view(self.dashboard_view), name='sales_commandcenter_changelist'),
        ]
        return custom_urls + urls

    def dashboard_view(self, request: HttpRequest):
        if not (request.user.is_superuser or request.user.has_perm('sales.view_executive_dashboard')):
            messages.error(request, "⛔ Acceso Denegado: Tu rango no permite acceso al Dashboard Ejecutivo.")
            return redirect('admin:index')

        if request.method == "POST":
            action_type = request.POST.get('action_type')
            
            mission_control = {
                'radar': {
                    'task': task_run_osm_radar,
                    'kwargs': {
                        'country': request.POST.get('country', 'Colombia'),
                        'city': request.POST.get('city', '')
                    },
                    'success_msg': "🛰️ Satélite OSM desplegado. Analizando cuadrante en segundo plano."
                },
                'serp': {
                    'task': task_run_serp_resolver,
                    'kwargs': {'limit': int(request.POST.get('limit', 50))},
                    'success_msg': "🔍 Escuadrón SERP resolviendo URLs en los clústers de búsqueda."
                },
                'sniper': {
                    'task': task_run_ghost_sniper_fleet, 
                    'kwargs': {'limit': int(request.POST.get('limit', 500)), 'city': request.POST.get('city', '')},
                    'success_msg': f"🕵️‍♂️ Flota Ghost Sniper activada. Escaneando {request.POST.get('limit', 500)} objetivos."
                },
                'cosmic': {
                    'task': task_run_ghost_sniper_fleet,
                    'kwargs': {'limit': int(request.POST.get('limit', 100)), 'city': request.POST.get('city', '')},
                    'success_msg': f"🌌 Generación de reportes cósmicos iniciada para {request.POST.get('limit', 100)} objetivos."
                }
            }

            mission = mission_control.get(action_type)
            if mission:
                try:
                    mission['task'].delay(**mission['kwargs'])
                    self.message_user(request, mission['success_msg'], level='SUCCESS')
                    cache.delete('b2b_dashboard_metrics') 
                except Exception as e:
                    logger.critical(f"Falla de conexión con Celery: {str(e)}")
                    self.message_user(request, "🚨 ERROR CRÍTICO: Infraestructura Celery inalcanzable.", level='ERROR')
            return HttpResponseRedirect(request.path)

        # Obtener métricas con caché
        metrics = cache.get('b2b_dashboard_metrics')
        if not metrics:
            base_metrics = Institution.objects.aggregate(
                total_leads=Count('id'),
                blind_leads=Count('id', filter=Q(website__isnull=True) | Q(website='')),
                enriched_leads=Count('id', filter=Q(tech_profile__isnull=False)),
                cosmic_reports=Count('id', filter=Q(forensic_profile__ai_comprehensive_report__isnull=False)),
                avg_score=Coalesce(Avg('lead_score', output_field=FloatField()), Value(0.0)),
                private_schools=Count('id', filter=Q(is_private=True))
            )

            lms_distribution = list(Institution.objects.filter(tech_profile__isnull=False)
                .annotate(
                    lms_clean=Case(
                        When(tech_profile__lms_provider__isnull=True, then=Value('Ninguno/In-House')),
                        When(tech_profile__lms_provider='', then=Value('Ninguno/In-House')),
                        default=F('tech_profile__lms_provider')
                    )
                )
                .values('lms_clean')
                .annotate(total=Count('id'))
                .order_by('-total')[:8]
            )
            
            lms_labels = [str(item['lms_clean']).upper() for item in lms_distribution]
            lms_data = [item['total'] for item in lms_distribution]

            pipeline_health = Institution.objects.aggregate(
                hot=Count('id', filter=Q(lead_score__gte=75)),
                warm=Count('id', filter=Q(lead_score__gte=50, lead_score__lt=75)),
                cold=Count('id', filter=Q(lead_score__lt=50))
            )

            metrics = {
                'kpis': base_metrics,
                'lms_labels': json.dumps(lms_labels),
                'lms_data': json.dumps(lms_data),
                'pipeline_data': json.dumps([pipeline_health['hot'], pipeline_health['warm'], pipeline_health['cold']])
            }
            cache.set('b2b_dashboard_metrics', metrics, 300)

        context = dict(self.admin_site.each_context(request))
        context.update({
            'title': '🌌 Sovereign C-Level Dashboard | Cosmic Intelligence',
            'metrics': metrics['kpis'],
            'lms_labels': metrics['lms_labels'],
            'lms_data': metrics['lms_data'],
            'pipeline_data': metrics['pipeline_data']
        })
        return TemplateResponse(request, "admin/sales/commandcenter/dashboard.html", context)


# ==========================================
# 4. SNIPER CONSOLE - VERSIÓN OMEGA
# ==========================================
@admin.register(SniperConsole)
class SniperConsoleAdmin(ModelAdmin):
    def has_add_permission(self, request): return False
    
    def changelist_view(self, request, extra_context=None):
        context = dict(self.admin_site.each_context(request))
        mission_id = str(uuid.uuid4())
        context.update({
            'title': mark_safe('<span class="flex items-center gap-2">🌌 Ghost Swarm <span class="text-[10px] bg-gradient-to-r from-purple-500/20 to-indigo-500/20 text-purple-400 px-2 py-0.5 rounded border border-purple-500/30 shadow-[0_0_10px_rgba(168,85,247,0.3)]">V99.9 COSMIC OMEGA</span></span>'),
            'mission_id': mission_id,
        })
        return TemplateResponse(request, "admin/sales/sniper_console.html", context)

    def get_urls(self):
        urls = super().get_urls()
        return [
            path('search/', self.admin_site.admin_view(self.search_targets), name='sniper_search'),
            path('engage/', self.admin_site.admin_view(self.launch_sniper), name='sniper_engage'),
            path('telemetry/<str:mission_id>/', self.admin_site.admin_view(self.get_telemetry), name='sniper_telemetry'),
        ] + urls

    def search_targets(self, request):
        query = request.GET.get('search_query', '').strip()
        mission_id = request.GET.get('mission_id', '')

        if len(query) < 3:
            return HttpResponse('<div class="flex items-center justify-center p-12 text-slate-500 font-mono text-xs uppercase tracking-widest"><span class="material-symbols-outlined mr-2">radar</span> Ingresa nombres, dominios o pega una lista...</div>')

        raw_targets = [t.strip() for t in query.replace('\n', ',').split(',') if t.strip()]
        is_swarm = len(raw_targets) > 1

        mode_badge = '<span class="bg-red-500/20 text-red-400 border border-red-500/50 px-2 py-1 rounded text-[10px] uppercase font-black tracking-widest animate-pulse">🌊 SWARM MODE</span>' if is_swarm else '<span class="bg-blue-500/20 text-blue-400 border border-blue-500/50 px-2 py-1 rounded text-[10px] uppercase font-black tracking-widest">🎯 SNIPER MODE</span>'
        
        html_output = f'<div class="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">'
        html_output += f'<div class="flex justify-between items-center border-b border-white/10 pb-4"><h3 class="text-white font-black uppercase text-sm tracking-widest flex items-center gap-2"><span class="material-symbols-outlined">psychology</span> Análisis Forense Vectorial</h3>{mode_badge}</div>'

        known_targets, zero_day_targets = [], []

        for target in raw_targets:
            db_match = Institution.objects.filter(Q(name__icontains=target) | Q(website__icontains=target) | Q(city__icontains=target)).first()
            if db_match:
                known_targets.append(db_match)
            else:
                zero_day_targets.append(target)

        if known_targets:
            html_output += '<div class="space-y-2"><h4 class="text-[10px] font-bold text-emerald-500 uppercase tracking-widest mb-3 flex items-center gap-2"><span class="material-symbols-outlined text-sm">database</span> Registros Existentes (Re-Escanear)</h4>'
            for inst in known_targets:
                tech = inst.tech_profile.lms_provider if hasattr(inst, 'tech_profile') and inst.tech_profile else 'UNKNOWN'
                has_cosmic = hasattr(inst, 'forensic_profile') and inst.forensic_profile and inst.forensic_profile.ai_comprehensive_report
                cosmic_badge = '<span class="ml-2 text-[8px] bg-purple-500/20 text-purple-400 px-1 rounded">🌌</span>' if has_cosmic else ''
                score_color = "text-emerald-400" if inst.lead_score >= 70 else "text-amber-400" if inst.lead_score >= 40 else "text-red-400"
                html_output += f'''
                <div class="bg-[#111] border border-emerald-500/20 p-3 rounded-lg flex justify-between items-center">
                    <div>
                        <p class="text-white text-xs font-bold">{inst.name}{cosmic_badge} <span class="text-slate-500 font-mono text-[9px] ml-2">{inst.website or 'Sin URL'}</span></p>
                        <p class="text-[10px] {score_color} font-mono mt-1 font-bold">🎯 Score: {inst.lead_score} PTS | ⚙️ Tech: {tech}</p>
                    </div>
                    <span class="material-symbols-outlined text-emerald-500/50 text-sm">verified</span>
                </div>
                '''
            html_output += '</div>'

        if zero_day_targets:
            html_output += '<div class="space-y-2 mt-4"><h4 class="text-[10px] font-bold text-purple-400 uppercase tracking-widest mb-3 flex items-center gap-2"><span class="material-symbols-outlined text-sm">travel_explore</span> Zero-Day Targets (Extracción Cósmica)</h4>'
            for z_target in zero_day_targets:
                html_output += f'''
                <div class="bg-purple-900/10 border border-purple-500/30 p-3 rounded-lg flex justify-between items-center">
                    <p class="text-purple-300 text-xs font-mono truncate max-w-[80%]">{z_target}</p>
                    <span class="material-symbols-outlined text-purple-500/80 text-sm animate-spin">radar</span>
                </div>
                '''
            html_output += '</div>'

        target_payload = ",".join([str(t.id) for t in known_targets] + zero_day_targets)
        
        html_output += f'''
        <form hx-post="{reverse('admin:sniper_engage')}" hx-target="#sniper-display" class="mt-8 bg-black border border-slate-800 p-6 rounded-2xl shadow-2xl">
            <input type="hidden" name="mission_id" value="{mission_id}">
            <input type="hidden" name="target_payload" value="{target_payload}">
            
            <h4 class="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-4">⚙️ Parámetros de Infiltración Cósmica</h4>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                <label class="flex items-center gap-3 cursor-pointer group"><input type="checkbox" name="deep_scan" value="1" checked class="w-4 h-4 rounded bg-slate-900 border-slate-700 text-purple-600 focus:ring-purple-600"><span class="text-xs text-slate-400 font-mono group-hover:text-white transition-colors">Deep Crawl (Subdominios y PDFs)</span></label>
                <label class="flex items-center gap-3 cursor-pointer group"><input type="checkbox" name="extract_contacts" value="1" checked class="w-4 h-4 rounded bg-slate-900 border-slate-700 text-emerald-600 focus:ring-emerald-600"><span class="text-xs text-slate-400 font-mono group-hover:text-white transition-colors">IA Extractor JSON Mode</span></label>
                <label class="flex items-center gap-3 cursor-pointer group"><input type="checkbox" name="bypass_waf" value="1" class="w-4 h-4 rounded bg-slate-900 border-slate-700 text-red-600 focus:ring-red-600"><span class="text-xs text-slate-400 font-mono group-hover:text-white transition-colors">Bypass WAF (Cloudflare Evade)</span></label>
                <label class="flex items-center gap-3 cursor-pointer group"><input type="checkbox" name="force_serp" value="1" checked class="w-4 h-4 rounded bg-slate-900 border-slate-700 text-blue-600 focus:ring-blue-600"><span class="text-xs text-slate-400 font-mono group-hover:text-white transition-colors">Auto-Validar URL</span></label>
            </div>
            
            <div class="bg-purple-950/20 border border-purple-500/30 rounded-lg p-3 mb-6">
                <p class="text-[9px] text-purple-300 flex items-center gap-2"><span class="material-symbols-outlined text-[12px]">psychology</span> La generación de reporte cósmico (IB, Cambridge, STEM, Robótica, ICFES) se realizará automáticamente después del escaneo.</p>
            </div>
            
            <button type="submit" class="w-full bg-gradient-to-r from-red-600 via-purple-600 to-indigo-700 hover:from-red-500 hover:via-purple-500 hover:to-indigo-600 text-white p-4 rounded-xl font-black text-sm uppercase tracking-[0.2em] transition-all shadow-[0_0_30px_rgba(168,85,247,0.5)] flex justify-center items-center gap-3 group">
                <span class="material-symbols-outlined group-hover:animate-bounce">rocket_launch</span>
                EJECUTAR ENJAMBRE CÓSMICO ({len(known_targets) + len(zero_day_targets)} OBJETIVOS)
            </button>
        </form>
        </div>
        '''
        return HttpResponse(html_output)

    def launch_sniper(self, request):
        mission_id = request.POST.get('mission_id')
        target_payload = request.POST.get('target_payload', '').split(',')
        
        active_ids = []
        for target in target_payload:
            target = target.strip()
            if not target: continue

            if target.isdigit():
                inst = Institution.objects.get(id=target)
            else:
                is_url = target.startswith(('http', 'www.'))
                if is_url:
                    inst, _ = Institution.objects.get_or_create(website=target.lower(), defaults={'name': 'Validating Domain...', 'mission_id': mission_id, 'processing_status': 'RAW'})
                else:
                    inst, _ = Institution.objects.get_or_create(name=target, defaults={'mission_id': mission_id, 'discovery_source': 'manual', 'processing_status': 'RAW'})
            
            active_ids.append(inst.id)
            cache.set(f"telemetry_{inst.id}", [f"🛰️ [GHOST SWARM] Enlazando objetivo cósmico..."], timeout=1800)
            cache.set(f"scan_in_progress_{inst.id}", True, timeout=1800)
            
            task_run_single_recon.delay(inst.id)

        cache.set(f"swarm_mission_{mission_id}", active_ids, timeout=1800)
        telemetry_url = reverse('admin:sniper_telemetry', args=[mission_id])
        
        return HttpResponse(f'''
            <div id="sniper-display" hx-get="{telemetry_url}" hx-trigger="every 2s" hx-swap="innerHTML">
                <div class="p-12 border border-purple-500/30 bg-gradient-to-br from-[#050000] to-purple-950/20 rounded-2xl flex flex-col items-center shadow-[inset_0_0_80px_rgba(168,85,247,0.15)] relative overflow-hidden">
                    <div class="absolute inset-0 bg-gradient-to-b from-purple-500/10 to-transparent animate-pulse"></div>
                    <span class="material-symbols-outlined text-purple-500 text-6xl mb-6 animate-spin drop-shadow-[0_0_30px_rgba(168,85,247,1)]">radar</span>
                    <p class="font-mono text-white text-lg font-black tracking-[0.4em] uppercase z-10">🌌 COSMIC FLEET DEPLOYED</p>
                    <p class="font-mono text-purple-400 text-xs mt-3 z-10 tracking-widest">{len(active_ids)} DRONES INFILTRANDO REALIDADES</p>
                    <div class="w-full max-w-md bg-slate-900 h-1 mt-8 rounded-full overflow-hidden z-10">
                        <div class="bg-gradient-to-r from-purple-500 to-indigo-500 h-full animate-[progress_2s_ease-in-out_infinite]"></div>
                    </div>
                </div>
            </div>
        ''')

    def get_telemetry(self, request, mission_id):
        active_ids = cache.get(f"swarm_mission_{mission_id}", [])
        if not active_ids: return HttpResponse("<div>Error 404: Enlace satelital perdido.</div>")

        institutions = Institution.objects.filter(id__in=active_ids).select_related('tech_profile', 'forensic_profile')
        
        all_completed = True
        html_output = '<div class="space-y-4 animate-in fade-in duration-300">'
        
        for inst in institutions:
            is_active = cache.get(f"scan_in_progress_{inst.id}")
            if is_active: all_completed = False
            
            has_cosmic = hasattr(inst, 'forensic_profile') and inst.forensic_profile and inst.forensic_profile.ai_comprehensive_report
            cosmic_badge = '🌌' if has_cosmic else '🔄'
            
            status_color = "text-amber-500 border-amber-500/30 bg-amber-500/5" if is_active else "text-emerald-500 border-emerald-500/30 bg-emerald-500/10"
            status_icon = "sync animate-spin" if is_active else "verified_user"
            score = f"{inst.lead_score} PTS" if not is_active else "ANALYZING..."
            tech = inst.tech_profile.lms_provider if hasattr(inst, 'tech_profile') and inst.tech_profile else 'SCANNING...'
            
            logs = cache.get(f"telemetry_{inst.id}", ["Awaiting data..."])
            last_log = logs[-1] if logs else "Processing..."

            html_output += f'''
            <div class="p-4 rounded-xl border {status_color} flex flex-col md:flex-row justify-between items-start md:items-center gap-4 transition-all hover:scale-[1.01]">
                <div class="flex-1 w-full">
                    <h5 class="text-white font-bold text-sm flex items-center gap-2"><span class="material-symbols-outlined {status_color.split()[0]} text-lg">{status_icon}</span> {cosmic_badge} {inst.name}</h5>
                    <div class="mt-2 bg-black/50 p-2 rounded border border-white/5 w-full">
                        <p class="text-[10px] font-mono {status_color.split()[0]} opacity-90 truncate">> {last_log}</p>
                    </div>
                </div>
                <div class="flex gap-6 font-mono text-[10px] uppercase font-bold tracking-widest bg-black/40 p-3 rounded-lg border border-white/5">
                    <div class="flex flex-col items-end"><span class="text-slate-600">LMS Engine</span><span class="text-purple-400">{tech}</span></div>
                    <div class="flex flex-col items-end"><span class="text-slate-600">Cosmic Score</span><span class="{status_color.split()[0]}">{score}</span></div>
                </div>
            </div>
            '''
        html_output += '</div>'

        if all_completed:
            return HttpResponse(f'''
            <div class="mb-6 p-6 border border-emerald-500/50 bg-gradient-to-r from-[#010a05] to-emerald-950/20 rounded-2xl flex flex-col md:flex-row justify-between items-center shadow-[0_0_40px_rgba(16,185,129,0.15)] animate-in zoom-in duration-700">
                <div class="mb-4 md:mb-0 text-center md:text-left">
                    <h3 class="text-emerald-400 font-black text-2xl tracking-[0.2em] uppercase flex items-center gap-3">
                        <span class="material-symbols-outlined text-3xl">task_alt</span> OPERACIÓN CÓSMICA COMPLETADA
                    </h3>
                    <p class="text-emerald-500/70 text-xs font-mono mt-2">Enjambre cuántico regresando a base. Datos encriptados y asegurados en el Vault Cósmico.</p>
                </div>
                <a href="/admin/sales/institution/" class="bg-gradient-to-r from-emerald-500 to-teal-500 text-black px-8 py-4 rounded-xl font-black uppercase tracking-[0.2em] hover:from-emerald-400 hover:to-teal-400 transition-all shadow-[0_0_30px_rgba(16,185,129,0.5)]">
                    ABRIR VAULT CÓSMICO
                </a>
            </div>
            {html_output}
            ''')
        else:
            return HttpResponse(html_output)


# ==========================================
# 5. GEO RADAR WORKSPACE - VERSIÓN OMEGA
# ==========================================
@admin.register(GeoRadarWorkspace)
class GeoRadarWorkspaceAdmin(ModelAdmin):
    def has_add_permission(self, request): return False
    
    def changelist_view(self, request, extra_context=None):
        context = dict(self.admin_site.each_context(request))
        context.update({'title': '🛰️ Geospatial Radar Command | Cosmic Edition', 'mission_id': str(uuid.uuid4())})
        return TemplateResponse(request, "admin/sales/geo_radar.html", context)

    def get_urls(self):
        urls = super().get_urls()
        return [
            path('deploy/', self.admin_site.admin_view(self.deploy_radar), name='radar_deploy'),
            path('results/<str:mission_id>/', self.admin_site.admin_view(self.get_radar_results), name='radar_results'),
        ] + urls

    '''def deploy_radar(self, request):
        country = request.POST.get('country')
        city = request.POST.get('city')
        mission_id = request.POST.get('mission_id')
        task_run_osm_radar.delay(country, city, mission_id)
        return HttpResponse('<div class="p-4 bg-gradient-to-r from-purple-950/30 to-indigo-950/30 border border-purple-500/30 rounded-xl animate-pulse text-purple-400 text-xs font-bold uppercase tracking-widest flex items-center gap-3"><span class="material-symbols-outlined animate-spin">sync</span> 🌌 Satélite Cósmico Desplegado. Barrido multidimensional en progreso...</div>')'''# En la clase GeoRadarWorkspaceAdmin, modifica el método deploy_radar:

    def deploy_radar(self, request):
        country = request.POST.get('country', '').strip()
        city = request.POST.get('city', '').strip()
        mission_id = request.POST.get('mission_id', '')
        limit = int(request.POST.get('limit', 500))
        extreme_mode = request.POST.get('extreme_mode') == 'true'
        
        # Validación básica
        if not country or not city:
            return HttpResponse('<div class="p-4 bg-red-950/50 border border-red-500/50 rounded-xl text-red-400 text-xs font-bold uppercase tracking-widest">❌ País y ciudad son requeridos</div>')
        
        # Importar el orquestador
        from sales.engine.osm_engine import execute_radar_mission
        
        # Ejecutar la misión
        result = execute_radar_mission(
            country=country,
            city=city,
            limit=limit,
            mission_id=mission_id,
            extreme_mode=extreme_mode
        )
        
        if result.get('error'):
            return HttpResponse(f'<div class="p-4 bg-red-950/50 border border-red-500/50 rounded-xl text-red-400 text-xs font-bold uppercase tracking-widest">❌ {result["error"]}</div>')
        
        # Construir mensaje de respuesta con animación
        if result.get('extreme_mode') and result.get('sniper_triggered'):
            message = f"""
            <div class="p-4 bg-gradient-to-r from-amber-950/50 to-red-950/50 border border-amber-500/50 rounded-xl animate-pulse">
                <div class="flex items-center gap-3">
                    <span class="material-symbols-outlined text-amber-500 animate-spin">rocket_launch</span>
                    <div>
                        <div class="text-amber-400 text-xs font-bold uppercase tracking-widest">🔥 MODO EXTREMO ACTIVADO</div>
                        <div class="text-amber-300/80 text-[10px] font-mono mt-1">🎯 {result['created']} instituciones extraídas | 🕵️‍♂️ Ghost Sniper desplegado en segundo plano</div>
                    </div>
                </div>
            </div>
            """
        elif result.get('created') > 0:
            message = f"""
            <div class="p-4 bg-gradient-to-r from-emerald-950/30 to-teal-950/30 border border-emerald-500/30 rounded-xl">
                <div class="flex items-center gap-3">
                    <span class="material-symbols-outlined text-emerald-500">satellite_alt</span>
                    <div>
                        <div class="text-emerald-400 text-xs font-bold uppercase tracking-widest">✅ MISIÓN COMPLETADA</div>
                        <div class="text-emerald-300/80 text-[10px] font-mono mt-1">📊 {result['created']} instituciones insertadas | ⏱️ {result['elapsed_ms']:.0f}ms</div>
                    </div>
                </div>
            </div>
            """
        else:
            message = f"""
            <div class="p-4 bg-slate-800/50 border border-slate-600/50 rounded-xl">
                <div class="flex items-center gap-3">
                    <span class="material-symbols-outlined text-slate-400">info</span>
                    <div>
                        <div class="text-slate-400 text-xs font-bold uppercase tracking-widest">ℹ️ SIN NUEVOS REGISTROS</div>
                        <div class="text-slate-500 text-[10px] font-mono mt-1">No se encontraron nuevas instituciones en {city}, {country}</div>
                    </div>
                </div>
            </div>
            """
        
        return HttpResponse(message)
            
   
    def get_radar_results(self, request, mission_id):
        from django.urls import reverse
        from django.http import HttpResponse
        
        results = Institution.objects.filter(mission_id=mission_id).select_related(
            'tech_profile', 'forensic_profile'
        ).order_by('-created_at')
        
        count = results.count()
        
        html_counter = f'''
        <div id="result-counter" hx-swap-oob="true" 
             class="bg-gradient-to-r from-purple-950/50 to-indigo-950/50 px-4 py-2 rounded-lg border border-purple-700/50 font-mono text-[11px] font-black text-purple-400 tracking-[0.2em] shadow-[inset_0_0_10px_rgba(168,85,247,0.2)]">
            🌌 {count} TARGETS ASEGURADOS
        </div>
        '''
        
        table_rows = []
        for i in results:
            if i.website:
                clean_url = i.website.replace("https://","").replace("http://","").replace("www.","").split("/")[0]
                url_display = f'<a href="{i.website}" target="_blank" class="url-link">{clean_url}</a>'
            else:
                url_display = '<span class="text-slate-600 text-[10px] font-mono italic flex items-center gap-1"><span class="material-symbols-outlined text-[12px] animate-spin">radar</span> Buscando...</span>'
            
            lms_badge = '<span class="badge-lms-none animate-pulse">⏳ INFILTRANDO...</span>'
            if hasattr(i, 'tech_profile') and i.tech_profile:
                if i.tech_profile.lms_provider:
                    lms = i.tech_profile.lms_provider.upper()
                    
                    if 'MOODLE' in lms: 
                        lms_badge = f'<span class="badge-lms-custom" style="background:#f59e0b20; color:#fbbf24; border-color:#f59e0b40;">🟠 {lms}</span>'
                    elif 'PHIDIAS' in lms: 
                        lms_badge = f'<span class="badge-lms-custom" style="background:#8b5cf620; color:#c084fc; border-color:#8b5cf640;">🟣 {lms}</span>'
                    elif 'CANVAS' in lms: 
                        lms_badge = f'<span class="badge-lms-custom" style="background:#ef444420; color:#f87171; border-color:#ef444440;">🔴 {lms}</span>'
                    elif 'SCHOOLNET' in lms: 
                        lms_badge = f'<span class="badge-lms-custom" style="background:#0ea5e920; color:#38bdf8; border-color:#0ea5e940;">🔵 {lms}</span>'
                    elif 'CIBERCOLEGIOS' in lms:
                        lms_badge = f'<span class="badge-lms-custom" style="background:#14b8a620; color:#2dd4bf; border-color:#14b8a640;">🟢 CIBERCOLEGIOS</span>'
                    elif 'SISTEMA SABERES' in lms or 'SABERES' in lms:
                        lms_badge = f'<span class="badge-lms-custom" style="background:#f9731620; color:#fb923c; border-color:#f9731640;">🟧 SABERES</span>'
                    elif 'COLEGIOS COLOMBIA' in lms or 'CIUDAD EDUCATIVA' in lms:
                        lms_badge = f'<span class="badge-lms-custom" style="background:#eab30820; color:#facc15; border-color:#eab30840;">🟨 CIUDAD ED.</span>'
                    elif 'SIGA' in lms:
                        lms_badge = f'<span class="badge-lms-custom" style="background:#6366f120; color:#818cf8; border-color:#6366f140;">🟦 SIGA</span>'
                    elif 'BLACKBOARD' in lms:
                        lms_badge = f'<span class="badge-lms-custom" style="background:#3f3f4620; color:#a1a1aa; border-color:#3f3f4640;">⚫ BLACKBOARD</span>'
                    elif 'GOOGLE CLASSROOM' in lms:
                        lms_badge = f'<span class="badge-lms-custom" style="background:#10b98120; color:#34d399; border-color:#10b98140;">📗 G-CLASSROOM</span>'
                    else: 
                        lms_badge = f'<span class="badge-lms-custom" style="background:#64748b20; color:#cbd5e1; border-color:#64748b40;">🔘 {lms}</span>'
                
                elif getattr(i.tech_profile, 'has_lms', None) == False:
                    lms_badge = '<span class="badge-lms-none">❌ SIN LMS</span>'

            lang_badges = []
            if hasattr(i, 'forensic_profile') and i.forensic_profile:
                if getattr(i.forensic_profile, 'is_trilingual', False): 
                    lang_badges.append('<span class="badge-lang" style="background:#f59e0b20; color:#fbbf24; border-color:#f59e0b40;">🌟 TRILINGÜE</span>')
                elif getattr(i.forensic_profile, 'is_bilingual', False): 
                    lang_badges.append('<span class="badge-lang" style="background:#10b98120; color:#34d399; border-color:#10b98140;">⭐ BILINGÜE</span>')
                
                if getattr(i.forensic_profile, 'has_ib_cert', False): 
                    lang_badges.append('<span class="badge-lang" style="background:#db277720; color:#f472b6; border-color:#db277740;">🏆 IB BACHILLERATO</span>')
                if getattr(i.forensic_profile, 'has_cambridge_cert', False): 
                    lang_badges.append('<span class="badge-lang" style="background:#1e3a8a40; color:#60a5fa; border-color:#1e3a8a80;">🇬🇧 CAMBRIDGE</span>')
                if getattr(i.forensic_profile, 'has_efqm_cert', False):
                    lang_badges.append('<span class="badge-lang" style="background:#6366f120; color:#818cf8; border-color:#6366f140;">💎 EFQM</span>')

            lang_html = '<div class="flex flex-wrap gap-1.5 max-w-[200px]">' + " ".join(lang_badges) + '</div>' if lang_badges else '<span class="text-slate-600 text-[10px] font-mono italic">Escaneando HTML...</span>'

            contact_html = ""
            if getattr(i, 'email', None):
                contact_html += f'<div class="text-emerald-400 text-[10px] font-mono truncate max-w-[140px]" title="{i.email}">📧 {i.email}</div>'
            if getattr(i, 'phone', None):
                contact_html += f'<div class="text-emerald-500 text-[10px] font-mono mt-0.5">💬 {i.phone}</div>'
            
            if not contact_html:
                contact_html = '<span class="text-slate-600 text-[10px] font-mono italic">Buscando endpoints...</span>'

            profile_url = reverse("admin:sales_globalpipeline_change", args=[i.id])
            sniper_url = reverse("admin:sales_globalpipeline_auto_sniper", args=[i.id]) 
            
            row = f'''
            <tr>
                <td class="col-name">
                    <div class="flex flex-col">
                        <span class="truncate max-w-[220px]" title="{i.name}">{i.name}</span>
                        <span class="text-[9px] text-slate-500 font-mono mt-0.5 uppercase tracking-widest">{i.city}</span>
                    </div>
                </td>
                <td>{url_display}</td>
                <td>{lms_badge}</td>
                <td>{lang_html}</td>
                <td>{contact_html}</td>
                <td class="text-right">
                    <div class="flex items-center justify-end gap-2">
                        <a href="{profile_url}" target="_blank" class="bg-[#111] text-slate-400 hover:text-white px-3 py-1.5 rounded text-[9px] font-black uppercase tracking-widest transition-all border border-slate-700 hover:border-slate-500">
                            Vault
                        </a>
                        <button hx-get="{sniper_url}" hx-swap="none" onclick="this.innerHTML='<span class=\\'material-symbols-outlined text-[12px] animate-spin\\'>sync</span> EJECUTANDO'; this.classList.add('opacity-50')" class="btn-individual-scan">
                            <span class="material-symbols-outlined text-[12px]">my_location</span> Sniper
                        </button>
                    </div>
                </td>
            </tr>
            '''
            table_rows.append(row)
            
        rows_html = "".join(table_rows)
        
        table_html = f'''
        <table class="w-full text-left">
            <thead>
                <tr>
                    <th>Target (Institución)</th>
                    <th>Vector (URL)</th>
                    <th>Tecnología (LMS)</th>
                    <th>Nivel Académico (IB/Idiomas)</th>
                    <th>Contactos</th>
                    <th class="text-right">Acción Manual</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
        '''
        return HttpResponse(f'{html_counter}{table_html}')


# ==========================================
# 6. INTERACTION ADMIN - VERSIÓN OMEGA
# ==========================================
class EngagementFilter(admin.SimpleListFilter):
    title = '🔥 Temperatura del Lead'
    parameter_name = 'engagement_temp'

    def lookups(self, request, model_admin):
        return (
            ('critical', '🔥 HOT (Respondido / Agendado)'),
            ('active', '👀 WARM (Leído / Monitoreado)'),
            ('dormant', '🧊 COLD (Enviado / Ignorado)'),
            ('compromised', '💀 DEAD (Rebotado / Fallido)'),
        )

    def queryset(self, request, queryset):
        val = self.value()
        if val == 'critical': return queryset.filter(status__in=['REPLIED', 'MEETING'])
        if val == 'active': return queryset.filter(status='OPENED')
        if val == 'dormant': return queryset.filter(status__in=['NEW', 'SENT'])
        if val == 'compromised': return queryset.filter(status__in=['BOUNCED', 'FAILED'])
        return queryset

@admin.register(Interaction)
class InteractionAdmin(ModelAdmin):
    list_display = (
        'display_hash_id', 
        'target_identity', 
        'display_channel_tag', 
        'display_payload_preview', 
        'display_tactical_status', 
        'timeline_telemetry'
    )
    list_filter = (EngagementFilter, 'status', 'created_at', 'channel')
    search_fields = ('institution__name', 'contact__email', 'subject', 'message_sent', 'thread_id')
    search_help_text = _("Búsqueda Vectorial: Nombre, Email, UUID, Thread ID, o Texto del Payload.")
    
    list_select_related = ('institution', 'contact')
    list_per_page = 50
    show_full_result_count = True

    def get_queryset(self, request):
        return super().get_queryset(request).select_related(
            'institution', 'contact'
        ).only(
            'id', 'status', 'subject', 'message_sent', 'message_received',
            'created_at', 'updated_at', 'channel', 'opened_count', 'replied',
            'institution__name', 'institution__id', 'contact__email', 'contact__name',
            'thread_id', 'ai_sentiment'
        )
    
    def has_add_permission(self, request): return False
    def has_change_permission(self, request, obj=None): return False
    def has_delete_permission(self, request, obj=None): return False

    fieldsets = (
        ('📡 TELEMETRÍA DE LA OPERACIÓN', {
            'classes': ('collapse', 'wide'),
            'fields': (('institution', 'contact'), ('status', 'channel', 'thread_id'), ('created_at', 'updated_at'), ('opened_count', 'replied'), ('ai_sentiment',))
        }),
        ('🕵️‍♂️ HISTORIAL DE COMUNICACIÓN (HILO COMPLETO)', {
            'classes': ('wide',),
            'fields': ('communication_thread',)
        }),
    )
    readonly_fields = ('institution', 'contact', 'status', 'created_at', 'updated_at', 'communication_thread', 'channel', 'thread_id', 'opened_count', 'replied', 'ai_sentiment')
    
    @display(description='HASH ID', ordering='id')
    def display_hash_id(self, obj):
        short_id = str(obj.id).split('-')[0]
        return format_html(
            '<div title="UUID: {}" class="flex flex-col gap-0.5 group cursor-crosshair">'
            '  <div class="flex items-center gap-1">'
            '    <svg class="w-3 h-3 text-slate-400 group-hover:text-blue-500 transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"></path></svg>'
            '    <span class="font-mono text-[11px] font-bold text-slate-700 dark:text-slate-300 group-hover:text-blue-500 transition-colors">{}</span>'
            '  </div>'
            '  <span class="text-[8px] font-mono text-slate-400 dark:text-slate-500 tracking-[0.2em] uppercase pl-4">Thread: {}</span>'
            '</div>', 
            str(obj.id), short_id, obj.thread_id[:8] if obj.thread_id else 'N/A'
        )

    @display(description='TARGET IDENTITY', ordering='institution__name')
    def target_identity(self, obj):
        inst_name = obj.institution.name if obj.institution else "GHOST_TARGET"
        email = obj.contact.email if obj.contact else "NULL_VECTOR"
        return format_html(
            '<div class="flex flex-col justify-center leading-tight min-w-[200px] max-w-[280px]">'
            '  <strong class="text-[13px] text-slate-900 dark:text-white truncate font-black tracking-tight flex items-center gap-1">'
            '    <span class="material-symbols-outlined text-[14px] text-slate-500">business</span> {}'
            '  </strong>'
            '  <span class="text-[10px] text-blue-600 dark:text-blue-400 font-mono mt-1 truncate bg-blue-50 dark:bg-blue-900/20 px-1.5 py-0.5 rounded w-fit border border-blue-100 dark:border-blue-800/30 flex items-center gap-1">'
            '    <span class="material-symbols-outlined text-[10px]">mail</span> {}'
            '  </span>'
            '</div>', inst_name, email
        )

    @display(description='VECTOR', ordering='subject')
    def display_channel_tag(self, obj):
        subject = obj.subject.upper() if obj.subject else ""
        channel_icons = {
            'EMAIL': ('📧', 'SMTP/TLS', 'bg-blue-500/10 text-blue-600 border-blue-500/20'),
            'WHATSAPP': ('💬', 'WABA_API', 'bg-emerald-500/10 text-emerald-600 border-emerald-500/20'),
            'LINKEDIN': ('🔗', 'LINKEDIN API', 'bg-indigo-500/10 text-indigo-600 border-indigo-500/20'),
            'CALL': ('📞', 'VOICE', 'bg-orange-500/10 text-orange-600 border-orange-500/20'),
        }
        
        icon, protocol, style = channel_icons.get(obj.channel, ('⚡', 'UNKNOWN', 'bg-slate-500/10 text-slate-500 border-slate-500/20'))
        
        return format_html(
            '<div class="flex items-center gap-1.5 w-fit px-2 py-1 rounded border {}">'
            '  <span class="text-[12px]">{}</span>'
            '  <span class="text-[9px] font-black uppercase tracking-[0.15em]">{}</span>'
            '</div>', style, icon, protocol
        )

    @display(description='TACTICAL STATUS', ordering='status')
    def display_tactical_status(self, obj):
        styles = {
            'NEW': ('bg-slate-800 text-slate-300 border-slate-600', '', '📝'),
            'SENT': ('bg-blue-900/50 text-blue-400 border-blue-500/50', '', '📨'),
            'OPENED': ('bg-purple-900/50 text-purple-400 border-purple-500/50 shadow-[0_0_10px_rgba(168,85,247,0.2)]', 'animate-pulse', '👁️'),
            'REPLIED': ('bg-emerald-950/50 text-emerald-400 border-emerald-500/50 shadow-[0_0_15px_rgba(16,185,129,0.5)] font-extrabold', 'animate-pulse', '💬'),
            'MEETING': ('bg-amber-900/50 text-amber-400 border-amber-500/50 shadow-[0_0_10px_rgba(251,191,36,0.2)]', '', '📅'),
            'BOUNCED': ('bg-red-900/50 text-red-400 border-red-500/50', '', '⚠️'),
            'FAILED': ('bg-red-600 text-white border-red-800 shadow-[0_0_10px_rgba(220,38,38,0.5)]', '', '❌')
        }
        style, animation, icon = styles.get(obj.status, styles['NEW'])
        
        ping_html = ""
        if obj.status in ['REPLIED', 'MEETING', 'OPENED']:
            c_ping = "bg-emerald-500" if obj.status == 'REPLIED' else "bg-purple-500"
            ping_html = f'<span class="absolute -top-1 -right-1 flex h-2.5 w-2.5"><span class="animate-ping absolute inline-flex h-full w-full rounded-full {c_ping} opacity-75"></span><span class="relative inline-flex rounded-full h-2.5 w-2.5 {c_ping}"></span></span>'

        return format_html(
            '<div class="relative w-fit">'
            '  <div class="px-3 py-1.5 rounded text-[10px] uppercase tracking-[0.2em] border {} {} flex items-center gap-1">'
            '    <span>{}</span> <span>{}</span>'
            '  </div>{}'
            '</div>', style, animation, icon, obj.status, mark_safe(ping_html)
        )

    @display(description='RESUMEN DEL HILO (PAYLOAD)')
    def display_payload_preview(self, obj):
        subject_clean = obj.subject.replace('[EMAIL] ', '').replace('[WHATSAPP] ', '') if obj.subject else "NULL_SUBJECT"
        body_clean = obj.message_sent[:100] + "..." if obj.message_sent and len(obj.message_sent) > 100 else (obj.message_sent or "NO_DATA")
        
        reply_badge = ""
        if obj.status == 'REPLIED':
            reply_badge = '<span class="inline-block mt-1 bg-emerald-500/20 text-emerald-500 border border-emerald-500/30 text-[9px] px-1 rounded font-bold tracking-widest uppercase">✨ Respuesta Cósmica Capturada</span>'
        
        sentiment_badge = ""
        if obj.ai_sentiment:
            sentiment_color = "text-emerald-400 bg-emerald-500/20" if "POSITIVE" in obj.ai_sentiment.upper() else "text-red-400 bg-red-500/20" if "NEGATIVE" in obj.ai_sentiment.upper() else "text-yellow-400 bg-yellow-500/20"
            sentiment_badge = f'<span class="inline-block mt-1 ml-1 {sentiment_color} text-[8px] px-1 rounded">{obj.ai_sentiment[:15]}</span>'

        return format_html(
            '<div class="min-w-[300px] max-w-[500px] group">'
            '  <div title="{}" class="text-[12px] text-slate-900 dark:text-slate-100 font-bold truncate group-hover:text-blue-500 transition-colors flex items-center gap-1">'
            '    <span class="material-symbols-outlined text-[14px]">description</span> {} {}'
            '  </div>'
            '  <div class="text-[11px] text-slate-500 dark:text-slate-400 truncate mt-1 font-serif italic border-l-2 border-slate-300 dark:border-slate-700 pl-2">{}</div>'
            '  {}'
            '</div>', subject_clean, subject_clean[:50], mark_safe(sentiment_badge), body_clean, mark_safe(reply_badge)
        )

    @display(description='TELEMETRÍA & TTR', ordering='updated_at')
    def timeline_telemetry(self, obj):
        created = obj.created_at.strftime("%d %b, %H:%M:%S") if obj.created_at else "-"
        opened_count = obj.opened_count or 0
        
        ttr_html = ""
        if obj.status in ['OPENED', 'REPLIED', 'MEETING'] and obj.updated_at and obj.created_at:
            delta = obj.updated_at - obj.created_at
            hours, remainder = divmod(delta.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            
            if hours == 0 and minutes < 60:
                ttr_text = f"{int(minutes)}m {int(seconds)}s"
                color = "text-emerald-400 border-emerald-500/30 bg-emerald-500/10" if obj.status == 'REPLIED' else "text-purple-400 border-purple-500/30 bg-purple-500/10"
            else:
                ttr_text = f"{int(hours)}h {int(minutes)}m"
                color = "text-emerald-500 border-emerald-500/20 bg-emerald-500/5" if obj.status == 'REPLIED' else "text-purple-500 border-purple-500/20 bg-purple-500/5"
            
            ttr_html = f'<div class="mt-1 {color} font-black px-1.5 py-0.5 rounded w-fit text-[9px] border uppercase tracking-widest flex items-center gap-1"><svg class="w-2.5 h-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>TTR: {ttr_text}</div>'

        return format_html(
            '<div class="text-[10px] font-mono text-slate-500 dark:text-slate-400 min-w-[130px]">'
            '  <div class="flex items-center justify-between border-b border-slate-100 dark:border-slate-800/50 pb-0.5 mb-0.5">'
            '    <span class="font-bold tracking-widest uppercase text-[8px]">OUT</span>'
            '    <span class="text-slate-700 dark:text-slate-300 flex items-center gap-1">{}</span>'
            '  </div>'
            '  <div class="flex items-center justify-between text-[8px]">'
            '    <span class="text-slate-400">👁️ {}</span>'
            '    <span>{}</span>'
            '  </div>'
            '  {}'
            '</div>', created, opened_count, obj.updated_at.strftime("%H:%M:%S") if obj.updated_at else "-", mark_safe(ttr_html)
        )

    @display(description='HISTORIAL DEL HILO (THREAD HISTORY)')
    def communication_thread(self, obj):
        outbound_content = obj.message_sent.replace('\n', '<br>') if obj.message_sent else "Sin contenido."
        subject_clean = obj.subject.replace('[EMAIL] ', '').replace('[WHATSAPP] ', '') if obj.subject else "Sin Asunto"
        target_email = obj.contact.email if obj.contact else "unknown@target.com"
        target_name = obj.contact.name if obj.contact else "Contacto"
        out_time = obj.created_at.strftime("%d %b %Y, %H:%M:%S UTC") if obj.created_at else "---"
        
        inbound_content = getattr(obj, 'message_received', getattr(obj, 'reply_text', getattr(obj, 'inbound_payload', None)))
        in_time = obj.updated_at.strftime("%d %b %Y, %H:%M:%S UTC") if obj.updated_at else "---"

        outbound_html = f"""
        <div class="bg-gradient-to-r from-[#0f172a] to-[#1e1a3a] border border-slate-700/50 rounded-xl overflow-hidden shadow-lg mb-6 relative">
            <div class="absolute top-0 left-0 w-1 h-full bg-gradient-to-b from-blue-500 to-purple-500"></div>
            <div class="px-5 py-3 bg-[#1e293b]/50 border-b border-slate-700/50 flex justify-between items-center">
                <div class="flex items-center gap-3">
                    <span class="flex items-center justify-center w-6 h-6 rounded-full bg-gradient-to-r from-blue-500 to-purple-500 text-white font-black text-xs">AI</span>
                    <div>
                        <div class="text-[11px] font-black tracking-widest text-blue-400 uppercase">Sovereign Engine <span class="text-slate-500 lowercase font-normal">envió a</span> <span class="text-slate-300">{target_email}</span></div>
                        <div class="text-xs text-slate-300 font-bold mt-0.5 flex items-center gap-2">
                            <span class="material-symbols-outlined text-[12px]">subject</span> {subject_clean}
                        </div>
                    </div>
                </div>
                <div class="text-[10px] font-mono text-slate-500">{out_time}</div>
            </div>
            <div class="p-5 text-[13px] text-slate-300 font-sans leading-relaxed">
                {outbound_content}
            </div>
            <div class="px-5 py-2 bg-black/30 text-right text-[8px] text-slate-500 font-mono border-t border-slate-700/30">
                📨 Enviado vía {obj.get_channel_display()} | Tracking ID: {obj.id[:8]}
            </div>
        </div>
        """

        inbound_html = ""
        if obj.status in ['REPLIED', 'MEETING']:
            display_reply = inbound_content.replace('\n', '<br>') if inbound_content else "<i>[El texto de respuesta fue procesado por el Neural Engine, pero no se almacenó el payload crudo en la base de datos de Interacciones. El sistema determinó que el Lead es positivo.]</i>"
            
            inbound_html = f"""
            <div class="ml-8 md:ml-12 bg-gradient-to-r from-[#022c22] to-[#1a3a2a] border border-emerald-700/50 rounded-xl overflow-hidden shadow-[0_0_20px_rgba(16,185,129,0.1)] relative">
                <div class="absolute top-0 left-0 w-1 h-full bg-emerald-500"></div>
                <div class="px-5 py-3 bg-[#064e3b]/50 border-b border-emerald-700/50 flex justify-between items-center">
                    <div class="flex items-center gap-3">
                        <span class="flex items-center justify-center w-6 h-6 rounded-full bg-emerald-500 text-black font-black text-xs uppercase">{target_name[:2]}</span>
                        <div>
                            <div class="text-[11px] font-black tracking-widest text-emerald-400 uppercase">{target_name} <span class="text-emerald-700 lowercase font-normal">respondió</span></div>
                            <div class="text-[10px] text-emerald-500 font-mono mt-0.5">INTENT DETECTADO: 🟢 {obj.ai_sentiment or 'POSITIVE'} / INTERESTED</div>
                        </div>
                    </div>
                    <div class="text-[10px] font-mono text-emerald-600">{in_time}</div>
                </div>
                <div class="p-5 text-[14px] text-emerald-100 font-sans leading-relaxed">
                    {display_reply}
                </div>
            </div>
            """
        elif obj.status == 'OPENED':
            inbound_html = f"""
            <div class="ml-12 flex items-center gap-3 opacity-60">
                <div class="w-2 h-2 rounded-full bg-purple-500 animate-pulse"></div>
                <div class="text-[11px] font-mono text-purple-400 tracking-widest uppercase flex items-center gap-2">
                    <span class="material-symbols-outlined text-[12px]">visibility</span> Pixel Tracking: El objetivo abrió el archivo ({in_time})
                </div>
            </div>
            """
        else:
            inbound_html = f"""
            <div class="ml-12 flex items-center gap-3 opacity-40">
                <div class="w-2 h-2 rounded-full bg-slate-600"></div>
                <div class="text-[10px] font-mono text-slate-500 tracking-widest uppercase flex items-center gap-2">
                    <span class="material-symbols-outlined text-[12px]">hourglass_empty</span> Esperando transmisión de retorno...
                </div>
            </div>
            """

        return format_html(
            '<div class="bg-gradient-to-br from-[#050505] to-[#0a0a1a] p-6 rounded-2xl border border-white/5 max-w-4xl mx-auto shadow-2xl">'
            '  <div class="mb-4 flex items-center gap-2 border-b border-white/5 pb-3">'
            '    <svg class="w-4 h-4 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 8h2a2 2 0 012 2v6a2 2 0 01-2 2h-2v4l-4-4H9a1.994 1.994 0 01-1.414-.586m0 0L11 14h4a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2v4l.586-.586z"></path></svg>'
            '    <span class="text-[10px] font-black text-slate-500 tracking-[0.3em] uppercase">Log de Comunicaciones Encriptadas</span>'
            '    <span class="ml-auto text-[8px] text-slate-600 font-mono">Thread: {}</span>'
            '  </div>'
            '  {}'
            '  <div class="h-6 border-l-2 border-dashed border-slate-700/50 ml-16 md:ml-20 my-2"></div>'
            '  {}'
            '</div>', 
            obj.thread_id[:16] if obj.thread_id else 'N/A',
            mark_safe(outbound_html), mark_safe(inbound_html)
        )



# Si quieres agregar las animaciones al CSS del admin, puedes añadir esto:
ADMIN_CUSTOM_CSS = """
<style>
@keyframes progress {
    0% {
        width: 10%;
        opacity: 0.5;
    }
    50% {
        width: 70%;
        opacity: 1;
    }
    100% {
        width: 90%;
        opacity: 0.8;
    }
}

@keyframes shimmer {
    0% {
        transform: translateX(-100%);
    }
    100% {
        transform: translateX(200%);
    }
}

.animate-shimmer {
    animation: shimmer 1.5s ease-in-out infinite;
}

/* Estados de escaneo */
.scan-phase {
    transition: all 0.3s ease;
}

.scan-phase.completed {
    color: #10b981;
}

.scan-phase.active {
    color: #f59e0b;
    text-shadow: 0 0 5px rgba(245, 158, 11, 0.5);
}

/* Indicadores de estado */
.status-badge {
    position: relative;
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
    padding: 0.125rem 0.375rem;
    border-radius: 0.25rem;
    font-size: 0.625rem;
    font-weight: 600;
}

.status-badge.active {
    background: rgba(245, 158, 11, 0.2);
    color: #f59e0b;
    border: 1px solid rgba(245, 158, 11, 0.3);
}

.status-badge.completed {
    background: rgba(16, 185, 129, 0.2);
    color: #10b981;
    border: 1px solid rgba(16, 185, 129, 0.3);
}

/* Efecto de brillo para botones */
.glow-effect {
    position: relative;
    overflow: hidden;
}

.glow-effect::after {
    content: '';
    position: absolute;
    top: -50%;
    left: -60%;
    width: 200%;
    height: 200%;
    background: linear-gradient(
        to right,
        rgba(255, 255, 255, 0) 0%,
        rgba(255, 255, 255, 0.3) 50%,
        rgba(255, 255, 255, 0) 100%
    );
    transform: rotate(30deg);
    animation: shimmer 2s infinite;
    pointer-events: none;
}
</style>
"""