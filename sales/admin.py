import json
import logging
import uuid
from typing import Any, Dict
from django.db.models import FloatField

from django.contrib import admin, messages
from django.core.cache import cache
from django.core.exceptions import PermissionDenied
from django.db.models import Count, Q, F, Avg, Case, When, Value, IntegerField, Prefetch
from django.http import HttpResponseRedirect, HttpRequest, JsonResponse, HttpResponse
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html
from django.utils.safestring import mark_safe
from django.conf import settings
from django.db.models.functions import Coalesce

from celery.exceptions import TimeoutError as CeleryTimeoutError
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from unfold.admin import ModelAdmin
from unfold.decorators import action, display

# --- Importaciones locales ---
from .models import (
    Institution, CommandCenter, TechProfile, DeepForensicProfile, 
    GlobalPipeline, SniperConsole, GeoRadarWorkspace 
)
from .engine.recon_engine import execute_recon, AIInsightsGenerator
from .engine.serp_resolver import SERPResolverEngine
from .tasks import task_run_osm_radar, task_run_serp_resolver, task_run_ghost_sniper, task_run_single_recon

# ==========================================
# TELEMETRÍA Y LOGGING CENTRALIZADO
# ==========================================
logger = logging.getLogger("SovereignAdminGateway")

# ==========================================
# 1. FILTROS ESTRATÉGICOS (DATA WAREHOUSE)
# ==========================================
class StrategicIntentFilter(admin.SimpleListFilter):
    title = '🎯 Prioridad de Ejecución'
    parameter_name = 'intent_priority'

    def lookups(self, request, model_admin):
        return (
            ('strike', '🎯 Ready to Strike (Score > 75 + Email)'),
            ('hunt', '🏹 Needs Hunting (High Score + No Email)'),
            ('stale', '🧊 Leads Estancados (+15 días sin scan)'),
        )

    def queryset(self, request, queryset):
        if self.value() == 'strike':
            return queryset.filter(lead_score__gte=75).exclude(email__isnull=True).exclude(email='')
        if self.value() == 'hunt':
            return queryset.filter(lead_score__gte=60, email__isnull=True)
        if self.value() == 'stale':
            threshold = timezone.now() - timezone.timedelta(days=15)
            return queryset.filter(last_scored_at__lt=threshold)

class EnterpriseTechFilter(admin.SimpleListFilter):
    title = '🛠️ Arquitectura Digital'
    parameter_name = 'tech_stack_granular'

    def lookups(self, request, model_admin):
        return (
            ('premium', '💎 Premium Stack (Phidias/Schoolnet/Canvas)'),
            ('open_source', '🟢 Open Source (Moodle)'),
            ('blue_ocean', '🌊 Blue Ocean (Sin LMS)'),
        )

    def queryset(self, request, queryset):
        val = self.value()
        if val == 'premium':
            return queryset.filter(tech_profile__lms_provider__in=['phidias', 'schoolnet', 'canvas', 'cibercolegios'])
        if val == 'open_source':
            return queryset.filter(tech_profile__lms_provider__in=['moodle'])
        if val == 'blue_ocean':
            return queryset.filter(Q(tech_profile__has_lms=False) | Q(tech_profile__isnull=True))
        return queryset

# ==========================================
# 2. EL CENTRO DE MANDO B2B (THE GRID)
# ==========================================
try:
    admin.site.unregister(Institution)
except admin.sites.NotRegistered:
    pass

@admin.register(GlobalPipeline)
class GlobalPipelineAdmin(ModelAdmin):
    """
    SDR Intelligence Interface.
    Estructura 100% estática: DOM simplificado para evitar el Resize Observer Loop de Unfold.
    """
    list_display = (
        'display_institution_identity',
        'advanced_recon_trigger',
        'display_intelligence_radar',
        'display_performance_score',
        'display_contact_card',
        'display_ai_readiness',
        'display_sync_metrics'
    )

    list_filter = (
        StrategicIntentFilter,
        EnterpriseTechFilter,
        'country',
        'state_region',
        'city',
        'institution_type',
        'discovery_source',
        'is_private'
    )

    search_fields = ('name', 'website', 'email', 'city', 'country')
    list_select_related = ('tech_profile', 'forensic_profile')
    readonly_fields = (
        'id', 'last_scored_at', 'display_performance_score',
        'ai_executive_panel', 'ai_tactical_panel', 'ai_copywriting_panel'
    )

    list_per_page = 40
    list_filter_submit = True
    show_full_result_count = True

    class Media:
        js = ('js/websocket_handler.js',)

    def get_queryset(self, request):
        return super().get_queryset(request).select_related(
            'tech_profile', 'forensic_profile'
        ).annotate(
            priority_rank=Case(
                When(website__isnull=False, last_scored_at__isnull=False, then=Value(3)),
                When(website__isnull=False, last_scored_at__isnull=True, then=Value(2)),
                default=Value(1),
                output_field=IntegerField(),
            )
        ).order_by('-priority_rank', '-lead_score', '-updated_at')
        
    def changelist_view(self, request, extra_context=None):
        qs = self.get_queryset(request)
        metrics = qs.aggregate(
            total=Count('id'),
            hot=Count('id', filter=Q(lead_score__gte=75)),
            avg_score=Avg('lead_score', output_field=FloatField()),
            enriched=Count('id', filter=Q(tech_profile__isnull=False))
        )

        extra_context = extra_context or {}
        extra_context["kpi"] = [
            {"title": "Total Leads Pipeline", "metric": metrics['total'], "footer": "Leads capturados globalmente"},
            {"title": "🎯 Ready to Strike", "metric": metrics['hot'], "footer": "Score > 75 pts"},
            {"title": "🧠 Data Coverage", "metric": f"{(metrics['enriched']/metrics['total']*100 if metrics['total'] > 0 else 0):.1f}%", "footer": "Prospectos con Tech Stack"},
            {"title": "📈 Calidad Promedio", "metric": f"{metrics['avg_score'] or 0:.1f}", "footer": "Nivel de madurez del pipeline"},
        ]
        return super().changelist_view(request, extra_context=extra_context)

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('resolve-url/<str:inst_id>/', self.admin_site.admin_view(self.run_resolve_url), name='sales_globalpipeline_resolve_url'),
            path('scan-lms/<str:inst_id>/', self.admin_site.admin_view(self.run_scan_lms), name='sales_globalpipeline_scan_lms'),
            path('scan-deep/<str:inst_id>/', self.admin_site.admin_view(self.run_scan_deep), name='sales_globalpipeline_scan_deep'),
            path('check-scan/<str:inst_id>/', self.admin_site.admin_view(self.check_scan_status), name='sales_globalpipeline_check_scan'),
            path('ws/status/<str:inst_id>/', self.admin_site.admin_view(self.ws_status), name='ws_status'),
        ]
        return custom_urls + urls

    def _get_polling_html(self, inst_id):
        poll_url = reverse('admin:sales_globalpipeline_check_scan', args=[inst_id])
        return format_html(
            '<div id="recon-panel-{}" class="whitespace-nowrap min-w-[120px]" '
            'hx-get="{}" hx-trigger="every 3s" hx-swap="outerHTML">'
            '  <span class="inline-block px-3 py-1 rounded text-[10px] font-bold uppercase text-slate-800 bg-amber-300 animate-pulse w-full text-center">'
            '    ⏳ Analizando...'
            '  </span>'
            '</div>', inst_id, poll_url
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
        cache.set(f"scan_in_progress_{inst_id}", True, timeout=300)
        task_run_single_recon.delay(inst_id)
        return HttpResponse(self._get_polling_html(inst_id))

    def run_scan_deep(self, request, inst_id):
        cache.set(f"scan_in_progress_{inst_id}", True, timeout=300)
        task_run_single_recon.delay(inst_id)
        return HttpResponse(self._get_polling_html(inst_id))

    def check_scan_status(self, request, inst_id):
        is_scanning = cache.get(f"scan_in_progress_{inst_id}")
        
        if is_scanning:
            return HttpResponse(self._get_polling_html(inst_id))
            
        inst = Institution.objects.select_related('tech_profile', 'forensic_profile').get(pk=inst_id)
        
        btn_html = self.advanced_recon_trigger(inst)
        
        tech_html = self.display_intelligence_radar(inst)
        tech_oob = tech_html.replace(f'id="tech-radar-{inst.pk}"', f'id="tech-radar-{inst.pk}" hx-swap-oob="true"')
        
        score_html = self.display_performance_score(inst)
        score_oob = score_html.replace(f'id="score-panel-{inst.pk}"', f'id="score-panel-{inst.pk}" hx-swap-oob="true"')

        return HttpResponse(f"{btn_html}\n{tech_oob}\n{score_oob}")

    def ws_status(self, request, inst_id):
        return JsonResponse({"status": "ok", "message": f"Canal abierto para {inst_id}"})

    def _send_ws_update(self, inst_id, message):
        try:
            channel_layer = get_channel_layer()
            async_to_sync(channel_layer.group_send)(
                f"institution_{inst_id}",
                {"type": "recon.update", "message": message}
            )
        except Exception as e:
            logger.warning(f"WebSocket no disponible: {e}")

    # ==========================================
    # CELDAS ESTÁTICAS - ANTI LAYOUT SHIFT
    # Adiós a los flex-col conflictivos. Usamos DOM tradicional (<span>, <br>).
    # ==========================================

    @display(description='Identidad', ordering='name')
    def display_institution_identity(self, obj):
        url = obj.website or ""
        clean_url = url.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0] if url else "URL Pendiente"
        icon = "🏫" if obj.institution_type == "school" else "🏢"
        flag = "🇨🇴" if "colombia" in (obj.country or "").lower() else "🌎"
        city = obj.city or "Global"
        
        url_html = format_html('<a href="{}" target="_blank" class="text-blue-600 dark:text-blue-400 text-xs font-mono">{}</a>', url, clean_url[:25]) if url else format_html('<span class="text-red-500 text-xs font-mono">URL Pendiente</span>')

        return format_html(
            '<div class="whitespace-nowrap min-w-[240px]">'
            '  <span class="text-sm">{}</span> <strong class="text-sm text-gray-900 dark:text-white">{}</strong><br>'
            '  <span class="text-xs text-gray-500 dark:text-gray-400 mt-1 inline-block">{} {} &nbsp;|&nbsp; {}</span>'
            '</div>',
            icon, obj.name[:35], flag, city, url_html
        )

    @display(description="Mando")
    def advanced_recon_trigger(self, obj):
        url_resolve = reverse('admin:sales_globalpipeline_resolve_url', args=[obj.pk])
        url_lms = reverse('admin:sales_globalpipeline_scan_lms', args=[obj.pk])
        url_deep = reverse('admin:sales_globalpipeline_scan_deep', args=[obj.pk])

        btn_base = "inline-block px-3 py-1 rounded text-[10px] font-bold uppercase tracking-wider text-white"

        if not obj.website:
            return format_html(
                '<div id="recon-panel-{}" class="whitespace-nowrap min-w-[120px]">'
                '  <a href="{}" class="{} bg-blue-600 hover:bg-blue-700">🌐 Buscar URL</a>'
                '</div>', obj.pk, url_resolve, btn_base
            )

        return format_html(
            '<div id="recon-panel-{}" class="whitespace-nowrap min-w-[120px] leading-loose">'
            '  <button hx-post="{}" hx-target="#recon-panel-{}" hx-swap="outerHTML" class="{} bg-purple-600 hover:bg-purple-700 mb-1 w-full text-left">📡 Scan LMS</button><br>'
            '  <button hx-post="{}" hx-target="#recon-panel-{}" hx-swap="outerHTML" class="{} bg-gray-900 dark:bg-gray-100 dark:text-gray-900 w-full text-left">🧠 Deep Recon</button>'
            '</div>', obj.pk, url_lms, obj.pk, btn_base, url_deep, obj.pk, btn_base
        )

    @display(description='Tecnología')
    def display_intelligence_radar(self, obj):
        if not hasattr(obj, 'tech_profile') or not obj.tech_profile:
            return format_html('<div id="tech-radar-{}" class="whitespace-nowrap min-w-[100px]"><span class="text-xs text-gray-400 italic">Sin escanear</span></div>', obj.pk)

        tech = obj.tech_profile
        badges = []
        b_class = "inline-block px-2 py-0.5 rounded text-[9px] font-bold uppercase text-white mb-1"

        if tech.has_lms and tech.lms_provider:
            lms = str(tech.lms_provider).upper()
            color = "bg-orange-500" if "SCHOOLNET" in lms else "bg-purple-600" if "PHIDIAS" in lms else "bg-blue-600" if "CIBER" in lms else "bg-gray-700"
            badges.append(format_html('<span class="{} {}">{}</span><br>', b_class, color, lms))
        elif obj.last_scored_at:
            badges.append(format_html('<span class="inline-block px-2 py-0.5 rounded text-[9px] font-bold uppercase bg-gray-200 text-gray-600 dark:bg-gray-800 dark:text-gray-300 mb-1">SIN LMS</span><br>'))

        if tech.is_wordpress:
            badges.append(format_html('<span class="inline-block px-2 py-0.5 rounded text-[9px] font-bold uppercase bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300">WP</span>'))

        if not badges:
            return format_html('<div id="tech-radar-{}" class="whitespace-nowrap min-w-[100px]"><span class="text-xs text-gray-400 italic">-</span></div>', obj.pk)

        return format_html('<div id="tech-radar-{}" class="whitespace-nowrap min-w-[100px] leading-tight">{}</div>', obj.pk, format_html("".join(badges)))

    @display(description='Score', ordering='lead_score')
    def display_performance_score(self, obj):
        score = obj.lead_score or 0
        color = "text-emerald-600" if score >= 80 else "text-amber-500" if score >= 50 else "text-red-500"
        
        # ELIMINAMOS LA BARRA DE PROGRESO ANIMADA (EL VERDADERO CAUSANTE DEL GLITCH)
        # Mostramos un indicador de puntos sólido, rápido e inquebrantable.
        return format_html(
            '<div id="score-panel-{}" class="whitespace-nowrap min-w-[60px]">'
            '  <strong class="text-sm {}">{} PTS</strong>'
            '</div>', obj.pk, color, score
        )

    @display(description='Contacto')
    def display_contact_card(self, obj):
        if obj.email:
            return format_html(
                '<div class="whitespace-nowrap min-w-[140px] leading-tight">'
                '  <span class="text-[10px] font-bold text-emerald-600 dark:text-emerald-400 uppercase">✅ Validado</span><br>'
                '  <a href="mailto:{}" class="text-xs text-gray-600 dark:text-gray-400 hover:text-blue-500 mt-1 inline-block">{}</a>'
                '</div>',
                obj.email, obj.email[:20] + "..." if len(obj.email) > 20 else obj.email
            )
        return format_html('<div class="whitespace-nowrap min-w-[140px]"><span class="text-[10px] font-bold text-red-500 uppercase">❌ Missing</span></div>')

    @display(description='IA')
    def display_ai_readiness(self, obj):
        if hasattr(obj, 'forensic_profile') and obj.forensic_profile and obj.forensic_profile.ai_classification:
            return format_html(
                '<div class="whitespace-nowrap min-w-[80px]">'
                '  <span class="inline-block px-2 py-0.5 rounded-full text-[9px] font-bold bg-purple-100 text-purple-700 dark:bg-purple-900 dark:text-purple-300 border border-purple-200 dark:border-purple-800">✨ READY</span>'
                '</div>'
            )
        return format_html('<div class="whitespace-nowrap min-w-[80px]"><span class="text-xs text-gray-400 italic">No AI</span></div>')

    @display(description='Último Scan')
    def display_sync_metrics(self, obj):
        if not obj.last_scored_at:
            return format_html('<div class="whitespace-nowrap min-w-[60px]"><span class="text-xs text-gray-400 italic">-</span></div>')
        return format_html(
            '<div class="whitespace-nowrap min-w-[60px]"><span class="font-mono text-xs text-gray-500 dark:text-gray-400">{}</span></div>',
            obj.last_scored_at.strftime("%d %b")
        )

    # --- PANELES DE DETALLE (AI INTELLIGENCE) ---
    @display(description="🧠 Análisis Ejecutivo (DeepSeek AI)")
    def ai_executive_panel(self, obj):
        if not hasattr(obj, 'forensic_profile') or not obj.forensic_profile.executive_summary:
            return format_html('<div class="text-sm text-gray-500 italic p-4 bg-gray-50 dark:bg-gray-800/50 border border-dashed rounded-lg">Requiere ejecutar el escaneo "Deep Recon AI".</div>')

        profile = obj.forensic_profile
        classification = profile.ai_classification or "N/A"
        color = "text-emerald-600" if "Alto" in classification else "text-amber-500"

        return format_html(
            '<div class="p-6 bg-white dark:bg-[#161b22] rounded-lg border border-gray-200 dark:border-gray-800 shadow-sm">'
            '  <div class="mb-4">'
            '    <h4 class="text-[10px] uppercase font-bold text-gray-400 mb-1">Clasificación Estratégica</h4>'
            '    <p class="text-lg font-black {}">{}</p>'
            '  </div>'
            '  <div class="h-px bg-gray-100 dark:bg-gray-800 mb-4"></div>'
            '  <h4 class="text-[10px] uppercase font-bold text-gray-400 mb-2">Resumen de Oportunidad</h4>'
            '  <p class="text-[13px] text-gray-700 dark:text-gray-300 leading-relaxed">{}</p>'
            '</div>',
            color, classification, profile.executive_summary
        )

    @display(description="🎯 Sales Playbook (Recomendaciones)")
    def ai_tactical_panel(self, obj):
        if not hasattr(obj, 'forensic_profile') or not obj.forensic_profile.sales_playbook: return "-"
        recs = obj.forensic_profile.sales_playbook
        if not isinstance(recs, list): return "-"

        html_list = "".join([format_html('<li style="margin-bottom: 0.5rem; display: flex; align-items: start; gap: 0.5rem;"><span class="text-blue-500 text-sm">⚡</span><span class="text-[13px] text-gray-700 dark:text-gray-300 leading-snug">{}</span></li>', r) for r in recs])
        return format_html('<div class="p-4 bg-blue-50 dark:bg-blue-900/30 rounded-lg border border-blue-100 dark:border-blue-800"><ul class="m-0 p-0 list-none">{}</ul></div>', format_html(html_list))

    @display(description="📧 AI Copywriting (Borrador de Outreach)")
    def ai_copywriting_panel(self, obj):
        if not hasattr(obj, 'forensic_profile') or not obj.forensic_profile.predictive_copy: return "-"
        return format_html(
            '<div class="relative bg-gray-50 dark:bg-[#0d1117] p-6 rounded-lg border border-gray-200 dark:border-gray-800">'
            '  <div class="absolute top-3 right-3 text-[9px] font-bold text-gray-400 uppercase">Predictive Copy</div>'
            '  <pre class="whitespace-pre-wrap text-[13px] text-gray-800 dark:text-gray-300 font-sans leading-relaxed mt-2">{}</pre>'
            '</div>', obj.forensic_profile.predictive_copy
        )

    # --- ACCIONES MASIVAS ---
    actions = ['trigger_deep_recon']

    @action(description="🎯 Lote: Desplegar Misión Ghost Sniper (Deep Recon)")
    def trigger_deep_recon(self, request, queryset):
        success, skipped, failed = 0, 0, 0
        for inst in queryset:
            if inst.website:
                try:
                    task_run_single_recon.delay(inst.id)
                    success += 1
                except Exception as e:
                    logger.error(f"Fallo en bulk recon {inst.name}: {e}")
                    failed += 1
            else:
                skipped += 1
        self.message_user(request, f"🚀 Misión masiva completada: {success} encolados, {failed} fallos, {skipped} omitidos (Sin URL).")

    fieldsets = (
        ('Identidad Estratégica', {'classes': ('tab',), 'fields': (('name', 'institution_type'), ('country', 'state_region', 'city'), ('address',), ('website', 'email', 'phone'),),}),
        ('🧠 Sales Intelligence (AI Tier 2)', {'classes': ('tab',), 'fields': ('ai_executive_panel', 'ai_tactical_panel', 'ai_copywriting_panel')}),
        ('🔬 Analítica Base', {'classes': ('tab',), 'fields': (('lead_score', 'last_scored_at', 'discovery_source'),),}),
    )

# ==========================================
# 3. DASHBOARD CENTRAL (COMMAND CENTER)
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
                        'country': request.POST.get('country'),
                        'state': request.POST.get('state'),
                        'city': request.POST.get('city')
                    },
                    'success_msg': "🛰️ Satélite OSM desplegado. Analizando cuadrante en segundo plano."
                },
                'serp': {
                    'task': task_run_serp_resolver,
                    'kwargs': {'limit': int(request.POST.get('limit', 50))},
                    'success_msg': "🔍 Escuadrón SERP resolviendo URLs en los clústers de búsqueda."
                },
                'sniper': {
                    'task': task_run_ghost_sniper,
                    'kwargs': {'limit': int(request.POST.get('limit', 30))},
                    'success_msg': "🕵️‍♂️ Ghost Sniper activado. Extracción forense iniciada."
                }
            }

            mission = mission_control.get(action_type)
            if mission:
                try:
                    mission['task'].delay(**mission['kwargs'])
                    self.message_user(request, mission['success_msg'], level='SUCCESS')
                    cache.delete('b2b_dashboard_metrics')
                except Exception as e:
                    logger.critical(f"Falla de conexión con el Message Broker: {str(e)}")
                    self.message_user(request, "🚨 ERROR CRÍTICO: Infraestructura Celery/Redis inalcanzable.", level='ERROR')
            return HttpResponseRedirect(request.path)

        try:
            metrics = cache.get('b2b_dashboard_metrics')
        except Exception:
            metrics = None

        if not metrics:
            try:
                metrics = CommandCenter.objects.get_dashboard_stats()
            except AttributeError:
                metrics = Institution.objects.aggregate(
                    total_leads=Count('id'),
                    blind_leads=Count('id', filter=Q(website__isnull=True) | Q(website='')),
                    ready_to_scan=Count('id', filter=Q(website__isnull=False, last_scored_at__isnull=True) & ~Q(website='')),
                    enriched_leads=Count('id', filter=Q(tech_profile__isnull=False)),
                    avg_score=Coalesce(Avg('lead_score', output_field=FloatField()), Value(0.0))
                )
            try:
                cache.set('b2b_dashboard_metrics', metrics, timeout=30)
            except Exception:
                pass

        context = dict(self.admin_site.each_context(request))
        context.update({
            'title': 'The Sovereign Engine',
            'total_leads': metrics.get('total_leads', 0),
            'blind_leads': metrics.get('blind_leads', 0),
            'ready_to_scan': metrics.get('ready_to_scan', 0),
            'enriched_leads': metrics.get('enriched_leads', 0),
            'avg_score': metrics.get('avg_score', 0)
        })
        return TemplateResponse(request, "admin/sales/commandcenter/dashboard.html", context)


@admin.register(SniperConsole)
class SniperConsoleAdmin(ModelAdmin):
    def has_add_permission(self, request): return False
    
    def changelist_view(self, request, extra_context=None):
        context = dict(self.admin_site.each_context(request))
        # Generamos un Mission ID único para esta sesión de la ventana
        mission_id = str(uuid.uuid4())
        context.update({
            'title': mark_safe('<span class="flex items-center gap-2">🎯 Ghost Sniper <span class="text-xs bg-purple-500/20 text-purple-400 px-2 py-0.5 rounded border border-purple-500/30">V4.0-STABLE</span></span>'),
            'mission_id': mission_id,
        })
        return TemplateResponse(request, "admin/sales/sniper_console.html", context)

    def get_urls(self):
        urls = super().get_urls()
        return [
            path('engage/', self.admin_site.admin_view(self.launch_sniper), name='sniper_engage'),
            path('telemetry/<str:inst_id>/', self.admin_site.admin_view(self.get_telemetry), name='sniper_telemetry'),
        ] + urls

    def launch_sniper(self, request):
        """
        Punto de entrada de alta disponibilidad. 
        Resuelve nombres a URLs e inicia el motor forense.
        """
        raw_target = request.POST.get('target_url', '').strip()
        mission_id = request.POST.get('mission_id')

        # [TIER GOD] Resolución Inteligente de Entidad
        # Si no empieza con http, asumimos que es un nombre y activamos el Resolutor SERP
        is_url = raw_target.startswith(('http://', 'https://', 'www.'))
        
        if is_url:
            inst, created = Institution.objects.get_or_create(
                website=raw_target,
                defaults={'name': 'Objetivo Identificado...', 'mission_id': mission_id}
            )
        else:
            inst, created = Institution.objects.get_or_create(
                name=raw_target,
                defaults={'mission_id': mission_id, 'discovery_source': 'manual'}
            )

        # Inicializamos el Log de Telemetría en Caché
        cache.set(f"telemetry_{inst.id}", ["🛰️ Inicializando secuencia de comandos...", "📡 Localizando servidores..."], timeout=600)
        cache.set(f"scan_in_progress_{inst.id}", True, timeout=600)

        # Disparamos la Misión en Celery
        task_run_single_recon.delay(inst.id)

        # Devolvemos la consola de telemetría inmediata
        telemetry_url = reverse('admin:sniper_telemetry', args=[inst.id])
        return HttpResponse(f'''
            <div id="sniper-display" hx-get="{telemetry_url}" hx-trigger="every 2s" hx-swap="innerHTML">
                <div class="p-8 border border-purple-500/30 bg-purple-500/5 rounded-xl animate-pulse">
                    <p class="font-mono text-purple-400 text-sm">>> ENGAGING TARGET: {raw_target}</p>
                    <p class="font-mono text-slate-500 text-xs mt-2">Awaiting neural link...</p>
                </div>
            </div>
        ''')

    def get_telemetry(self, request, inst_id):
        """Devuelve el estado de la misión y los logs en tiempo real."""
        is_active = cache.get(f"scan_in_progress_{inst_id}")
        logs = cache.get(f"telemetry_{inst_id}", [])
        
        if not is_active:
            # Si terminó, traemos el resultado final (Card Forense)
            inst = Institution.objects.select_related('tech_profile', 'forensic_profile').get(id=inst_id)
            # Aquí podrías usar render_to_string para devolver una tarjeta de perfil elegante
            return HttpResponse(f"""
                <div class="animate-in fade-in zoom-in duration-500">
                    <div class="bg-emerald-500/10 border border-emerald-500/30 p-4 rounded-lg mb-6 flex justify-between items-center">
                        <span class="text-emerald-400 font-bold">✓ MISIÓN COMPLETADA</span>
                        <a href="{reverse('admin:sales_globalpipeline_change', args=[inst.id])}" class="text-xs bg-emerald-500 text-black px-3 py-1 rounded font-bold">VER EN CRM</a>
                    </div>
                    <div class="grid grid-cols-2 gap-6">
                        <div class="p-6 bg-[#111] rounded-xl border border-slate-800">
                             <h4 class="text-slate-500 text-xs uppercase font-bold mb-4">Tech Stack</h4>
                             <p class="text-white font-mono text-xl">{inst.tech_profile.lms_provider if hasattr(inst, 'tech_profile') and inst.tech_profile else 'N/A'}</p>
                        </div>
                        <div class="p-6 bg-[#111] rounded-xl border border-slate-800">
                             <h4 class="text-slate-500 text-xs uppercase font-bold mb-4">Lead Score</h4>
                             <p class="text-emerald-400 font-mono text-3xl">{inst.lead_score} PTS</p>
                        </div>
                    </div>
                </div>
            """)

        # Si sigue activo, mostramos los logs tipo terminal
        log_html = "".join([f'<p class="font-mono text-xs text-slate-400 mb-1">>> {log}</p>' for log in logs])
        return HttpResponse(f'''
            <div class="bg-black p-6 rounded-xl border border-slate-800 shadow-inner h-64 overflow-y-auto">
                <div class="flex justify-between mb-4 border-b border-slate-800 pb-2">
                    <span class="text-[10px] font-bold text-purple-500">ACTIVE TELEMETRY</span>
                    <span class="animate-ping h-2 w-2 rounded-full bg-purple-500"></span>
                </div>
                {log_html}
                <p class="font-mono text-xs text-white animate-pulse">_</p>
            </div>
        ''')


@admin.register(GeoRadarWorkspace)
class GeoRadarWorkspaceAdmin(ModelAdmin):
    def has_add_permission(self, request): return False
    
    def changelist_view(self, request, extra_context=None):
        context = dict(self.admin_site.each_context(request))
        context.update({'title': '🛰️ Geospatial Radar Command', 'mission_id': str(uuid.uuid4())})
        return TemplateResponse(request, "admin/sales/geo_radar.html", context)

    def get_urls(self):
        urls = super().get_urls()
        return [
            path('deploy/', self.admin_site.admin_view(self.deploy_radar), name='radar_deploy'),
            path('results/<str:mission_id>/', self.admin_site.admin_view(self.get_radar_results), name='radar_results'),
        ] + urls

    def deploy_radar(self, request):
        country = request.POST.get('country')
        city = request.POST.get('city')
        mission_id = request.POST.get('mission_id')
        task_run_osm_radar.delay(country, city, mission_id)
        return HttpResponse('<div class="p-4 bg-purple-500/10 border border-purple-500/30 rounded-xl animate-pulse text-purple-400 text-xs font-bold uppercase tracking-widest flex items-center gap-3"><span class="material-symbols-outlined animate-spin">sync</span> Satélite OSM Desplegado. Barrido en progreso...</div>')

    def get_radar_results(self, request, mission_id):
        results = Institution.objects.filter(mission_id=mission_id).order_by('-created_at')
        count = results.count()
        html_counter = f'<div id="result-counter" hx-swap-oob="true" class="bg-black px-4 py-2 rounded-full border border-white/5 font-mono text-[10px] text-purple-400">{count} OBJETIVOS DETECTADOS</div>'
        
        table_rows = "".join([f'<tr class="border-b border-white/5 hover:bg-white/[0.02] transition-colors"><td class="p-4 text-xs font-bold text-white uppercase">{i.name}</td><td class="p-4 text-[10px] text-slate-500 font-mono uppercase">{i.city}</td><td class="p-4 text-right"><a href="{reverse("admin:sales_globalpipeline_change", args=[i.id])}" class="bg-white text-black px-3 py-1 rounded text-[9px] font-black hover:bg-purple-600 hover:text-white transition-all uppercase">Ver Perfil</a></td></tr>' for i in results])
        table_html = f'<table class="w-full text-left"><thead><tr class="bg-[#0d0d0d] text-[10px] uppercase text-slate-500 font-black"><th class="p-4 text-xs">Institución</th><th class="p-4 text-xs">Ciudad</th><th class="p-4 text-right text-xs">Acción</th></tr></thead><tbody>{table_rows}</tbody></table>'
        return HttpResponse(f'{html_counter}{table_html}')