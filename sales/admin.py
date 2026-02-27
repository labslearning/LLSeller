import json
import logging
import uuid
from typing import Any, Dict
from django.db.models import FloatField
from .models import Interaction

from django.utils.html import format_html, mark_safe
from django.db.models import F

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
        js = (
            'https://unpkg.com/htmx.org@1.9.10',
            'js/websocket_handler.js',)

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
            path('auto-sniper/<str:inst_id>/', self.admin_site.admin_view(self.run_auto_sniper), name='sales_globalpipeline_auto_sniper'),
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
    def advanced_recon_trigger(self, obj) -> str:
        """
        [GOD TIER - APT LEVEL UI - ULTRA-STABLE]
        Renderiza el panel de control táctico blindado contra bloqueos de Unfold/Django.
        Optimizado para evasión de Form Hijacking, CSRF Blocks y DOM Layering.
        """
        from django.urls import reverse
        from django.utils.html import format_html
        
        # Generación segura de URLs para HTMX (vía GET para máxima compatibilidad)
        url_sniper = reverse('admin:sales_globalpipeline_auto_sniper', args=[obj.pk])
        
        # Clases base Tailwind con microinteracciones de grado militar
        btn_base = (
            "group relative inline-flex w-full items-center justify-start gap-2 px-3 py-1.5 mb-1.5 "
            "text-[10px] font-black uppercase tracking-[0.15em] rounded shadow-sm transition-all "
            "duration-300 overflow-hidden disabled:opacity-50 disabled:cursor-not-allowed disabled:grayscale"
        )

        # ==========================================
        # 1. BOTÓN MAESTRO: OMNI SNIPER (Siempre Activo)
        # ==========================================
        # 🛠️ FIX: type="button" + hx-get + pointer-events-none
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

        # ==========================================
        # 2. BOTONES SECUNDARIOS TÁCTICOS (Condicionales)
        # ==========================================
        secondary_btns = ""
        if obj.website:
            url_lms = reverse('admin:sales_globalpipeline_scan_lms', args=[obj.pk])
            url_deep = reverse('admin:sales_globalpipeline_scan_deep', args=[obj.pk])
            
            secondary_btns = format_html(
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
                '</button>',
                url_lms=url_lms,
                url_deep=url_deep,
                pk=obj.pk,
                classes=btn_base
            )

        # ==========================================
        # 3. ENSAMBLAJE DEL PANEL (Anti-Layout Shift)
        # ==========================================
        return format_html(
            '<div id="recon-panel-{pk}" class="whitespace-nowrap min-w-[140px] flex flex-col '
            'animate-in fade-in zoom-in-95 duration-300 ease-out">'
            '{sniper_btn}'
            '{secondary_btns}'
            '</div>',
            pk=obj.pk,
            sniper_btn=sniper_btn,
            secondary_btns=secondary_btns
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

    def run_auto_sniper(self, request, inst_id):
        # 1. Bloqueamos la interfaz localmente
        cache.set(f"scan_in_progress_{inst_id}", True, timeout=300)
        
        # 2. Importamos y lanzamos la tarea que creamos en el Paso 1
        from .tasks import task_run_omni_sniper
        task_run_omni_sniper.delay(inst_id)
        
        # 3. Retornamos el snippet de HTMX que empezará a hacer "polling" automático
        return HttpResponse(self._get_polling_html(inst_id))

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

        # === 1. MANEJO DE MISIONES (Botones de acción masiva) ===
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
                    cache.delete('b2b_dashboard_metrics') # Forzar recálculo del dashboard
                except Exception as e:
                    logger.critical(f"Falla de conexión con el Message Broker: {str(e)}")
                    self.message_user(request, "🚨 ERROR CRÍTICO: Infraestructura Celery/Redis inalcanzable.", level='ERROR')
            return HttpResponseRedirect(request.path)

        # === 2. CÁLCULO ANALÍTICO DE ALTO RENDIMIENTO (BI) ===
        try:
            metrics = cache.get('b2b_dashboard_metrics')
        except Exception:
            metrics = None

        if not metrics:
            # A. KPIs Generales (Tarjetas Superiores)
            base_metrics = Institution.objects.aggregate(
                total_leads=Count('id'),
                blind_leads=Count('id', filter=Q(website__isnull=True) | Q(website='')),
                enriched_leads=Count('id', filter=Q(tech_profile__isnull=False)),
                avg_score=Coalesce(Avg('lead_score', output_field=FloatField()), Value(0.0)),
                private_schools=Count('id', filter=Q(is_private=True))
            )

            # B. Market Share de LMS (Para gráfico de Dona)
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
                .order_by('-total')[:6] # Agarra el Top 6 del mercado
            )
            
            lms_labels = [str(item['lms_clean']).upper() for item in lms_distribution]
            lms_data = [item['total'] for item in lms_distribution]

            # C. Salud del Pipeline B2B (Para gráfico de Barras)
            pipeline_health = Institution.objects.aggregate(
                hot=Count('id', filter=Q(lead_score__gte=75)),
                warm=Count('id', filter=Q(lead_score__gte=40, lead_score__lt=75)),
                cold=Count('id', filter=Q(lead_score__lt=40))
            )

            # Empaquetamos todo y lo convertimos a JSON para JavaScript
            metrics = {
                'kpis': base_metrics,
                'lms_labels': json.dumps(lms_labels),
                'lms_data': json.dumps(lms_data),
                'pipeline_data': json.dumps([pipeline_health['hot'], pipeline_health['warm'], pipeline_health['cold']])
            }
            try:
                cache.set('b2b_dashboard_metrics', metrics, timeout=60) # Caché de 1 minuto para no saturar DB
            except Exception:
                pass

        # === 3. RENDERIZADO ===
        context = dict(self.admin_site.each_context(request))
        context.update({
            'title': 'Sovereign C-Level Dashboard',
            'metrics': metrics['kpis'],
            'lms_labels': metrics['lms_labels'],
            'lms_data': metrics['lms_data'],
            'pipeline_data': metrics['pipeline_data']
        })
        return TemplateResponse(request, "admin/sales/commandcenter/dashboard.html", context)
@admin.register(SniperConsole)
class SniperConsoleAdmin(ModelAdmin):
    def has_add_permission(self, request): return False
    
    def changelist_view(self, request, extra_context=None):
        context = dict(self.admin_site.each_context(request))
        # Mission ID único para aislar la telemetría en Redis y coordinar el Enjambre
        mission_id = str(uuid.uuid4())
        context.update({
            'title': mark_safe('<span class="flex items-center gap-2">🌌 Ghost Swarm <span class="text-[10px] bg-red-500/20 text-red-400 px-2 py-0.5 rounded border border-red-500/30 shadow-[0_0_10px_rgba(239,68,68,0.3)]">V6.0 GOD-TIER</span></span>'),
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
        """
        [OMNI-SEARCH & SWARM DETECTOR - Nivel God Tier]
        Busca en vivo por nombre, URL o ciudad, y soporta pegado masivo de Excel.
        Clasifica al instante entre "Conocidos" (Vault) y "Nuevos" (Zero-Day).
        """
        query = request.GET.get('search_query', '').strip()
        mission_id = request.GET.get('mission_id', '')

        if len(query) < 3:
            return HttpResponse('<div class="flex items-center justify-center p-12 text-slate-500 font-mono text-xs uppercase tracking-widest"><span class="material-symbols-outlined mr-2">radar</span> Ingresa nombres, dominios o pega una lista separada por comas...</div>')

        # Detección Multiobjetivo (Swarm Mode)
        raw_targets = [t.strip() for t in query.replace('\n', ',').split(',') if t.strip()]
        is_swarm = len(raw_targets) > 1

        mode_badge = '<span class="bg-red-500/20 text-red-400 border border-red-500/50 px-2 py-1 rounded text-[10px] uppercase font-black tracking-widest animate-pulse">Swarm Mode Active</span>' if is_swarm else '<span class="bg-blue-500/20 text-blue-400 border border-blue-500/50 px-2 py-1 rounded text-[10px] uppercase font-black tracking-widest">Single Target Lock</span>'
        
        html_output = f'<div class="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">'
        html_output += f'<div class="flex justify-between items-center border-b border-white/10 pb-4"><h3 class="text-white font-black uppercase text-sm tracking-widest">Análisis Forense Vectorial</h3>{mode_badge}</div>'

        known_targets, zero_day_targets = [], []

        for target in raw_targets:
            # Búsqueda Vectorial Simulada (Cruce múltiple)
            db_match = Institution.objects.filter(Q(name__icontains=target) | Q(website__icontains=target) | Q(city__icontains=target)).first()
            if db_match:
                known_targets.append(db_match)
            else:
                zero_day_targets.append(target)

        # 1. EN EL VAULT (Actualización/Confirmación)
        if known_targets:
            html_output += '<div class="space-y-2"><h4 class="text-[10px] font-bold text-emerald-500 uppercase tracking-widest mb-3 flex items-center gap-2"><span class="material-symbols-outlined text-sm">database</span> Registros Existentes (Re-Escanear)</h4>'
            for inst in known_targets:
                tech = inst.tech_profile.lms_provider if hasattr(inst, 'tech_profile') and inst.tech_profile else 'UNKNOWN'
                score_color = "text-emerald-400" if inst.lead_score >= 70 else "text-amber-400" if inst.lead_score >= 40 else "text-red-400"
                html_output += f'''
                <div class="bg-[#111] border border-emerald-500/20 p-3 rounded-lg flex justify-between items-center">
                    <div>
                        <p class="text-white text-xs font-bold">{inst.name} <span class="text-slate-500 font-mono text-[9px] ml-2">{inst.website or 'Sin URL'}</span></p>
                        <p class="text-[10px] {score_color} font-mono mt-1 font-bold">🎯 Score: {inst.lead_score} PTS | ⚙️ Tech: {tech}</p>
                    </div>
                    <span class="material-symbols-outlined text-emerald-500/50 text-sm">verified</span>
                </div>
                '''
            html_output += '</div>'

        # 2. ZERO-DAY TARGETS (Nuevos Objetivos)
        if zero_day_targets:
            html_output += '<div class="space-y-2 mt-4"><h4 class="text-[10px] font-bold text-purple-400 uppercase tracking-widest mb-3 flex items-center gap-2"><span class="material-symbols-outlined text-sm">travel_explore</span> Zero-Day Targets (Extracción Profunda)</h4>'
            for z_target in zero_day_targets:
                html_output += f'''
                <div class="bg-purple-900/10 border border-purple-500/30 p-3 rounded-lg flex justify-between items-center">
                    <p class="text-purple-300 text-xs font-mono truncate max-w-[80%]">{z_target}</p>
                    <span class="material-symbols-outlined text-purple-500/80 text-sm animate-spin">radar</span>
                </div>
                '''
            html_output += '</div>'

        # 3. SWITCHES TÁCTICOS Y LANZAMIENTO
        target_payload = ",".join([str(t.id) for t in known_targets] + zero_day_targets)
        
        html_output += f'''
        <form hx-post="{reverse('admin:sniper_engage')}" hx-target="#sniper-display" class="mt-8 bg-black border border-slate-800 p-6 rounded-2xl shadow-2xl">
            <input type="hidden" name="mission_id" value="{mission_id}">
            <input type="hidden" name="target_payload" value="{target_payload}">
            
            <h4 class="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-4">Parámetros de Infiltración Forense</h4>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                <label class="flex items-center gap-3 cursor-pointer group"><input type="checkbox" name="deep_scan" value="1" checked class="w-4 h-4 rounded bg-slate-900 border-slate-700 text-purple-600 focus:ring-purple-600"><span class="text-xs text-slate-400 font-mono group-hover:text-white transition-colors">Deep Crawl (Subdominios y PDFs)</span></label>
                <label class="flex items-center gap-3 cursor-pointer group"><input type="checkbox" name="extract_contacts" value="1" checked class="w-4 h-4 rounded bg-slate-900 border-slate-700 text-emerald-600 focus:ring-emerald-600"><span class="text-xs text-slate-400 font-mono group-hover:text-white transition-colors">IA Extractor (Directivos & Emails)</span></label>
                <label class="flex items-center gap-3 cursor-pointer group"><input type="checkbox" name="bypass_waf" value="1" class="w-4 h-4 rounded bg-slate-900 border-slate-700 text-red-600 focus:ring-red-600"><span class="text-xs text-slate-400 font-mono group-hover:text-white transition-colors">Bypass WAF (Cloudflare/Imperva Evade)</span></label>
                <label class="flex items-center gap-3 cursor-pointer group"><input type="checkbox" name="force_serp" value="1" checked class="w-4 h-4 rounded bg-slate-900 border-slate-700 text-blue-600 focus:ring-blue-600"><span class="text-xs text-slate-400 font-mono group-hover:text-white transition-colors">Auto-Validar URL (SERP Auto-Fix)</span></label>
            </div>
            
            <button type="submit" class="w-full bg-gradient-to-r from-red-600 to-purple-700 hover:from-red-500 hover:to-purple-600 text-white p-4 rounded-xl font-black text-sm uppercase tracking-[0.2em] transition-all shadow-[0_0_20px_rgba(220,38,38,0.4)] flex justify-center items-center gap-3 group">
                <span class="material-symbols-outlined group-hover:animate-bounce">rocket_launch</span>
                EJECUTAR ENJAMBRE ({len(known_targets) + len(zero_day_targets)} OBJETIVOS)
            </button>
        </form>
        </div>
        '''
        return HttpResponse(html_output)

    def launch_sniper(self, request):
        """
        [LANZADOR ASÍNCRONO MULTIHILO]
        Crea los registros faltantes y dispara MÚLTIPLES misiones de Celery en paralelo.
        """
        mission_id = request.POST.get('mission_id')
        target_payload = request.POST.get('target_payload', '').split(',')
        
        # Opciones tácticas que puedes pasar a tus tareas de Celery en el futuro
        # deep_scan = request.POST.get('deep_scan') == '1'
        
        active_ids = []
        for target in target_payload:
            target = target.strip()
            if not target: continue

            # Instanciación Determinista
            if target.isdigit():
                inst = Institution.objects.get(id=target)
            else:
                is_url = target.startswith(('http', 'www.'))
                if is_url:
                    inst, _ = Institution.objects.get_or_create(website=target.lower(), defaults={'name': 'Validating Domain...', 'mission_id': mission_id})
                else:
                    inst, _ = Institution.objects.get_or_create(name=target, defaults={'mission_id': mission_id, 'discovery_source': 'manual'})
            
            active_ids.append(inst.id)
            
            # Inicializamos Logs en Caché (Aislados por ID)
            cache.set(f"telemetry_{inst.id}", [f"🛰️ [GHOST SWARM] Enlazando objetivo...", "⚡ Evasión inicial iniciada..."], timeout=1200)
            cache.set(f"scan_in_progress_{inst.id}", True, timeout=1200)
            
            # DISPARO PARALELO: Cada colegio va a un Worker distinto (DAG Deterministic)
            task_run_single_recon.delay(inst.id)

        # Registramos todos los IDs en la misión maestra
        cache.set(f"swarm_mission_{mission_id}", active_ids, timeout=1200)
        telemetry_url = reverse('admin:sniper_telemetry', args=[mission_id])
        
        return HttpResponse(f'''
            <div id="sniper-display" hx-get="{telemetry_url}" hx-trigger="every 2s" hx-swap="innerHTML">
                <div class="p-12 border border-red-500/30 bg-[#050000] rounded-2xl flex flex-col items-center shadow-[inset_0_0_80px_rgba(220,38,38,0.15)] relative overflow-hidden">
                    <div class="absolute inset-0 bg-gradient-to-b from-red-500/10 to-transparent animate-pulse"></div>
                    <span class="material-symbols-outlined text-red-500 text-6xl mb-6 animate-spin drop-shadow-[0_0_20px_rgba(220,38,38,1)]">radar</span>
                    <p class="font-mono text-white text-lg font-black tracking-[0.4em] uppercase z-10">GHOST FLEET DEPLOYED</p>
                    <p class="font-mono text-red-400 text-xs mt-3 z-10 tracking-widest">{len(active_ids)} DRONES INFILTRANDO OBJETIVOS</p>
                    <div class="w-full max-w-md bg-slate-900 h-1 mt-8 rounded-full overflow-hidden z-10">
                        <div class="bg-red-500 h-full animate-[progress_2s_ease-in-out_infinite]"></div>
                    </div>
                </div>
            </div>
        ''')

    def get_telemetry(self, request, mission_id):
        """
        [C2 TELEMETRY HUB]
        Monitoreo simultáneo y en tiempo real de todo el enjambre de Celery.
        """
        active_ids = cache.get(f"swarm_mission_{mission_id}", [])
        if not active_ids: return HttpResponse("<div>Error 404: Enlace satelital perdido.</div>")

        institutions = Institution.objects.filter(id__in=active_ids).select_related('tech_profile')
        
        all_completed = True
        html_output = '<div class="space-y-4 animate-in fade-in duration-300">'
        
        for inst in institutions:
            is_active = cache.get(f"scan_in_progress_{inst.id}")
            if is_active: all_completed = False
            
            status_color = "text-amber-500 border-amber-500/30 bg-amber-500/5" if is_active else "text-emerald-500 border-emerald-500/30 bg-emerald-500/10"
            status_icon = "sync animate-spin" if is_active else "verified_user"
            score = f"{inst.lead_score} PTS" if not is_active else "ANALYZING..."
            tech = inst.tech_profile.lms_provider if hasattr(inst, 'tech_profile') and inst.tech_profile else 'SCANNING...'
            
            logs = cache.get(f"telemetry_{inst.id}", ["Awaiting data..."])
            last_log = logs[-1] if logs else "Processing..."

            html_output += f'''
            <div class="p-4 rounded-xl border {status_color} flex flex-col md:flex-row justify-between items-start md:items-center gap-4 transition-all hover:scale-[1.01]">
                <div class="flex-1 w-full">
                    <h5 class="text-white font-bold text-sm flex items-center gap-2"><span class="material-symbols-outlined {status_color.split()[0]} text-lg">{status_icon}</span> {inst.name}</h5>
                    <div class="mt-2 bg-black/50 p-2 rounded border border-white/5 w-full">
                        <p class="text-[10px] font-mono {status_color.split()[0]} opacity-90 truncate">> {last_log}</p>
                    </div>
                </div>
                <div class="flex gap-6 font-mono text-[10px] uppercase font-bold tracking-widest bg-black/40 p-3 rounded-lg border border-white/5">
                    <div class="flex flex-col items-end"><span class="text-slate-600">LMS Engine</span><span class="text-purple-400">{tech}</span></div>
                    <div class="flex flex-col items-end"><span class="text-slate-600">Threat Score</span><span class="{status_color.split()[0]}">{score}</span></div>
                </div>
            </div>
            '''
        html_output += '</div>'

        if all_completed:
            # Termina el polling HTMX y muestra tarjeta final maestra
            return HttpResponse(f'''
            <div class="mb-6 p-6 border border-emerald-500/50 bg-[#010a05] rounded-2xl flex flex-col md:flex-row justify-between items-center shadow-[0_0_40px_rgba(16,185,129,0.15)] animate-in zoom-in duration-700">
                <div class="mb-4 md:mb-0 text-center md:text-left">
                    <h3 class="text-emerald-400 font-black text-2xl tracking-[0.2em] uppercase flex items-center gap-3">
                        <span class="material-symbols-outlined text-3xl">task_alt</span> OPERACIÓN FINALIZADA
                    </h3>
                    <p class="text-emerald-500/70 text-xs font-mono mt-2">Enjambre regresando a base. Datos encriptados y asegurados en el Vault.</p>
                </div>
                <a href="/admin/sales/institution/" class="bg-emerald-500 text-black px-8 py-4 rounded-xl font-black uppercase tracking-[0.2em] hover:bg-white transition-all shadow-[0_0_20px_rgba(16,185,129,0.4)]">
                    ABRIR VAULT
                </a>
            </div>
            {html_output}
            ''')
        else:
            # Polling HTMX Continúa
            return HttpResponse(html_output)


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

    # ==========================================
# 4. BÓVEDA FORENSE (LOG DE INTERACCIONES)
# ==========================================
# ==========================================
# 4. BÓVEDA FORENSE (LOG DE INTERACCIONES)
# ==========================================

# ==========================================
# 4. BÓVEDA FORENSE (LOG DE INTERACCIONES)
# ==========================================
# ==========================================
# 4. BÓVEDA FORENSE (LOG DE INTERACCIONES)
# ==========================================
# ==========================================
# 4. BÓVEDA FORENSE (LOG DE INTERACCIONES / FULL THREAD)
# ==========================================
from django.db.models import F
from django.utils.html import format_html, mark_safe
from django.utils.translation import gettext_lazy as _
from .models import Interaction

class EngagementFilter(admin.SimpleListFilter):
    """Filtro Heurístico de Temperatura Operativa."""
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
    """
    [GOD TIER ZENITH MAX] Bóveda Forense de Comunicación.
    Renderiza el historial completo (Thread) con motor O(1) y UI de Ciberseguridad.
    """
    list_display = (
        'display_hash_id', 
        'target_identity', 
        'display_channel_tag', 
        'display_payload_preview', 
        'display_tactical_status', 
        'timeline_telemetry'
    )
    list_filter = (EngagementFilter, 'status', 'created_at')
    search_fields = ('institution__name', 'contact__email', 'subject', 'message_sent')
    search_help_text = _("Búsqueda Vectorial: Nombre, Email, UUID, o Texto del Payload.")
    
    # ⚡ HYPER-OPTIMIZATION: Pre-fetch y proyección estricta
    list_select_related = ('institution', 'contact')
    list_per_page = 30
    show_full_result_count = True

    def get_queryset(self, request):
        return super().get_queryset(request).select_related(
            'institution', 'contact'
        ).only(
            'id', 'status', 'subject', 'message_sent', 
            'created_at', 'updated_at', 
            'institution__name', 'contact__email'
        )
    
    # 🔒 ZERO-TRUST SECURITY
    def has_add_permission(self, request): return False
    def has_change_permission(self, request, obj=None): return False
    def has_delete_permission(self, request, obj=None): return False

    # 🖥️ COMMAND CENTER (DETAIL VIEW)
    fieldsets = (
        ('📡 TELEMETRÍA DE LA OPERACIÓN', {
            'classes': ('collapse', 'wide'),
            'fields': (('institution', 'contact'), ('status', 'created_at', 'updated_at'))
        }),
        ('🕵️‍♂️ HISTORIAL DE COMUNICACIÓN (HILO COMPLETO)', {
            'classes': ('wide',),
            'fields': ('communication_thread',)
        }),
    )
    readonly_fields = ('institution', 'contact', 'status', 'created_at', 'updated_at', 'communication_thread')
    
    @display(description='HASH ID', ordering='id')
    def display_hash_id(self, obj):
        short_id = str(obj.id).split('-')[0]
        return format_html(
            '<div title="UUID: {}" class="flex flex-col gap-0.5 group cursor-crosshair">'
            '  <div class="flex items-center gap-1">'
            '    <svg class="w-3 h-3 text-slate-400 group-hover:text-blue-500 transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"></path></svg>'
            '    <span class="font-mono text-[11px] font-bold text-slate-700 dark:text-slate-300 group-hover:text-blue-500 transition-colors">{}</span>'
            '  </div>'
            '  <span class="text-[8px] font-mono text-slate-400 dark:text-slate-500 tracking-[0.2em] uppercase pl-4">SHA-256</span>'
            '</div>', 
            str(obj.id), short_id
        )

    @display(description='TARGET IDENTITY', ordering='institution__name')
    def target_identity(self, obj):
        inst_name = obj.institution.name if obj.institution else "GHOST_TARGET"
        email = obj.contact.email if obj.contact else "NULL_VECTOR"
        return format_html(
            '<div class="flex flex-col justify-center leading-tight min-w-[180px] max-w-[250px]">'
            '  <strong class="text-[13px] text-slate-900 dark:text-white truncate font-black tracking-tight flex items-center gap-1">{}</strong>'
            '  <span class="text-[10px] text-blue-600 dark:text-blue-400 font-mono mt-1 truncate bg-blue-50 dark:bg-blue-900/20 px-1.5 py-0.5 rounded w-fit border border-blue-100 dark:border-blue-800/30">{}</span>'
            '</div>', inst_name, email
        )

    @display(description='VECTOR', ordering='subject')
    def display_channel_tag(self, obj):
        subject = obj.subject.upper() if obj.subject else ""
        if "[EMAIL]" in subject:
            svg = '<svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"></path></svg>'
            protocol, style = 'SMTP/TLS', 'bg-blue-500/10 text-blue-600 border-blue-500/20 shadow-inner'
        elif "[WHATSAPP]" in subject:
            svg = '<svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"></path></svg>'
            protocol, style = 'WABA_API', 'bg-emerald-500/10 text-emerald-600 border-emerald-500/20 shadow-inner'
        else:
            svg = '<svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>'
            protocol, style = 'UNKNOWN', 'bg-slate-500/10 text-slate-500 border-slate-500/20'
        
        return format_html(
            '<div class="flex items-center gap-1.5 w-fit px-2 py-1 rounded border {}">'
            '  {} <span class="text-[9px] font-black uppercase tracking-[0.15em]">{}</span>'
            '</div>', style, mark_safe(svg), protocol
        )

    @display(description='TACTICAL STATUS', ordering='status')
    def display_tactical_status(self, obj):
        styles = {
            'NEW': ('bg-slate-800 text-slate-300 border-slate-600', ''),
            'SENT': ('bg-blue-900/50 text-blue-400 border-blue-500/50', ''),
            'OPENED': ('bg-purple-900/50 text-purple-400 border-purple-500/50 shadow-[0_0_10px_rgba(168,85,247,0.2)]', 'animate-pulse'),
            'REPLIED': ('bg-[#022c22] text-[#34d399] border-[#10b981] shadow-[0_0_15px_rgba(16,185,129,0.5)] font-extrabold', 'animate-pulse'),
            'MEETING': ('bg-amber-900/50 text-amber-400 border-amber-500/50 shadow-[0_0_10px_rgba(251,191,36,0.2)]', ''),
            'BOUNCED': ('bg-red-900/50 text-red-400 border-red-500/50', ''),
            'FAILED': ('bg-red-600 text-white border-red-800 shadow-[0_0_10px_rgba(220,38,38,0.5)]', '')
        }
        style, animation = styles.get(obj.status, styles['NEW'])
        
        ping_html = ""
        if obj.status in ['REPLIED', 'MEETING', 'OPENED']:
            c_ping = "bg-[#10b981]" if obj.status != 'OPENED' else "bg-purple-500"
            ping_html = f'<span class="absolute -top-1 -right-1 flex h-2.5 w-2.5"><span class="animate-ping absolute inline-flex h-full w-full rounded-full {c_ping} opacity-75"></span><span class="relative inline-flex rounded-full h-2.5 w-2.5 {c_ping}"></span></span>'

        return format_html(
            '<div class="relative w-fit">'
            '  <span class="px-3 py-1.5 rounded text-[10px] uppercase tracking-[0.2em] border {} {}">{}</span>{}'
            '</div>', style, animation, obj.status, mark_safe(ping_html)
        )

    @display(description='RESUMEN DEL HILO (PAYLOAD)')
    def display_payload_preview(self, obj):
        subject_clean = obj.subject.replace('[EMAIL] ', '').replace('[WHATSAPP] ', '') if obj.subject else "NULL_SUBJECT"
        body_clean = obj.message_sent[:85] + "..." if obj.message_sent and len(obj.message_sent) > 85 else (obj.message_sent or "NO_DATA")
        
        # Si el cliente respondió, mostramos un tag visual en la tabla
        reply_badge = ""
        if obj.status == 'REPLIED':
            reply_badge = '<span class="inline-block mt-1 bg-emerald-500/20 text-emerald-500 border border-emerald-500/30 text-[9px] px-1 rounded font-bold tracking-widest uppercase">Respuesta Capturada</span>'

        return format_html(
            '<div class="min-w-[280px] max-w-[450px] group">'
            '  <div title="{}" class="text-[12px] text-slate-900 dark:text-slate-100 font-bold truncate group-hover:text-blue-500 transition-colors">{}</div>'
            '  <div class="text-[11px] text-slate-500 dark:text-slate-400 truncate mt-1 font-serif italic border-l-2 border-slate-300 dark:border-slate-700 pl-2">{}</div>'
            '  {}'
            '</div>', subject_clean, subject_clean, body_clean, mark_safe(reply_badge)
        )

    @display(description='TELEMETRÍA & TTR', ordering='updated_at')
    def timeline_telemetry(self, obj):
        created = obj.created_at.strftime("%d %b, %H:%M") if obj.created_at else "-"
        
        ttr_html = ""
        if obj.status in ['OPENED', 'REPLIED', 'MEETING'] and obj.updated_at and obj.created_at:
            delta = obj.updated_at - obj.created_at
            hours, remainder = divmod(delta.total_seconds(), 3600)
            minutes, _ = divmod(remainder, 60)
            
            if hours == 0 and minutes < 60:
                ttr_text = f"{int(minutes)}m"
                color = "text-emerald-400 border-emerald-500/30 bg-emerald-500/10" if obj.status == 'REPLIED' else "text-purple-400 border-purple-500/30 bg-purple-500/10"
            else:
                ttr_text = f"{int(hours)}h {int(minutes)}m"
                color = "text-emerald-500 border-emerald-500/20 bg-emerald-500/5" if obj.status == 'REPLIED' else "text-purple-500 border-purple-500/20 bg-purple-500/5"
            
            ttr_html = f'<div class="mt-1 {color} font-black px-1.5 py-0.5 rounded w-fit text-[9px] border uppercase tracking-widest flex items-center gap-1"><svg class="w-2.5 h-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>TTR: {ttr_text}</div>'

        return format_html(
            '<div class="text-[10px] font-mono text-slate-500 dark:text-slate-400 min-w-[120px]">'
            '  <div class="flex items-center justify-between border-b border-slate-100 dark:border-slate-800/50 pb-0.5 mb-0.5">'
            '    <span class="font-bold tracking-widest uppercase text-[8px]">OUT</span> <span class="text-slate-700 dark:text-slate-300">{}</span>'
            '  </div>'
            '  {}'
            '</div>', created, mark_safe(ttr_html)
        )

    @display(description='HISTORIAL DEL HILO (THREAD HISTORY)')
    def communication_thread(self, obj):
        """
        [MAX LEVEL] Renderiza toda la comunicación en formato CRM Dual-Pane.
        Detecta y muestra qué envió la IA y qué respondió el humano.
        """
        outbound_content = obj.message_sent.replace('\n', '<br>') if obj.message_sent else "Sin contenido."
        subject_clean = obj.subject.replace('[EMAIL] ', '').replace('[WHATSAPP] ', '') if obj.subject else "Sin Asunto"
        target_email = obj.contact.email if obj.contact else "unknown@target.com"
        target_name = obj.contact.name if obj.contact else "Contacto"
        out_time = obj.created_at.strftime("%d %b %Y, %H:%M:%S UTC") if obj.created_at else "---"
        
        # Extracción dinámica del mensaje de respuesta (INBOUND)
        # Busca posibles campos en tu modelo de base de datos donde se guarde la respuesta
        inbound_content = getattr(obj, 'message_received', getattr(obj, 'reply_text', getattr(obj, 'inbound_payload', None)))
        in_time = obj.updated_at.strftime("%d %b %Y, %H:%M:%S UTC") if obj.updated_at else "---"

        # BLOQUE 1: MENSAJE DE SALIDA (OUTBOUND - SOVEREIGN AI)
        outbound_html = f"""
        <div class="bg-[#0f172a] border border-slate-700/50 rounded-xl overflow-hidden shadow-lg mb-6 relative">
            <div class="absolute top-0 left-0 w-1 h-full bg-blue-500"></div>
            <div class="px-5 py-3 bg-[#1e293b]/50 border-b border-slate-700/50 flex justify-between items-center">
                <div class="flex items-center gap-3">
                    <span class="flex items-center justify-center w-6 h-6 rounded-full bg-blue-500 text-white font-black text-xs">AI</span>
                    <div>
                        <div class="text-[11px] font-black tracking-widest text-blue-400 uppercase">Sovereign Engine <span class="text-slate-500 lowercase font-normal">envió a</span> <span class="text-slate-300">{target_email}</span></div>
                        <div class="text-xs text-slate-300 font-bold mt-0.5">Asunto: {subject_clean}</div>
                    </div>
                </div>
                <div class="text-[10px] font-mono text-slate-500">{out_time}</div>
            </div>
            <div class="p-5 text-[13px] text-slate-300 font-sans leading-relaxed">
                {outbound_content}
            </div>
        </div>
        """

        # BLOQUE 2: RESPUESTA DEL CLIENTE (INBOUND - HUMANO)
        inbound_html = ""
        if obj.status in ['REPLIED', 'MEETING']:
            # Si no hay un campo dedicado a guardar el texto exacto, mostramos una alerta forense
            display_reply = inbound_content.replace('\n', '<br>') if inbound_content else "<i>[El texto de respuesta fue procesado por el Neural Engine, pero no se almacenó el payload crudo en la base de datos de Interacciones. El sistema determinó que el Lead es positivo.]</i>"
            
            inbound_html = f"""
            <div class="ml-8 md:ml-12 bg-[#022c22] border border-emerald-700/50 rounded-xl overflow-hidden shadow-[0_0_20px_rgba(16,185,129,0.1)] relative">
                <div class="absolute top-0 left-0 w-1 h-full bg-emerald-500"></div>
                <div class="px-5 py-3 bg-[#064e3b]/50 border-b border-emerald-700/50 flex justify-between items-center">
                    <div class="flex items-center gap-3">
                        <span class="flex items-center justify-center w-6 h-6 rounded-full bg-emerald-500 text-black font-black text-xs uppercase">{target_name[:2]}</span>
                        <div>
                            <div class="text-[11px] font-black tracking-widest text-emerald-400 uppercase">{target_name} <span class="text-emerald-700 lowercase font-normal">respondió</span></div>
                            <div class="text-[10px] text-emerald-500 font-mono mt-0.5">INTENT DETECTADO: 🟢 POSITIVE / INTERESTED</div>
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
                <div class="text-[11px] font-mono text-purple-400 tracking-widest uppercase">Pixel Tracking: El objetivo abrio el archivo ({in_time})</div>
            </div>
            """
        else:
            inbound_html = f"""
            <div class="ml-12 flex items-center gap-3 opacity-40">
                <div class="w-2 h-2 rounded-full bg-slate-600"></div>
                <div class="text-[10px] font-mono text-slate-500 tracking-widest uppercase">Esperando transmisión de retorno...</div>
            </div>
            """

        # ENSAMBLAJE DEL COMUNICADOR
        return format_html(
            '<div class="bg-[#050505] p-6 rounded-2xl border border-white/5 max-w-4xl mx-auto shadow-2xl">'
            '  <div class="mb-4 flex items-center gap-2 border-b border-white/5 pb-3">'
            '    <svg class="w-4 h-4 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 8h2a2 2 0 012 2v6a2 2 0 01-2 2h-2v4l-4-4H9a1.994 1.994 0 01-1.414-.586m0 0L11 14h4a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2v4l.586-.586z"></path></svg>'
            '    <span class="text-[10px] font-black text-slate-500 tracking-[0.3em] uppercase">Log de Comunicaciones Encriptadas</span>'
            '  </div>'
            '  {}'
            '  <div class="h-6 border-l-2 border-dashed border-slate-700/50 ml-16 md:ml-20 my-2"></div>'
            '  {}'
            '</div>', 
            mark_safe(outbound_html), mark_safe(inbound_html)
        )