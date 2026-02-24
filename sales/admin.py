import json
import logging
from typing import Any, Dict
from django.db.models import FloatField

from django.contrib import admin, messages
from django.core.cache import cache
from django.core.exceptions import PermissionDenied
from django.db.models import Count, Q, F, Avg, Case, When, Value, IntegerField, Prefetch
from django.http import HttpResponseRedirect, HttpRequest, JsonResponse
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html
from django.conf import settings
from django.db.models.functions import Coalesce

from celery.exceptions import TimeoutError as CeleryTimeoutError
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from unfold.admin import ModelAdmin
from unfold.decorators import action, display

# --- Importaciones locales ---
from .models import Institution, CommandCenter, TechProfile, DeepForensicProfile
from .engine.recon_engine import execute_recon, AIInsightsGenerator
from .engine.serp_resolver import SERPResolverEngine
from .tasks import task_run_osm_radar, task_run_serp_resolver, task_run_ghost_sniper

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

@admin.register(Institution)
class InstitutionAdmin(ModelAdmin):
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
        ).prefetch_related(
            Prefetch('tech_profile__lms_provider', to_attr='cached_lms')
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
            path('resolve-url/<str:inst_id>/', self.admin_site.admin_view(self.run_resolve_url), name='sales_institution_resolve_url'),
            path('scan-lms/<str:inst_id>/', self.admin_site.admin_view(self.run_scan_lms), name='sales_institution_scan_lms'),
            path('scan-deep/<str:inst_id>/', self.admin_site.admin_view(self.run_scan_deep), name='sales_institution_scan_deep'),
            path('ws/status/<str:inst_id>/', self.admin_site.admin_view(self.ws_status), name='ws_status'),
        ]
        return custom_urls + urls

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
        return redirect('admin:sales_institution_changelist')

    def run_scan_lms(self, request, inst_id):
        try:
            inst = Institution.objects.get(pk=inst_id)
            if inst.website:
                messages.info(request, f"📡 Iniciando escaneo ligero de plataforma para {inst.name}...")
                execute_recon(inst.id)
                messages.success(request, f"✅ Perfil Tecnológico (Tier 1) actualizado para {inst.name}.")
                self._send_ws_update(inst_id, "Escaneo LMS completado")
            else:
                messages.error(request, "❌ Se requiere una URL válida antes de escanear.")
        except Exception as e:
            messages.error(request, f"❌ Fallo en escaneo LMS: {str(e)}")
        return redirect('admin:sales_institution_changelist')

    def run_scan_deep(self, request, inst_id):
        try:
            inst = Institution.objects.get(pk=inst_id)
            if inst.website:
                messages.info(request, f"🕵️‍♂️ Ghost Sniper: Iniciando extracción profunda e IA para {inst.name}...")
                execute_recon(inst.id)
                messages.success(request, f"🚀 Inteligencia Forense (Tier 2) completada para {inst.name}.")
                self._send_ws_update(inst_id, "Escaneo profundo completado")
            else:
                messages.error(request, "❌ Se requiere una URL válida antes de escanear.")
        except Exception as e:
            messages.error(request, f"❌ Fallo en escaneo profundo: {str(e)}")
        return redirect('admin:sales_institution_changelist')

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
        url_resolve = reverse('admin:sales_institution_resolve_url', args=[obj.pk])
        url_lms = reverse('admin:sales_institution_scan_lms', args=[obj.pk])
        url_deep = reverse('admin:sales_institution_scan_deep', args=[obj.pk])

        btn_base = "inline-block px-3 py-1 rounded text-[10px] font-bold uppercase tracking-wider text-white"

        if not obj.website:
            return format_html(
                '<div class="whitespace-nowrap min-w-[120px]">'
                '  <a href="{}" class="{} bg-blue-600 hover:bg-blue-700">🌐 Buscar URL</a>'
                '</div>', url_resolve, btn_base
            )

        return format_html(
            '<div class="whitespace-nowrap min-w-[120px] leading-loose">'
            '  <a href="{}" class="{} bg-purple-600 hover:bg-purple-700 mb-1">📡 Scan LMS</a><br>'
            '  <a href="{}" class="{} bg-gray-900 dark:bg-gray-100 dark:text-gray-900">🧠 Deep Recon</a>'
            '</div>', url_lms, btn_base, url_deep, btn_base
        )

    @display(description='Tecnología')
    def display_intelligence_radar(self, obj):
        if not hasattr(obj, 'tech_profile') or not obj.tech_profile:
            return format_html('<div class="whitespace-nowrap min-w-[100px]"><span class="text-xs text-gray-400 italic">Sin escanear</span></div>')

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
            return format_html('<div class="whitespace-nowrap min-w-[100px]"><span class="text-xs text-gray-400 italic">-</span></div>')

        return format_html('<div class="whitespace-nowrap min-w-[100px] leading-tight">{}</div>', format_html("".join(badges)))

    @display(description='Score', ordering='lead_score')
    def display_performance_score(self, obj):
        score = obj.lead_score or 0
        color = "text-emerald-600" if score >= 80 else "text-amber-500" if score >= 50 else "text-red-500"
        
        # ELIMINAMOS LA BARRA DE PROGRESO ANIMADA (EL VERDADERO CAUSANTE DEL GLITCH)
        # Mostramos un indicador de puntos sólido, rápido e inquebrantable.
        return format_html(
            '<div class="whitespace-nowrap min-w-[60px]">'
            '  <strong class="text-sm {}">{} PTS</strong>'
            '</div>', color, score
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
                    execute_recon(inst.id)
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