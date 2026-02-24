import json
import logging
from typing import Any, Dict

from django.contrib import admin, messages
from django.core.cache import cache
from django.core.exceptions import PermissionDenied
from django.db.models import Count, Q, F, Avg, Case, When, Value, IntegerField
from django.http import HttpResponseRedirect, HttpRequest
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html
from django.conf import settings

from celery.exceptions import TimeoutError as CeleryTimeoutError
from unfold.admin import ModelAdmin
from unfold.decorators import action, display

from .models import Institution, CommandCenter
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
    """Filtro de Intención: Identifica dónde está el ROI inmediato basándose en madurez."""
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
    """Segmentación algorítmica por ecosistema digital."""
    title = '🛠️ Arquitectura Digital'
    parameter_name = 'tech_stack_granular'

    def lookups(self, request, model_admin):
        return (
            ('premium', '💎 Premium Stack (Phidias/Schoolnet/Cibercolegios)'),
            ('open_source', '🟢 Open Source (Moodle/Canvas)'),
            ('blue_ocean', '🌊 Blue Ocean (Sin LMS)'),
        )

    def queryset(self, request, queryset):
        val = self.value()
        if val == 'premium':
            return queryset.filter(tech_stack__technologies__lms_type__in=['phidias', 'schoolnet', 'cibercolegios'])
        if val == 'open_source':
            return queryset.filter(tech_stack__technologies__lms_type__in=['moodle', 'canvas'])
        if val == 'blue_ocean':
            return queryset.filter(Q(tech_stack__technologies__has_lms=False) | Q(tech_stack__exact={}))
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
    Diseñado para reducir la carga cognitiva del equipo de ventas y maximizar el Speed-to-Lead.
    """
    
    # --- ARQUITECTURA DE COLUMNAS ---
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
        'city', 
        'institution_type', 
        'discovery_source',
        'is_private'
    )
    
    search_fields = ('name', 'website', 'email', 'city', 'country')
    
    readonly_fields = (
        'id', 'last_scored_at', 'pretty_tech_stack', 'display_performance_score',
        'ai_executive_panel', 'ai_tactical_panel', 'ai_copywriting_panel'
    )
    
    list_per_page = 40 
    list_filter_submit = True 
    show_full_result_count = True

    # --- LÓGICA DE ORDENAMIENTO SUPREMO (O(1) SORTING) ---
    def get_queryset(self, request):
        """
        Inyecta un 'Heat Score' a nivel SQL. 
        Evita cuellos de botella de memoria ordenando en el motor de DB.
        """
        qs = super().get_queryset(request)
        return qs.annotate(
            priority_rank=Case(
                When(website__isnull=False, last_scored_at__isnull=False, then=Value(3)),
                When(website__isnull=False, last_scored_at__isnull=True, then=Value(2)),
                default=Value(1),
                output_field=IntegerField(),
            )
        ).order_by('-priority_rank', '-lead_score', '-updated_at')

    # --- MÉTRICAS EJECUTIVAS (KPI CARDS EN VIVO) ---
    def changelist_view(self, request, extra_context=None):
        qs = self.get_queryset(request)
        metrics = qs.aggregate(
            total=Count('id'),
            hot=Count('id', filter=Q(lead_score__gte=75)),
            avg_score=Avg('lead_score'),
            enriched=Count('id', filter=Q(last_scored_at__isnull=False))
        )

        extra_context = extra_context or {}
        extra_context["kpi"] = [
            {"title": "Total Leads Pipeline", "metric": metrics['total'], "footer": "Leads capturados globalmente"},
            {"title": "🎯 Ready to Strike", "metric": metrics['hot'], "footer": "Score > 75 pts"},
            {"title": "🧠 Data Coverage", "metric": f"{(metrics['enriched']/metrics['total']*100 if metrics['total'] > 0 else 0):.1f}%", "footer": "Prospectos enriquecidos (Tech)"},
            {"title": "📈 Calidad Promedio", "metric": f"{metrics['avg_score'] or 0:.1f}", "footer": "Nivel de madurez del pipeline"},
        ]
        return super().changelist_view(request, extra_context=extra_context)

    # --- ENRUTADOR DE ACCIÓN INDIVIDUAL ---
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('deep-recon-action/<str:inst_id>/', self.admin_site.admin_view(self.run_deep_recon_single), name='sales_institution_deep_recon'),
        ]
        return custom_urls + urls

    def run_deep_recon_single(self, request, inst_id):
        """Misión de Rescate Quirúrgica (Operación Síncrona Controlada)."""
        try:
            inst = Institution.objects.get(pk=inst_id)
            
            # FASE 1: Resolución SERP si no hay URL
            if not inst.website:
                resolver = SERPResolverEngine(concurrency_limit=1)
                resolver.resolve_missing_urls(limit=1) 
                inst.refresh_from_db()
                
                if inst.website:
                    messages.success(request, f"🌐 ¡URL encontrada! {inst.website}. Presiona Escanear ahora.")
                else:
                    messages.warning(request, f"⚠️ El motor no pudo encontrar una URL oficial confiable para {inst.name}.")
                    return redirect('admin:sales_institution_changelist')

            # FASE 2: Escaneo Forense Ghost Sniper
            if inst.website:
                messages.info(request, f"🕵️‍♂️ Iniciando escaneo forense en {inst.website}...")
                execute_recon(inst.id)
                messages.success(request, f"🚀 Inteligencia Nivel 2 completada exitosamente para {inst.name}.")
                
        except Institution.DoesNotExist:
            messages.error(request, "❌ Entidad no encontrada. Posible colisión de base de datos.")
        except Exception as e:
            logger.error(f"Fallo en Recon Individual: {str(e)}")
            messages.error(request, f"❌ Fallo crítico en la operación: {str(e)}")
            
        return redirect('admin:sales_institution_changelist')

    # --- RENDERIZADORES UI (COMPONENTES TAILWIND ABSOLUTOS) ---

    @display(description="Deep Recon")
    def advanced_recon_trigger(self, obj):
        """Genera el botón de Misión. Usa URLs absolutas a prueba de fallos."""
        action_url = reverse('admin:sales_institution_deep_recon', args=[obj.pk])
        
        if obj.website and obj.last_scored_at:
            return format_html(
                f'<a href="{action_url}" class="px-3 py-1 bg-emerald-50 text-emerald-600 border border-emerald-200 hover:bg-emerald-100 rounded text-[10px] font-bold uppercase tracking-wider flex items-center justify-center gap-1 transition-all w-full">'
                f'<span class="material-symbols-outlined text-[14px]">refresh</span> Re-escanear</a>'
            )
        if not obj.website:
            return format_html(
                f'<a href="{action_url}" class="px-3 py-1 bg-blue-600 hover:bg-blue-700 text-white rounded text-[10px] font-bold uppercase tracking-wider flex items-center justify-center gap-1 transition-all shadow-sm w-full">'
                f'<span class="material-symbols-outlined text-[14px]">travel_explore</span> Buscar Web</a>'
            )
        return format_html(
            f'<a href="{action_url}" class="px-3 py-1 bg-violet-600 hover:bg-violet-700 text-white rounded text-[10px] font-bold uppercase tracking-wider flex items-center justify-center gap-1 transition-all shadow-sm w-full">'
            f'<span class="material-symbols-outlined text-[14px]">radar</span> Escanear LMS</a>'
        )

    @display(description='Identidad del Prospecto', ordering='name')
    def display_institution_identity(self, obj):
        url = obj.website or ""
        clean_url = url.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0] if url else "URL Pendiente"
        icon = "school" if obj.institution_type == "school" else "account_balance"
        c_lower = obj.country.lower() if obj.country else ""
        flag = "🇪🇸" if "españa" in c_lower else "🇨🇴" if "colombia" in c_lower else "🇲🇽" if "mex" in c_lower else "🌎"

        url_markup = f'<a href="{url}" target="_blank" class="text-[11px] text-blue-600 hover:underline font-mono truncate max-w-[150px]">{clean_url}</a>' if url else f'<span class="text-[11px] text-rose-500 font-mono">URL Pendiente</span>'

        return format_html(
            '<div class="flex items-center gap-3">'
            '  <div class="flex-shrink-0 w-9 h-9 rounded-xl bg-slate-100 dark:bg-slate-800 flex items-center justify-center border border-slate-200 dark:border-slate-700 shadow-sm">'
            '    <span class="material-symbols-outlined text-slate-500 text-[20px]">{}</span>'
            '  </div>'
            '  <div class="flex flex-col min-w-0">'
            '    <span class="text-[12px] font-bold text-slate-900 dark:text-slate-100 truncate" title="{}">{}</span>'
            '    <div class="flex items-center gap-1 mt-0.5">'
            '      <span class="text-[10px] text-slate-500 font-medium">{} {}</span>'
            '      <span class="text-slate-300 dark:text-slate-600">|</span>'
            '      {}'
            '    </div>'
            '  </div>'
            '</div>',
            icon, obj.name, obj.name[:35] + '...' if len(obj.name) > 35 else obj.name, flag, obj.city or "Sin Ciudad", format_html(url_markup)
        )

    @display(description='Plataforma / Radar Tech')
    def display_intelligence_radar(self, obj):
        if not isinstance(obj.tech_stack, dict): 
            return format_html('<span class="text-slate-400 text-[10px] italic">Sin escanear</span>')

        techs = obj.tech_stack.get('technologies', {})
        badges = []
        
        if techs.get('has_lms'):
            lms = str(techs.get('lms_type', '')).upper()
            color_map = {
                'SCHOOLNET': 'bg-orange-500 text-white border-orange-600',
                'PHIDIAS': 'bg-indigo-600 text-white border-indigo-700',
                'MOODLE': 'bg-amber-500 text-white border-amber-600',
                'CIBERCOLEGIOS': 'bg-cyan-600 text-white border-cyan-700',
                'CANVAS': 'bg-rose-600 text-white border-rose-700'
            }
            css_classes = color_map.get(lms, 'bg-slate-700 text-white border-slate-800')
            badges.append(f'<span class="px-2 py-0.5 rounded text-[9px] font-bold uppercase tracking-widest shadow-sm {css_classes}">{lms}</span>')
        elif obj.last_scored_at:
            badges.append('<span class="px-2 py-0.5 rounded text-[9px] font-bold text-slate-500 bg-slate-100 border border-slate-200">SIN LMS</span>')
        
        if 'cert_ib' in obj.tech_stack.get('business_intel', {}).get('premium_flags', []):
            badges.append(f'<span class="px-2 py-0.5 rounded text-[9px] font-bold uppercase tracking-widest shadow-sm bg-amber-100 text-amber-700 border-amber-200">IB World</span>')

        if not badges:
            return format_html('<span class="text-slate-400 text-[10px] italic">-</span>')
            
        return format_html('<div class="flex flex-col gap-1">{}</div>', format_html("".join(badges)))

    @display(description='Health Score', ordering='lead_score')
    def display_performance_score(self, obj):
        score = obj.lead_score or 0
        hue = "emerald" if score >= 80 else "amber" if score >= 50 else "rose" if score > 0 else "slate"
        
        return format_html(
            '<div class="flex items-center gap-2">'
            '  <div class="w-12 bg-slate-200 dark:bg-slate-800 rounded-full h-1.5 overflow-hidden shadow-inner">'
            '    <div class="h-1.5 rounded-full bg-{}-500 transition-all duration-700" style="width: {}%"></div>'
            '  </div>'
            '  <span class="text-[11px] font-black text-{}-600 w-6 text-right">{}</span>'
            '</div>',
            hue, score, hue, score
        )

    @display(description='Contacto (Outreach)')
    def display_contact_card(self, obj):
        if obj.email:
            return format_html(
                '<div class="flex flex-col">'
                '  <span class="text-emerald-600 dark:text-emerald-500 text-[9px] font-bold uppercase tracking-widest flex items-center gap-1"><span class="material-symbols-outlined text-[10px]">verified</span> Validado</span>'
                '  <a href="mailto:{}" class="text-[11px] text-slate-600 dark:text-slate-400 hover:text-blue-500 truncate max-w-[120px]" title="{}">{}</a>'
                '</div>',
                obj.email, obj.email, obj.email
            )
        return format_html('<span class="text-rose-500 text-[10px] font-bold uppercase tracking-widest flex items-center gap-1"><span class="material-symbols-outlined text-[12px]">cancel</span> Missing</span>')

    @display(description='AI Strategy')
    def display_ai_readiness(self, obj):
        if not isinstance(obj.tech_stack, dict): return format_html('<span class="text-slate-300 text-[10px] italic">Pending</span>')
        ai_data = obj.tech_stack.get('business_intel', {}).get('ai_insights', {})
        if ai_data and 'error' not in ai_data:
            return format_html('<span class="bg-violet-100 text-violet-700 border border-violet-200 dark:bg-violet-900/30 dark:text-violet-400 dark:border-violet-800 text-[9px] px-2 py-0.5 rounded-full font-bold flex items-center gap-1 w-max"><span class="material-symbols-outlined text-[12px]">auto_awesome</span> AI Ready</span>')
        return format_html('<span class="text-slate-400 text-[10px] italic">No AI</span>')

    @display(description='Último Escaneo')
    def display_sync_metrics(self, obj):
        if not obj.last_scored_at: return format_html('<span class="text-[10px] text-slate-300">-</span>')
        return format_html('<span class="text-[10px] font-mono text-slate-500 dark:text-slate-400">{}</span>', obj.last_scored_at.strftime("%d %b"))

    # --- PANELES DE VISTA DE DETALLE (AI INTELLIGENCE) ---

    @display(description="🧠 Análisis Ejecutivo (DeepSeek AI)")
    def ai_executive_panel(self, obj):
        if not isinstance(obj.tech_stack, dict): return "Falta Escaneo Forense previo."
        ai = obj.tech_stack.get('business_intel', {}).get('ai_insights', {})
        if not ai or 'error' in ai: 
            return format_html('<div class="text-sm text-slate-500 italic p-4 bg-slate-50 dark:bg-slate-800 border border-dashed rounded-lg">Inicie la síntesis de IA para generar un reporte ejecutivo.</div>')
        
        color = "text-emerald-600" if "Alto" in ai.get('prospect_classification', '') else "text-amber-600"

        return format_html(
            '<div class="p-6 bg-white dark:bg-[#161b22] rounded-lg border border-slate-200 dark:border-slate-800 shadow-sm">'
            '  <div class="mb-4">'
            '    <h4 class="text-[10px] uppercase font-bold text-slate-400 mb-1">Clasificación Estratégica</h4>'
            '    <p class="text-lg font-black {}">{}</p>'
            '  </div>'
            '  <div class="h-px bg-slate-100 dark:bg-slate-800 mb-4"></div>'
            '  <h4 class="text-[10px] uppercase font-bold text-slate-400 mb-2">Resumen de Oportunidad</h4>'
            '  <p class="text-[13px] text-slate-700 dark:text-slate-300 leading-relaxed">{}</p>'
            '</div>',
            color, ai.get('prospect_classification', 'N/A'), ai.get('executive_summary', 'Sin resumen.')
        )

    @display(description="🎯 Sales Playbook (Recomendaciones)")
    def ai_tactical_panel(self, obj):
        if not isinstance(obj.tech_stack, dict): return "-"
        ai = obj.tech_stack.get('business_intel', {}).get('ai_insights', {})
        if not ai or 'error' in ai: return "-"
        
        recs = ai.get('sales_recommendations', [])
        html_list = "".join([f'<li class="mb-2 flex items-start gap-2"><span class="text-blue-500 text-sm">⚡</span><span class="text-[13px] text-slate-700 dark:text-slate-300 leading-snug">{r}</span></li>' for r in recs])
        
        return format_html('<div class="p-4 bg-blue-50/50 dark:bg-blue-900/10 rounded-lg border border-blue-100 dark:border-blue-900/30"><ul class="m-0 p-0 list-none">{}</ul></div>', format_html(html_list))

    @display(description="📧 AI Copywriting (Borrador de Outreach)")
    def ai_copywriting_panel(self, obj):
        if not isinstance(obj.tech_stack, dict): return "-"
        ai = obj.tech_stack.get('business_intel', {}).get('ai_insights', {})
        if not ai or 'error' in ai: return "-"
        
        return format_html(
            '<div class="relative bg-slate-50 dark:bg-[#0d1117] p-6 rounded-lg border border-slate-200 dark:border-slate-800">'
            '  <div class="absolute top-3 right-3 text-[9px] font-bold text-slate-400 uppercase">Predictive Copy</div>'
            '  <pre class="whitespace-pre-wrap text-[13px] text-slate-800 dark:text-slate-300 font-sans leading-relaxed mt-2">{}</pre>'
            '</div>', ai.get('sales_email_draft', '')
        )

    @display(description="Raw Data Forensic (JSON)")
    def pretty_tech_stack(self, obj):
        if not obj.tech_stack: return "No data."
        return format_html('<div class="bg-slate-900 p-4 rounded-lg shadow-inner"><pre class="text-emerald-400 text-[10px] font-mono leading-normal overflow-auto max-h-96">{}</pre></div>', json.dumps(obj.tech_stack, indent=2))

    # --- ACCIONES EMPRESARIALES MASIVAS ---
    actions = ['trigger_deep_recon', 'trigger_ai_synthesis']

    @action(description="🎯 Lote: Desplegar Escaneo Forense OSINT")
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
        self.message_user(request, f"🚀 Misión completada: {success} enriquecidos, {failed} fallos, {skipped} omitidos (Sin URL).")

    @action(description="🤖 Lote: Inyectar Inteligencia DeepSeek (AI)")
    def trigger_ai_synthesis(self, request, queryset):
        api_key = getattr(settings, 'DEEPSEEK_API_KEY', None)
        if not api_key:
            self.message_user(request, "❌ API Key de DeepSeek faltante en Settings.", level='ERROR')
            return

        ai_engine = AIInsightsGenerator(api_key=api_key, model="deepseek-chat")
        if ai_engine.client: 
            ai_engine.client.base_url = "https://api.deepseek.com/v1"

        count = 0
        for inst in queryset:
            if not isinstance(inst.tech_stack, dict) or not inst.tech_stack.get('technologies'): 
                continue 
            
            insights = ai_engine.generate_insights({'name': inst.name, 'website': inst.website, 'city': inst.city, 'tech_stack': inst.tech_stack})
            
            if 'error' not in insights:
                if 'business_intel' not in inst.tech_stack: 
                    inst.tech_stack['business_intel'] = {}
                inst.tech_stack['business_intel']['ai_insights'] = insights
                inst.save(update_fields=['tech_stack'])
                count += 1
                
        self.message_user(request, f"🔥 {count} prospectos analizados y sintetizados por la IA.")

    fieldsets = (
        ('Identidad Estratégica', {
            'classes': ('tab',), 
            'fields': (
                ('name', 'institution_type'),
                ('country', 'state_region', 'city'),
                ('address',),
                ('website', 'email', 'phone'),
            ),
        }),
        ('🧠 Sales Intelligence (AI)', {
            'classes': ('tab',),
            'fields': (
                'ai_executive_panel', 
                'ai_tactical_panel', 
                'ai_copywriting_panel'
            )
        }),
        ('🔬 Forense y Ecosistema', {
            'classes': ('tab',), 
            'fields': (
                ('lead_score', 'last_scored_at', 'discovery_source'), 
                'pretty_tech_stack'
            ),
        }),
    )

# ==========================================
# 3. EL DASHBOARD CENTRAL (COMMAND CENTER)
# ==========================================
@admin.register(CommandCenter)
class CommandCenterAdmin(ModelAdmin):
    """
    Controlador de Misiones Asíncronas (The Executive Gateway).
    Implementa: Strategy Pattern, Redis Caching, RBAC y Graceful Degradation.
    """
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('', self.admin_site.admin_view(self.dashboard_view), name='sales_commandcenter_changelist'),
        ]
        return custom_urls + urls

    def dashboard_view(self, request: HttpRequest):
        """Renderiza el tablero y orquesta la inyección de tareas asíncronas hacia Redis/Celery."""
        
        # 1. SEGURIDAD (Graceful Degradation en Permisos)
        # Si el usuario no es un Superuser o no tiene el permiso explícito, lo bloqueamos amablemente.
        if not (request.user.is_superuser or request.user.has_perm('sales.view_executive_dashboard')):
            messages.error(request, "⛔ Acceso Denegado: Tu rango no permite acceso al Dashboard Ejecutivo.")
            return redirect('admin:index')

        # 2. CAPA DE ACCIÓN (STRATEGY PATTERN & INYECCIÓN DE TAREAS)
        if request.method == "POST":
            action_type = request.POST.get('action_type')
            
            # Matriz de Decisión O(1)
            mission_control = {
                'radar': {
                    'task': task_run_osm_radar,
                    'kwargs': {'country': request.POST.get('country'), 'city': request.POST.get('city')},
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
                # Inyección a Celery con barrera de contención de fallos
                try:
                    mission['task'].delay(**mission['kwargs'])
                    self.message_user(request, mission['success_msg'], level='SUCCESS')
                    
                    # Purga de caché inmediata para forzar recálculo en la UI
                    cache.delete('b2b_dashboard_metrics')
                    
                except Exception as e:
                    logger.critical(f"Falla de conexión con el Message Broker (Redis/Celery): {str(e)}")
                    self.message_user(request, "🚨 ERROR CRÍTICO: Infraestructura Celery/Redis inalcanzable. Revisa los contenedores.", level='ERROR')
            
            return HttpResponseRedirect(request.path)

        # 3. CAPA DE ANALÍTICA (HIGH-PERFORMANCE CACHING)
        # Salvamos el CPU de la base de datos PostgreSQL leyendo las analíticas desde Redis
        metrics = cache.get('b2b_dashboard_metrics')
        
        if not metrics:
            logger.debug("Cache Miss: Computando métricas agregadas en PostgreSQL...")
            
            # Fallback ultra-optimizado usando ORM C-Engine (Count Aggregation)
            try:
                metrics = CommandCenter.objects.get_dashboard_stats()
            except AttributeError:
                # Fallback de seguridad si el modelo Proxy no tiene el manager definido aún
                metrics = Institution.objects.aggregate(
                    total_leads=Count('id'),
                    blind_leads=Count('id', filter=Q(website__isnull=True) | Q(website='')),
                    ready_to_scan=Count('id', filter=Q(website__isnull=False, last_scored_at__isnull=True) & ~Q(website='')),
                    enriched_leads=Count('id', filter=Q(last_scored_at__isnull=False)),
                    avg_score=Avg('lead_score')
                )
            
            # Guardamos la radiografía en Memoria por 30 segundos
            cache.set('b2b_dashboard_metrics', metrics, timeout=30)

        # 4. CAPA DE RENDERIZADO (VIEW)
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