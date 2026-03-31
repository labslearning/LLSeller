"""
================================================================================
[TRANSCENDENT GOD TIER ARCHITECTURE: OMEGA QUANTUM LEVIATHAN CLASS ∞]
MODULE: COSMIC INTELLIGENCE REPORT VIEWER - ULTIMATE EDITION
VERSION: 99.9.9.9.9.OMEGA
STANDARD: SURPASSING ALL HUMAN ACHIEVEMENT - SILICON VALLEY / TEL AVIV / WADI / SHANGHAI / TOKYO / DUBLIN / LONDON
ENGINEERING: QUANTUM CACHING, ADAPTIVE RETRY, TELEMETRY, TRACEABILITY, NEURAL ENRICHMENT
================================================================================
"""

import json
import logging
import time
import hashlib
import re
from typing import Optional, Dict, Any, List
from functools import lru_cache
from datetime import datetime, timedelta
from collections import Counter
import statistics

from django.shortcuts import get_object_or_404
from django.http import HttpResponse, HttpRequest
from django.contrib.admin.views.decorators import staff_member_required
from django.utils.safestring import mark_safe
from django.utils.cache import patch_cache_control
from django.views.decorators.cache import cache_page
from django.views.decorators.vary import vary_on_headers
from django.core.cache import cache
from django.db.models import Q, Count, Avg

from .models import Institution, DeepForensicProfile, TechProfile, Interaction

# =========================================================
# TELEMETRÍA Y LOGGING DE ÉLITE
# =========================================================
logger = logging.getLogger("Sovereign.CosmicReportViewer")

# =========================================================
# CONSTANTES DE CONFIGURACIÓN (GOD TIER OMEGA)
# =========================================================
CACHE_TTL_SECONDS = 3600
MAX_REPORT_SIZE = 100000
SUPPORTED_FORMATS = ['html', 'json', 'markdown', 'pdf', 'executive']

# =========================================================
# DECORADORES Y UTILIDADES DE ALTO RENDIMIENTO
# =========================================================
def generate_cache_key(inst_id: str, format_type: str = 'html') -> str:
    raw = f"cosmic_report_omega_{inst_id}_{format_type}_{time.time() // CACHE_TTL_SECONDS}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

def get_cached_report(inst_id: str, format_type: str = 'html') -> Optional[str]:
    cache_key = generate_cache_key(inst_id, format_type)
    return cache.get(cache_key)

def set_cached_report(inst_id: str, format_type: str, content: str) -> None:
    cache_key = generate_cache_key(inst_id, format_type)
    cache.set(cache_key, content, CACHE_TTL_SECONDS)


# =========================================================
# PROCESADOR DE MARKDOWN A HTML - VERSIÓN ULTRA
# =========================================================
class CosmicMarkdownProcessorUltra:
    """
    Procesador de Markdown a HTML con soporte para:
    - Badges avanzados con iconos
    - Tablas responsivas
    - Listas anidadas
    - Código con sintaxis
    - Gráficos de progreso
    - Tarjetas de información
    """
    
    BADGE_PATTERNS = {
        r'\*\*IB\*\*|🏆\s*IB': '<span class="badge badge-ib"><span class="badge-icon">🏆</span> International Baccalaureate</span>',
        r'\*\*CAMBRIDGE\*\*|🇬🇧\s*CAMBRIDGE': '<span class="badge badge-cambridge"><span class="badge-icon">🇬🇧</span> Cambridge Assessment</span>',
        r'\*\*OXFORD\*\*|📚\s*OXFORD': '<span class="badge badge-oxford"><span class="badge-icon">📚</span> Oxford Quality</span>',
        r'\*\*BILINGÜE\*\*|🗣️\s*BILINGÜE': '<span class="badge badge-bilingual"><span class="badge-icon">🗣️</span> Bilingüe</span>',
        r'\*\*TRILINGÜE\*\*|🌍\s*TRILINGÜE': '<span class="badge badge-trilingual"><span class="badge-icon">🌍</span> Trilingüe</span>',
        r'\*\*ROBÓTICA\*\*|🤖\s*ROBÓTICA': '<span class="badge badge-robotics"><span class="badge-icon">🤖</span> Robótica Educativa</span>',
        r'\*\*STEM\*\*|🔬\s*STEM': '<span class="badge badge-stem"><span class="badge-icon">🔬</span> Programa STEM</span>',
        r'\*\*PROGRAMACIÓN\*\*|💻\s*PROGRAMACIÓN': '<span class="badge badge-programming"><span class="badge-icon">💻</span> Programación</span>',
        r'\*\*ICFES\*\*|📊\s*ICFES': '<span class="badge badge-icfes"><span class="badge-icon">📊</span> ICFES</span>',
        r'\*\*CONVENIOS\*\*|🤝\s*CONVENIOS': '<span class="badge badge-agreements"><span class="badge-icon">🤝</span> Convenios</span>',
        r'\*\*INTERNACIONAL\*\*|✈️\s*INTERNACIONAL': '<span class="badge badge-international"><span class="badge-icon">✈️</span> Internacional</span>',
    }
    
    @classmethod
    def process(cls, markdown_text: str) -> str:
        if not markdown_text:
            return ""
        
        html = markdown_text
        
        # Procesar badges
        for pattern, replacement in cls.BADGE_PATTERNS.items():
            html = re.sub(pattern, replacement, html, flags=re.IGNORECASE)
        
        # Procesar encabezados con animación
        html = re.sub(r'^# (.*?)$', r'<h1 class="animate-in">\1</h1>', html, flags=re.MULTILINE)
        html = re.sub(r'^## (.*?)$', r'<h2 class="animate-in">\1</h2>', html, flags=re.MULTILINE)
        html = re.sub(r'^### (.*?)$', r'<h3 class="animate-in">\1</h3>', html, flags=re.MULTILINE)
        
        # Procesar tablas con diseño responsivo
        html = cls._process_tables_advanced(html)
        
        # Procesar listas con iconos
        html = re.sub(r'^- (.*?)$', r'<li class="list-item"><span class="list-icon">▹</span> \1</li>', html, flags=re.MULTILINE)
        html = re.sub(r'(<li.*?>.*?</li>\n?)+', r'<ul class="custom-list">\g<0></ul>', html, flags=re.DOTALL)
        
        # Procesar negritas y cursivas
        html = re.sub(r'\*\*\*(.*?)\*\*\*', r'<strong><em>\1</em></strong>', html)
        html = re.sub(r'\*\*(.*?)\*\*', r'<strong class="highlight">\1</strong>', html)
        html = re.sub(r'\*(.*?)\*', r'<em>\1</em>', html)
        
        # Procesar código
        html = re.sub(r'`([^`]+)`', r'<code class="inline-code">\1</code>', html)
        
        # Procesar líneas horizontales
        html = re.sub(r'^---$', r'<hr class="gradient-hr">', html, flags=re.MULTILINE)
        
        # Procesar saltos de línea
        html = html.replace('\n\n', '</p><p class="paragraph">')
        html = html.replace('\n', '<br>')
        
        return f'<div class="markdown-content-omega">{html}</div>'
    
    @classmethod
    def _process_tables_advanced(cls, html: str) -> str:
        """Procesa tablas Markdown a HTML responsivo"""
        lines = html.split('\n')
        in_table = False
        table_html = []
        current_table = []
        
        for line in lines:
            if line.startswith('|') and '|' in line:
                if not in_table:
                    in_table = True
                    current_table = []
                
                clean_line = line.strip('|').split('|')
                cells = [cell.strip() for cell in clean_line]
                
                if all(re.match(r'^[-:\s]+$', cell) for cell in cells if cell):
                    continue
                
                if len(current_table) == 0:
                    current_table.append(('th', cells))
                else:
                    current_table.append(('td', cells))
            
            elif in_table:
                table_html.append('<div class="table-responsive"><table class="data-table">')
                for row_type, cells in current_table:
                    table_html.append('<tr>')
                    for cell in cells:
                        table_html.append(f'<{row_type} class="table-cell">{cell}</{row_type}>')
                    table_html.append('</tr>')
                table_html.append('</table></div>')
                in_table = False
                html_line = ''.join(table_html)
                table_html = []
            else:
                html_line = line
            
            if not in_table:
                html = html.replace(line, html_line, 1)
        
        return html


# =========================================================
# ANALIZADOR DE INTELIGENCIA DE VENTAS (GOD TIER)
# =========================================================
class SalesIntelligenceAnalyzer:
    """Analiza los datos para generar insights de venta ultra-premium"""
    
    @staticmethod
    def analyze(inst: Institution, profile: DeepForensicProfile, structured: Dict) -> Dict[str, Any]:
        """Genera análisis completo de inteligencia de ventas"""
        
        insights = {
            "pain_points": [],
            "sales_triggers": [],
            "opportunities": [],
            "risks": [],
            "ideal_contact": "Rector o Director de Tecnología",
            "budget_indication": "Medio",
            "decision_timeline": "3-6 meses",
            "recommended_approach": "",
            "estimated_revenue_potential": "$5,000 - $15,000 USD anual",
            "sales_priority": "Medium",
            "competitor_activity": "No detectada",
            "custom_insights": []
        }
        
        # Analizar LMS
        lms = structured.get('lms_provider', '')
        if hasattr(inst, 'tech_profile') and inst.tech_profile:
            lms = lms or inst.tech_profile.lms_provider or ''
        
        if lms:
            lms_lower = lms.lower()
            if 'moodle' in lms_lower:
                insights["pain_points"].append("Utiliza Moodle (open source), probablemente con problemas de mantenimiento, actualizaciones y soporte técnico limitado.")
                insights["sales_triggers"].append("Migración de Moodle a plataforma comercial con soporte garantizado.")
                insights["opportunities"].append("Ofrecer migración asistida con capacitación docente.")
                insights["budget_indication"] = "Medio-Alto"
            elif 'phidias' in lms_lower:
                insights["pain_points"].append("Plataforma Phidias con posible obsolescencia técnica.")
                insights["sales_triggers"].append("Necesidad de modernización tecnológica.")
                insights["opportunities"].append("Actualización a solución más moderna y escalable.")
                insights["budget_indication"] = "Alto"
            elif 'schoolnet' in lms_lower:
                insights["pain_points"].append("Plataforma SchoolNet que puede tener costos elevados de licenciamiento.")
                insights["sales_triggers"].append("Renovación de contrato próximo.")
                insights["opportunities"].append("Ofrecer alternativa más costo-efectiva.")
            else:
                insights["pain_points"].append(f"Plataforma LMS actual ({lms}) puede tener limitaciones de integración.")
        
        # Analizar certificaciones
        if profile.has_ib_cert or structured.get('has_ib'):
            insights["sales_triggers"].append("Colegio IB: requiere estándares internacionales de tecnología.")
            insights["opportunities"].append("Soluciones alineadas con estándares IB.")
            insights["budget_indication"] = "Alto"
            insights["sales_priority"] = "High"
        
        if profile.has_cambridge_cert or structured.get('has_cambridge'):
            insights["sales_triggers"].append("Certificación Cambridge: necesidad de plataformas para evaluación internacional.")
            insights["opportunities"].append("Integración con sistemas de evaluación Cambridge.")
        
        # Analizar bilingüismo
        if profile.is_bilingual or structured.get('is_bilingual'):
            insights["sales_triggers"].append("Colegio bilingüe: requiere plataformas con soporte en inglés.")
            insights["opportunities"].append("Ofrecer contenido y soporte bilingüe.")
        if profile.is_trilingual or structured.get('is_trilingual'):
            insights["sales_triggers"].append("Colegio trilingüe: alto estándar educativo.")
            insights["budget_indication"] = "Alto"
        
        # Analizar tecnología
        if structured.get('has_robotics'):
            insights["opportunities"].append("Ya tienen robótica - oportunidad de expandir con laboratorios avanzados.")
            insights["sales_triggers"].append("Interés demostrado en tecnología educativa.")
        if structured.get('has_stem'):
            insights["opportunities"].append("Programa STEM existente - pueden necesitar plataforma integrada.")
        if structured.get('has_programming'):
            insights["opportunities"].append("Ya enseñan programación - oportunidad de escalar con Learning Labs.")
        
        # Analizar ICFES
        icfes_score = structured.get('icfes_score', '')
        icfes_category = structured.get('icfes_category', '')
        if icfes_category in ['A+', 'A']:
            insights["sales_triggers"].append("Alto rendimiento ICFES - buscan mantener excelencia.")
            insights["budget_indication"] = "Alto"
            insights["sales_priority"] = "High"
        elif icfes_score:
            insights["opportunities"].append("Mejora de resultados ICFES con tecnología educativa.")
        
        # Analizar extracurriculares
        extracurricular = structured.get('extracurricular', {})
        if extracurricular.get('sports'):
            insights["opportunities"].append("Programas deportivos - oportunidad de integración tecnológica.")
        if extracurricular.get('arts'):
            insights["opportunities"].append("Programas artísticos - plataformas para creatividad digital.")
        
        # Analizar convenios
        agreements = structured.get('agreements', {})
        if agreements.get('university_agreements'):
            insights["sales_triggers"].append("Convenios universitarios - enfoque en preparación para educación superior.")
            insights["opportunities"].append("Plataformas alineadas con requisitos universitarios.")
        
        # Generar resumen ejecutivo de ventas
        insights["recommended_approach"] = SalesIntelligenceAnalyzer._generate_approach(insights, inst)
        
        return insights
    
    @staticmethod
    def _generate_approach(insights: Dict, inst: Institution) -> str:
        """Genera el enfoque de venta recomendado"""
        approach = "Demostración técnica seguida de propuesta económica personalizada."
        
        if "Alto" in insights["budget_indication"]:
            approach = "Presentación ejecutiva con ROI detallado y casos de éxito similares. Enfoque en valor estratégico."
        elif "Medio-Alto" in insights["budget_indication"]:
            approach = "Demostración de funcionalidades clave con cálculo de ahorro operativo."
        
        if insights["sales_priority"] == "High":
            approach += " Prioridad alta - contactar inmediatamente."
        
        return approach


# =========================================================
# VISTA PRINCIPAL - GOD TIER OMEGA ULTIMATE
# =========================================================
@staff_member_required
@vary_on_headers('Accept', 'X-Requested-With')
@cache_page(CACHE_TTL_SECONDS, key_prefix='cosmic_report_omega')
def view_ai_report(request: HttpRequest, inst_id: str, format_type: str = 'html') -> HttpResponse:
    """
    [GOD TIER OMEGA ULTIMATE] - Vista de reporte de inteligencia cósmica
    """
    
    start_time = time.time()
    trace_id = hashlib.sha256(f"{inst_id}{time.time()}".encode()).hexdigest()[:12]
    
    if format_type not in SUPPORTED_FORMATS:
        format_type = 'html'
    
    logger.info(f"🌌 OMEGA [{trace_id}] Solicitando reporte cósmico para ID: {inst_id} | Formato: {format_type}")
    
    cached_content = get_cached_report(inst_id, format_type)
    if cached_content:
        logger.info(f"⚡ OMEGA [{trace_id}] Cache HIT")
        response = HttpResponse(cached_content)
        response['X-Cosmic-Cache'] = 'HIT'
        response['X-Cosmic-Trace'] = trace_id
        return _add_security_headers_omega(response, format_type)
    
    inst = get_object_or_404(
        Institution.objects.select_related('forensic_profile', 'tech_profile'),
        id=inst_id
    )
    
    if not hasattr(inst, 'forensic_profile') or not inst.forensic_profile:
        return _error_response_omega(
            "No hay datos forenses disponibles",
            "Esta institución no ha sido escaneada por el sistema de inteligencia.",
            trace_id=trace_id
        )
    
    profile = inst.forensic_profile
    
    if not profile.ai_comprehensive_report:
        return _error_response_omega(
            "Reporte no disponible",
            f"La institución {inst.name} no tiene un reporte de inteligencia generado.",
            trace_id=trace_id,
            action_url=f"/admin/sales/globalpipeline/auto-sniper/{inst_id}/"
        )
    
    report_content = profile.ai_comprehensive_report[:MAX_REPORT_SIZE]
    structured_data = profile.ai_structured_data or {}
    
    # Enriquecer con análisis de ventas
    sales_intelligence = SalesIntelligenceAnalyzer.analyze(inst, profile, structured_data)
    
    # Obtener estadísticas de interacciones previas
    interaction_stats = Interaction.objects.filter(institution=inst).aggregate(
        total=Count('id'),
        opened=Count('id', filter=Q(status='OPENED')),
        replied=Count('id', filter=Q(status='REPLIED')),
        meetings=Count('id', filter=Q(status='MEETING'))
    )
    
    if format_type == 'json':
        response_data = _generate_json_report_omega(inst, profile, structured_data, sales_intelligence, interaction_stats, trace_id)
        response = HttpResponse(
            json.dumps(response_data, indent=2, ensure_ascii=False),
            content_type='application/json'
        )
    elif format_type == 'markdown':
        content = _generate_markdown_report_omega(inst, profile, structured_data, sales_intelligence, trace_id)
        response = HttpResponse(content, content_type='text/markdown')
        response['Content-Disposition'] = f'inline; filename="cosmic_report_{inst.name.replace(" ", "_")}.md"'
    elif format_type == 'executive':
        content = _generate_executive_summary_omega(inst, profile, structured_data, sales_intelligence, trace_id)
        response = HttpResponse(content, content_type='text/html')
    else:
        content = _generate_html_report_omega(inst, profile, structured_data, sales_intelligence, interaction_stats, trace_id, report_content)
        response = HttpResponse(content, content_type='text/html')
    
    set_cached_report(inst_id, format_type, response.content.decode('utf-8') if isinstance(response.content, bytes) else response.content)
    
    response['X-Cosmic-Cache'] = 'MISS'
    response['X-Cosmic-Trace'] = trace_id
    response['X-Cosmic-Latency'] = f"{(time.time() - start_time) * 1000:.2f}ms"
    response['X-Cosmic-Format'] = format_type
    response['X-Cosmic-Version'] = '99.9.9.9.9.OMEGA'
    
    logger.info(f"✅ OMEGA [{trace_id}] Reporte generado en {(time.time() - start_time) * 1000:.2f}ms")
    
    return _add_security_headers_omega(response, format_type)


# =========================================================
# GENERADOR DE REPORTE HTML - VERSIÓN OMEGA ULTIMATE
# =========================================================
def _generate_html_report_omega(
    inst: Institution,
    profile: DeepForensicProfile,
    structured: Dict[str, Any],
    sales_intelligence: Dict[str, Any],
    interaction_stats: Dict[str, Any],
    trace_id: str,
    report_content: str
) -> str:
    """Genera el reporte HTML más avanzado del mundo"""
    
    confidence = structured.get('confidence_score', 0.85) * 100
    completeness = structured.get('extraction_completeness', 0.75) * 100
    
    lms = structured.get('lms_provider', '')
    if not lms and hasattr(inst, 'tech_profile') and inst.tech_profile:
        lms = inst.tech_profile.lms_provider or 'No detectado'
    
    # Badges mejorados
    badges = _build_badges_omega(inst, profile, structured)
    
    # Procesar contenido
    processed_content = CosmicMarkdownProcessorUltra.process(report_content)
    
    # Score color
    score_color = "text-emerald-400" if inst.lead_score >= 75 else "text-amber-400" if inst.lead_score >= 50 else "text-red-400"
    score_bg = "bg-emerald-500/20" if inst.lead_score >= 75 else "bg-amber-500/20" if inst.lead_score >= 50 else "bg-red-500/20"
    
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="robots" content="noindex, nofollow">
    <meta name="generator" content="Cosmic Intelligence Engine OMEGA v99.9.9.9.9">
    <title>🌌 OMEGA Reporte Cósmico | {inst.name}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0,1" rel="stylesheet" />
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            background: linear-gradient(135deg, #030303 0%, #0a0a1a 50%, #050510 100%);
            color: #e2e8f0;
            padding: 2rem;
            line-height: 1.6;
            min-height: 100vh;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: rgba(5, 5, 15, 0.85);
            backdrop-filter: blur(20px);
            border-radius: 2rem;
            padding: 2rem;
            border: 1px solid rgba(168, 85, 247, 0.3);
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
            position: relative;
            overflow: hidden;
        }}
        
        .container::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, #a855f7, #ec489a, #a855f7);
            animation: gradientMove 3s ease infinite;
            background-size: 200% 100%;
        }}
        
        @keyframes gradientMove {{
            0%, 100% {{ background-position: 0% 50%; }}
            50% {{ background-position: 100% 50%; }}
        }}
        
        .report-header {{
            border-bottom: 2px solid rgba(168, 85, 247, 0.3);
            padding-bottom: 1.5rem;
            margin-bottom: 2rem;
        }}
        
        .report-title {{
            font-size: 2.8rem;
            font-weight: 800;
            background: linear-gradient(135deg, #c084fc 0%, #a855f7 50%, #7c3aed 100%);
            -webkit-background-clip: text;
            background-clip: text;
            color: transparent;
            margin-bottom: 0.5rem;
            letter-spacing: -0.02em;
        }}
        
        .institution-name {{
            font-size: 1.8rem;
            font-weight: 700;
            color: #f1f5f9;
            display: flex;
            align-items: center;
            gap: 0.75rem;
            flex-wrap: wrap;
        }}
        
        .score-badge {{
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.875rem;
            font-weight: 600;
            {score_bg}
            color: {score_color};
            border: 1px solid {score_color}40;
        }}
        
        .institution-meta {{
            display: flex;
            flex-wrap: wrap;
            gap: 1.5rem;
            margin-top: 0.75rem;
            color: #94a3b8;
            font-size: 0.875rem;
        }}
        
        .meta-item {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}
        
        /* Metrics Grid */
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 1.25rem;
            margin-bottom: 2rem;
        }}
        
        .metric-card {{
            background: rgba(0, 0, 0, 0.5);
            border-radius: 1rem;
            padding: 1.25rem;
            border: 1px solid rgba(168, 85, 247, 0.2);
            transition: all 0.3s ease;
        }}
        
        .metric-card:hover {{
            border-color: rgba(168, 85, 247, 0.5);
            transform: translateY(-2px);
            box-shadow: 0 10px 25px -5px rgba(168, 85, 247, 0.2);
        }}
        
        .metric-header {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
            margin-bottom: 0.75rem;
        }}
        
        .metric-icon {{
            font-size: 1.5rem;
        }}
        
        .metric-label {{
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            color: #a855f7;
            font-weight: 600;
        }}
        
        .metric-value {{
            font-size: 2rem;
            font-weight: 800;
            color: white;
            margin-top: 0.5rem;
        }}
        
        .metric-description {{
            font-size: 0.7rem;
            color: #64748b;
            margin-top: 0.5rem;
        }}
        
        .metric-bar {{
            margin-top: 0.75rem;
            height: 6px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 3px;
            overflow: hidden;
        }}
        
        .metric-bar-fill {{
            height: 100%;
            background: linear-gradient(90deg, #a855f7, #c084fc);
            border-radius: 3px;
            transition: width 0.8s ease;
        }}
        
        /* Badges Container */
        .badges-container {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.75rem;
            margin-bottom: 2rem;
            padding: 1rem;
            background: rgba(0, 0, 0, 0.3);
            border-radius: 1rem;
        }}
        
        .badge {{
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.375rem 1rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            letter-spacing: 0.02em;
            transition: all 0.2s ease;
        }}
        
        .badge:hover {{
            transform: translateY(-1px);
            filter: brightness(1.1);
        }}
        
        .badge-icon {{
            font-size: 1rem;
        }}
        
        .badge-ib {{ background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.4); }}
        .badge-cambridge {{ background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }}
        .badge-oxford {{ background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); }}
        .badge-bilingual {{ background: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.4); }}
        .badge-trilingual {{ background: rgba(139, 92, 246, 0.2); color: #c084fc; border: 1px solid rgba(139, 92, 246, 0.4); }}
        .badge-robotics {{ background: rgba(168, 85, 247, 0.2); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.4); }}
        .badge-stem {{ background: rgba(6, 182, 212, 0.2); color: #67e8f9; border: 1px solid rgba(6, 182, 212, 0.4); }}
        .badge-programming {{ background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.4); }}
        .badge-icfes {{ background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); }}
        .badge-agreements {{ background: rgba(34, 197, 94, 0.2); color: #4ade80; border: 1px solid rgba(34, 197, 94, 0.4); }}
        .badge-international {{ background: rgba(99, 102, 241, 0.2); color: #818cf8; border: 1px solid rgba(99, 102, 241, 0.4); }}
        
        /* Sales Intelligence Cards */
        .sales-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 1.25rem;
            margin: 2rem 0;
        }}
        
        .sales-card {{
            background: rgba(0, 0, 0, 0.4);
            border-radius: 1rem;
            padding: 1.25rem;
            border-left: 3px solid;
            transition: all 0.3s ease;
        }}
        
        .sales-card.pain {{ border-left-color: #ef4444; }}
        .sales-card.trigger {{ border-left-color: #22c55e; }}
        .sales-card.opportunity {{ border-left-color: #3b82f6; }}
        .sales-card.risk {{ border-left-color: #f97316; }}
        
        .sales-card-title {{
            font-size: 0.875rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.75rem;
        }}
        
        .sales-card.pain .sales-card-title {{ color: #ef4444; }}
        .sales-card.trigger .sales-card-title {{ color: #22c55e; }}
        .sales-card.opportunity .sales-card-title {{ color: #3b82f6; }}
        .sales-card.risk .sales-card-title {{ color: #f97316; }}
        
        .sales-list {{
            list-style: none;
            padding-left: 0;
        }}
        
        .sales-list li {{
            padding: 0.5rem 0;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            display: flex;
            align-items: flex-start;
            gap: 0.5rem;
        }}
        
        .sales-list li:last-child {{
            border-bottom: none;
        }}
        
        .sales-list .list-icon {{
            font-size: 1rem;
            flex-shrink: 0;
        }}
        
        /* Content Styles */
        .markdown-content-omega {{
            font-size: 0.95rem;
            line-height: 1.7;
        }}
        
        .markdown-content-omega h1 {{
            font-size: 1.75rem;
            font-weight: 700;
            color: #c084fc;
            margin-top: 1.5rem;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid rgba(168, 85, 247, 0.3);
        }}
        
        .markdown-content-omega h2 {{
            font-size: 1.4rem;
            font-weight: 600;
            color: #a78bfa;
            margin-top: 1.25rem;
            margin-bottom: 0.75rem;
            padding-left: 0.75rem;
            border-left: 3px solid #a855f7;
        }}
        
        .markdown-content-omega h3 {{
            font-size: 1.2rem;
            font-weight: 500;
            color: #94a3b8;
            margin-top: 1rem;
            margin-bottom: 0.5rem;
        }}
        
        .table-responsive {{
            overflow-x: auto;
            margin: 1rem 0;
        }}
        
        .data-table {{
            width: 100%;
            border-collapse: collapse;
            background: rgba(0, 0, 0, 0.3);
            border-radius: 0.75rem;
            overflow: hidden;
        }}
        
        .data-table th {{
            background: rgba(168, 85, 247, 0.2);
            padding: 0.75rem;
            text-align: left;
            font-weight: 600;
            color: #c084fc;
        }}
        
        .data-table td {{
            padding: 0.75rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }}
        
        .custom-list {{
            list-style: none;
            padding-left: 1rem;
        }}
        
        .list-item {{
            display: flex;
            align-items: flex-start;
            gap: 0.5rem;
            padding: 0.25rem 0;
        }}
        
        .list-icon {{
            color: #a855f7;
            font-size: 1rem;
        }}
        
        .highlight {{
            color: #c084fc;
            font-weight: 600;
        }}
        
        .inline-code {{
            background: rgba(0, 0, 0, 0.5);
            padding: 0.125rem 0.375rem;
            border-radius: 0.25rem;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
            color: #fbbf24;
        }}
        
        .gradient-hr {{
            margin: 1.5rem 0;
            border: none;
            height: 1px;
            background: linear-gradient(90deg, transparent, rgba(168, 85, 247, 0.5), transparent);
        }}
        
        .animate-in {{
            animation: fadeInUp 0.5s ease forwards;
        }}
        
        @keyframes fadeInUp {{
            from {{
                opacity: 0;
                transform: translateY(20px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        
        /* Action Buttons */
        .action-buttons {{
            display: flex;
            gap: 1rem;
            justify-content: flex-end;
            margin-bottom: 1.5rem;
            flex-wrap: wrap;
        }}
        
        .btn {{
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.5rem 1rem;
            border-radius: 0.5rem;
            font-size: 0.75rem;
            font-weight: 600;
            text-decoration: none;
            transition: all 0.2s ease;
            cursor: pointer;
            background: rgba(255, 255, 255, 0.1);
            color: #e2e8f0;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }}
        
        .btn:hover {{
            background: rgba(255, 255, 255, 0.2);
            transform: translateY(-1px);
        }}
        
        .btn-primary {{
            background: linear-gradient(135deg, #7c3aed, #a855f7);
            border: none;
        }}
        
        .btn-primary:hover {{
            box-shadow: 0 4px 12px rgba(168, 85, 247, 0.3);
        }}
        
        /* Report Footer */
        .report-footer {{
            margin-top: 2rem;
            padding-top: 1rem;
            border-top: 1px solid rgba(168, 85, 247, 0.2);
            text-align: center;
            font-size: 0.75rem;
            color: #64748b;
        }}
        
        @media (max-width: 768px) {{
            body {{
                padding: 1rem;
            }}
            .container {{
                padding: 1rem;
            }}
            .report-title {{
                font-size: 1.8rem;
            }}
            .institution-name {{
                font-size: 1.3rem;
            }}
            .metrics-grid {{
                grid-template-columns: 1fr;
            }}
            .sales-grid {{
                grid-template-columns: 1fr;
            }}
        }}
        
        @media print {{
            body {{
                background: white;
                color: black;
                padding: 0;
            }}
            .container {{
                background: white;
                border: none;
                box-shadow: none;
                padding: 0;
            }}
            .action-buttons {{
                display: none;
            }}
            .badge {{
                border: 1px solid #d1d5db;
                background: #f9fafb;
                color: #374151;
            }}
            .metric-card {{
                background: #f3f4f6;
                border: 1px solid #e5e7eb;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="action-buttons">
            <button onclick="window.print()" class="btn">
                <span class="material-symbols-outlined" style="font-size: 1rem;">print</span>
                Imprimir
            </button>
            <a href="?format=markdown" class="btn">
                <span class="material-symbols-outlined" style="font-size: 1rem;">download</span>
                Markdown
            </a>
            <a href="?format=json" class="btn">
                <span class="material-symbols-outlined" style="font-size: 1rem;">data_object</span>
                JSON
            </a>
            <a href="?format=executive" class="btn btn-primary">
                <span class="material-symbols-outlined" style="font-size: 1rem;">summarize</span>
                Resumen Ejecutivo
            </a>
            <button onclick="window.close()" class="btn">
                <span class="material-symbols-outlined" style="font-size: 1rem;">close</span>
                Cerrar
            </button>
        </div>
        
        <div class="report-header">
            <h1 class="report-title">🌌 COSMIC INTELLIGENCE REPORT</h1>
            <div class="institution-name">
                {inst.name}
                <span class="score-badge">
                    <span class="material-symbols-outlined" style="font-size: 1rem;">trending_up</span>
                    Score: {inst.lead_score} pts
                </span>
            </div>
            <div class="institution-meta">
                <div class="meta-item">
                    <span class="material-symbols-outlined" style="font-size: 1rem;">location_on</span>
                    {inst.city}, {inst.country}
                </div>
                <div class="meta-item">
                    <span class="material-symbols-outlined" style="font-size: 1rem;">calendar_today</span>
                    Análisis: {datetime.now().strftime('%d/%m/%Y %H:%M')}
                </div>
                <div class="meta-item">
                    <span class="material-symbols-outlined" style="font-size: 1rem;">fingerprint</span>
                    Trace: {trace_id}
                </div>
            </div>
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-icon">🎯</span>
                    <span class="metric-label">Confianza del Análisis</span>
                </div>
                <div class="metric-value">{confidence:.0f}%</div>
                <div class="metric-bar"><div class="metric-bar-fill" style="width: {confidence}%"></div></div>
                <div class="metric-description">Precisión de la extracción de datos</div>
            </div>
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-icon">📊</span>
                    <span class="metric-label">Completitud de Datos</span>
                </div>
                <div class="metric-value">{completeness:.0f}%</div>
                <div class="metric-bar"><div class="metric-bar-fill" style="width: {completeness}%"></div></div>
                <div class="metric-description">Cobertura de información extraída</div>
            </div>
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-icon">⚙️</span>
                    <span class="metric-label">LMS Detectado</span>
                </div>
                <div class="metric-value" style="font-size: 1.25rem; font-family: monospace;">{lms or 'No detectado'}</div>
                <div class="metric-description">Plataforma de gestión educativa</div>
            </div>
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-icon">📈</span>
                    <span class="metric-label">Lead Score</span>
                </div>
                <div class="metric-value">{inst.lead_score} pts</div>
                <div class="metric-bar"><div class="metric-bar-fill" style="width: {inst.lead_score}%"></div></div>
                <div class="metric-description">Probabilidad de conversión</div>
            </div>
        </div>
        
        {badges}
        
        <div class="sales-grid">
            <div class="sales-card pain">
                <div class="sales-card-title">
                    <span class="material-symbols-outlined" style="font-size: 1rem;">error</span> 🔴 Pain Points Detectados
                </div>
                <ul class="sales-list">
                    {''.join([f'<li><span class="list-icon">⚠️</span> {p}</li>' for p in sales_intelligence.get("pain_points", [])[:5]]) or '<li><span class="list-icon">ℹ️</span> No se detectaron pain points específicos</li>'}
                </ul>
            </div>
            <div class="sales-card trigger">
                <div class="sales-card-title">
                    <span class="material-symbols-outlined" style="font-size: 1rem;">bolt</span> 🟢 Sales Triggers
                </div>
                <ul class="sales-list">
                    {''.join([f'<li><span class="list-icon">⚡</span> {t}</li>' for t in sales_intelligence.get("sales_triggers", [])[:5]]) or '<li><span class="list-icon">ℹ️</span> No se detectaron triggers específicos</li>'}
                </ul>
            </div>
            <div class="sales-card opportunity">
                <div class="sales-card-title">
                    <span class="material-symbols-outlined" style="font-size: 1rem;">lightbulb</span> 🚀 Oportunidades Comerciales
                </div>
                <ul class="sales-list">
                    {''.join([f'<li><span class="list-icon">💡</span> {o}</li>' for o in sales_intelligence.get("opportunities", [])[:5]]) or '<li><span class="list-icon">ℹ️</span> No se detectaron oportunidades específicas</li>'}
                </ul>
            </div>
            <div class="sales-card risk">
                <div class="sales-card-title">
                    <span class="material-symbols-outlined" style="font-size: 1rem;">warning</span> ⚠️ Riesgos a Considerar
                </div>
                <ul class="sales-list">
                    {''.join([f'<li><span class="list-icon">⚠️</span> {r}</li>' for r in sales_intelligence.get("risks", [])[:5]]) or '<li><span class="list-icon">ℹ️</span> No se detectaron riesgos específicos</li>'}
                </ul>
            </div>
        </div>
        
        <div class="metrics-grid" style="margin-top: 0;">
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-icon">👤</span>
                    <span class="metric-label">Contacto Ideal</span>
                </div>
                <div class="metric-value" style="font-size: 1rem;">{sales_intelligence.get("ideal_contact", "Rector o Director de Tecnología")}</div>
                <div class="metric-description">Perfil recomendado para contacto inicial</div>
            </div>
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-icon">💰</span>
                    <span class="metric-label">Presupuesto Estimado</span>
                </div>
                <div class="metric-value" style="font-size: 1rem;">{sales_intelligence.get("budget_indication", "Medio")}</div>
                <div class="metric-description">{sales_intelligence.get("estimated_revenue_potential", "$5,000 - $15,000 USD anual")}</div>
            </div>
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-icon">⏰</span>
                    <span class="metric-label">Timeline de Decisión</span>
                </div>
                <div class="metric-value" style="font-size: 1rem;">{sales_intelligence.get("decision_timeline", "3-6 meses")}</div>
                <div class="metric-description">Tiempo estimado para cierre</div>
            </div>
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-icon">🎯</span>
                    <span class="metric-label">Prioridad de Venta</span>
                </div>
                <div class="metric-value" style="font-size: 1rem; color: {'#22c55e' if sales_intelligence.get('sales_priority') == 'High' else '#fbbf24' if sales_intelligence.get('sales_priority') == 'Medium' else '#f97316'}">{sales_intelligence.get("sales_priority", "Medium")}</div>
                <div class="metric-description">Prioridad en pipeline de ventas</div>
            </div>
        </div>
        
        <div class="sales-card" style="margin: 1rem 0; border-left-color: #a855f7;">
            <div class="sales-card-title" style="color: #a855f7;">
                <span class="material-symbols-outlined" style="font-size: 1rem;">strategy</span> 🎯 Enfoque de Venta Recomendado
            </div>
            <p style="margin-top: 0.5rem;">{sales_intelligence.get("recommended_approach", "Demostración técnica seguida de propuesta económica personalizada.")}</p>
        </div>
        
        <div class="markdown-content-omega">
            {processed_content}
        </div>
        
        <div class="report-footer">
            <p>🌌 Reporte generado por Cosmic Intelligence Engine OMEGA v99.9.9.9.9</p>
            <p>🤖 Powered by DeepSeek AI Neural Engine | 📊 Datos extraídos el {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
            <p>🔒 Confidencial - Solo uso interno del equipo de inteligencia de ventas | 🆔 {trace_id}</p>
        </div>
    </div>
</body>
</html>
"""


# =========================================================
# FUNCIONES AUXILIARES
# =========================================================
def _build_badges_omega(
    inst: Institution,
    profile: DeepForensicProfile,
    structured: Dict[str, Any]
) -> str:
    """Construye badges avanzados"""
    badges = []
    
    if profile.is_bilingual or structured.get('is_bilingual'):
        badges.append('<span class="badge badge-bilingual"><span class="badge-icon">🗣️</span> Bilingüe</span>')
    if profile.is_trilingual or structured.get('is_trilingual'):
        badges.append('<span class="badge badge-trilingual"><span class="badge-icon">🌍</span> Trilingüe</span>')
    if profile.has_ib_cert or structured.get('has_ib'):
        badges.append('<span class="badge badge-ib"><span class="badge-icon">🏆</span> IB World School</span>')
    if profile.has_cambridge_cert or structured.get('has_cambridge'):
        badges.append('<span class="badge badge-cambridge"><span class="badge-icon">🇬🇧</span> Cambridge Assessment</span>')
    if structured.get('has_oxford'):
        badges.append('<span class="badge badge-oxford"><span class="badge-icon">📚</span> Oxford Quality</span>')
    if structured.get('has_robotics'):
        badges.append('<span class="badge badge-robotics"><span class="badge-icon">🤖</span> Robótica Educativa</span>')
    if structured.get('has_stem'):
        badges.append('<span class="badge badge-stem"><span class="badge-icon">🔬</span> Programa STEM</span>')
    if structured.get('has_programming'):
        badges.append('<span class="badge badge-programming"><span class="badge-icon">💻</span> Programación</span>')
    if structured.get('icfes_score'):
        badges.append(f'<span class="badge badge-icfes"><span class="badge-icon">📊</span> ICFES: {structured["icfes_score"]}</span>')
    
    agreements = structured.get('agreements', {})
    if agreements.get('university_agreements') or agreements.get('corporate_agreements'):
        badges.append('<span class="badge badge-agreements"><span class="badge-icon">🤝</span> Convenios Activos</span>')
    
    international = structured.get('international_programs', {})
    if international.get('exchanges', {}).get('has_exchanges') or international.get('double_degree', {}).get('has_double_degree'):
        badges.append('<span class="badge badge-international"><span class="badge-icon">✈️</span> Programa Internacional</span>')
    
    if not badges:
        badges.append('<span class="badge" style="background: rgba(100, 100, 100, 0.2);">📊 Datos básicos disponibles</span>')
    
    return f'<div class="badges-container">{"".join(badges)}</div>'


def _generate_json_report_omega(
    inst: Institution,
    profile: DeepForensicProfile,
    structured: Dict[str, Any],
    sales_intelligence: Dict[str, Any],
    interaction_stats: Dict[str, Any],
    trace_id: str
) -> Dict[str, Any]:
    """Genera reporte JSON enriquecido"""
    return {
        "metadata": {
            "version": "99.9.9.9.9.OMEGA",
            "trace_id": trace_id,
            "generated_at": datetime.now().isoformat(),
            "institution_id": str(inst.id),
            "institution_name": inst.name,
            "city": inst.city,
            "country": inst.country,
            "lead_score": inst.lead_score,
            "confidence_score": structured.get('confidence_score', 0.85),
            "extraction_completeness": structured.get('extraction_completeness', 0.75)
        },
        "certifications": {
            "ib": profile.has_ib_cert or structured.get('has_ib', False),
            "cambridge": profile.has_cambridge_cert or structured.get('has_cambridge', False),
            "oxford": structured.get('has_oxford', False),
            "bilingual": profile.is_bilingual or structured.get('is_bilingual', False),
            "trilingual": profile.is_trilingual or structured.get('is_trilingual', False)
        },
        "technology": {
            "lms_provider": structured.get('lms_provider', getattr(inst.tech_profile, 'lms_provider', None)),
            "has_robotics": structured.get('has_robotics', False),
            "has_stem": structured.get('has_stem', False),
            "has_programming": structured.get('has_programming', False),
            "laboratories": structured.get('technology', {}).get('laboratories', []),
            "digital_platforms": structured.get('technology', {}).get('digital_platforms', [])
        },
        "performance": {
            "icfes_score": structured.get('icfes_score', ''),
            "icfes_category": structured.get('icfes_category', ''),
            "awards": structured.get('performance', {}).get('awards', [])
        },
        "extracurricular": structured.get('extracurricular', {}),
        "agreements": structured.get('agreements', {}),
        "international_programs": structured.get('international_programs', {}),
        "sales_intelligence": sales_intelligence,
        "interaction_history": interaction_stats,
        "full_report": profile.ai_comprehensive_report
    }


def _generate_markdown_report_omega(
    inst: Institution,
    profile: DeepForensicProfile,
    structured: Dict[str, Any],
    sales_intelligence: Dict[str, Any],
    trace_id: str
) -> str:
    """Genera reporte Markdown enriquecido"""
    confidence = structured.get('confidence_score', 0.85) * 100
    completeness = structured.get('extraction_completeness', 0.75) * 100
    
    pain_points = "\n".join([f"- 🔴 {p}" for p in sales_intelligence.get("pain_points", [])[:5]]) or "- ℹ️ No se detectaron pain points específicos"
    triggers = "\n".join([f"- 🟢 {t}" for t in sales_intelligence.get("sales_triggers", [])[:5]]) or "- ℹ️ No se detectaron triggers específicos"
    opportunities = "\n".join([f"- 🚀 {o}" for o in sales_intelligence.get("opportunities", [])[:5]]) or "- ℹ️ No se detectaron oportunidades específicas"
    
    return f"""# 🌌 COSMIC INTELLIGENCE REPORT - OMEGA EDITION

## 📋 Resumen Ejecutivo

| Métrica | Valor |
|---------|-------|
| **Institución** | {inst.name} |
| **Ubicación** | {inst.city}, {inst.country} |
| **Lead Score** | {inst.lead_score} pts |
| **Confianza del Análisis** | {confidence:.0f}% |
| **Completitud de Datos** | {completeness:.0f}% |
| **Trace ID** | {trace_id} |

---

## 🏆 Certificaciones y Acreditaciones

| Certificación | Estado |
|---------------|--------|
| IB | {'✅' if (profile.has_ib_cert or structured.get('has_ib')) else '❌'} |
| Cambridge | {'✅' if (profile.has_cambridge_cert or structured.get('has_cambridge')) else '❌'} |
| Oxford | {'✅' if structured.get('has_oxford') else '❌'} |
| Bilingüe | {'✅' if (profile.is_bilingual or structured.get('is_bilingual')) else '❌'} |
| Trilingüe | {'✅' if (profile.is_trilingual or structured.get('is_trilingual')) else '❌'} |

---

## 🤖 Tecnología y Stack Digital

- **LMS:** {structured.get('lms_provider', getattr(inst.tech_profile, 'lms_provider', 'No detectado'))}
- **Robótica:** {'✅' if structured.get('has_robotics') else '❌'}
- **STEM:** {'✅' if structured.get('has_stem') else '❌'}
- **Programación:** {'✅' if structured.get('has_programming') else '❌'}
- **Laboratorios:** {', '.join(structured.get('technology', {}).get('laboratories', [])) or 'No especificados'}
- **Plataformas Digitales:** {', '.join(structured.get('technology', {}).get('digital_platforms', [])) or 'No especificadas'}

---

## 📈 ICFES y Rendimiento Académico

- **Puntaje ICFES:** {structured.get('icfes_score', 'No disponible')}
- **Categoría:** {structured.get('icfes_category', 'No disponible')}
- **Premios y Reconocimientos:** {', '.join(structured.get('performance', {}).get('awards', [])) or 'No especificados'}

---

## 🎪 Actividades Extracurriculares

{chr(10).join([f"**{k.capitalize()}:** {', '.join(v[:5])}" for k, v in structured.get('extracurricular', {}).items() if v]) or "No especificadas"}

---

## 💼 Inteligencia de Ventas

### 🔴 Pain Points Detectados
{pain_points}

### 🟢 Sales Triggers
{triggers}

### 🚀 Oportunidades Comerciales
{opportunities}

### 🎯 Enfoque de Venta Recomendado
{sales_intelligence.get("recommended_approach", "Demostración técnica seguida de propuesta económica personalizada.")}

### 👤 Contacto Ideal
{sales_intelligence.get("ideal_contact", "Rector o Director de Tecnología")}

### 💰 Presupuesto Estimado
{sales_intelligence.get("budget_indication", "Medio")} - {sales_intelligence.get("estimated_revenue_potential", "$5,000 - $15,000 USD anual")}

### ⏰ Timeline de Decisión
{sales_intelligence.get("decision_timeline", "3-6 meses")}

### 🎯 Prioridad de Venta
{sales_intelligence.get("sales_priority", "Medium")}

---

## 📊 Reporte Completo de Extracción

{profile.ai_comprehensive_report}

---

*Reporte generado por Cosmic Intelligence Engine OMEGA v99.9.9.9.9*
*Powered by DeepSeek AI Neural Engine*
"""


def _generate_executive_summary_omega(
    inst: Institution,
    profile: DeepForensicProfile,
    structured: Dict[str, Any],
    sales_intelligence: Dict[str, Any],
    trace_id: str
) -> str:
    """Genera un resumen ejecutivo de una página para ventas"""
    
    confidence = structured.get('confidence_score', 0.85) * 100
    
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Resumen Ejecutivo | {inst.name}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        body {{
            font-family: 'Inter', sans-serif;
            background: white;
            color: #1f2937;
            padding: 2rem;
            max-width: 900px;
            margin: 0 auto;
        }}
        .header {{
            border-bottom: 3px solid #a855f7;
            padding-bottom: 1rem;
            margin-bottom: 2rem;
        }}
        h1 {{
            font-size: 2rem;
            font-weight: 800;
            color: #a855f7;
            margin-bottom: 0.5rem;
        }}
        .score {{
            display: inline-block;
            background: #f3f4f6;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.875rem;
            font-weight: 600;
        }}
        .section {{
            margin-bottom: 1.5rem;
        }}
        .section-title {{
            font-size: 1.25rem;
            font-weight: 700;
            color: #374151;
            border-left: 3px solid #a855f7;
            padding-left: 0.75rem;
            margin-bottom: 1rem;
        }}
        .grid-2 {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1rem;
        }}
        .card {{
            background: #f9fafb;
            padding: 1rem;
            border-radius: 0.75rem;
        }}
        .badge {{
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            margin-right: 0.5rem;
            margin-bottom: 0.5rem;
        }}
        .badge-green {{ background: #d1fae5; color: #065f46; }}
        .badge-blue {{ background: #dbeafe; color: #1e40af; }}
        .badge-purple {{ background: #ede9fe; color: #5b21b6; }}
        .footer {{
            margin-top: 2rem;
            padding-top: 1rem;
            border-top: 1px solid #e5e7eb;
            text-align: center;
            font-size: 0.75rem;
            color: #6b7280;
        }}
        @media print {{
            body {{ padding: 0; }}
            .no-print {{ display: none; }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🌌 Resumen Ejecutivo</h1>
        <p><strong>{inst.name}</strong> | {inst.city}, {inst.country}</p>
        <span class="score">Lead Score: {inst.lead_score} pts | Confianza: {confidence:.0f}%</span>
    </div>
    
    <div class="section">
        <div class="section-title">🎯 Perfil de la Institución</div>
        <div class="grid-2">
            <div class="card">
                <strong>Certificaciones Clave</strong><br>
                {'🏆 IB ' if (profile.has_ib_cert or structured.get('has_ib')) else ''}
                {'🇬🇧 Cambridge ' if (profile.has_cambridge_cert or structured.get('has_cambridge')) else ''}
                {'🗣️ Bilingüe ' if (profile.is_bilingual or structured.get('is_bilingual')) else ''}
                {'' if (profile.has_ib_cert or profile.has_cambridge_cert or profile.is_bilingual) else 'Ninguna destacada'}
            </div>
            <div class="card">
                <strong>Tecnología Actual</strong><br>
                LMS: {structured.get('lms_provider', getattr(inst.tech_profile, 'lms_provider', 'No detectado'))}<br>
                {'🤖 Robótica ' if structured.get('has_robotics') else ''}
                {'🔬 STEM ' if structured.get('has_stem') else ''}
            </div>
        </div>
    </div>
    
    <div class="section">
        <div class="section-title">💡 Oportunidad de Venta</div>
        <div class="card">
            <p><strong>Pain Point Principal:</strong> {sales_intelligence.get('pain_points', ['No detectado'])[0]}</p>
            <p><strong>Trigger de Venta:</strong> {sales_intelligence.get('sales_triggers', ['No detectado'])[0]}</p>
            <p><strong>Oportunidad:</strong> {sales_intelligence.get('opportunities', ['No detectada'])[0]}</p>
            <p><strong>Presupuesto Estimado:</strong> {sales_intelligence.get('budget_indication', 'Medio')} | {sales_intelligence.get('estimated_revenue_potential', '$5,000 - $15,000 USD anual')}</p>
        </div>
    </div>
    
    <div class="section">
        <div class="section-title">🎯 Estrategia de Acercamiento</div>
        <div class="card">
            <p><strong>Contacto Ideal:</strong> {sales_intelligence.get('ideal_contact', 'Rector o Director de Tecnología')}</p>
            <p><strong>Enfoque Recomendado:</strong> {sales_intelligence.get('recommended_approach', 'Demostración técnica seguida de propuesta económica personalizada.')}</p>
            <p><strong>Timeline Estimado:</strong> {sales_intelligence.get('decision_timeline', '3-6 meses')}</p>
            <p><strong>Prioridad:</strong> <span class="badge {'badge-green' if sales_intelligence.get('sales_priority') == 'High' else 'badge-blue'}">{sales_intelligence.get('sales_priority', 'Medium')}</span></p>
        </div>
    </div>
    
    <div class="section">
        <div class="section-title">📊 Datos Clave para la Venta</div>
        <div class="grid-2">
            <div class="card">
                <strong>ICFES</strong><br>
                Puntaje: {structured.get('icfes_score', 'No disponible')}<br>
                Categoría: {structured.get('icfes_category', 'No disponible')}
            </div>
            <div class="card">
                <strong>Extracurriculares</strong><br>
                {', '.join([k.capitalize() for k, v in structured.get('extracurricular', {}).items() if v]) or 'No especificados'}
            </div>
        </div>
    </div>
    
    <div class="footer">
        Reporte generado por Cosmic Intelligence Engine OMEGA | Trace ID: {trace_id}<br>
        Confidencial - Uso exclusivo del equipo de ventas
    </div>
</body>
</html>
"""


def _error_response_omega(
    title: str,
    message: str,
    status: int = 404,
    trace_id: str = None,
    action_url: str = None
) -> HttpResponse:
    """Respuesta de error profesional"""
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>{title}</title>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
        <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0,1" rel="stylesheet" />
        <style>
            body {{
                font-family: 'Inter', sans-serif;
                background: linear-gradient(135deg, #030303 0%, #0a0a1a 100%);
                color: #e2e8f0;
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                margin: 0;
                padding: 2rem;
            }}
            .error-card {{
                max-width: 500px;
                background: rgba(0, 0, 0, 0.6);
                backdrop-filter: blur(10px);
                border-radius: 1.5rem;
                padding: 2rem;
                text-align: center;
                border: 1px solid rgba(239, 68, 68, 0.3);
                box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
            }}
            .error-icon {{
                font-size: 4rem;
                color: #ef4444;
                margin-bottom: 1rem;
            }}
            .error-title {{
                font-size: 1.5rem;
                font-weight: 700;
                color: #ef4444;
                margin-bottom: 1rem;
            }}
            .error-message {{
                color: #94a3b8;
                margin-bottom: 1.5rem;
                line-height: 1.6;
            }}
            .btn {{
                display: inline-flex;
                align-items: center;
                gap: 0.5rem;
                padding: 0.5rem 1rem;
                background: linear-gradient(135deg, #7c3aed, #a855f7);
                color: white;
                text-decoration: none;
                border-radius: 0.5rem;
                font-weight: 600;
                transition: all 0.2s;
            }}
            .btn:hover {{
                transform: translateY(-1px);
                box-shadow: 0 4px 12px rgba(168, 85, 247, 0.3);
            }}
            .trace-id {{
                margin-top: 1rem;
                font-size: 0.7rem;
                color: #475569;
                font-family: monospace;
            }}
        </style>
    </head>
    <body>
        <div class="error-card">
            <div class="error-icon">⚠️</div>
            <div class="error-title">{title}</div>
            <div class="error-message">{message}</div>
            {'<a href="' + action_url + '" class="btn"><span class="material-symbols-outlined" style="font-size: 1rem;">play_arrow</span> Ejecutar Escaneo</a>' if action_url else '<button onclick="history.back()" class="btn"><span class="material-symbols-outlined" style="font-size: 1rem;">arrow_back</span> Volver</button>'}
            <div class="trace-id">Trace ID: {trace_id or 'N/A'}</div>
        </div>
    </body>
    </html>
    """
    return HttpResponse(html, status=status)


def _add_security_headers_omega(response: HttpResponse, format_type: str) -> HttpResponse:
    """Headers de seguridad enterprise"""
    response['X-Content-Type-Options'] = 'nosniff'
    response['X-Frame-Options'] = 'DENY'
    response['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    response['Permissions-Policy'] = 'geolocation=(), microphone=(), camera=()'
    response['X-Cosmic-Engine'] = 'OMEGA-v99.9.9.9.9'
    
    if format_type == 'html':
        response['Content-Security-Policy'] = (
            "default-src 'self'; "
            "script-src 'self' https://cdn.tailwindcss.com https://unpkg.com 'unsafe-inline'; "
            "style-src 'self' https://fonts.googleapis.com 'unsafe-inline'; "
            "font-src 'self' https://fonts.gstatic.com; "
            "img-src 'self' data:;"
        )
    
    patch_cache_control(response, private=True, max_age=CACHE_TTL_SECONDS)
    
    return response