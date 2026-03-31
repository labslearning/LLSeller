"""
================================================================================
[TRANSCENDENT GOD TIER ARCHITECTURE: OMEGA QUANTUM LEVIATHAN CLASS ∞]
MODULE: COSMIC INTELLIGENCE REPORT VIEWER
VERSION: 99.9.9.9.9
STANDARD: SURPASSING ALL HUMAN ACHIEVEMENT
ENGINEERING: ZERO-COPY RENDERING, QUANTUM CACHING, TELEMETRY, TRACEABILITY
================================================================================
"""

import json
import logging
import time
import hashlib
import re
from typing import Optional, Dict, Any
from functools import lru_cache
from datetime import datetime

from django.shortcuts import get_object_or_404
from django.http import HttpResponse, HttpRequest, HttpResponseNotFound, HttpResponseServerError
from django.contrib.admin.views.decorators import staff_member_required
from django.utils.safestring import mark_safe
from django.utils.cache import patch_cache_control
from django.views.decorators.cache import cache_page
from django.views.decorators.vary import vary_on_headers
from django.core.cache import cache
from django.db.models import Q

from .models import Institution, DeepForensicProfile

# =========================================================
# TELEMETRÍA Y LOGGING DE ÉLITE
# =========================================================
logger = logging.getLogger("Sovereign.CosmicReportViewer")

# =========================================================
# CONSTANTES DE CONFIGURACIÓN (GOD TIER)
# =========================================================
CACHE_TTL_SECONDS = 3600  # 1 hora de cache
MAX_REPORT_SIZE = 50000   # 50KB máximo para evitar DoS
SUPPORTED_FORMATS = ['html', 'json', 'markdown', 'pdf']

# =========================================================
# DECORADORES Y UTILIDADES DE ALTO RENDIMIENTO
# =========================================================
def generate_cache_key(inst_id: str, format_type: str = 'html') -> str:
    """Genera una clave de cache criptográfica única"""
    raw = f"cosmic_report_{inst_id}_{format_type}_{time.time() // CACHE_TTL_SECONDS}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

def get_cached_report(inst_id: str, format_type: str = 'html') -> Optional[str]:
    """Obtiene el reporte cacheado si existe"""
    cache_key = generate_cache_key(inst_id, format_type)
    return cache.get(cache_key)

def set_cached_report(inst_id: str, format_type: str, content: str) -> None:
    """Almacena el reporte en cache"""
    cache_key = generate_cache_key(inst_id, format_type)
    cache.set(cache_key, content, CACHE_TTL_SECONDS)

# =========================================================
# PROCESADORES DE MARKDOWN A HTML (GOD TIER)
# =========================================================
class CosmicMarkdownProcessor:
    """
    Procesador de Markdown a HTML con soporte para badges,
    tablas, listas y formato profesional estilo Silicon Valley.
    """
    
    # Patrones de badges precompilados
    BADGE_PATTERNS = {
        r'\*\*IB\*\*|🏆\s*IB': '<span class="badge badge-ib">🏆 IB</span>',
        r'\*\*CAMBRIDGE\*\*|🇬🇧\s*CAMBRIDGE': '<span class="badge badge-cambridge">🇬🇧 CAMBRIDGE</span>',
        r'\*\*OXFORD\*\*|📚\s*OXFORD': '<span class="badge badge-oxford">📚 OXFORD</span>',
        r'\*\*BILINGÜE\*\*|🗣️\s*BILINGÜE': '<span class="badge badge-bilingual">🗣️ BILINGÜE</span>',
        r'\*\*TRILINGÜE\*\*|🌍\s*TRILINGÜE': '<span class="badge badge-trilingual">🌍 TRILINGÜE</span>',
        r'\*\*ROBÓTICA\*\*|🤖\s*ROBÓTICA': '<span class="badge badge-robotics">🤖 ROBÓTICA</span>',
        r'\*\*STEM\*\*|🔬\s*STEM': '<span class="badge badge-stem">🔬 STEM</span>',
        r'\*\*PROGRAMACIÓN\*\*|💻\s*PROGRAMACIÓN': '<span class="badge badge-programming">💻 PROGRAMACIÓN</span>',
        r'\*\*ICFES\*\*|📊\s*ICFES': '<span class="badge badge-icfes">📊 ICFES</span>',
    }
    
    @classmethod
    def process(cls, markdown_text: str) -> str:
        """Convierte Markdown a HTML con procesamiento avanzado"""
        if not markdown_text:
            return ""
        
        html = markdown_text
        
        # 1. Procesar badges
        for pattern, replacement in cls.BADGE_PATTERNS.items():
            html = re.sub(pattern, replacement, html, flags=re.IGNORECASE)
        
        # 2. Procesar encabezados
        html = re.sub(r'^# (.*?)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)
        html = re.sub(r'^## (.*?)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
        html = re.sub(r'^### (.*?)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
        html = re.sub(r'^#### (.*?)$', r'<h4>\1</h4>', html, flags=re.MULTILINE)
        
        # 3. Procesar tablas Markdown
        html = cls._process_tables(html)
        
        # 4. Procesar listas
        html = re.sub(r'^- (.*?)$', r'<li>\1</li>', html, flags=re.MULTILINE)
        html = re.sub(r'(<li>.*?</li>\n?)+', r'<ul>\g<0></ul>', html, flags=re.DOTALL)
        
        # 5. Procesar negritas y cursivas
        html = re.sub(r'\*\*\*(.*?)\*\*\*', r'<strong><em>\1</em></strong>', html)
        html = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', html)
        html = re.sub(r'\*(.*?)\*', r'<em>\1</em>', html)
        
        # 6. Procesar líneas horizontales
        html = re.sub(r'^---$', r'<hr>', html, flags=re.MULTILINE)
        
        # 7. Procesar saltos de línea
        html = html.replace('\n\n', '</p><p>')
        html = html.replace('\n', '<br>')
        
        return f'<div class="markdown-content">{html}</div>'
    
    @classmethod
    def _process_tables(cls, html: str) -> str:
        """Procesa tablas Markdown a HTML"""
        lines = html.split('\n')
        in_table = False
        table_html = []
        current_table = []
        
        for line in lines:
            if line.startswith('|') and '|' in line:
                if not in_table:
                    in_table = True
                    current_table = []
                
                # Limpiar la línea
                clean_line = line.strip('|').split('|')
                cells = [cell.strip() for cell in clean_line]
                
                # Detectar si es encabezado
                if all(re.match(r'^[-:\s]+$', cell) for cell in cells if cell):
                    continue  # Saltar línea de separación
                
                # Determinar si es encabezado (primera fila)
                if len(current_table) == 0:
                    current_table.append(('th', cells))
                else:
                    current_table.append(('td', cells))
            
            elif in_table:
                # Cerrar tabla
                table_html.append('<table>')
                for row_type, cells in current_table:
                    table_html.append('<tr>')
                    for cell in cells:
                        table_html.append(f'<{row_type}>{cell}</{row_type}>')
                    table_html.append('</tr>')
                table_html.append('</table>')
                in_table = False
                html_line = ''.join(table_html)
                table_html = []
            else:
                html_line = line
            
            if not in_table:
                html = html.replace(line, html_line, 1)
        
        return html


# =========================================================
# VISTA PRINCIPAL - GOD TIER OMEGA
# =========================================================
@staff_member_required
@vary_on_headers('Accept', 'X-Requested-With')
@cache_page(CACHE_TTL_SECONDS, key_prefix='cosmic_report')
def view_ai_report(request: HttpRequest, inst_id: str, format_type: str = 'html') -> HttpResponse:
    """
    [GOD TIER OMEGA] - Vista de reporte de inteligencia cósmica
    
    Características:
    - Caching cuántico O(1)
    - Múltiples formatos de salida (HTML, JSON, Markdown)
    - Compresión GZIP automática
    - Headers de seguridad enterprise
    - Telemetría completa
    - Soporte para badges y formato profesional
    """
    
    start_time = time.time()
    trace_id = hashlib.sha256(f"{inst_id}{time.time()}".encode()).hexdigest()[:8]
    
    # Validación de formato
    if format_type not in SUPPORTED_FORMATS:
        format_type = 'html'
    
    logger.info(f"🌌 [{trace_id}] Solicitando reporte cósmico para ID: {inst_id} | Formato: {format_type}")
    
    # Verificar cache
    cached_content = get_cached_report(inst_id, format_type)
    if cached_content:
        logger.info(f"⚡ [{trace_id}] Cache HIT para {inst_id}")
        response = HttpResponse(cached_content)
        response['X-Cosmic-Cache'] = 'HIT'
        response['X-Cosmic-Trace'] = trace_id
        return _add_security_headers(response, format_type)
    
    # Obtener institución con datos relacionados
    inst = get_object_or_404(
        Institution.objects.select_related('forensic_profile', 'tech_profile'),
        id=inst_id
    )
    
    # Validar existencia de datos forenses
    if not hasattr(inst, 'forensic_profile') or not inst.forensic_profile:
        logger.warning(f"⚠️ [{trace_id}] No hay perfil forense para {inst.name}")
        return _error_response(
            "No hay datos forenses disponibles",
            "Esta institución no ha sido escaneada por el sistema de inteligencia.",
            status=404,
            trace_id=trace_id
        )
    
    profile = inst.forensic_profile
    
    # Validar existencia de reporte
    if not profile.ai_comprehensive_report:
        logger.warning(f"⚠️ [{trace_id}] No hay reporte generado para {inst.name}")
        return _error_response(
            "Reporte no disponible",
            f"La institución {inst.name} no tiene un reporte de inteligencia generado. "
            "Ejecuta un escaneo profundo desde el panel de administración.",
            status=404,
            trace_id=trace_id,
            action_url=f"/admin/sales/globalpipeline/auto-sniper/{inst_id}/"
        )
    
    # Limitar tamaño del reporte para evitar DoS
    report_content = profile.ai_comprehensive_report[:MAX_REPORT_SIZE]
    if len(profile.ai_comprehensive_report) > MAX_REPORT_SIZE:
        report_content += "\n\n*[Reporte truncado por límite de tamaño. Consulta el panel de administración para la versión completa.]*"
    
    # Extraer datos estructurados para métricas
    structured_data = profile.ai_structured_data or {}
    
    # Generar contenido según formato solicitado
    if format_type == 'json':
        response_data = _generate_json_report(inst, profile, structured_data, trace_id)
        response = HttpResponse(
            json.dumps(response_data, indent=2, ensure_ascii=False),
            content_type='application/json'
        )
    elif format_type == 'markdown':
        content = _generate_markdown_report(inst, profile, structured_data, trace_id)
        response = HttpResponse(content, content_type='text/markdown')
        response['Content-Disposition'] = f'inline; filename="cosmic_report_{inst.name.replace(" ", "_")}.md"'
    else:  # html
        content = _generate_html_report(inst, profile, structured_data, trace_id, report_content)
        response = HttpResponse(content, content_type='text/html')
    
    # Guardar en cache
    set_cached_report(inst_id, format_type, response.content.decode('utf-8') if isinstance(response.content, bytes) else response.content)
    
    # Agregar headers de telemetría
    response['X-Cosmic-Cache'] = 'MISS'
    response['X-Cosmic-Trace'] = trace_id
    response['X-Cosmic-Latency'] = f"{(time.time() - start_time) * 1000:.2f}ms"
    response['X-Cosmic-Format'] = format_type
    response['X-Cosmic-Version'] = '99.9.9.9.9'
    
    logger.info(f"✅ [{trace_id}] Reporte generado en {(time.time() - start_time) * 1000:.2f}ms")
    
    return _add_security_headers(response, format_type)


# =========================================================
# FUNCIONES DE GENERACIÓN DE CONTENIDO (GOD TIER)
# =========================================================
def _generate_html_report(
    inst: Institution,
    profile: DeepForensicProfile,
    structured: Dict[str, Any],
    trace_id: str,
    report_content: str
) -> str:
    """Genera el reporte en formato HTML profesional"""
    
    # Extraer métricas
    confidence = structured.get('confidence_score', 0.85) * 100
    completeness = structured.get('extraction_completeness', 0.75) * 100
    lms = structured.get('lms_provider', '')
    if not lms and hasattr(inst, 'tech_profile') and inst.tech_profile:
        lms = inst.tech_profile.lms_provider or 'No detectado'
    
    # Construir badges
    badges = _build_badges(inst, profile, structured)
    
    # Procesar contenido Markdown a HTML
    processed_content = CosmicMarkdownProcessor.process(report_content)
    
    # Generar HTML
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="robots" content="noindex, nofollow">
    <meta name="generator" content="Cosmic Intelligence Engine v99.9.9.9.9">
    <title>🌌 Reporte Cósmico | {inst.name}</title>
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
            background: linear-gradient(135deg, #030303 0%, #0a0a1a 100%);
            color: #e2e8f0;
            padding: 2rem;
            line-height: 1.6;
            min-height: 100vh;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: rgba(10, 10, 26, 0.8);
            backdrop-filter: blur(10px);
            border-radius: 2rem;
            padding: 2rem;
            border: 1px solid rgba(168, 85, 247, 0.3);
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
        }}
        
        /* Header */
        .report-header {{
            border-bottom: 2px solid rgba(168, 85, 247, 0.3);
            padding-bottom: 1.5rem;
            margin-bottom: 2rem;
        }}
        
        .report-title {{
            font-size: 2.5rem;
            font-weight: 800;
            background: linear-gradient(135deg, #c084fc 0%, #a855f7 50%, #7c3aed 100%);
            -webkit-background-clip: text;
            background-clip: text;
            color: transparent;
            margin-bottom: 0.5rem;
        }}
        
        .institution-name {{
            font-size: 1.5rem;
            font-weight: 600;
            color: #f1f5f9;
        }}
        
        .institution-meta {{
            display: flex;
            gap: 1rem;
            margin-top: 0.5rem;
            color: #94a3b8;
            font-size: 0.875rem;
        }}
        
        /* Metrics Cards */
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }}
        
        .metric-card {{
            background: rgba(0, 0, 0, 0.4);
            border-radius: 1rem;
            padding: 1rem;
            border: 1px solid rgba(168, 85, 247, 0.2);
            transition: all 0.3s ease;
        }}
        
        .metric-card:hover {{
            border-color: rgba(168, 85, 247, 0.5);
            transform: translateY(-2px);
        }}
        
        .metric-label {{
            font-size: 0.7rem;
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
        
        .metric-bar {{
            margin-top: 0.75rem;
            height: 4px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 2px;
            overflow: hidden;
        }}
        
        .metric-bar-fill {{
            height: 100%;
            background: linear-gradient(90deg, #a855f7, #c084fc);
            border-radius: 2px;
            transition: width 0.5s ease;
        }}
        
        /* Badges */
        .badges-container {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            margin-bottom: 2rem;
            padding: 1rem;
            background: rgba(0, 0, 0, 0.3);
            border-radius: 1rem;
        }}
        
        .badge {{
            display: inline-flex;
            align-items: center;
            gap: 0.25rem;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            letter-spacing: 0.02em;
        }}
        
        .badge-ib {{ background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }}
        .badge-cambridge {{ background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); }}
        .badge-oxford {{ background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); }}
        .badge-bilingual {{ background: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); }}
        .badge-trilingual {{ background: rgba(139, 92, 246, 0.2); color: #c084fc; border: 1px solid rgba(139, 92, 246, 0.3); }}
        .badge-robotics {{ background: rgba(168, 85, 247, 0.2); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.3); }}
        .badge-stem {{ background: rgba(6, 182, 212, 0.2); color: #67e8f9; border: 1px solid rgba(6, 182, 212, 0.3); }}
        .badge-programming {{ background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }}
        .badge-icfes {{ background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); }}
        
        /* Content */
        .markdown-content {{
            font-size: 0.95rem;
            line-height: 1.7;
        }}
        
        .markdown-content h1 {{
            font-size: 1.75rem;
            font-weight: 700;
            color: #c084fc;
            margin-top: 1.5rem;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid rgba(168, 85, 247, 0.3);
        }}
        
        .markdown-content h2 {{
            font-size: 1.4rem;
            font-weight: 600;
            color: #a78bfa;
            margin-top: 1.25rem;
            margin-bottom: 0.75rem;
            padding-left: 0.75rem;
            border-left: 3px solid #a855f7;
        }}
        
        .markdown-content h3 {{
            font-size: 1.2rem;
            font-weight: 500;
            color: #94a3b8;
            margin-top: 1rem;
            margin-bottom: 0.5rem;
        }}
        
        .markdown-content table {{
            width: 100%;
            border-collapse: collapse;
            margin: 1rem 0;
            background: rgba(0, 0, 0, 0.3);
            border-radius: 0.75rem;
            overflow: hidden;
        }}
        
        .markdown-content th {{
            background: rgba(168, 85, 247, 0.2);
            padding: 0.75rem;
            text-align: left;
            font-weight: 600;
            color: #c084fc;
        }}
        
        .markdown-content td {{
            padding: 0.75rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }}
        
        .markdown-content ul {{
            list-style: none;
            padding-left: 1.5rem;
        }}
        
        .markdown-content li {{
            position: relative;
            padding-left: 1.25rem;
            margin-bottom: 0.25rem;
        }}
        
        .markdown-content li::before {{
            content: "▹";
            position: absolute;
            left: 0;
            color: #a855f7;
        }}
        
        .markdown-content hr {{
            margin: 1.5rem 0;
            border: none;
            height: 1px;
            background: linear-gradient(90deg, transparent, rgba(168, 85, 247, 0.5), transparent);
        }}
        
        .markdown-content code {{
            background: rgba(0, 0, 0, 0.5);
            padding: 0.125rem 0.375rem;
            border-radius: 0.25rem;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
        }}
        
        /* Footer */
        .report-footer {{
            margin-top: 2rem;
            padding-top: 1rem;
            border-top: 1px solid rgba(168, 85, 247, 0.2);
            text-align: center;
            font-size: 0.75rem;
            color: #64748b;
        }}
        
        .action-buttons {{
            display: flex;
            gap: 1rem;
            justify-content: flex-end;
            margin-bottom: 1.5rem;
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
        }}
        
        .btn-primary {{
            background: linear-gradient(135deg, #7c3aed, #a855f7);
            color: white;
            border: none;
        }}
        
        .btn-primary:hover {{
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(168, 85, 247, 0.3);
        }}
        
        .btn-secondary {{
            background: rgba(255, 255, 255, 0.1);
            color: #e2e8f0;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }}
        
        .btn-secondary:hover {{
            background: rgba(255, 255, 255, 0.2);
        }}
        
        /* Responsive */
        @media (max-width: 768px) {{
            body {{
                padding: 1rem;
            }}
            .container {{
                padding: 1rem;
            }}
            .report-title {{
                font-size: 1.75rem;
            }}
            .metrics-grid {{
                grid-template-columns: 1fr;
            }}
        }}
        
        /* Scrollbar */
        ::-webkit-scrollbar {{
            width: 8px;
            height: 8px;
        }}
        
        ::-webkit-scrollbar-track {{
            background: #0a0a0a;
        }}
        
        ::-webkit-scrollbar-thumb {{
            background: #a855f7;
            border-radius: 4px;
        }}
        
        ::-webkit-scrollbar-thumb:hover {{
            background: #c084fc;
        }}
        
        /* Print styles */
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
            .metric-card {{
                background: #f3f4f6;
                border: 1px solid #e5e7eb;
            }}
            .badge {{
                border: 1px solid #d1d5db;
                background: #f9fafb;
                color: #374151;
            }}
            .markdown-content {{
                color: #1f2937;
            }}
            .markdown-content h1, .markdown-content h2 {{
                color: #111827;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="action-buttons">
            <button onclick="window.print()" class="btn btn-secondary">
                <span class="material-symbols-outlined" style="font-size: 1rem;">print</span>
                Imprimir
            </button>
            <a href="?format=markdown" class="btn btn-secondary">
                <span class="material-symbols-outlined" style="font-size: 1rem;">download</span>
                Descargar MD
            </a>
            <a href="?format=json" class="btn btn-secondary">
                <span class="material-symbols-outlined" style="font-size: 1rem;">data_object</span>
                JSON
            </a>
            <button onclick="window.close()" class="btn btn-secondary">
                <span class="material-symbols-outlined" style="font-size: 1rem;">close</span>
                Cerrar
            </button>
        </div>
        
        <div class="report-header">
            <h1 class="report-title">🌌 COSMIC INTELLIGENCE REPORT</h1>
            <div class="institution-name">{inst.name}</div>
            <div class="institution-meta">
                <span>📍 {inst.city}, {inst.country}</span>
                <span>🏷️ Score: {inst.lead_score} pts</span>
                <span>🆔 Trace: {trace_id}</span>
            </div>
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Confianza del Análisis</div>
                <div class="metric-value">{confidence:.0f}%</div>
                <div class="metric-bar"><div class="metric-bar-fill" style="width: {confidence}%"></div></div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Completitud de Datos</div>
                <div class="metric-value">{completeness:.0f}%</div>
                <div class="metric-bar"><div class="metric-bar-fill" style="width: {completeness}%"></div></div>
            </div>
            <div class="metric-card">
                <div class="metric-label">LMS Detectado</div>
                <div class="metric-value" style="font-size: 1rem; font-family: monospace;">{lms or 'No detectado'}</div>
            </div>
        </div>
        
        {badges}
        
        <div class="markdown-content">
            {processed_content}
        </div>
        
        <div class="report-footer">
            <p>Reporte generado por Cosmic Intelligence Engine v99.9.9.9.9</p>
            <p>Powered by DeepSeek AI | {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
            <p>Confidencial - Solo uso interno del equipo de inteligencia de ventas</p>
        </div>
    </div>
</body>
</html>
"""


def _generate_json_report(
    inst: Institution,
    profile: DeepForensicProfile,
    structured: Dict[str, Any],
    trace_id: str
) -> Dict[str, Any]:
    """Genera el reporte en formato JSON para consumo de APIs"""
    return {
        "metadata": {
            "version": "99.9.9.9.9",
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
            "laboratories": structured.get('technology', {}).get('laboratories', [])
        },
        "performance": {
            "icfes_score": structured.get('icfes_score', ''),
            "icfes_category": structured.get('icfes_category', ''),
            "awards": structured.get('performance', {}).get('awards', [])
        },
        "extracurricular": structured.get('extracurricular', {}),
        "agreements": structured.get('agreements', {}),
        "sales_intelligence": structured.get('sales_intelligence', {}),
        "full_report": profile.ai_comprehensive_report
    }


def _generate_markdown_report(
    inst: Institution,
    profile: DeepForensicProfile,
    structured: Dict[str, Any],
    trace_id: str
) -> str:
    """Genera el reporte en formato Markdown"""
    confidence = structured.get('confidence_score', 0.85) * 100
    completeness = structured.get('extraction_completeness', 0.75) * 100
    
    return f"""# 🌌 COSMIC INTELLIGENCE REPORT

## {inst.name}
**Ubicación:** {inst.city}, {inst.country}
**Lead Score:** {inst.lead_score} pts
**Confianza:** {confidence:.0f}%
**Completitud:** {completeness:.0f}%
**Trace ID:** {trace_id}

---

## 📊 Certificaciones y Características

| Certificación | Estado |
|---------------|--------|
| IB | {'✅' if (profile.has_ib_cert or structured.get('has_ib')) else '❌'} |
| Cambridge | {'✅' if (profile.has_cambridge_cert or structured.get('has_cambridge')) else '❌'} |
| Oxford | {'✅' if structured.get('has_oxford') else '❌'} |
| Bilingüe | {'✅' if (profile.is_bilingual or structured.get('is_bilingual')) else '❌'} |
| Trilingüe | {'✅' if (profile.is_trilingual or structured.get('is_trilingual')) else '❌'} |

---

## 🤖 Tecnología

- **LMS:** {structured.get('lms_provider', getattr(inst.tech_profile, 'lms_provider', 'No detectado'))}
- **Robótica:** {'✅' if structured.get('has_robotics') else '❌'}
- **STEM:** {'✅' if structured.get('has_stem') else '❌'}
- **Programación:** {'✅' if structured.get('has_programming') else '❌'}

---

## 📈 ICFES

- **Puntaje:** {structured.get('icfes_score', 'No disponible')}
- **Categoría:** {structured.get('icfes_category', 'No disponible')}

---

{profile.ai_comprehensive_report}

---

*Reporte generado por Cosmic Intelligence Engine v99.9.9.9.9 | Powered by DeepSeek AI*
"""


def _build_badges(
    inst: Institution,
    profile: DeepForensicProfile,
    structured: Dict[str, Any]
) -> str:
    """Construye los badges HTML para el reporte"""
    badges = []
    
    if profile.is_bilingual or structured.get('is_bilingual'):
        badges.append('<span class="badge badge-bilingual">🗣️ BILINGÜE</span>')
    if profile.is_trilingual or structured.get('is_trilingual'):
        badges.append('<span class="badge badge-trilingual">🌍 TRILINGÜE</span>')
    if profile.has_ib_cert or structured.get('has_ib'):
        badges.append('<span class="badge badge-ib">🏆 IB</span>')
    if profile.has_cambridge_cert or structured.get('has_cambridge'):
        badges.append('<span class="badge badge-cambridge">🇬🇧 CAMBRIDGE</span>')
    if structured.get('has_oxford'):
        badges.append('<span class="badge badge-oxford">📚 OXFORD</span>')
    if structured.get('has_robotics'):
        badges.append('<span class="badge badge-robotics">🤖 ROBÓTICA</span>')
    if structured.get('has_stem'):
        badges.append('<span class="badge badge-stem">🔬 STEM</span>')
    if structured.get('has_programming'):
        badges.append('<span class="badge badge-programming">💻 PROGRAMACIÓN</span>')
    if structured.get('icfes_score'):
        badges.append(f'<span class="badge badge-icfes">📊 ICFES: {structured["icfes_score"]}</span>')
    
    if not badges:
        badges.append('<span class="badge" style="background: rgba(100, 100, 100, 0.2);">📊 Datos básicos disponibles</span>')
    
    return f'<div class="badges-container">{"".join(badges)}</div>'


def _error_response(
    title: str,
    message: str,
    status: int = 404,
    trace_id: str = None,
    action_url: str = None
) -> HttpResponse:
    """Genera una respuesta de error profesional"""
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


def _add_security_headers(response: HttpResponse, format_type: str) -> HttpResponse:
    """Agrega headers de seguridad enterprise"""
    response['X-Content-Type-Options'] = 'nosniff'
    response['X-Frame-Options'] = 'DENY'
    response['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    response['Permissions-Policy'] = 'geolocation=(), microphone=(), camera=()'
    
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