import math
import logging
from typing import Dict, Any
from django.utils import timezone
from sales.models import Institution

logger = logging.getLogger("PredictiveScorer")

class PredictiveLeadScorer:
    """
    Motor de Scoring Predictivo B2B (Enterprise-Grade).
    Combina Firmographics, Technographics y Premium Business Intelligence 
    con decaimiento temporal asintótico.
    """
    
    # [NIVEL DIOS 1]: Matriz de Pesos Multidimensional
    WEIGHTS = {
        'FIRMOGRAPHICS': {
            'is_private': 20.0,       # Privados tienen mayor agilidad de presupuesto
            'has_email': 15.0,        # Lead accionable
            'size_enterprise': 15.0,  # Colegios grandes (>800)
        },
        'TECHNOGRAPHICS': {
            'tech_no_lms': 25.0,      # Océano Azul: Urgencia absoluta de digitalización
            'tech_legacy_lms': 15.0,  # Listos para reemplazo (Moodle, Chamilo)
            'tech_premium_lms': 20.0, # Presupuesto Alto Confirmado (Schoolnet, Phidias, Canvas)
            'has_analytics': 10.0,    # Tomadores de decisiones basados en datos (GA4, HubSpot)
        },
        'BUSINESS_INTEL': {
            'cert_ib': 30.0,          # [VIP] Bachillerato Internacional = High Ticket Client
            'cert_cambridge': 20.0,   # [VIP] Certificación Cambridge
            'is_bilingual': 15.0,     # Perfil socioeconómico alto
            'is_campestre': 5.0,      # Infraestructura amplia
            'has_linkedin': 15.0,     # [VIP] Accesibilidad corporativa (B2B Outreach posible)
        }
    }
    
    # [NIVEL DIOS 2]: Constante de Media Vida (Half-Life)
    HALF_LIFE_DAYS = 60.0 
    DECAY_FLOOR_MULTIPLIER = 0.4 # Un lead nunca perderá más del 60% de su valor por tiempo

    @classmethod
    def _calculate_base_score(cls, inst: Institution) -> float:
        """Auditoría profunda del ADN del prospecto para calcular el Score Puro."""
        score = 0.0
        tech = inst.tech_stack or {}
        bi = tech.get('business_intel', {})
        flags = bi.get('premium_flags', [])
        social = bi.get('social_media', {})
        
        # --- 1. FIRMOGRAPHICS ---
        if inst.is_private: score += cls.WEIGHTS['FIRMOGRAPHICS']['is_private']
        if inst.email: score += cls.WEIGHTS['FIRMOGRAPHICS']['has_email']
        if inst.student_count and inst.student_count > 800:
            score += cls.WEIGHTS['FIRMOGRAPHICS']['size_enterprise']

        # --- 2. TECHNOGRAPHICS ---
        if tech:
            if tech.get('has_lms'):
                lms_type = tech.get('lms_type', '')
                if lms_type in ['moodle', 'chamilo', 'blackboard']:
                    score += cls.WEIGHTS['TECHNOGRAPHICS']['tech_legacy_lms']
                elif lms_type in ['schoolnet', 'phidias', 'canvas', 'educamos']:
                    score += cls.WEIGHTS['TECHNOGRAPHICS']['tech_premium_lms']
            else:
                score += cls.WEIGHTS['TECHNOGRAPHICS']['tech_no_lms']
                
            if tech.get('analytics_ga') or tech.get('crm_hubspot'):
                score += cls.WEIGHTS['TECHNOGRAPHICS']['has_analytics']

        # --- 3. BUSINESS INTELLIGENCE (PRESTIGIO Y VENTAS) ---
        if 'cert_ib' in flags: score += cls.WEIGHTS['BUSINESS_INTEL']['cert_ib']
        if 'cert_cambridge' in flags: score += cls.WEIGHTS['BUSINESS_INTEL']['cert_cambridge']
        if 'is_bilingual' in flags: score += cls.WEIGHTS['BUSINESS_INTEL']['is_bilingual']
        if 'is_campestre' in flags: score += cls.WEIGHTS['BUSINESS_INTEL']['is_campestre']
        if 'linkedin' in social: score += cls.WEIGHTS['BUSINESS_INTEL']['has_linkedin']

        # El algoritmo permite sumar más de 100 internamente, pero el tope visual es 100
        return min(score, 100.0)

    @classmethod
    def _apply_time_decay(cls, base_score: float, last_updated) -> int:
        """
        [NIVEL DIOS 3]: Decaimiento Exponencial con Límite Asintótico.
        Un colegio élite viejo sigue siendo mejor que un colegio pobre nuevo.
        """
        if not last_updated:
            return int(base_score)
            
        days_old = (timezone.now() - last_updated).days
        if days_old <= 0:
            return int(base_score)
            
        # Fórmula: S(t) = S_0 * [(1 - Floor) * (1/2)^(t/t_half) + Floor]
        decay_factor = math.pow(0.5, days_old / cls.HALF_LIFE_DAYS)
        floor = cls.DECAY_FLOOR_MULTIPLIER
        
        final_score = base_score * ((1.0 - floor) * decay_factor + floor)
        
        return int(max(final_score, 0))

    @classmethod
    def score_single(cls, inst: Institution) -> int:
        """Califica, aplica decaimiento y persiste un solo prospecto."""
        base_score = cls._calculate_base_score(inst)
        # Usamos updated_at o last_scored_at para el decaimiento de inteligencia
        time_reference = inst.last_scored_at or inst.created_at
        final_score = cls._apply_time_decay(base_score, time_reference)
        
        # Solo impactamos la BD si el score realmente cambió (Ahorro de I/O)
        if inst.lead_score != final_score:
            inst.lead_score = final_score
            inst.save(update_fields=['lead_score'])
            
        return final_score

    @classmethod
    def bulk_score_all(cls, batch_size=2000):
        """
        [NIVEL DIOS 4]: Procesamiento en Lote de Ultra Alta Velocidad.
        Capaz de recalcular el pipeline de ventas entero en O(1) queries de escritura.
        """
        logger.info("⚙️ [SCORING] Iniciando recálculo masivo del Pipeline de Ventas...")
        
        # Usamos .iterator() para no saturar la RAM si la base de datos crece a +50k leads
        institutions = []
        updates_needed = []
        
        queryset = Institution.objects.filter(is_active=True)
        
        for inst in queryset.iterator(chunk_size=batch_size):
            old_score = inst.lead_score
            
            base = cls._calculate_base_score(inst)
            time_ref = inst.last_scored_at or inst.created_at
            new_score = cls._apply_time_decay(base, time_ref)
            
            if old_score != new_score:
                inst.lead_score = new_score
                updates_needed.append(inst)
                
            # Ejecutar guardado en bloques para evitar Timeout en PostgreSQL
            if len(updates_needed) >= batch_size:
                Institution.objects.bulk_update(updates_needed, ['lead_score'])
                institutions.extend(updates_needed)
                updates_needed = []

        # Guardar el remanente
        if updates_needed:
            Institution.objects.bulk_update(updates_needed, ['lead_score'])
            institutions.extend(updates_needed)
            
        logger.info(f"⚡ [SCORING] Pipeline Optimizado. {len(institutions)} leads alterados/actualizados.")
        return len(institutions)