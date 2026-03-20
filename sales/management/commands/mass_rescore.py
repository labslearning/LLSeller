import os
import time
import pandas as pd
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from django.db.models import Count, Q

# Importamos nuestro motor God-Tier
from sales.engine.ml_scoring import (
    get_active_model_path, 
    get_cached_model, 
    extract_inference_data
)
from sales.models import Institution

class Command(BaseCommand):
    help = "🚀 [GOD TIER] Backtesting Masivo: Recalcula el Score de TODA la base de datos."

    def handle(self, *args, **kwargs):
        self.stdout.write(self.style.SUCCESS(
            "\n╔══════════════════════════════════════════════════════════════════════════════╗"
            "\n║ 🧠 INICIANDO RECALIBRACIÓN GLOBAL Y BACKTESTING DEL ORÁCULO AI                 ║"
            "\n╚══════════════════════════════════════════════════════════════════════════════╝\n"
        ))

        # 1. VERIFICACIÓN DEL CEREBRO
        model_path = get_active_model_path()
        if not model_path or not os.path.exists(model_path):
            self.stdout.write(self.style.ERROR("❌ [FATAL] No hay modelo activo. Ejecuta train_model() primero."))
            return
            
        self.stdout.write(self.style.WARNING(f"📡 Cargando Matriz Neuronal en L1 Cache: {os.path.basename(model_path)}"))
        pipeline = get_cached_model(model_path)

        # 2. EXTRACCIÓN GLOBAL (Ignoramos el filtro de 'contacted=False' para testear a todos)
        start_time = time.time()
        qs = Institution.objects.all().order_by('id')
        total_targets = qs.count()
        
        if total_targets == 0:
            self.stdout.write(self.style.ERROR("📭 Base de datos vacía."))
            return

        self.stdout.write(self.style.WARNING(f"⚡ Extrayendo vectores de {total_targets} instituciones..."))
        
        # Usamos nuestra función de streaming optimizada
        df_inference = extract_inference_data(qs, chunk_size=5000)

        # 3. INFERENCIA VECTORIAL MASIVA
        self.stdout.write(self.style.WARNING("🔮 Aplicando tensores de predicción (Inferencia)..."))
        probabilities = pipeline.predict_proba(df_inference)[:, 1]
        df_inference['calculated_score'] = (probabilities * 100).astype(int)

        # 4. ACTUALIZACIÓN ATÓMICA EN POSTGRESQL
        now = timezone.now()
        institutions_to_update = []
        
        # Iterador RAM-Safe
        inst_dict = {inst.id: inst for inst in qs.only('id', 'lead_score', 'last_scored_at').iterator(chunk_size=5000)}
        
        for inst_id, row in df_inference.iterrows():
            inst = inst_dict.get(inst_id)
            if inst:
                inst.lead_score = row['calculated_score']
                inst.last_scored_at = now
                institutions_to_update.append(inst)

        self.stdout.write(self.style.WARNING("💾 Inyectando nuevos Scores en PostgreSQL (Bulk Mode)..."))
        
        with transaction.atomic():
            chunk_size = 2000
            for i in range(0, len(institutions_to_update), chunk_size):
                chunk = institutions_to_update[i:i + chunk_size]
                Institution.objects.bulk_update(chunk, ['lead_score', 'last_scored_at'])

        elapsed = round(time.time() - start_time, 2)
        
        # 5. REPORTE DE EFECTIVIDAD (ESTADÍSTICA FORENSE)
        self.stdout.write(self.style.SUCCESS(
            f"\n✅ BACKTESTING COMPLETADO EN {elapsed}s. {len(institutions_to_update)} nodos re-programados.\n"
        ))

        self.stdout.write(self.style.MIGRATE_HEADING("📊 DISTRIBUCIÓN DEL NUEVO LEAD SCORE (CAMPANA DE GAUSS B2B):"))
        
        # Calculamos la distribución por rangos para ver qué tan exigente es la IA
        dist = Institution.objects.aggregate(
            tier_s=Count('id', filter=Q(lead_score__gte=80)),
            tier_a=Count('id', filter=Q(lead_score__gte=60, lead_score__lt=80)),
            tier_b=Count('id', filter=Q(lead_score__gte=40, lead_score__lt=60)),
            tier_c=Count('id', filter=Q(lead_score__gte=20, lead_score__lt=40)),
            tier_d=Count('id', filter=Q(lead_score__lt=20)),
        )

        total = len(institutions_to_update)
        
        self.stdout.write(f"   👑 TIER S (80-100) : {dist['tier_s']} leads ({(dist['tier_s']/total)*100:.1f}%) -> [Alta Probabilidad de Cierre]")
        self.stdout.write(f"   🔥 TIER A (60-79)  : {dist['tier_a']} leads ({(dist['tier_a']/total)*100:.1f}%) -> [Seguimiento Prioritario]")
        self.stdout.write(f"   ⚠️ TIER B (40-59)  : {dist['tier_b']} leads ({(dist['tier_b']/total)*100:.1f}%) -> [Terreno Incierto]")
        self.stdout.write(f"   🧊 TIER C (20-39)  : {dist['tier_c']} leads ({(dist['tier_c']/total)*100:.1f}%) -> [Baja Prioridad]")
        self.stdout.write(f"   🗑️ TIER D (0-19)   : {dist['tier_d']} leads ({(dist['tier_d']/total)*100:.1f}%) -> [Ignorar / Descartar]\n")

        self.stdout.write(self.style.SUCCESS(
            "💡 CONSEJO TÁCTICO: Configura tu Celery Beat para atacar primero a los TIER S y TIER A."
        ))