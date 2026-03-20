import os
import time
import json
import logging
import gc
import itertools
import numpy as np
import pandas as pd
from typing import List, Optional, Tuple, Iterator, Dict, Any
from datetime import datetime
from functools import lru_cache

from django.db import transaction
from django.conf import settings
from django.db.models import Count, Q, F, BooleanField, ExpressionWrapper, Value
from django.db.models.functions import Coalesce
from django.utils import timezone

# =========================================================
# 🧠 ENTERPRISE MLOps IMPORTS
# =========================================================
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import RandomizedSearchCV, train_test_split, StratifiedKFold
from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss, precision_recall_curve
from sklearn.calibration import CalibratedClassifierCV
import joblib

# Project Imports
from sales.models import Institution, Interaction

# =========================================================
# ⚙️ TIER GOD CONFIGURATION & TELEMETRY
# =========================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s.%(msecs)03d - [%(levelname)s] [AI-Oracle] %(message)s', 
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.MLOps")

MODEL_DIR = os.path.join(settings.BASE_DIR, 'ml_models')
os.makedirs(MODEL_DIR, exist_ok=True)

# El JSON actúa como nuestro Servidor de Registro MLOps (Model Registry)
METRICS_PATH = os.path.join(MODEL_DIR, 'model_registry.json')

# =========================================================
# 🛡️ SYSTEM UTILITIES (Memory Management & Caching)
# =========================================================
def chunked_iterable(iterable: Iterator, size: int) -> Iterator[Tuple]:
    """Generador O(1) de memoria. Fracciona iterables infinitos en bloques estables."""
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk

def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
    """
    [MEMORY DOWNCASTING & TYPE CASTING]: 
    Transforma la data para que Scikit-Learn (Cython) la procese matemáticamente.
    Convierte Bool -> int8 (Crítico para SimpleImputer) y Object -> Category.
    """
    for col in ['is_private', 'has_lms']:
        if col in df.columns:
            df[col] = df[col].fillna(False).astype('int8')
            
    for col in df.select_dtypes(include=['object']).columns:
        num_unique_values = len(df[col].unique())
        num_total_values = len(df[col])
        if num_total_values > 0 and (num_unique_values / num_total_values) < 0.5:
            df[col] = df[col].astype('category')
            
    for col in df.select_dtypes(include=['int64', 'float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')
        
    return df

def get_active_model_path() -> Optional[str]:
    """Busca dinámicamente el modelo campeón actual en el registro."""
    if not os.path.exists(METRICS_PATH):
        return None
    try:
        with open(METRICS_PATH, 'r') as f:
            registry = json.load(f)
        active_model = registry.get('active_model_filename')
        if active_model:
            return os.path.join(MODEL_DIR, active_model)
    except Exception as e:
        logger.error(f"Fallo al leer el Model Registry: {e}")
    return None

@lru_cache(maxsize=1)
def get_cached_model(model_path: str) -> Any:
    """[SINGLETON PATTERN]: Mantiene la Matriz Neuronal en RAM L1 Cache."""
    logger.info(f"🧠 Anclando Matriz Predictiva al Heap (L1 RAM Cache): {os.path.basename(model_path)}")
    return joblib.load(model_path)

# =========================================================
# 📊 LAYER 1: DATA PIPELINE (POSTGRESQL KERNEL -> RAM)
# =========================================================
def extract_training_data(chunk_size: int = 10000) -> pd.DataFrame:
    """[STREAMING EXTRACTION]: Extrae millones de registros sin colapsar el SO."""
    start_time = time.time()
    logger.info("📡 Iniciando extracción Streaming desde el Data Warehouse...")
    
    success_q = Q(interactions__status__in=[
        Interaction.Status.OPENED, 
        Interaction.Status.REPLIED, 
        Interaction.Status.MEETING
    ])

    qs = Institution.objects.filter(contacted=True, is_active=True).annotate(
        success_hits=Count('interactions', filter=success_q),
        is_success=ExpressionWrapper(Q(success_hits__gt=0), output_field=BooleanField()),
        lms_prov=Coalesce(F('tech_profile__lms_provider'), Value('Unknown')),
        has_lms_flag=Coalesce(F('tech_profile__has_lms'), Value(False))
    ).values(
        'id', 'city', 'institution_type', 'is_private', 
        'has_lms_flag', 'lms_prov', 'is_success'
    )
    
    chunks = []
    for dict_chunk in chunked_iterable(qs.iterator(chunk_size=chunk_size), chunk_size):
        chunks.append(pd.DataFrame.from_records(dict_chunk))
        
    if not chunks:
        logger.warning("📭 Data Warehouse vacío de objetivos históricos.")
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)
    
    df.rename(columns={
        'id': 'institution_id',
        'has_lms_flag': 'has_lms',
        'lms_prov': 'lms_provider',
        'is_success': 'target'
    }, inplace=True)
    
    df['target'] = df['target'].astype(int)
    df.set_index('institution_id', inplace=True)
    df = optimize_dataframe_memory(df)
    
    elapsed = round(time.time() - start_time, 2)
    logger.info(f"✅ Ensamblado Dimensional Completado: {len(df)} vectores en {elapsed}s.")
    return df

def extract_inference_data(qs, chunk_size: int = 5000) -> pd.DataFrame:
    """Proyección de datos protegida contra rebanado (slicing) de Django."""
    annotated_qs = qs.annotate(
        lms_prov=Coalesce(F('tech_profile__lms_provider'), Value('Unknown')),
        has_lms_flag=Coalesce(F('tech_profile__has_lms'), Value(False))
    ).values(
        'id', 'city', 'institution_type', 'is_private', 
        'has_lms_flag', 'lms_prov'
    )
    
    chunks = []
    for dict_chunk in chunked_iterable(annotated_qs.iterator(chunk_size=chunk_size), chunk_size):
        chunks.append(pd.DataFrame.from_records(dict_chunk))
        
    if not chunks:
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)
    df.rename(columns={
        'id': 'institution_id',
        'has_lms_flag': 'has_lms',
        'lms_prov': 'lms_provider'
    }, inplace=True)
    
    df.set_index('institution_id', inplace=True)
    return optimize_dataframe_memory(df)

# =========================================================
# 🧠 LAYER 2: CHAMPION/CHALLENGER TRAINING PIPELINE
# =========================================================
def train_model() -> bool:
    """
    [THE ORACLE CORE]: MLOps Pipeline con Calibración Dinámica Nativa.
    """
    df = extract_training_data()
    
    if len(df) < 100:
        logger.warning("⚠️ Insignificancia Estadística (< 100 registros). Entrenamiento abortado.")
        return False
        
    X = df.drop(columns=['target'])
    y = df['target']
    
    if len(y.unique()) < 2:
        logger.warning("⚠️ Datos uniformes (No hay varianza en Target). Entrenamiento abortado.")
        return False

    # 1. SPLIT ESTRATIFICADO BIPARTITO (Train/Test puro)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.15, random_state=42, stratify=y)
    
    # 2. INGENIERÍA DE CARACTERÍSTICAS
    categorical_features = ['city', 'institution_type', 'lms_provider']
    numerical_features = ['is_private', 'has_lms']
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='constant', fill_value='Unknown')),
                ('onehot', OneHotEncoder(handle_unknown='infrequent_if_exist', min_frequency=0.02, sparse_output=False))
            ]), categorical_features),
            ('num', SimpleImputer(strategy='most_frequent'), numerical_features)
        ],
        remainder='drop'
    )
    
    # 3. MOTOR BASE: Random Forest Vectorizado
    base_rf = RandomForestClassifier(class_weight='balanced_subsample', random_state=42, n_jobs=-1)
    pipeline = Pipeline(steps=[('preprocessor', preprocessor), ('classifier', base_rf)])
    
    param_distributions = {
        'classifier__n_estimators': [100, 200, 300],
        'classifier__max_depth': [10, 20, None],
        'classifier__min_samples_split': [2, 5, 10],
        'classifier__min_samples_leaf': [1, 2]
    }
    
    logger.info("🔬 Desplegando Matriz de Búsqueda Estocástica (Hyperparameter Tuning)...")
    search = RandomizedSearchCV(
        pipeline, 
        param_distributions, 
        n_iter=10, 
        cv=StratifiedKFold(n_splits=3, shuffle=True, random_state=42), 
        scoring='roc_auc', 
        random_state=42,
        n_jobs=-1
    )
    # Búsqueda del mejor pipeline
    search.fit(X_train, y_train)
    best_pipeline = search.best_estimator_
    
    # 4. CALIBRACIÓN NATIVA CON CROSS-VALIDATION (cv=3)
    # CalibratedClassifierCV se encarga de dividir X_train internamente. Evita StrictType Errors y Data Leakage.
    calib_method = 'isotonic' if len(X_train) > 1000 else 'sigmoid'
    logger.info(f"⚖️ Calibrando Curva de Probabilidades B2B (Método: {calib_method.upper()}, CV=3)...")
    
    # [GOD TIER FIX]: Pasamos 'estimator=' y 'cv=3'
    calibrated_classifier = CalibratedClassifierCV(estimator=best_pipeline, method=calib_method, cv=3)
    calibrated_classifier.fit(X_train, y_train)
    
    # 5. AUDITORÍA FORENSE SOBRE DATOS INVISIBLES (X_test)
    y_pred_proba = calibrated_classifier.predict_proba(X_test)[:, 1]
    
    auc_score = roc_auc_score(y_test, y_pred_proba)
    loss = log_loss(y_test, y_pred_proba)
    brier = brier_score_loss(y_test, y_pred_proba) 
    
    precision, recall, thresholds = precision_recall_curve(y_test, y_pred_proba)
    fscore = (2 * precision * recall) / (precision + recall + 1e-8)
    opt_f1 = np.max(fscore)
    
    logger.info(f"📊 Métricas | ROC-AUC: {auc_score:.4f} | Brier: {brier:.4f} | Optimal F1: {opt_f1:.4f}")
    
    # 6. PERSISTENCIA Y VERSIONADO (MLOps)
    if auc_score > 0.40:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_filename = f"b2b_scorer_v{timestamp}.pkl"
        model_filepath = os.path.join(MODEL_DIR, model_filename)
        
        joblib.dump(calibrated_classifier, model_filepath)
        
        registry = {
            "active_model_filename": model_filename,
            "trained_at": datetime.now().isoformat(),
            "metrics": {
                "roc_auc": float(round(auc_score, 4)),
                "log_loss": float(round(loss, 4)),
                "brier_score": float(round(brier, 4)),
                "optimal_f1": float(round(opt_f1, 4)),
            },
            "samples_processed": int(len(df)),
            "hyperparameters": search.best_params_
        }
        
        with open(METRICS_PATH, 'w') as f:
            json.dump(registry, f, indent=4)
            
        logger.info(f"🏆 NUEVO MODELO CHAMPION DESPLEGADO: {model_filename}")
        
        #del df, X, y, X_temp, search, calibrated_classifier
        del df, X, y, search, calibrated_classifier
        gc.collect()

        return True
    else:
        logger.warning(f"📉 CHALLENGER RECHAZADO: ROC-AUC ({auc_score:.4f}) subóptimo. Se conserva el modelo anterior.")
        return False

# =========================================================
# 🔮 LAYER 3: HIGH-THROUGHPUT BATCH INFERENCE
# =========================================================
def score_unrated_leads(limit: int = 5000, chunk_size: int = 1000):
    """
    [MASS-INFERENCE ENGINE]: Asigna Score predictivo eludiendo colisiones de ORM.
    """
    model_path = get_active_model_path()
    if not model_path or not os.path.exists(model_path):
        logger.error("❌ [FATAL] No hay modelo activo en el Registro MLOps. Requiere entrenamiento.")
        return
        
    calibrated_pipeline = get_cached_model(model_path)
    
    thirty_days_ago = timezone.now() - pd.Timedelta(days=30)
    
    target_ids_qs = Institution.objects.filter(
        contacted=False, is_active=True
    ).filter(
        Q(last_scored_at__isnull=True) | Q(last_scored_at__lt=thirty_days_ago)
    ).order_by('id').values_list('id', flat=True)[:limit]
    
    target_ids = list(target_ids_qs)
    
    if not target_ids:
        logger.info("📭 Operación abortada. Zero targets en la cola de inferencia.")
        return
        
    clean_qs = Institution.objects.filter(id__in=target_ids)
    df_inference = extract_inference_data(clean_qs, chunk_size=chunk_size)
    
    if df_inference.empty:
        return

    logger.info(f"⚡ Corriendo Inferencia Vectorial sobre {len(df_inference)} instituciones objetivo...")
    
    success_probabilities = calibrated_pipeline.predict_proba(df_inference)[:, 1]
    
    df_inference['predicted_prob'] = success_probabilities
    df_inference['calculated_score'] = (df_inference['predicted_prob'] * 100).astype(int)
    
    now = timezone.now()
    institutions_to_update = []
    
    queryset_to_update = Institution.objects.filter(id__in=df_inference.index).only('id', 'lead_score', 'last_scored_at')
    inst_dict = {inst.id: inst for inst in queryset_to_update.iterator(chunk_size=chunk_size)}
    
    for inst_id, row in df_inference.iterrows():
        inst = inst_dict.get(inst_id)
        if inst:
            inst.lead_score = row['calculated_score']
            inst.last_scored_at = now
            institutions_to_update.append(inst)
            
    if institutions_to_update:
        with transaction.atomic():
            for i in range(0, len(institutions_to_update), chunk_size):
                chunk = institutions_to_update[i:i + chunk_size]
                Institution.objects.bulk_update(chunk, ['lead_score', 'last_scored_at'])
                
        logger.info(f"✅ INFERENCIA MASIVA COMPLETADA. Score Predictivo asignado a {len(institutions_to_update)} leads.")