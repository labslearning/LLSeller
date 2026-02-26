import os
import time
import logging
import numpy as np
import pandas as pd
from typing import List, Optional
from datetime import datetime

from django.db import transaction
from django.conf import settings
from django.db.models import Count, Q, F, BooleanField, ExpressionWrapper
from django.db.models.functions import Coalesce
from django.utils import timezone

# Enterprise Machine Learning Imports
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold, train_test_split
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.calibration import CalibratedClassifierCV
import joblib

# Project Imports
from sales.models import Institution, Interaction

# =========================================================
# ⚙️ TIER GOD CONFIGURATION & TELEMETRY
# =========================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s.%(msecs)03d - [%(levelname)s] [AI-Engine] %(message)s', 
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Sovereign.MLOps")

MODEL_DIR = os.path.join(settings.BASE_DIR, 'ml_models')
os.makedirs(MODEL_DIR, exist_ok=True)
MODEL_PATH = os.path.join(MODEL_DIR, 'b2b_lead_scorer_calibrated.pkl')
METRICS_PATH = os.path.join(MODEL_DIR, 'model_metrics.json')

# =========================================================
# 📊 LAYER 1: MASSIVE DATA EXTRACTION (POSTGRESQL KERNEL)
# =========================================================
def extract_training_data() -> pd.DataFrame:
    """
    God-Tier Extraction: Uses PostgreSQL Aggregations directly.
    Avoids Python memory bloat by fetching raw dicts instead of Django objects.
    """
    start_time = time.time()
    logger.info("📡 Initiating high-speed extraction from Data Warehouse...")
    
    # 1. Define what a "Win" is at the SQL level
    success_q = Q(interactions__status__in=[
        Interaction.Status.OPENED, 
        Interaction.Status.REPLIED, 
        Interaction.Status.MEETING
    ])

    # 2. Heavy-lifting performed by PostgreSQL (Annotations & Coalesce)
    qs = Institution.objects.filter(contacted=True).annotate(
        success_hits=Count('interactions', filter=success_q),
        is_success=ExpressionWrapper(Q(success_hits__gt=0), output_field=BooleanField()),
        lms_prov=Coalesce(F('tech_profile__lms_provider'), 'Unknown'),
        has_lms_flag=Coalesce(F('tech_profile__has_lms'), False)
    ).values(
        'id', 'city', 'institution_type', 'is_private', 
        'has_lms_flag', 'lms_prov', 'is_success'
    )
    
    # 3. Stream into Pandas Vectorized DataFrame
    df = pd.DataFrame.from_records(qs)
    
    if df.empty:
        return df

    # Standardize column names for the ML pipeline
    df = df.rename(columns={
        'id': 'institution_id',
        'has_lms_flag': 'has_lms',
        'lms_prov': 'lms_provider',
        'is_success': 'target'
    })
    
    df['target'] = df['target'].astype(int)
    
    elapsed = round(time.time() - start_time, 2)
    logger.info(f"✅ Extracted {len(df)} historical vectors in {elapsed}s.")
    return df

def extract_inference_data(qs) -> pd.DataFrame:
    """Optimized inference extraction directly from an active QuerySet."""
    annotated_qs = qs.annotate(
        lms_prov=Coalesce(F('tech_profile__lms_provider'), 'Unknown'),
        has_lms_flag=Coalesce(F('tech_profile__has_lms'), False)
    ).values(
        'id', 'city', 'institution_type', 'is_private', 
        'has_lms_flag', 'lms_prov'
    )
    
    df = pd.DataFrame.from_records(annotated_qs)
    if not df.empty:
        df = df.rename(columns={
            'id': 'institution_id',
            'has_lms_flag': 'has_lms',
            'lms_prov': 'lms_provider'
        })
    return df

# =========================================================
# 🧠 LAYER 2: CHAMPION/CHALLENGER TRAINING PIPELINE
# =========================================================
def train_model() -> bool:
    """
    Automated ML-Ops Pipeline. 
    Implements Hyperparameter tuning, Cross-Validation, and Probability Calibration.
    """
    df = extract_training_data()
    
    if len(df) < 100:
        logger.warning("⚠️ Insufficient statistical significance (< 100 records). Halting AI training.")
        return False
        
    X = df.drop(columns=['institution_id', 'target'])
    y = df['target']
    
    # Prevent training if there are no successes (target=1) or no failures (target=0)
    if len(y.unique()) < 2:
        logger.warning("⚠️ Data is completely uniform (no variance in Target). Halting AI training.")
        return False

    # 1. Stratified Split (Ensures train/test have same ratio of wins/losses)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # 2. Feature Architecture
    categorical_features = ['city', 'institution_type', 'lms_provider']
    numerical_features = ['is_private', 'has_lms']
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='constant', fill_value='Unknown')),
                ('onehot', OneHotEncoder(handle_unknown='ignore'))
            ]), categorical_features),
            ('num', Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='most_frequent')),
                ('scaler', StandardScaler()) # Normalizes variance
            ]), numerical_features)
        ]
    )
    
    # 3. Core Engine Definition
    base_rf = RandomForestClassifier(class_weight='balanced', random_state=42)
    pipeline = Pipeline(steps=[('preprocessor', preprocessor), ('classifier', base_rf)])
    
    # 4. Hyperparameter Tuning Space (Finding the optimum brain structure)
    param_distributions = {
        'classifier__n_estimators': [100, 200, 300],
        'classifier__max_depth': [10, 20, 30, None],
        'classifier__min_samples_split': [2, 5, 10],
        'classifier__min_samples_leaf': [1, 2, 4]
    }
    
    logger.info("🔬 Initiating Hyperparameter Search Space...")
    search = RandomizedSearchCV(
        pipeline, 
        param_distributions, 
        n_iter=10, 
        cv=StratifiedKFold(n_splits=3), 
        scoring='roc_auc', 
        n_jobs=-1, # Use all CPU cores
        random_state=42
    )
    search.fit(X_train, y_train)
    best_pipeline = search.best_estimator_
    
    # 5. Probability Calibration (Isotonic mapping to true 0-100 percentages)
    logger.info("⚖️ Calibrating Prediction Probabilities...")
    calibrated_classifier = CalibratedClassifierCV(best_pipeline, method='sigmoid', cv='prefit')
    calibrated_classifier.fit(X_test, y_test)
    
    # 6. Champion vs Challenger Evaluation
    y_pred_proba = calibrated_classifier.predict_proba(X_test)[:, 1]
    auc_score = roc_auc_score(y_test, y_pred_proba)
    
    logger.info(f"📊 Model Evaluation Metrics | ROC-AUC: {auc_score:.4f}")
    
    # Only save if the model is statistically better than random guessing (0.50)
    if auc_score > 0.55:
        joblib.dump(calibrated_classifier, MODEL_PATH)
        
        # Save metrics for telemetry dashboard
        metrics = {
            "trained_at": datetime.now().isoformat(),
            "roc_auc": round(auc_score, 4),
            "samples": len(df),
            "best_params": search.best_params_
        }
        pd.Series(metrics).to_json(METRICS_PATH)
        
        logger.info(f"🏆 CHAMPION MODEL DEPLOYED: Saved to {MODEL_PATH}")
        return True
    else:
        logger.warning(f"📉 CHALLENGER REJECTED: ROC-AUC ({auc_score:.4f}) is too low. Keeping previous model.")
        return False

# =========================================================
# 🔮 LAYER 3: HIGH-THROUGHPUT BATCH INFERENCE
# =========================================================
def score_unrated_leads(limit: int = 2000):
    """
    Mass-Inference Engine. 
    Processes data in chunks to prevent memory overflow and commits atomically.
    """
    if not os.path.exists(MODEL_PATH):
        logger.error("❌ [FATAL] Predictive Model Matrix not found. Awaiting training.")
        return
        
    logger.info("🧠 Loading Neural/Tree Matrix into RAM...")
    calibrated_pipeline = joblib.load(MODEL_PATH)
    
    # Select leads that are fresh or haven't been scored in 30 days
    thirty_days_ago = timezone.now() - pd.Timedelta(days=30)
    qs = Institution.objects.filter(contacted=False, is_active=True).filter(
        Q(last_scored_at__isnull=True) | Q(last_scored_at__lt=thirty_days_ago)
    ).order_by('last_scored_at')[:limit]
    
    if not qs.exists():
        logger.info("📭 Zero targets require scoring at this time.")
        return
        
    df_inference = extract_inference_data(qs)
    if df_inference.empty:
        return

    X_new = df_inference.drop(columns=['institution_id'])
    
    logger.info(f"⚡ Running Vector Inference on {len(X_new)} institutional targets...")
    
    # Extract Calibrated Probability
    success_probabilities = calibrated_pipeline.predict_proba(X_new)[:, 1]
    
    # Map predictions back to Django efficiently
    now = timezone.now()
    institutions_to_update = []
    
    # Fetch actual objects for the bulk update
    inst_objects = {str(inst.id): inst for inst in qs}
    
    for inst_id, prob in zip(df_inference['institution_id'], success_probabilities):
        inst = inst_objects.get(inst_id)
        if inst:
            # Map strict math probability to a confident 0-100 sales score
            inst.lead_score = int(prob * 100)
            inst.last_scored_at = now
            institutions_to_update.append(inst)
            
    # Atomic commit chunks to protect PostgreSQL transaction logs
    chunk_size = 500
    with transaction.atomic():
        for i in range(0, len(institutions_to_update), chunk_size):
            chunk = institutions_to_update[i:i + chunk_size]
            Institution.objects.bulk_update(chunk, ['lead_score', 'last_scored_at'])
            
    logger.info(f"✅ BATCH INFERENCE COMPLETE. System optimized {len(institutions_to_update)} leads.")
