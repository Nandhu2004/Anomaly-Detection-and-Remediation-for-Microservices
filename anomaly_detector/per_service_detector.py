import os
import sys
import time
import logging
import pickle
import sqlite3
from datetime import datetime, timezone

import pandas as pd
from sklearn.ensemble import IsolationForest


# ============================================================
# PATHS
# ============================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ANOMALY_DIR = os.path.join(BASE_DIR, "anomaly_detector")
PROFILES_DB = os.path.join(ANOMALY_DIR, "profiles.db")


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# IMPORT METRIC COLLECTOR AND ADAPTIVE THRESHOLD
# ============================================================

if ANOMALY_DIR not in sys.path:
    sys.path.insert(0, ANOMALY_DIR)

from metric_collector import collect_service_metrics
from adaptive_threshold import get_current_threshold


# ============================================================
# CONFIGURATION
# ============================================================

SERVICES = [
    "order-service",
    "payment-service",
    "inventory-service"
]

WARMUP_SAMPLES = 30
ANOMALY_THRESHOLD = 0.60

SERVICE_CONFIG = {
    "order-service": {
        "features": ["cpu_usage", "latency_avg"],
        "indicator": "spike_active"
    },
    "payment-service": {
        "features": ["error_rate", "latency_avg"],
        "indicator": "burst_active"
    },
    "inventory-service": {
        "features": ["cpu_usage", "latency_avg"],
        "indicator": "overload_active"
    }
}


# ============================================================
# INITIALIZE PROFILES DATABASE
# ============================================================

def init_profiles_db():
    os.makedirs(ANOMALY_DIR, exist_ok=True)

    with sqlite3.connect(PROFILES_DB) as conn:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                service_name TEXT PRIMARY KEY,
                sample_count INTEGER DEFAULT 0,
                updated_at TEXT,
                threshold REAL DEFAULT 0.60
            )
        """)

        # Migrate existing databases without losing profile data.
        cursor.execute("PRAGMA table_info(profiles)")
        columns = [row[1] for row in cursor.fetchall()]

        if "threshold" not in columns:
            cursor.execute("""
                ALTER TABLE profiles
                ADD COLUMN threshold REAL DEFAULT 0.60
            """)

        conn.commit()

    logger.info("Profiles database initialized: %s", PROFILES_DB)


# ============================================================
# MODEL PATH
# ============================================================

def get_model_path(service_name):
    preferred = os.path.join(
        ANOMALY_DIR, f"{service_name}_model.pkl"
    )

    fallback = os.path.join(
        ANOMALY_DIR, f"model_{service_name}.pkl"
    )

    if os.path.exists(preferred):
        return preferred

    if os.path.exists(fallback):
        return fallback

    return preferred


# ============================================================
# LOAD EXISTING MODEL
# ============================================================

def load_existing_model(service_name):
    model_path = get_model_path(service_name)

    if not os.path.exists(model_path):
        return None

    try:
        with open(model_path, "rb") as file:
            model = pickle.load(file)

        logger.info(
            "[%s] Existing model loaded: %s",
            service_name,
            os.path.basename(model_path)
        )

        return model

    except Exception as exc:
        logger.error(
            "[%s] Failed to load model: %s",
            service_name,
            exc
        )
        return None


# ============================================================
# SAVE MODEL
# ============================================================

def save_model(service_name, model):
    model_path = os.path.join(
        ANOMALY_DIR, f"{service_name}_model.pkl"
    )

    try:
        with open(model_path, "wb") as file:
            pickle.dump(model, file)

        logger.info(
            "[%s] Model saved: %s",
            service_name,
            os.path.basename(model_path)
        )

    except Exception as exc:
        logger.error(
            "[%s] Failed to save model: %s",
            service_name,
            exc
        )


# ============================================================
# TRAIN MODEL
# ============================================================

def train_model(service_name, metrics):
    if service_name not in SERVICE_CONFIG:
        raise ValueError(f"Unknown service: {service_name}")

    if metrics is None or metrics.empty:
        logger.warning(
            "[%s] No metrics available for training.",
            service_name
        )
        return None

    features = SERVICE_CONFIG[service_name]["features"]

    missing_features = [
        feature for feature in features
        if feature not in metrics.columns
    ]

    if missing_features:
        logger.error(
            "[%s] Missing training features: %s",
            service_name,
            missing_features
        )
        return None

    training_data = metrics[features].copy().dropna()

    if len(training_data) < WARMUP_SAMPLES:
        logger.info(
            "[%s] Not enough samples for training: %d/%d",
            service_name,
            len(training_data),
            WARMUP_SAMPLES
        )
        return None

    model = IsolationForest(
        n_estimators=200,
        contamination=0.05,
        random_state=42
    )

    model.fit(training_data)
    save_model(service_name, model)

    logger.info(
        "[%s] Isolation Forest trained using %d samples.",
        service_name,
        len(training_data)
    )

    return model


# ============================================================
# SEVERITY
# ============================================================

def calculate_severity(service_name, anomaly, score, latest):
    if not anomaly:
        return "normal"

    if service_name == "payment-service":
        try:
            error_rate = float(latest.get("error_rate", 0))
        except (TypeError, ValueError):
            error_rate = 0.0

        if error_rate >= 10:
            return "high"

        if error_rate >= 5:
            return "medium"

    if score >= -0.70:
        return "low"

    if score >= -0.80:
        return "medium"

    return "high"


# ============================================================
# SCORE LATEST METRIC
# ============================================================

def score_latest_metric(service_name, model, metrics):
    if service_name not in SERVICE_CONFIG:
        raise ValueError(f"Unknown service: {service_name}")

    if model is None:
        logger.error(
            "[%s] Cannot score because model is None.",
            service_name
        )
        return None

    if metrics is None or metrics.empty:
        logger.warning(
            "[%s] No metrics available for scoring.",
            service_name
        )
        return None

    config = SERVICE_CONFIG[service_name]
    features = config["features"]

    missing_features = [
        feature for feature in features
        if feature not in metrics.columns
    ]

    if missing_features:
        logger.error(
            "[%s] Missing features: %s",
            service_name,
            missing_features
        )
        return None

    latest = metrics.iloc[-1]
    values = {}

    for feature in features:
        try:
            value = float(latest[feature])

            if pd.isna(value):
                raise ValueError("NaN value")

            values[feature] = value

        except (TypeError, ValueError):
            logger.error(
                "[%s] Invalid value for feature '%s': %s",
                service_name,
                feature,
                latest[feature]
            )
            return None

    X_latest = pd.DataFrame([values], columns=features)

    # --------------------------------------------------------
    # Isolation Forest scores
    # --------------------------------------------------------

    try:
        decision_score = float(
            model.decision_function(X_latest)[0]
        )

        prediction = int(
            model.predict(X_latest)[0]
        )

        # Higher values now indicate more anomalous observations.
        anomaly_score = float(
            -model.score_samples(X_latest)[0]
        )

    except Exception as exc:
        logger.error(
            "[%s] Model scoring failed: %s",
            service_name,
            exc
        )
        return None

    # --------------------------------------------------------
    # Read adaptive threshold
    # --------------------------------------------------------

    threshold = get_current_threshold(service_name)

    # Both anomaly_score and threshold use the same scale.
    threshold_anomaly = anomaly_score > threshold

    # --------------------------------------------------------
    # Operational indicator
    # --------------------------------------------------------

    indicator_name = config["indicator"]

    try:
        indicator_value = float(latest.get(indicator_name, 0))
    except (TypeError, ValueError):
        indicator_value = 0.0

    indicator_active = indicator_value > 0

    # --------------------------------------------------------
    # Final anomaly decision
    # --------------------------------------------------------
    # The adaptive threshold controls the ML score decision.
    # The operational indicator independently flags known events.

    anomaly = threshold_anomaly or indicator_active

    # Retained for diagnostics, not used as the final ML cutoff.
    ml_prediction_anomaly = prediction == -1

    severity = calculate_severity(
        service_name,
        anomaly,
        decision_score,
        latest
    )

    result = {
        "service": service_name,
        "score": decision_score,
        "anomaly_score": anomaly_score,
        "threshold": threshold,
        "threshold_anomaly": threshold_anomaly,
        "prediction": prediction,
        "ml_prediction_anomaly": ml_prediction_anomaly,
        "anomaly": anomaly,
        "severity": severity,
        "indicator": indicator_name,
        "indicator_active": indicator_active,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metrics": latest.to_dict()
    }

    logger.info(
        "%s | decision_score=%.4f | anomaly_score=%.4f | "
        "threshold=%.2f | threshold_anomaly=%s | "
        "model_prediction_anomaly=%s | indicator_active=%s | "
        "anomaly=%s | severity=%s",
        service_name,
        decision_score,
        anomaly_score,
        threshold,
        threshold_anomaly,
        ml_prediction_anomaly,
        indicator_active,
        anomaly,
        severity
    )

    return result


# ============================================================
# RUN DETECTION FOR ONE SERVICE
# ============================================================

def run_service_detection(service_name):
    if service_name not in SERVICE_CONFIG:
        logger.error("Unsupported service: %s", service_name)
        return None

    try:
        metrics = collect_service_metrics(service_name)

    except Exception as exc:
        logger.error(
            "[%s] Metric collection failed: %s",
            service_name,
            exc
        )
        return None

    if metrics is None or metrics.empty:
        logger.warning("[%s] No metrics returned.", service_name)
        return None

    model = load_existing_model(service_name)

    if model is not None:
        logger.info(
            "[%s] Existing model found. Skipping warm-up.",
            service_name
        )
        return score_latest_metric(service_name, model, metrics)

    if len(metrics) < WARMUP_SAMPLES:
        logger.info(
            "[%s] Warming up: %d/%d samples.",
            service_name,
            len(metrics),
            WARMUP_SAMPLES
        )
        return None

    model = train_model(service_name, metrics)

    if model is None:
        return None

    return score_latest_metric(service_name, model, metrics)


# ============================================================
# RUN DETECTION FOR ALL SERVICES
# ============================================================

def run_detection():
    results = []

    for service_name in SERVICES:
        try:
            result = run_service_detection(service_name)

            if result is not None:
                results.append(result)

        except Exception as exc:
            logger.exception(
                "[%s] Detection failed: %s",
                service_name,
                exc
            )

    return results


# ============================================================
# CONTINUOUS DETECTION LOOP
# ============================================================

def detection_loop(interval=30):
    logger.info("Starting per-service anomaly detection loop.")
    logger.info("Services: %s", ", ".join(SERVICES))
    logger.info("Detection interval: %s seconds", interval)

    while True:
        try:
            run_detection()

        except Exception as exc:
            logger.exception("Detection cycle failed: %s", exc)

        time.sleep(interval)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    init_profiles_db()
    detection_loop(interval=30)