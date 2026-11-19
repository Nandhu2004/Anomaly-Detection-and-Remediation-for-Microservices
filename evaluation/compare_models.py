import os
import sqlite3
import logging
import pandas as pd

from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix
)


# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

INCIDENTS_DB = os.path.join(
    BASE_DIR,
    "incident_manager",
    "incidents.db"
)

PROFILES_DB = os.path.join(
    BASE_DIR,
    "anomaly_detector",
    "profiles.db"
)

RESULTS_DIR = os.path.join(
    BASE_DIR,
    "evaluation",
    "results"
)

os.makedirs(RESULTS_DIR, exist_ok=True)


# Same values used by adaptive_threshold.py
DEFAULT_THRESHOLD = 0.60
ADJUST_EVERY = 10
FP_RATE_LIMIT = 0.30
THRESHOLD_STEP = 0.10
THRESHOLD_MIN = 0.30
THRESHOLD_MAX = 0.90


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# =============================================================================
# DATABASE HELPERS
# =============================================================================

def get_connection(db_path):
    return sqlite3.connect(db_path)


# =============================================================================
# LOAD INCIDENT DATA
# =============================================================================

def load_incidents(service_name):
    """
    Load completed incidents for one service.

    Only incidents with an outcome are included because the evaluation
    requires a completed incident record.
    """

    if not os.path.exists(INCIDENTS_DB):
        logging.error(
            "Incidents database not found: %s",
            INCIDENTS_DB
        )
        return pd.DataFrame()

    query = """
        SELECT
            incident_id,
            service_name,
            anomaly_type,
            anomaly_score,
            risk_level,
            risk_score,
            routing_decision,
            outcome,
            human_approved,
            human_reject_reason,
            created_at,
            resolved_at,
            time_to_recover
        FROM incidents
        WHERE service_name = ?
          AND outcome IS NOT NULL
        ORDER BY created_at ASC
    """

    try:
        with get_connection(INCIDENTS_DB) as conn:
            df = pd.read_sql_query(
                query,
                conn,
                params=(service_name,)
            )

        logging.info(
            "%s completed incidents loaded for %s",
            len(df),
            service_name
        )

        return df

    except Exception as e:
        logging.error(
            "Failed to load incidents for %s: %s",
            service_name,
            e
        )
        return pd.DataFrame()


# =============================================================================
# GROUND-TRUTH / FEEDBACK LABELS
# =============================================================================

def normalize_value(value):
    """
    Convert database values into a normalized lowercase string.
    """

    if value is None:
        return ""

    return str(value).strip().lower()


def is_true_value(value):
    """
    Handles SQLite values such as:
    True, 1, yes, true
    """

    value = normalize_value(value)

    return value in {
        "1",
        "true",
        "yes",
        "y",
        "approved"
    }


def get_ground_truth_labels(df):
    """
    Build feedback-derived labels.

    Label:
        0 = explicitly identified as false positive
        1 = treated as a real anomaly

    Important:
    This is a feedback-derived proxy label, not independent ground truth.

    The system only confidently knows an incident is a false positive when
    human feedback explicitly says so.
    """

    labels = []

    successful_outcomes = {
        "success",
        "successful",
        "resolved",
        "recovered",
        "auto_healed",
        "healed",
        "human_resolved"
    }

    for _, row in df.iterrows():

        reject_reason = normalize_value(
            row.get("human_reject_reason")
        )

        outcome = normalize_value(
            row.get("outcome")
        )

        human_approved = row.get(
            "human_approved"
        )

        # -------------------------------------------------------------
        # Explicit false-positive feedback
        # -------------------------------------------------------------

        if reject_reason in {
            "false_positive",
            "false positive",
            "false-positive",
            "fp"
        }:
            labels.append(0)
            continue

        # -------------------------------------------------------------
        # Explicit human confirmation
        # -------------------------------------------------------------

        if is_true_value(human_approved):
            labels.append(1)
            continue

        # -------------------------------------------------------------
        # Successful recovery/remediation means the incident was real
        # -------------------------------------------------------------

        if outcome in successful_outcomes:
            labels.append(1)
            continue

        # -------------------------------------------------------------
        # Failure/partial outcomes do NOT mean false positive.
        #
        # A remediation can fail even when the anomaly was real.
        # Therefore, without explicit false-positive feedback, retain
        # the incident as a real-anomaly proxy label.
        # -------------------------------------------------------------

        labels.append(1)

    return pd.Series(
        labels,
        index=df.index,
        name="ground_truth"
    )


# =============================================================================
# CURRENT PER-SERVICE THRESHOLD
# =============================================================================

def get_service_threshold(service_name):
    """
    Read the current threshold from anomaly_detector/profiles.db.

    Current schema uses the 'profiles' table.
    """

    if not os.path.exists(PROFILES_DB):
        logging.warning(
            "Profiles database not found. Using default threshold %.2f",
            DEFAULT_THRESHOLD
        )
        return DEFAULT_THRESHOLD

    query = """
        SELECT threshold
        FROM profiles
        WHERE service_name = ?
        LIMIT 1
    """

    try:
        with get_connection(PROFILES_DB) as conn:

            cursor = conn.cursor()

            cursor.execute(
                query,
                (service_name,)
            )

            row = cursor.fetchone()

        if row is None:
            logging.warning(
                "No threshold found for %s. Using %.2f",
                service_name,
                DEFAULT_THRESHOLD
            )

            return DEFAULT_THRESHOLD

        threshold = float(row[0])

        return threshold

    except Exception as e:

        logging.warning(
            "Could not read threshold for %s: %s. "
            "Using default %.2f. Error: %s",
            service_name,
            DEFAULT_THRESHOLD,
            e
        )

        return DEFAULT_THRESHOLD


# =============================================================================
# PER-SERVICE PREDICTIONS
# =============================================================================

def get_per_service_predictions(df, service_name):
    """
    Generate predictions using the threshold currently configured for
    that service.

    anomaly_score comes from the service-specific Isolation Forest.

    Prediction:
        1 = anomaly
        0 = normal
    """

    threshold = get_service_threshold(
        service_name
    )

    predictions = (
        df["anomaly_score"].astype(float)
        < -threshold
    ).astype(int)

    return predictions, threshold


# =============================================================================
# GLOBAL FIXED-THRESHOLD BASELINE
# =============================================================================

def get_global_baseline_predictions(df):
    """
    Global Fixed-Threshold Baseline.

    This is NOT a separately trained global Isolation Forest.

    It simply applies the same fixed threshold of 0.60 to the anomaly
    scores generated for the incidents.

    It is therefore a fixed-threshold baseline for comparison against
    the adaptive per-service threshold.
    """

    global_threshold = DEFAULT_THRESHOLD

    predictions = (
        df["anomaly_score"].astype(float)
        < -global_threshold
    ).astype(int)

    return predictions, global_threshold


# =============================================================================
# METRICS
# =============================================================================

def calculate_metrics(y_true, y_pred):
    """
    Calculate classification metrics.
    """

    if len(y_true) == 0:
        return {
            "samples": 0,
            "false_positive_rate": 0.0,
            "precision": 0.0,
            "recall": 0.0,
            "f1_score": 0.0,
            "true_positives": 0,
            "true_negatives": 0,
            "false_positives": 0,
            "false_negatives": 0
        }

    tn, fp, fn, tp = confusion_matrix(
        y_true,
        y_pred,
        labels=[0, 1]
    ).ravel()

    false_positive_rate = (
        fp / (fp + tn)
        if (fp + tn) > 0
        else 0.0
    )

    precision = precision_score(
        y_true,
        y_pred,
        zero_division=0
    )

    recall = recall_score(
        y_true,
        y_pred,
        zero_division=0
    )

    f1 = f1_score(
        y_true,
        y_pred,
        zero_division=0
    )

    return {
        "samples": len(y_true),
        "false_positive_rate": round(
            false_positive_rate,
            4
        ),
        "precision": round(
            precision,
            4
        ),
        "recall": round(
            recall,
            4
        ),
        "f1_score": round(
            f1,
            4
        ),
        "true_positives": int(tp),
        "true_negatives": int(tn),
        "false_positives": int(fp),
        "false_negatives": int(fn)
    }


# =============================================================================
# THRESHOLD EVOLUTION SIMULATION
# =============================================================================

def analyze_threshold_evolution(df):
    """
    Simulate the same threshold adaptation logic used by
    adaptive_threshold.py.

    Every 10 completed incidents:

        FP rate > 30%
            -> threshold increases by 0.10

        Otherwise
            -> threshold remains unchanged

    Remediation failure is reported but does NOT directly lower
    the detector threshold.
    """

    if df.empty:
        return pd.DataFrame()

    working_df = df.copy()

    working_df["ground_truth"] = (
        get_ground_truth_labels(working_df)
    )

    working_df = working_df.reset_index(
        drop=True
    )

    threshold = DEFAULT_THRESHOLD

    history = []

    total_incidents = len(working_df)

    for end_index in range(
        ADJUST_EVERY,
        total_incidents + 1,
        ADJUST_EVERY
    ):

        batch = working_df.iloc[
            end_index - ADJUST_EVERY:
            end_index
        ]

        y_true = batch["ground_truth"]

        # Predictions using threshold BEFORE adjustment
        y_pred = (
            batch["anomaly_score"].astype(float)
            < -threshold
        ).astype(int)

        metrics = calculate_metrics(
            y_true,
            y_pred
        )

        fp_rate = metrics[
            "false_positive_rate"
        ]

        # -------------------------------------------------------------
        # Remediation failure is reported for analysis only.
        # It does not lower the threshold.
        # -------------------------------------------------------------

        failure_outcomes = {
            "failure",
            "failed"
        }

        failure_count = sum(
            normalize_value(outcome)
            in failure_outcomes
            for outcome in batch["outcome"]
        )

        failure_rate = (
            failure_count / len(batch)
            if len(batch) > 0
            else 0.0
        )

        old_threshold = threshold

        adjustment = "none"

        if fp_rate > FP_RATE_LIMIT:

            threshold = min(
                threshold + THRESHOLD_STEP,
                THRESHOLD_MAX
            )

            adjustment = "increase"

        history.append({
            "incidents_processed": end_index,
            "batch_size": len(batch),
            "threshold_before": round(
                old_threshold,
                2
            ),
            "false_positive_rate": round(
                fp_rate,
                4
            ),
            "remediation_failure_rate": round(
                failure_rate,
                4
            ),
            "adjustment": adjustment,
            "threshold_after": round(
                threshold,
                2
            )
        })

    return pd.DataFrame(history)


# =============================================================================
# MAIN COMPARISON
# =============================================================================

def run_comparison(service_name):
    """
    Compare the service-specific adaptive threshold against the
    global fixed-threshold baseline.
    """

    print()
    print("=" * 72)
    print(
        f"MODEL COMPARISON: {service_name}"
    )
    print("=" * 72)

    # -----------------------------------------------------------------
    # Load incidents
    # -----------------------------------------------------------------

    df = load_incidents(
        service_name
    )

    if df.empty:

        print(
            f"\nNo completed incidents found for "
            f"{service_name}."
        )

        print(
            "\nAt least one completed incident is required "
            "for evaluation."
        )

        return

    # -----------------------------------------------------------------
    # Ground truth
    # -----------------------------------------------------------------

    df["ground_truth"] = (
        get_ground_truth_labels(df)
    )

    y_true = df["ground_truth"]

    # -----------------------------------------------------------------
    # Per-service threshold
    # -----------------------------------------------------------------

    service_predictions, service_threshold = (
        get_per_service_predictions(
            df,
            service_name
        )
    )

    service_metrics = calculate_metrics(
        y_true,
        service_predictions
    )

    # -----------------------------------------------------------------
    # Global fixed threshold
    # -----------------------------------------------------------------

    global_predictions, global_threshold = (
        get_global_baseline_predictions(
            df
        )
    )

    global_metrics = calculate_metrics(
        y_true,
        global_predictions
    )

    # -----------------------------------------------------------------
    # Print service result
    # -----------------------------------------------------------------

    print()
    print(
        "Per-Service Adaptive Threshold"
    )

    print(
        f"Current threshold : "
        f"{service_threshold:.2f}"
    )

    print(
        f"Samples           : "
        f"{service_metrics['samples']}"
    )

    print(
        f"False Positive Rate: "
        f"{service_metrics['false_positive_rate']:.2%}"
    )

    print(
        f"Precision          : "
        f"{service_metrics['precision']:.4f}"
    )

    print(
        f"Recall             : "
        f"{service_metrics['recall']:.4f}"
    )

    print(
        f"F1 Score           : "
        f"{service_metrics['f1_score']:.4f}"
    )

    print(
        f"TP                 : "
        f"{service_metrics['true_positives']}"
    )

    print(
        f"TN                 : "
        f"{service_metrics['true_negatives']}"
    )

    print(
        f"FP                 : "
        f"{service_metrics['false_positives']}"
    )

    print(
        f"FN                 : "
        f"{service_metrics['false_negatives']}"
    )

    # -----------------------------------------------------------------
    # Print global baseline
    # -----------------------------------------------------------------

    print()
    print(
        "Global Fixed-Threshold Baseline"
    )

    print(
        f"Fixed threshold    : "
        f"{global_threshold:.2f}"
    )

    print(
        f"Samples            : "
        f"{global_metrics['samples']}"
    )

    print(
        f"False Positive Rate: "
        f"{global_metrics['false_positive_rate']:.2%}"
    )

    print(
        f"Precision          : "
        f"{global_metrics['precision']:.4f}"
    )

    print(
        f"Recall             : "
        f"{global_metrics['recall']:.4f}"
    )

    print(
        f"F1 Score           : "
        f"{global_metrics['f1_score']:.4f}"
    )

    print(
        f"TP                 : "
        f"{global_metrics['true_positives']}"
    )

    print(
        f"TN                 : "
        f"{global_metrics['true_negatives']}"
    )

    print(
        f"FP                 : "
        f"{global_metrics['false_positives']}"
    )

    print(
        f"FN                 : "
        f"{global_metrics['false_negatives']}"
    )

    # -----------------------------------------------------------------
    # Comparison summary
    # -----------------------------------------------------------------

    comparison = pd.DataFrame([
        {
            "service": service_name,
            "model": "Per-Service Adaptive Threshold",
            "threshold": service_threshold,
            **service_metrics
        },
        {
            "service": service_name,
            "model": "Global Fixed-Threshold Baseline",
            "threshold": global_threshold,
            **global_metrics
        }
    ])

    print()
    print("=" * 72)
    print("COMPARISON SUMMARY")
    print("=" * 72)

    print(
        comparison[
            [
                "model",
                "threshold",
                "false_positive_rate",
                "precision",
                "recall",
                "f1_score"
            ]
        ].to_string(
            index=False
        )
    )

    # -----------------------------------------------------------------
    # Threshold evolution
    # -----------------------------------------------------------------

    print()
    print("=" * 72)
    print("THRESHOLD ADAPTATION SIMULATION")
    print("=" * 72)

    evolution = analyze_threshold_evolution(
        df
    )

    if evolution.empty:

        print(
            "Not enough completed incidents "
            "for threshold adaptation."
        )

    else:

        print(
            evolution.to_string(
                index=False
            )
        )

    # -----------------------------------------------------------------
    # Save comparison result
    # -----------------------------------------------------------------

    comparison_path = os.path.join(
        RESULTS_DIR,
        "comparison.csv"
    )

    comparison.to_csv(
        comparison_path,
        index=False
    )

    # -----------------------------------------------------------------
    # Save threshold evolution
    # -----------------------------------------------------------------

    evolution_path = os.path.join(
        RESULTS_DIR,
        f"{service_name}_threshold_evolution.csv"
    )

    if not evolution.empty:

        evolution.to_csv(
            evolution_path,
            index=False
        )

    # -----------------------------------------------------------------
    # Save detailed evaluation data
    # -----------------------------------------------------------------

    detailed_df = df.copy()

    detailed_df["per_service_prediction"] = (
        service_predictions
    )

    detailed_df["global_baseline_prediction"] = (
        global_predictions
    )

    detailed_path = os.path.join(
        RESULTS_DIR,
        f"{service_name}_evaluation.csv"
    )

    detailed_df.to_csv(
        detailed_path,
        index=False
    )

    print()
    print(
        f"Comparison saved to:\n"
        f"{comparison_path}"
    )

    if not evolution.empty:

        print(
            f"Threshold evolution saved to:\n"
            f"{evolution_path}"
        )

    print(
        f"Detailed evaluation saved to:\n"
        f"{detailed_path}"
    )

    print()


# =============================================================================
# RUN
# =============================================================================

if __name__ == "__main__":

    # Change this to:
    # order-service
    # payment-service
    # inventory-service
    #
    # when evaluating another service.

    run_comparison(
        "payment-service"
    )