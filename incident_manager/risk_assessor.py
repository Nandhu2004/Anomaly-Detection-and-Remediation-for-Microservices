import os
import sqlite3
import logging
from typing import Dict, Any


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)


# =============================================================================
# SERVICE CONFIGURATION
# =============================================================================

SERVICE_CRITICALITY = {
    "payment-service": 1.5,
    "order-service": 1.2,
    "inventory-service": 1.0,
}


# =============================================================================
# RISK THRESHOLDS
# =============================================================================

LOW_RISK_THRESHOLD = 0.30
HIGH_RISK_THRESHOLD = 0.50

# Minimum historical confidence required for automatic remediation
MIN_CONFIDENCE = 0.40


# =============================================================================
# DATABASE PATH
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


# =============================================================================
# HELPERS
# =============================================================================

def get_metric(
    detection_result: Dict[str, Any],
    key: str,
    default: float = 0.0
):
    """
    Safely retrieve a metric.

    The detector can provide metrics either:

        detection_result["cpu_usage"]

    or:

        detection_result["metrics"]["cpu_usage"]

    If the metric is missing:
        - returns the supplied default
        - if default=None, returns None
    """

    # ---------------------------------------------------------
    # First check top-level field.
    # ---------------------------------------------------------

    value = detection_result.get(key)

    if value is not None:

        try:
            return float(value)

        except (
            TypeError,
            ValueError
        ):
            pass

    # ---------------------------------------------------------
    # Then check nested metrics.
    # ---------------------------------------------------------

    metrics = detection_result.get(
        "metrics",
        {}
    )

    if isinstance(
        metrics,
        dict
    ):

        value = metrics.get(key)

        if value is not None:

            try:
                return float(value)

            except (
                TypeError,
                ValueError
            ):
                pass

    # ---------------------------------------------------------
    # Missing metric.
    #
    # IMPORTANT:
    # default=None is used when we only want to preserve
    # metrics that actually exist.
    # ---------------------------------------------------------

    if default is None:
        return None

    return float(default)

# =============================================================================
# SERVICE-SPECIFIC ANOMALY DETECTION
# =============================================================================

def detect_anomaly_type(
    detection_result: Dict[str, Any]
) -> str:
    """
    Determines the anomaly type according to the service.

    Supports both flat detector results and the actual nested
    detector structure:

        detection_result["metrics"]

    Payment:
        error burst is the primary anomaly.

    Order:
        CPU + latency rise together.

    Inventory:
        CPU overload above an already-high normal baseline.
    """

    service = detection_result.get(
        "service",
        "unknown"
    )


    # =========================================================================
    # PAYMENT SERVICE
    # =========================================================================

    if service == "payment-service":

        burst_active = get_metric(
            detection_result,
            "burst_active"
        )

        error_rate = get_metric(
            detection_result,
            "error_rate"
        )

        latency_avg = get_metric(
            detection_result,
            "latency_avg"
        )

        if (
            burst_active >= 1.0
            or error_rate >= 10.0
        ):

            return "error_burst"

        if latency_avg >= 0.5:

            return "latency_spike"

        return "general_anomaly"


    # =========================================================================
    # ORDER SERVICE
    # =========================================================================

    if service == "order-service":

        # ---------------------------------------------------------
        # Actual detector currently exposes:
        #
        # spike_active
        #
        # Older/alternative versions may expose:
        #
        # cpu_spike_active
        # latency_spike_active
        # ---------------------------------------------------------

        spike_active = get_metric(
            detection_result,
            "spike_active"
        )

        cpu_spike_active = get_metric(
            detection_result,
            "cpu_spike_active"
        )

        latency_spike_active = get_metric(
            detection_result,
            "latency_spike_active"
        )

        cpu_usage = get_metric(
            detection_result,
            "cpu_usage"
        )

        latency_avg = get_metric(
            detection_result,
            "latency_avg"
        )

        # ---------------------------------------------------------
        # If the combined detector signal is active, determine
        # whether CPU and/or latency explains it.
        # ---------------------------------------------------------

        if spike_active >= 1.0:

            cpu_signal = (
                cpu_spike_active >= 1.0
                or cpu_usage >= 75.0
            )

            latency_signal = (
                latency_spike_active >= 1.0
                or latency_avg >= 1.0
            )

            if (
                cpu_signal
                and latency_signal
            ):

                return "cpu_latency_spike"

            if cpu_signal:

                return "cpu_spike"

            if latency_signal:

                return "latency_spike"

        # ---------------------------------------------------------
        # Explicit CPU + latency signals.
        # ---------------------------------------------------------

        if (
            cpu_spike_active >= 1.0
            and latency_spike_active >= 1.0
        ):

            return "cpu_latency_spike"

        # ---------------------------------------------------------
        # Fallback based on actual metric values.
        # ---------------------------------------------------------

        if (
            cpu_usage >= 75.0
            and latency_avg >= 1.0
        ):

            return "cpu_latency_spike"

        if (
            cpu_spike_active >= 1.0
            or cpu_usage >= 75.0
        ):

            return "cpu_spike"

        if (
            latency_spike_active >= 1.0
            or latency_avg >= 1.0
        ):

            return "latency_spike"

        return "general_anomaly"


    # =========================================================================
    # INVENTORY SERVICE
    # =========================================================================

    if service == "inventory-service":

        cpu_overload_active = get_metric(
            detection_result,
            "cpu_overload_active"
        )

        # Actual detector field.
        overload_active = get_metric(
            detection_result,
            "overload_active"
        )

        cpu_usage = get_metric(
            detection_result,
            "cpu_usage"
        )

        latency_avg = get_metric(
            detection_result,
            "latency_avg"
        )

        # ---------------------------------------------------------
        # Support both overload field names.
        # ---------------------------------------------------------

        if (
            cpu_overload_active >= 1.0
            or overload_active >= 1.0
            or cpu_usage >= 90.0
        ):

            return "cpu_overload"

        if latency_avg >= 3.0:

            return "latency_spike"

        return "general_anomaly"


    # =========================================================================
    # UNKNOWN SERVICE
    # =========================================================================

    return "general_anomaly"


# =============================================================================
# HISTORICAL REMEDIATION CONFIDENCE
# =============================================================================

def get_remediation_confidence(
    service_name: str,
    anomaly_type: str
) -> float:
    """
    Reads previous incidents for the same service + anomaly type.

    If enough history exists, confidence is based on successful outcomes.

    If there is no history yet, a neutral default of 0.7 is used.
    """

    if not os.path.exists(
        INCIDENTS_DB
    ):

        log.warning(
            "Incident database does not exist — "
            f"using default confidence for "
            f"{service_name}/{anomaly_type}"
        )

        return 0.70

    try:

        conn = sqlite3.connect(
            INCIDENTS_DB
        )

        conn.row_factory = sqlite3.Row

        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                outcome,
                risk_level,
                routing_decision,
                status
            FROM incidents
            WHERE service_name = ?
              AND anomaly_type = ?
            ORDER BY created_at DESC
            LIMIT 10
            """,
            (
                service_name,
                anomaly_type,
            )
        )

        rows = cursor.fetchall()

        conn.close()

        if not rows:

            log.info(
                f"No history for "
                f"{service_name}/{anomaly_type} "
                f"— using default confidence 0.7"
            )

            return 0.70

        successful = 0
        completed = 0

        for row in rows:

            outcome = row["outcome"]

            if outcome is None:
                continue

            completed += 1

            normalized = (
                str(outcome)
                .strip()
                .lower()
            )

            if normalized in (
                "success",
                "successful",
                "resolved",
                "recovered",
                "auto_healed",
                "healed",
            ):

                successful += 1

        if completed == 0:

            return 0.70

        confidence = (
            successful / completed
        )

        # Keep a small floor so one failed incident does not
        # immediately destroy all historical confidence.
        confidence = max(
            0.10,
            min(
                1.0,
                confidence
            )
        )

        log.info(
            f"Historical confidence | "
            f"service={service_name} | "
            f"anomaly={anomaly_type} | "
            f"successful={successful}/{completed} | "
            f"confidence={confidence:.2f}"
        )

        return confidence

    except sqlite3.Error as exc:

        log.error(
            f"Could not read incident history: {exc}"
        )

        return 0.70


# =============================================================================
# SUGGESTED ACTION
# =============================================================================

def get_suggested_action(
    service_name: str,
    anomaly_type: str
) -> str:
    """
    Returns the initial remediation strategy.

    The policy engine/remediation executor performs the
    actual action.
    """

    # -------------------------------------------------------------------------
    # PAYMENT
    # -------------------------------------------------------------------------

    if service_name == "payment-service":

        if anomaly_type == "error_burst":

            return "inspect_logs_then_sighup"

        if anomaly_type == "latency_spike":

            return "sighup_worker"

        return "restart_container"


    # -------------------------------------------------------------------------
    # ORDER
    # -------------------------------------------------------------------------

    if service_name == "order-service":

        if anomaly_type in (
            "cpu_latency_spike",
            "cpu_spike",
            "latency_spike",
        ):

            return "sighup_worker"

        return "restart_container"


    # -------------------------------------------------------------------------
    # INVENTORY
    # -------------------------------------------------------------------------

    if service_name == "inventory-service":

        if anomaly_type in (
            "cpu_overload",
            "latency_spike",
        ):

            return "cpu_throttle"

        return "restart_container"


    return "restart_container"


# =============================================================================
# RISK CALCULATION
# =============================================================================

def calculate_risk(
    anomaly_score: float,
    threshold: float,
    service_name: str,
    confidence: float
) -> Dict[str, Any]:
    """
    Converts anomaly score into operational risk.

    Isolation Forest:
        normal values are generally positive
        anomalous values are negative

    More negative score = stronger anomaly.

    Risk score is an operational risk value, NOT a probability.
    """

    criticality = SERVICE_CRITICALITY.get(
        service_name,
        1.0
    )

    # -------------------------------------------------------------------------
    # Calculate anomaly severity.
    # -------------------------------------------------------------------------

    severity = (
        abs(float(anomaly_score))
        - float(threshold)
    )

    severity = max(
        0.0,
        severity
    )

    # -------------------------------------------------------------------------
    # Protect against zero confidence.
    # -------------------------------------------------------------------------

    safe_confidence = max(
        float(confidence),
        0.10
    )

    # -------------------------------------------------------------------------
    # Calculate operational risk.
    # -------------------------------------------------------------------------

    risk_score = (
        severity
        * criticality
    ) / safe_confidence

    # -------------------------------------------------------------------------
    # Determine risk level + routing.
    # -------------------------------------------------------------------------

    if confidence < MIN_CONFIDENCE:

        risk_level = "HIGH"

        routing = "human_review"

        reason = (
            "Historical remediation confidence is below "
            f"the minimum threshold ({MIN_CONFIDENCE:.2f})."
        )

    elif risk_score < LOW_RISK_THRESHOLD:

        risk_level = "LOW"

        routing = "auto_heal"

        reason = (
            "Low operational risk based on anomaly severity, "
            "service criticality, and historical confidence."
        )

    elif risk_score < HIGH_RISK_THRESHOLD:

        risk_level = "MEDIUM"

        routing = "auto_heal"

        reason = (
            "Medium operational risk; automatic remediation "
            "is permitted."
        )

    else:

        risk_level = "HIGH"

        routing = "human_review"

        reason = (
            "High operational risk; human approval is required."
        )

    return {
        "risk_level": risk_level,

        "risk_score": round(
            risk_score,
            4
        ),

        "routing": routing,

        "reason": reason,

        "confidence": round(
            confidence,
            4
        ),

        "criticality": criticality,
    }


# =============================================================================
# PUBLIC API
# =============================================================================

def assess_risk(
    detection_result: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Complete risk-assessment pipeline.

    Input:
        Detection result from per_service_detector.py

    Output:
        Service-aware risk result consumed by main.py,
        incident manager and policy engine.
    """

    service_name = detection_result.get(
        "service",
        "unknown"
    )

    anomaly_score = float(
    detection_result.get(
        "anomaly_score",
detection_result.get(
"score",
0.0
        )
    )
)

    threshold = float(
        detection_result.get(
            "threshold",
            0.6
        )
    )

    # -------------------------------------------------------------------------
    # Determine anomaly type.
    # -------------------------------------------------------------------------

    anomaly_type = detect_anomaly_type(
        detection_result
    )

    # -------------------------------------------------------------------------
    # Historical confidence.
    # -------------------------------------------------------------------------

    confidence = get_remediation_confidence(
        service_name,
        anomaly_type
    )

    # -------------------------------------------------------------------------
    # Risk.
    # -------------------------------------------------------------------------

    risk = calculate_risk(
        anomaly_score=anomaly_score,
        threshold=threshold,
        service_name=service_name,
        confidence=confidence,
    )

    # -------------------------------------------------------------------------
    # Suggested action.
    # -------------------------------------------------------------------------

    suggested_action = get_suggested_action(
        service_name,
        anomaly_type
    )

    result = {

        "service":
            service_name,

        "anomaly_type":
            anomaly_type,

        "anomaly_score":
            anomaly_score,

        "threshold":
            threshold,

        "severity":
            detection_result.get(
                "severity",
                "high"
            ),

        "risk_level":
            risk["risk_level"],

        "risk_score":
            risk["risk_score"],

        "routing":
            risk["routing"],

        "reason":
            risk["reason"],

        "confidence":
            risk["confidence"],

        "criticality":
            risk["criticality"],

        "suggested_action":
            suggested_action,
    }

    # -------------------------------------------------------------------------
    # Preserve original detection information.
    # -------------------------------------------------------------------------

    for key in (
        "error_rate",
        "latency_avg",
        "cpu_usage",
        "burst_active",
        "spike_active",
        "cpu_spike_active",
        "latency_spike_active",
        "overload_active",
        "cpu_overload_active",
        "batch_job_active",
    ):

        value = get_metric(
            detection_result,
            key,
            default=None
        )

        if value is not None:

            result[key] = value

    # -------------------------------------------------------------------------
    # Preserve nested metrics as well.
    # -------------------------------------------------------------------------

    metrics = detection_result.get(
        "metrics"
    )

    if isinstance(
        metrics,
        dict
    ):

        result["metrics"] = metrics

    log.info(
        "Risk assessment | "
        f"service={service_name} | "
        f"type={anomaly_type} | "
        f"score={anomaly_score:.4f} | "
        f"risk={risk['risk_level']} | "
        f"risk_score={risk['risk_score']:.4f} | "
        f"routing={risk['routing']} | "
        f"action={suggested_action}"
    )

    return result


# =============================================================================
# STANDALONE TEST
# =============================================================================

if __name__ == "__main__":

    test_cases = [

        {
            "service": "payment-service",
            "anomaly_score": -1.0,
            "threshold": 0.6,
            "severity": "high",
            "metrics": {
                "error_rate": 85.0,
                "latency_avg": 1.2,
                "burst_active": 1.0,
            },
        },

        {
            "service": "order-service",
            "anomaly_score": -1.0,
            "threshold": 0.6,
            "severity": "high",
            "metrics": {
                "cpu_usage": 96.68,
                "latency_avg": 1.254,
                "spike_active": 1.0,
            },
        },

        {
            "service": "inventory-service",
            "anomaly_score": -0.9,
            "threshold": 0.6,
            "severity": "high",
            "metrics": {
                "cpu_usage": 95.0,
                "latency_avg": 7.0,
                "overload_active": 1.0,
            },
        },
    ]

    for test in test_cases:

        print(
            "\n" + "=" * 70
        )

        result = assess_risk(
            test
        )

        for key, value in result.items():

            print(
                f"{key:20}: {value}"
            )