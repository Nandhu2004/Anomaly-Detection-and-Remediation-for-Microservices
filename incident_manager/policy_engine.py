import os
import sqlite3
import logging
from datetime import datetime, timezone

from risk_assessor import assess_risk


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)


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

LOW_RISK_THRESHOLD = 0.3
HIGH_RISK_THRESHOLD = 0.5


# =============================================================================
# DATABASE SETUP / MIGRATION
# =============================================================================

def ensure_reason_column():
    """
    Make sure the incidents table contains the reason column.

    Existing databases created before the reason field was introduced
    will automatically be migrated.
    """

    if not os.path.exists(INCIDENTS_DB):
        log.warning(
            "Incidents database does not exist yet: %s",
            INCIDENTS_DB
        )
        return

    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        cursor = conn.cursor()

        cursor.execute(
            "PRAGMA table_info(incidents)"
        )

        columns = [
            row[1]
            for row in cursor.fetchall()
        ]

        if "reason" not in columns:

            cursor.execute(
                """
                ALTER TABLE incidents
                ADD COLUMN reason TEXT
                """
            )

            conn.commit()

            log.info(
                "Database migration complete: "
                "added incidents.reason column"
            )

        else:

            log.debug(
                "Database already contains incidents.reason"
            )

    except Exception as exc:

        conn.rollback()

        log.error(
            "Failed to migrate incidents database: %s",
            exc
        )

        raise

    finally:

        conn.close()


# Run database migration when this module is imported.
ensure_reason_column()


# =============================================================================
# SERVICE-SPECIFIC ACTION MAP
# =============================================================================
#
# Tier 1 actions:
#
# PAYMENT
#   error_burst    -> inspect_logs_then_sighup
#   latency_spike  -> sighup_worker
#
# ORDER
#   cpu_latency_spike -> sighup_worker
#   cpu_spike         -> sighup_worker
#   latency_spike     -> sighup_worker
#
# INVENTORY
#   cpu_overload  -> cpu_throttle
#   latency_spike -> cpu_throttle
#
# Tier 2:
#   restart_container
#
# Tier 3:
#   human_review
#
# Tier 4:
#   circuit_breaker
#
# =============================================================================

ACTION_MAP = {

    # -------------------------------------------------------------------------
    # PAYMENT SERVICE
    # -------------------------------------------------------------------------

    ("payment-service", "error_burst"):
        "inspect_logs_then_sighup",

    ("payment-service", "latency_spike"):
        "sighup_worker",

    ("payment-service", "general_anomaly"):
        "inspect_logs_then_sighup",


    # -------------------------------------------------------------------------
    # ORDER SERVICE
    # -------------------------------------------------------------------------

    ("order-service", "cpu_latency_spike"):
        "sighup_worker",

    ("order-service", "cpu_spike"):
        "sighup_worker",

    ("order-service", "latency_spike"):
        "sighup_worker",

    ("order-service", "general_anomaly"):
        "sighup_worker",


    # -------------------------------------------------------------------------
    # INVENTORY SERVICE
    # -------------------------------------------------------------------------

    ("inventory-service", "cpu_overload"):
        "cpu_throttle",

    ("inventory-service", "latency_spike"):
        "cpu_throttle",

    ("inventory-service", "general_anomaly"):
        "cpu_throttle",
}


# =============================================================================
# TIER 2 ACTION MAP
# =============================================================================

TIER2_ACTION_MAP = {

    "payment-service":
        "restart_container",

    "order-service":
        "restart_container",

    "inventory-service":
        "restart_container",
}


# =============================================================================
# TIER 3 ACTION
# =============================================================================

TIER3_ACTION = "human_review"


# =============================================================================
# TIER 4 ACTION
# =============================================================================

TIER4_ACTION = "circuit_breaker"


# =============================================================================
# GET TIER 1 ACTION
# =============================================================================

def get_suggested_action(
    anomaly_type: str,
    service_name: str = "payment-service"
) -> str:
    """
    Return the correct Tier 1 remediation action for a service.
    """

    action = ACTION_MAP.get(
        (
            service_name,
            anomaly_type,
        )
    )

    if action is not None:
        return action

    log.warning(
        "No specific Tier 1 action configured for "
        f"{service_name}/{anomaly_type}; "
        "falling back to container restart."
    )

    return TIER2_ACTION_MAP.get(
        service_name,
        "restart_container"
    )


# =============================================================================
# GET TIER 2 ACTION
# =============================================================================

def get_tier2_action(
    service_name: str
) -> str:

    return TIER2_ACTION_MAP.get(
        service_name,
        "restart_container"
    )


# =============================================================================
# UPDATE INCIDENT STATUS
# =============================================================================

def update_incident_status(
    incident_id: str,
    status: str,
    routing: str
):
    """
    Updates the current incident status and routing decision.
    """

    conn = sqlite3.connect(
        INCIDENTS_DB
    )

    try:

        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE incidents
            SET
                status = ?,
                routing_decision = ?
            WHERE incident_id = ?
            """,
            (
                status,
                routing,
                incident_id,
            )
        )

        conn.commit()

    finally:

        conn.close()


# =============================================================================
# UPDATE INCIDENT ACTION
# =============================================================================

def update_incident_action(
    incident_id: str,
    action: str
):
    """
    Stores the remediation action selected by the policy engine.
    """

    conn = sqlite3.connect(
        INCIDENTS_DB
    )

    try:

        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE incidents
            SET suggested_action = ?
            WHERE incident_id = ?
            """,
            (
                action,
                incident_id,
            )
        )

        conn.commit()

    finally:

        conn.close()


# =============================================================================
# EXTRACT ROUTING DECISION
# =============================================================================

def get_routing_decision(
    risk_result: dict
) -> str:
    """
    Extract the routing decision from the risk assessment.

    Current risk_assessor.py uses:
        routing_decision

    Older versions may use:
        routing

    routing_decision is preferred.
    """

    routing = risk_result.get(
        "routing_decision"
    )

    if routing is None:
        routing = risk_result.get(
            "routing"
        )

    if routing not in (
        "auto_heal",
        "human_review"
    ):
        routing = "human_review"

    return routing


# =============================================================================
# ROUTE INCIDENT
# =============================================================================

def route_incident(
    risk_result: dict,
    incident_id: str
) -> dict:
    """
    Takes a risk assessment result and makes the routing decision.

    LOW/MEDIUM:
        auto_heal

    HIGH:
        human_review

    The routing decision comes from risk_assessor.py.

    The policy engine selects the service-specific remediation action.
    """

    # -------------------------------------------------------------------------
    # Basic information
    # -------------------------------------------------------------------------

    service_name = risk_result.get(
        "service",
        "unknown"
    )

    risk_level = risk_result.get(
        "risk_level",
        "HIGH"
    )

    anomaly_type = risk_result.get(
        "anomaly_type",
        "general_anomaly"
    )

    risk_score = risk_result.get(
        "risk_score"
    )

    reason = risk_result.get(
        "reason"
    )

    # -------------------------------------------------------------------------
    # IMPORTANT:
    #
    # risk_assessor.py returns:
    #
    #     routing_decision="auto_heal"
    #
    # not necessarily:
    #
    #     routing="auto_heal"
    #
    # Therefore use the helper above.
    # -------------------------------------------------------------------------

    routing = get_routing_decision(
        risk_result
    )

    # -------------------------------------------------------------------------
    # Safety fallback for missing reason
    # -------------------------------------------------------------------------

    if not reason:

        if routing == "human_review":

            reason = (
                "Incident requires human approval "
                "before remediation."
            )

        elif routing == "auto_heal":

            reason = (
                "Risk assessment allows automated remediation."
            )

        else:

            reason = (
                "No routing reason was provided."
            )

    # -------------------------------------------------------------------------
    # Select service-specific Tier 1 action
    # -------------------------------------------------------------------------

    tier1_action = get_suggested_action(
        anomaly_type=anomaly_type,
        service_name=service_name,
    )

    # -------------------------------------------------------------------------
    # Select Tier 2 action
    # -------------------------------------------------------------------------

    tier2_action = get_tier2_action(
        service_name
    )

    # -------------------------------------------------------------------------
    # Final decision object
    # -------------------------------------------------------------------------

    decision = {

        "incident_id":
            incident_id,

        "service":
            service_name,

        "anomaly_type":
            anomaly_type,

        "risk_level":
            risk_level,

        "risk_score":
            risk_score,

        "routing":
            routing,

        "routing_decision":
            routing,

        "suggested_action":
            tier1_action,

        "tier1_action":
            tier1_action,

        "tier2_action":
            tier2_action,

        "tier3_action":
            TIER3_ACTION,

        "tier4_action":
            TIER4_ACTION,

        "reason":
            reason,

        "decided_at":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }

    # -------------------------------------------------------------------------
    # Database status
    # -------------------------------------------------------------------------

    if routing == "human_review":

        status = "pending"

    elif routing == "auto_heal":

        status = "auto_healing"

    else:

        status = "pending"

    # -------------------------------------------------------------------------
    # Update incident
    # -------------------------------------------------------------------------

    conn = sqlite3.connect(
        INCIDENTS_DB
    )

    try:

        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE incidents
            SET
                status = ?,
                routing_decision = ?,
                suggested_action = ?,
                reason = ?
            WHERE incident_id = ?
            """,
            (
                status,
                routing,
                tier1_action,
                reason,
                incident_id,
            )
        )

        conn.commit()

    finally:

        conn.close()

    # -------------------------------------------------------------------------
    # Logging
    # -------------------------------------------------------------------------

    if routing == "auto_heal":

        log.info(
            "AUTO-HEAL | "
            f"{incident_id} | "
            f"service={service_name} | "
            f"anomaly={anomaly_type} | "
            f"tier1={tier1_action} | "
            f"tier2={tier2_action} | "
            f"risk={risk_level} | "
            f"reason={reason}"
        )

    else:

        log.warning(
            "HUMAN REVIEW | "
            f"{incident_id} | "
            f"service={service_name} | "
            f"anomaly={anomaly_type} | "
            f"tier1={tier1_action} | "
            f"risk={risk_level} | "
            f"reason={reason}"
        )

    return decision


# =============================================================================
# MAKE DECISION
# =============================================================================

def make_decision(
    detection_result: dict,
    incident_id: str,
    risk_result: dict = None
) -> dict:
    """
    Complete policy-engine pipeline.

    Correct usage:

        risk = assess_risk(detection)

        decision = make_decision(
            detection,
            incident_id,
            risk
        )

    If risk_result is not supplied, the policy engine
    calculates it itself.
    """

    # -------------------------------------------------------------------------
    # Use already-calculated risk when available
    # -------------------------------------------------------------------------

    if risk_result is None:

        risk = assess_risk(
            detection_result
        )

    else:

        risk = risk_result

    # -------------------------------------------------------------------------
    # Safety check
    # -------------------------------------------------------------------------
    #
    # per_service_detector.py returns:
    #
    #     "anomaly": True/False
    #
    # -------------------------------------------------------------------------

    if not detection_result.get(
        "anomaly",
        False
    ):

        log.info(
            "POLICY | "
            "No anomaly detected | "
            "incident=%s",
            incident_id
        )

        return {

            "incident_id":
                incident_id,

            "service":
                detection_result.get(
                    "service"
                ),

            "routing":
                "none",

            "routing_decision":
                "none",

            "suggested_action":
                "none",

            "reason":
                "no anomaly — no action needed",
        }

    # -------------------------------------------------------------------------
    # Route incident
    # -------------------------------------------------------------------------

    decision = route_incident(
        risk,
        incident_id
    )

    return decision


# =============================================================================
# TEST EACH SERVICE'S POLICY
# =============================================================================

def run_policy_tests():

    test_cases = [

        # ---------------------------------------------------------------------
        # PAYMENT
        # ---------------------------------------------------------------------

        {
            "service":
                "payment-service",

            "anomaly_type":
                "error_burst",
        },

        # ---------------------------------------------------------------------
        # ORDER
        # ---------------------------------------------------------------------

        {
            "service":
                "order-service",

            "anomaly_type":
                "cpu_latency_spike",
        },

        # ---------------------------------------------------------------------
        # INVENTORY
        # ---------------------------------------------------------------------

        {
            "service":
                "inventory-service",

            "anomaly_type":
                "cpu_overload",
        },
    ]

    print(
        "\n" +
        "=" * 70
    )

    print(
        "POLICY ENGINE — SERVICE ACTION TEST"
    )

    print(
        "=" * 70
    )

    for test in test_cases:

        service = test["service"]

        anomaly = test["anomaly_type"]

        action = get_suggested_action(
            anomaly,
            service
        )

        tier2 = get_tier2_action(
            service
        )

        print(
            f"\nService       : {service}"
        )

        print(
            f"Anomaly       : {anomaly}"
        )

        print(
            f"Tier 1        : {action}"
        )

        print(
            f"Tier 2        : {tier2}"
        )

        print(
            f"Tier 3        : {TIER3_ACTION}"
        )

        print(
            f"Tier 4        : {TIER4_ACTION}"
        )

    print(
        "\n" +
        "=" * 70
    )


# =============================================================================
# STANDALONE TEST
# =============================================================================

if __name__ == "__main__":

    run_policy_tests()
