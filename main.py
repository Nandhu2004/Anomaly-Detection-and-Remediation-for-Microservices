
import os
import sys
import time
import logging
import sqlite3
import uuid
from datetime import datetime, timezone


# =============================================================================
# PATH CONFIGURATION
# =============================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

sys.path.insert(0, os.path.join(BASE_DIR, "anomaly_detector"))
sys.path.insert(0, os.path.join(BASE_DIR, "incident_manager"))
sys.path.insert(0, os.path.join(BASE_DIR, "remediation_executor"))
sys.path.insert(0, os.path.join(BASE_DIR, "feedback"))


# =============================================================================
# IMPORTS
# =============================================================================

from per_service_detector import (
    run_service_detection,
    init_profiles_db
)

from risk_assessor import assess_risk
from policy_engine import make_decision
from executor import (
    execute_remediation,
    is_still_anomalous,
)
from outcome_monitor import monitor_outcome
from adaptive_threshold import adjust_threshold


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

DETECTION_INTERVAL = int(
    os.getenv("DETECTION_INTERVAL", "30")
)

AUTO_HEAL = (
    os.getenv("AUTO_HEAL", "true").lower() == "true"
)

INCIDENTS_DB = os.path.join(
    BASE_DIR,
    "incident_manager",
    "incidents.db"
)

SERVICES = [
    "order-service",
    "payment-service",
    "inventory-service"
]


# =============================================================================
# DATABASE INITIALIZATION
# =============================================================================

def init_incidents_db():

    os.makedirs(
        os.path.dirname(INCIDENTS_DB),
        exist_ok=True
    )

    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS incidents (
                incident_id TEXT PRIMARY KEY,
                service_name TEXT,
                anomaly_type TEXT,
                anomaly_score REAL,
                risk_level TEXT,
                risk_score REAL,
                routing_decision TEXT,
                reason TEXT,
                status TEXT DEFAULT 'pending',
                suggested_action TEXT,
                action_taken TEXT,
                outcome TEXT,
                human_approved TEXT,
                human_reject_reason TEXT,
                tier1_attempted INTEGER DEFAULT 0,
                tier1_outcome TEXT,
                tier2_attempted INTEGER DEFAULT 0,
                tier2_outcome TEXT,
                created_at TEXT,
                resolved_at TEXT,
                time_to_recover REAL
            )
        """)

        # Migrate existing databases without deleting their data.
        columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(incidents)"
            ).fetchall()
        }

        if "reason" not in columns:
            conn.execute("""
                ALTER TABLE incidents
                ADD COLUMN reason TEXT
            """)

        # One tracked episode per service.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS anomaly_episodes (
                service_name TEXT PRIMARY KEY,
                anomaly_type TEXT,
                active INTEGER DEFAULT 0,
                last_seen_at TEXT
            )
        """)

        conn.commit()

    finally:
        conn.close()


# =============================================================================
# TIME HELPER
# =============================================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# INCIDENT ID
# =============================================================================

def generate_incident_id():

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:4].upper()

    return f"INC_{timestamp}_{suffix}"


# =============================================================================
# EPISODE MANAGEMENT
# =============================================================================

def get_episode(service_name):

    conn = sqlite3.connect(INCIDENTS_DB)
    conn.row_factory = sqlite3.Row

    try:
        row = conn.execute("""
            SELECT *
            FROM anomaly_episodes
            WHERE service_name = ?
        """, (service_name,)).fetchone()

        return dict(row) if row else None

    finally:
        conn.close()


def start_anomaly_episode(service_name, anomaly_type):

    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        conn.execute("""
            INSERT INTO anomaly_episodes (
                service_name,
                anomaly_type,
                active,
                last_seen_at
            )
            VALUES (?, ?, 1, ?)

            ON CONFLICT(service_name)
            DO UPDATE SET
                anomaly_type = excluded.anomaly_type,
                active = 1,
                last_seen_at = excluded.last_seen_at
        """, (
            service_name,
            anomaly_type,
            utc_now()
        ))

        conn.commit()

    finally:
        conn.close()


def update_anomaly_episode_seen(service_name, anomaly_type):

    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        conn.execute("""
            UPDATE anomaly_episodes
            SET
                anomaly_type = ?,
                last_seen_at = ?
            WHERE service_name = ?
              AND active = 1
        """, (
            anomaly_type,
            utc_now(),
            service_name
        ))

        conn.commit()

    finally:
        conn.close()


def end_anomaly_episode(service_name):

    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        conn.execute("""
            UPDATE anomaly_episodes
            SET
                active = 0,
                last_seen_at = ?
            WHERE service_name = ?
        """, (
            utc_now(),
            service_name
        ))

        conn.commit()

    finally:
        conn.close()

    log.info(
        "ANOMALY EPISODE ENDED | service=%s",
        service_name
    )


def is_same_anomaly_episode(service_name, anomaly_type):
    """
    Only one active anomaly episode is tracked per service.

    If a different anomaly type is detected while that episode remains
    active, it is treated as part of the existing episode. This prevents
    repeated detections from creating multiple incidents for one service.
    """

    episode = get_episode(service_name)

    if not episode:
        return False

    return episode.get("active") == 1


# =============================================================================
# RESTORE EPISODE STATE AFTER RESTART
# =============================================================================

def seed_existing_episodes():
    """
    Restore episode state from unresolved incidents after a restart.

    Only the newest unresolved incident per service is used to restore
    an inactive or missing episode. Existing active episodes are preserved.
    """

    conn = sqlite3.connect(INCIDENTS_DB)
    conn.row_factory = sqlite3.Row

    try:
        rows = conn.execute("""
            SELECT
                service_name,
                anomaly_type,
                created_at
            FROM incidents
            WHERE status IN ('pending', 'auto_healing')
              AND anomaly_type IS NOT NULL
            ORDER BY rowid DESC
        """).fetchall()

        processed_services = set()

        for row in rows:

            service_name = row["service_name"]
            anomaly_type = row["anomaly_type"]

            if not service_name or not anomaly_type:
                continue

            # Process only the newest unresolved incident per service.
            if service_name in processed_services:
                continue

            processed_services.add(service_name)

            existing = conn.execute("""
                SELECT active
                FROM anomaly_episodes
                WHERE service_name = ?
            """, (service_name,)).fetchone()

            if existing and existing["active"] == 1:
                continue

            conn.execute("""
                INSERT INTO anomaly_episodes (
                    service_name,
                    anomaly_type,
                    active,
                    last_seen_at
                )
                VALUES (?, ?, 1, ?)

                ON CONFLICT(service_name)
                DO UPDATE SET
                    anomaly_type = excluded.anomaly_type,
                    active = 1,
                    last_seen_at = excluded.last_seen_at
            """, (
                service_name,
                anomaly_type,
                row["created_at"] or utc_now()
            ))

        conn.commit()

    finally:
        conn.close()


# =============================================================================
# CREATE INCIDENT
# =============================================================================

def log_incident(risk_result):

    incident_id = generate_incident_id()
    created_at = utc_now()

    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        conn.execute("""
            INSERT INTO incidents (
                incident_id,
                service_name,
                anomaly_type,
                anomaly_score,
                risk_level,
                risk_score,
                routing_decision,
                reason,
                status,
                suggested_action,
                action_taken,
                outcome,
                human_approved,
                human_reject_reason,
                tier1_attempted,
                tier1_outcome,
                tier2_attempted,
                tier2_outcome,
                created_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, (
            incident_id,
            risk_result.get("service_name"),
            risk_result.get("anomaly_type"),
            risk_result.get("anomaly_score"),
            risk_result.get("risk_level"),
            risk_result.get("risk_score"),
            risk_result.get("routing_decision"),
            risk_result.get("reason"),
            "pending",
            risk_result.get("suggested_action"),
            None,
            None,
            None,
            None,
            0,
            None,
            0,
            None,
            created_at
        ))

        conn.commit()

    finally:
        conn.close()

    log.info(
        "INCIDENT CREATED | incident=%s | service=%s | "
        "anomaly=%s | risk=%s",
        incident_id,
        risk_result.get("service_name"),
        risk_result.get("anomaly_type"),
        risk_result.get("risk_level")
    )

    return incident_id


# =============================================================================
# UPDATE INCIDENT
# =============================================================================

def update_incident(incident_id, **fields):

    allowed_fields = {
        "status",
        "routing_decision",
        "reason",
        "suggested_action",
        "action_taken",
        "outcome",
        "human_approved",
        "human_reject_reason",
        "tier1_attempted",
        "tier1_outcome",
        "tier2_attempted",
        "tier2_outcome",
        "resolved_at",
        "time_to_recover"
    }

    fields = {
        key: value
        for key, value in fields.items()
        if key in allowed_fields
    }

    if not fields:
        return

    assignments = ", ".join(
        f"{key} = ?"
        for key in fields
    )

    values = list(fields.values())
    values.append(incident_id)

    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        conn.execute(
            f"""
            UPDATE incidents
            SET {assignments}
            WHERE incident_id = ?
            """,
            values
        )

        conn.commit()

    finally:
        conn.close()


# =============================================================================
# REMEDIATION RESULT NORMALIZATION
# =============================================================================

def normalize_remediation_result(success, result):

    if isinstance(result, dict):
        return (
            bool(success),
            result.get("result"),
            result.get("action"),
            result.get("tier")
        )

    return (
        bool(success),
        result,
        None,
        None
    )


# =============================================================================
# ROUTING HELPER
# =============================================================================

def determine_routing(risk, policy):
    """
    Human review from either component takes priority over auto-healing.
    Unknown or missing routing defaults to human review.
    """

    risk_routing = risk.get(
        "routing_decision",
        risk.get("routing")
    )

    policy_routing = None

    if isinstance(policy, dict):
        policy_routing = policy.get(
            "routing_decision",
            policy.get("routing", policy.get("decision"))
        )

    if isinstance(risk_routing, str):
        risk_routing = risk_routing.strip().lower()

    if isinstance(policy_routing, str):
        policy_routing = policy_routing.strip().lower()

    human_review_values = {
        "human_review",
        "human_review_required"
    }

    # Safest route wins.
    if (
        risk_routing in human_review_values
        or policy_routing in human_review_values
    ):
        return "human_review"

    if (
        risk_routing == "auto_heal"
        or policy_routing == "auto_heal"
    ):
        return "auto_heal"

    return "human_review"


# =============================================================================
# PROCESS ONE SERVICE
# =============================================================================

def process_service(service_name):

    try:

        # =====================================================================
        # DETECTION
        # =====================================================================

        detection = run_service_detection(service_name)

        if not detection:
            log.warning(
                "No detection result | service=%s",
                service_name
            )
            return

        anomaly = bool(detection.get("anomaly", False))

        # Prefer the positive anomaly score used by the detector threshold.
        score = detection.get(
            "anomaly_score",
            detection.get("score")
        )

        threshold = detection.get("threshold", 0.60)
        severity = detection.get("severity", "normal")
        indicator_active = detection.get("indicator_active")

        # =====================================================================
        # NORMAL / EPISODE RECOVERY
        # =====================================================================

        if not anomaly:

            if score is None:
                log.info(
                    "NORMAL | service=%s | score=N/A | threshold=%.2f",
                    service_name,
                    threshold
                )
            else:
                log.info(
                    "NORMAL | service=%s | score=%.4f | threshold=%.2f",
                    service_name,
                    score,
                    threshold
                )

            episode = get_episode(service_name)

            if episode and episode.get("active") == 1:
                end_anomaly_episode(service_name)

            return

        # =====================================================================
        # ANOMALY DETECTED
        # =====================================================================

        log.warning(
            "ANOMALY DETECTED | service=%s | score=%s | "
            "threshold=%s | severity=%s | indicator_active=%s",
            service_name,
            score,
            threshold,
            severity,
            indicator_active
        )

        # =====================================================================
        # RISK ASSESSMENT
        # =====================================================================

        risk = assess_risk(detection)

        if not risk:
            log.warning(
                "Risk assessment returned no result | service=%s",
                service_name
            )
            return

        risk["service_name"] = service_name
        risk["anomaly"] = anomaly
        risk["severity"] = severity
        risk["threshold"] = threshold

        # Keep the score consistent with the detector output.
        if detection.get("anomaly_score") is not None:
            risk["anomaly_score"] = detection["anomaly_score"]
        elif risk.get("anomaly_score") is None:
            risk["anomaly_score"] = score

        # =====================================================================
        # ANOMALY TYPE
        # =====================================================================

        anomaly_type = risk.get("anomaly_type")

        if not anomaly_type:
            log.warning(
                "No anomaly type returned by risk assessor | service=%s",
                service_name
            )
            return

        # =====================================================================
        # DUPLICATE INCIDENT PREVENTION
        # =====================================================================

        # The detector's final anomaly decision is used. An ML-detected
        # anomaly is not discarded just because the service indicator is off.
        if not anomaly:
            return

        if is_same_anomaly_episode(service_name, anomaly_type):

            update_anomaly_episode_seen(
                service_name,
                anomaly_type
            )

            log.info(
                "ACTIVE ANOMALY EPISODE | service=%s | "
                "detected_type=%s | no duplicate incident created",
                service_name,
                anomaly_type
            )

            return

        # =====================================================================
        # NEW ANOMALY EPISODE
        # =====================================================================

        log.warning(
            "NEW ANOMALY EPISODE | service=%s | anomaly_type=%s",
            service_name,
            anomaly_type
        )

        # =====================================================================
        # CREATE INCIDENT
        # =====================================================================

        incident_id = log_incident(risk)

        if not incident_id:
            log.error(
                "Incident creation failed | service=%s",
                service_name
            )
            return

        # Activate the episode after successfully creating the incident.
        start_anomaly_episode(
            service_name,
            anomaly_type
        )

        # =====================================================================
        # POLICY ENGINE
        # =====================================================================

        try:
            policy = make_decision(
                detection,
                incident_id,
                risk
            )

        except Exception:
            log.exception(
                "Policy engine failed | incident=%s",
                incident_id
            )
            policy = None

        # =====================================================================
        # AUTHORITATIVE ROUTING
        # =====================================================================

        routing_decision = determine_routing(
            risk,
            policy
        )

        # =====================================================================
        # POLICY ACTION / REASON
        # =====================================================================

        reason = risk.get("reason")
        suggested_action = risk.get("suggested_action")

        if isinstance(policy, dict):

            if policy.get("reason"):
                reason = policy["reason"]

            if policy.get("suggested_action"):
                suggested_action = policy["suggested_action"]

        # =====================================================================
        # UPDATE INCIDENT WITH FINAL ROUTING
        # =====================================================================

        update_incident(
            incident_id,
            routing_decision=routing_decision,
            reason=reason,
            suggested_action=suggested_action
        )

        log.info(
            "POLICY | incident=%s | routing=%s | reason=%s",
            incident_id,
            routing_decision,
            reason
        )

        # =====================================================================
        # HUMAN REVIEW
        # =====================================================================

        if routing_decision == "human_review":

            update_incident(
                incident_id,
                status="pending",
                action_taken="awaiting_human_review",
                human_approved=None
            )

            log.warning(
                "HUMAN REVIEW REQUIRED | incident=%s | service=%s",
                incident_id,
                service_name
            )

            return

        # =====================================================================
        # AUTO HEAL DISABLED
        # =====================================================================

        if not AUTO_HEAL:

            update_incident(
                incident_id,
                status="pending",
                action_taken="awaiting_manual_approval",
                human_approved=None
            )

            log.info(
                "AUTO HEAL DISABLED | incident=%s",
                incident_id
            )

            return

        # =====================================================================
        # AUTO HEAL
        # =====================================================================

        if routing_decision == "auto_heal":

            update_incident(
                incident_id,
                status="auto_healing",
                action_taken="remediation_started",
                human_approved="auto"
            )

            log.info(
                "AUTO-HEAL | incident=%s | service=%s | "
                "anomaly=%s | suggested_action=%s | risk=%s | reason=%s",
                incident_id,
                service_name,
                anomaly_type,
                suggested_action,
                risk.get("risk_level"),
                reason
            )

            try:

                success, result = execute_remediation(incident_id)

                (
                    remediation_success,
                    result_name,
                    result_action,
                    result_tier
                ) = normalize_remediation_result(
                    success,
                    result
                )

                # -------------------------------------------------------------
                # NATURAL RECOVERY
                # -------------------------------------------------------------

                if result_name in (
                    "auto_resolved",
                    "recovered",
                    "recovered_without_remediation"
                ):

                    update_incident(
                        incident_id,
                        status="recovered",
                        action_taken="recovered_without_remediation",
                        outcome="recovered",
                        resolved_at=utc_now()
                    )

                    log.info(
                        "RECOVERED WITHOUT REMEDIATION | incident=%s",
                        incident_id
                    )

                    return

                # -------------------------------------------------------------
                # REMEDIATION SUCCESS
                # -------------------------------------------------------------

                if remediation_success:

                    update_incident(
                        incident_id,
                        status="resolved",
                        outcome="success",
                        resolved_at=utc_now()
                    )

                    log.info(
                        "REMEDIATION SUCCESS | incident=%s | "
                        "tier=%s | action=%s",
                        incident_id,
                        result_tier,
                        result_action or result_name
                    )

                    return

                # -------------------------------------------------------------
                # TIER 3 / HUMAN ESCALATION
                # -------------------------------------------------------------

                if result_name in (
                    "human_review_required",
                    "escalated_to_human_tier3"
                ):

                    update_incident(
                        incident_id,
                        status="pending",
                        action_taken="awaiting_human_review",
                        human_approved=None
                    )

                    log.warning(
                        "ESCALATED TO HUMAN | incident=%s",
                        incident_id
                    )

                    return

                # -------------------------------------------------------------
                # CIRCUIT BREAKER ALREADY ACTIVE
                # -------------------------------------------------------------

                if result_name == "circuit_breaker_active":

                    update_incident(
                        incident_id,
                        status="pending",
                        action_taken="circuit_breaker_already_active",
                        outcome="failure"
                    )

                    log.warning(
                        "CIRCUIT BREAKER ALREADY ACTIVE | incident=%s",
                        incident_id
                    )

                    return

                # -------------------------------------------------------------
                # REMEDIATION PERFORMED — MONITOR OUTCOME
                # -------------------------------------------------------------

                if result_name in (
                    "remediated",
                    "remediation_started",
                    "tier1_sighup",
                    "tier2_restart"
                ):

                    update_incident(
                        incident_id,
                        status="remediated",
                        action_taken=result_action or result_name
                    )

                    try:
                        monitor_outcome(
                            incident_id,
                            service_name
                        )

                    except Exception:
                        log.exception(
                            "Outcome monitoring failed | incident=%s",
                            incident_id
                        )

                    return

                # -------------------------------------------------------------
                # UNKNOWN RESULT
                # -------------------------------------------------------------

                update_incident(
                    incident_id,
                    status="pending",
                    action_taken=str(result)
                )

                log.warning(
                    "UNKNOWN REMEDIATION RESULT | incident=%s | result=%s",
                    incident_id,
                    result
                )

            except Exception:

                log.exception(
                    "Auto remediation failed | incident=%s",
                    incident_id
                )

                update_incident(
                    incident_id,
                    status="failed",
                    outcome="failure",
                    action_taken="remediation_exception"
                )

            return

        # =====================================================================
        # UNKNOWN ROUTING — FAIL SAFE
        # =====================================================================

        update_incident(
            incident_id,
            status="pending",
            action_taken="awaiting_manual_approval"
        )

        log.warning(
            "UNKNOWN ROUTING | incident=%s | routing=%s",
            incident_id,
            routing_decision
        )

    except Exception:
        log.exception(
            "Service processing failed | service=%s",
            service_name
        )


# =============================================================================
# STARTUP RECONCILIATION FOR INTERRUPTED REMEDIATION
# =============================================================================

def reconcile_interrupted_incidents():
    """Re-check eligible incidents left mid-remediation after a shutdown.

    Reconcile unresolved records only when a Tier 1 or Tier 2 attempt was
    recorded. If the service-specific indicator confirms recovery, close the
    stale incident as recovered; otherwise leave its existing state unchanged.
    """
    conn = sqlite3.connect(INCIDENTS_DB)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT incident_id, service_name, status, action_taken,
                   tier1_attempted, tier2_attempted
            FROM incidents
            WHERE status IN ('auto_healing', 'remediated')
               OR (
                    status = 'pending'
                    AND (tier1_attempted = 1 OR tier2_attempted = 1)
               )
            ORDER BY created_at ASC
        """).fetchall()
    finally:
        conn.close()

    recovered_count = 0
    checked_count = 0

    for row in rows:
        incident_id = row['incident_id']
        service_name = row['service_name']
        checked_count += 1

        try:
            # True means the anomaly is still active OR could not be verified.
            still_anomalous = is_still_anomalous(incident_id, service_name)
            if still_anomalous:
                log.warning(
                    "INTERRUPTED INCIDENT STILL ACTIVE/UNVERIFIED | "
                    "incident=%s | service=%s | status=%s",
                    incident_id, service_name, row['status']
                )
                continue

            # Only mark recovery after the service-specific indicator confirms
            # that the anomaly is no longer active. Preserve tier outcomes.
            conn = sqlite3.connect(INCIDENTS_DB)
            try:
                cursor = conn.execute("""
                    UPDATE incidents
                    SET status = 'recovered',
                        action_taken = 'recovered_after_restart',
                        outcome = 'recovered',
                        resolved_at = ?
                    WHERE incident_id = ?
                      AND status IN ('pending', 'auto_healing', 'remediated')
                      AND (tier1_attempted = 1 OR tier2_attempted = 1)
                """, (utc_now(), incident_id))
                conn.commit()
                updated = cursor.rowcount
            finally:
                conn.close()

            if updated:
                recovered_count += 1
                log.info(
                    "INTERRUPTED INCIDENT RECOVERED | "
                    "incident=%s | service=%s",
                    incident_id, service_name
                )

        except Exception:
            log.exception(
                "Startup reconciliation failed | incident=%s | service=%s",
                incident_id, service_name
            )

    log.info(
        "STARTUP RECONCILIATION COMPLETE | checked=%s | recovered=%s",
        checked_count, recovered_count
    )


# =============================================================================
# MAIN LOOP
# =============================================================================

def main():

    log.info("Starting AIOps anomaly detection system")

    log.info(
        "Detection interval: %s seconds",
        DETECTION_INTERVAL
    )

    log.info("Auto heal: %s", AUTO_HEAL)

    # -------------------------------------------------------------------------
    # INITIALIZE DATABASES
    # -------------------------------------------------------------------------

    init_incidents_db()
    init_profiles_db()

    # -------------------------------------------------------------------------
    # RECONCILE INTERRUPTED REMEDIATION BEFORE RESTORING EPISODES
    # -------------------------------------------------------------------------

    reconcile_interrupted_incidents()

    # -------------------------------------------------------------------------
    # RESTORE EPISODE STATE
    # -------------------------------------------------------------------------

    seed_existing_episodes()

    # -------------------------------------------------------------------------
    # CONTINUOUS DETECTION
    # -------------------------------------------------------------------------

    while True:

        cycle_start = time.time()

        log.info("=" * 60)
        log.info("STARTING DETECTION CYCLE")

        # Process every configured service.
        for service_name in SERVICES:
            process_service(service_name)

        # Adaptive threshold feedback.
        for service_name in SERVICES:

            try:
                adjust_threshold(service_name)

            except Exception:
                log.exception(
                    "Threshold adjustment failed | service=%s",
                    service_name
                )

        # Maintain the configured detection interval.
        elapsed = time.time() - cycle_start

        sleep_time = max(
            0,
            DETECTION_INTERVAL - elapsed
        )

        log.info(
            "Detection cycle completed in %.2f seconds | "
            "sleeping %.2f seconds",
            elapsed,
            sleep_time
        )

        time.sleep(sleep_time)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    init_incidents_db()
    main()