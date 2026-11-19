
import os
import time
import logging
import sqlite3
from datetime import datetime, timezone

from recovery import tier1_recovery
from restart_container import restart_container, stop_and_start
from circuit_breaker import (
    activate_circuit_breaker,
    get_circuit_breaker_status,
)


# =============================================================================
# LOGGING AND CONFIGURATION
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)

INCIDENTS_DB = os.path.join(
    PROJECT_DIR, "incident_manager", "incidents.db"
)

MIN_RESTART_GAP_SEC = 300
TIER1_WAIT_MINS = 2
TIER2_WAIT_MINS = 3

SERVICE_CONTAINER_MAP = {
    "payment-service": "payment-service",
    "order-service": "order-service",
    "inventory-service": "inventory-service",
}

SERVICE_HEALTH_URL_MAP = {
    "payment-service": "http://localhost:8002/health",
    "order-service": "http://localhost:8001/health",
    "inventory-service": "http://localhost:8003/health",
}


# =============================================================================
# DATABASE HELPERS
# =============================================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def get_incident(incident_id):
    conn = sqlite3.connect(INCIDENTS_DB)
    conn.row_factory = sqlite3.Row

    try:
        row = conn.execute(
            "SELECT * FROM incidents WHERE incident_id = ?",
            (incident_id,),
        ).fetchone()

        return dict(row) if row else None
    finally:
        conn.close()


def update_incident_action(
    incident_id, action, tier, outcome=None
):
    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        if tier == 1:
            conn.execute(
                """
                UPDATE incidents
                SET action_taken = ?,
                    tier1_attempted = 1,
                    tier1_outcome = ?
                WHERE incident_id = ?
                """,
                (action, outcome or action, incident_id),
            )

        elif tier == 2:
            conn.execute(
                """
                UPDATE incidents
                SET action_taken = ?,
                    tier2_attempted = 1,
                    tier2_outcome = ?
                WHERE incident_id = ?
                """,
                (action, outcome or action, incident_id),
            )

        else:
            conn.execute(
                """
                UPDATE incidents
                SET action_taken = ?
                WHERE incident_id = ?
                """,
                (action, incident_id),
            )

        conn.commit()
    finally:
        conn.close()


def mark_remediation_success(incident_id, action):
    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        if action == "tier1_sighup":
            conn.execute(
                """
                UPDATE incidents
                SET status = 'resolved',
                    action_taken = ?,
                    outcome = 'success',
                    tier1_outcome = 'success',
                    resolved_at = ?
                WHERE incident_id = ?
                """,
                (action, utc_now(), incident_id),
            )

        elif action == "tier2_restart":
            conn.execute(
                """
                UPDATE incidents
                SET status = 'resolved',
                    action_taken = ?,
                    outcome = 'success',
                    tier2_outcome = 'success',
                    resolved_at = ?
                WHERE incident_id = ?
                """,
                (action, utc_now(), incident_id),
            )

        else:
            conn.execute(
                """
                UPDATE incidents
                SET status = 'resolved',
                    action_taken = ?,
                    outcome = 'success',
                    resolved_at = ?
                WHERE incident_id = ?
                """,
                (action, utc_now(), incident_id),
            )

        conn.commit()
    finally:
        conn.close()


def mark_remediation_failure(incident_id):
    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        conn.execute(
            """
            UPDATE incidents
            SET outcome = 'failure'
            WHERE incident_id = ?
            """,
            (incident_id,),
        )
        conn.commit()
    finally:
        conn.close()


def mark_recovered(incident_id):
    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        conn.execute(
            """
            UPDATE incidents
            SET status = 'recovered',
                action_taken = 'recovered_without_remediation',
                outcome = 'recovered',
                human_approved = NULL,
                resolved_at = ?
            WHERE incident_id = ?
            """,
            (utc_now(), incident_id),
        )
        conn.commit()
    finally:
        conn.close()


def queue_human_review(incident_id, reason=None):
    """Record that automatic remediation requires human review."""
    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        conn.execute(
            """
            UPDATE incidents
            SET status = 'pending',
                action_taken = 'awaiting_human_review',
                human_approved = NULL,
                outcome = 'failure'
            WHERE incident_id = ?
            """,
            (incident_id,),
        )
        conn.commit()
    finally:
        conn.close()

    log.warning(
        "Human review required | incident=%s | reason=%s",
        incident_id,
        reason or "automatic remediation unsuccessful",
    )

    return False, {
        "result": "human_review_required",
        "incident_id": incident_id,
        "reason": reason,
    }


# =============================================================================
# RESTART SAFETY
# =============================================================================

def was_recently_restarted(service_name):
    conn = sqlite3.connect(INCIDENTS_DB)

    try:
        row = conn.execute(
            """
            SELECT resolved_at, created_at
            FROM incidents
            WHERE service_name = ?
              AND action_taken IN (
                  'tier2_restart', 'restart_container'
              )
            ORDER BY rowid DESC
            LIMIT 1
            """,
            (service_name,),
        ).fetchone()

        if not row:
            return False

        timestamp = row[0] or row[1]

        if not timestamp:
            return False

        try:
            last_restart = datetime.fromisoformat(
                timestamp.replace("Z", "+00:00")
            )
        except (ValueError, TypeError):
            log.warning(
                "Invalid restart timestamp for %s: %s",
                service_name,
                timestamp,
            )
            # Fail safe: don't restart if restart history is ambiguous.
            return True

        if last_restart.tzinfo is None:
            last_restart = last_restart.replace(
                tzinfo=timezone.utc
            )

        elapsed = (
            datetime.now(timezone.utc) - last_restart
        ).total_seconds()

        return elapsed < MIN_RESTART_GAP_SEC

    finally:
        conn.close()


# =============================================================================
# APPLICATION-LEVEL ANOMALY VERIFICATION
# =============================================================================

def is_still_anomalous(incident_id, service_name):
    import requests

    health_url = SERVICE_HEALTH_URL_MAP.get(service_name)

    if not health_url:
        log.warning(
            "No health URL configured | service=%s",
            service_name,
        )
        return True

    try:
        response = requests.get(health_url, timeout=5)

        if response.status_code != 200:
            return True

        data = response.json()

        if service_name == "payment-service":
            burst_active = data.get("error_burst_active", 0)
            error_rate = float(data.get("error_rate_pct", 0))

            return burst_active > 0 or error_rate >= 10

        if service_name == "order-service":
            return data.get("spike_active", 0) > 0

        if service_name == "inventory-service":
            return data.get("overload_active", 0) > 0

        return True

    except Exception as exc:
        log.warning(
            "Could not verify anomaly | incident=%s | "
            "service=%s | error=%s",
            incident_id,
            service_name,
            exc,
        )
        # Fail safe: inability to verify is not recovery.
        return True


# =============================================================================
# TIER 3 — HUMAN REVIEW
# =============================================================================

def escalate_to_human(incident_id, reason=None):
    mark_remediation_failure(incident_id)
    return queue_human_review(incident_id, reason)


# =============================================================================
# EXECUTE AUTOMATIC REMEDIATION: TIERS 1 AND 2
# =============================================================================

def execute_remediation(incident_id):
    incident = get_incident(incident_id)

    if not incident:
        log.error("Incident not found: %s", incident_id)
        return False, "incident_not_found"

    service_name = incident.get("service_name")
    anomaly_type = incident.get("anomaly_type")
    risk_level = incident.get("risk_level")

    container_name = SERVICE_CONTAINER_MAP.get(service_name)

    if not container_name:
        return escalate_to_human(
            incident_id,
            "missing_container_mapping",
        )

    log.info(
        "Executing remediation | incident=%s | service=%s | "
        "anomaly=%s | risk=%s",
        incident_id,
        service_name,
        anomaly_type,
        risk_level,
    )

    # First check whether the anomaly has already cleared.
    if not is_still_anomalous(incident_id, service_name):
        mark_recovered(incident_id)

        return True, {
            "result": "auto_resolved",
            "action": "recovered_without_remediation",
        }

    # -------------------------------------------------------------------------
    # TIER 1: LOG DIAGNOSIS AND SIGHUP RECOVERY
    # -------------------------------------------------------------------------

    log.info("TIER 1 | incident=%s", incident_id)

    try:
        # recovery.py expects container_name and incident_id.
        tier1_result = tier1_recovery(
            container_name,
            incident_id,
        )
    except Exception as exc:
        log.exception("Tier 1 exception | incident=%s", incident_id)

        tier1_result = {
            "success": False,
            "error": str(exc),
        }

    # The recovery function returns a dictionary, not a Boolean.
    if not isinstance(tier1_result, dict):
        tier1_result = {
            "success": False,
            "error": "invalid_tier1_result",
        }

    if tier1_result.get("action") == "escalate_to_human":
        update_incident_action(
            incident_id,
            "awaiting_human_review",
            3,
            "external_dependency",
        )

        return escalate_to_human(
            incident_id,
            tier1_result.get(
                "reason",
                "external dependency failure",
            ),
        )

    if tier1_result.get("success", False):
        update_incident_action(
            incident_id,
            "tier1_sighup",
            1,
            "attempted",
        )

        log.info(
            "Tier 1 command succeeded; waiting %s minutes",
            TIER1_WAIT_MINS,
        )

        time.sleep(TIER1_WAIT_MINS * 60)

        if not is_still_anomalous(
            incident_id, service_name
        ):
            mark_remediation_success(
                incident_id,
                "tier1_sighup",
            )

            return True, {
                "result": "success",
                "tier": 1,
                "action": "tier1_sighup",
            }

        update_incident_action(
            incident_id,
            "tier1_sighup",
            1,
            "failed",
        )

        log.warning(
            "Tier 1 did not resolve anomaly; trying Tier 2"
        )

    else:
        update_incident_action(
            incident_id,
            "tier1_sighup",
            1,
            "failed",
        )

        log.warning(
            "Tier 1 failed: %s",
            tier1_result.get("error", "unknown error"),
        )

    # -------------------------------------------------------------------------
    # TIER 2: RESTART WITH SAFETY CONTROL
    # -------------------------------------------------------------------------

    if was_recently_restarted(service_name):
        update_incident_action(
            incident_id,
            "tier2_restart",
            2,
            "skipped_recent_restart",
        )

        log.warning(
            "Tier 2 skipped because of restart safety interval"
        )

        return escalate_to_human(
            incident_id,
            "restart cooldown active",
        )

    log.info(
        "TIER 2 | restarting %s | incident=%s",
        container_name,
        incident_id,
    )

    try:
        # restart_container returns a dictionary.
        restart_result = restart_container(
            container_name,
            incident_id,
        )
    except Exception as exc:
        log.exception("Tier 2 exception | incident=%s", incident_id)

        restart_result = {
            "success": False,
            "error": str(exc),
        }

    if not isinstance(restart_result, dict):
        restart_result = {
            "success": False,
            "error": "invalid_tier2_result",
        }

    if not restart_result.get("success", False):
        update_incident_action(
            incident_id,
            "tier2_restart",
            2,
            "failed",
        )

        log.error(
            "Tier 2 failed: %s",
            restart_result.get("error", "unknown error"),
        )

        return escalate_to_human(
            incident_id,
            "container restart failed",
        )

    update_incident_action(
        incident_id,
        "tier2_restart",
        2,
        "attempted",
    )

    log.info(
        "Tier 2 restart succeeded; waiting %s minutes",
        TIER2_WAIT_MINS,
    )

    time.sleep(TIER2_WAIT_MINS * 60)

    if not is_still_anomalous(incident_id, service_name):
        mark_remediation_success(
            incident_id,
            "tier2_restart",
        )

        return True, {
            "result": "success",
            "tier": 2,
            "action": "tier2_restart",
        }

    update_incident_action(
        incident_id,
        "tier2_restart",
        2,
        "failed",
    )

    # Tier 3: keep unresolved incident pending for operator review.
    return escalate_to_human(
        incident_id,
        "anomaly persists after Tier 1 and Tier 2",
    )


# =============================================================================
# TIER 4 — HUMAN-APPROVED CIRCUIT BREAKER
# =============================================================================

def execute_circuit_breaker(incident_id):
    incident = get_incident(incident_id)

    if not incident:
        log.error("Incident not found: %s", incident_id)
        return False, "incident_not_found"

    # Enforce an approval recorded in the database before execution.
    approval = incident.get("human_approved")
    approved_values = (1, "1", "approved", "true", "yes")

    if str(approval).strip().lower() not in {
        str(value).lower() for value in approved_values
    }:
        log.warning(
            "Tier 4 denied: no recorded human approval | incident=%s",
            incident_id,
        )
        return False, "human_approval_required"

    if str(incident.get("status", "")).lower() != "pending":
        return False, "incident_not_pending_review"

    service_name = incident.get("service_name")
    container_name = SERVICE_CONTAINER_MAP.get(service_name)

    if not container_name:
        return False, "container_mapping_missing"

    log.warning(
        "TIER 4 | approved circuit breaker | incident=%s | "
        "service=%s | container=%s",
        incident_id,
        service_name,
        container_name,
    )

    try:
        result = activate_circuit_breaker(
            container_name,
            incident_id,
        )

        if not isinstance(result, dict):
            return False, "invalid_circuit_breaker_result"

        if not result.get("success", False):
            log.error(
                "Circuit breaker failed | incident=%s | result=%s",
                incident_id,
                result,
            )
            return False, result

        conn = sqlite3.connect(INCIDENTS_DB)

        try:
            conn.execute(
                """
                UPDATE incidents
                SET action_taken = 'circuit_breaker',
                    status = 'resolved',
                    outcome = 'success',
                    resolved_at = ?
                WHERE incident_id = ?
                """,
                (utc_now(), incident_id),
            )
            conn.commit()
        finally:
            conn.close()

        log.warning(
            "Tier 4 circuit breaker completed | incident=%s",
            incident_id,
        )

        return True, result

    except Exception as exc:
        log.exception(
            "Circuit breaker execution failed | incident=%s",
            incident_id,
        )
        return False, str(exc)


if __name__ == "__main__":
    print("Remediation executor loaded successfully.")