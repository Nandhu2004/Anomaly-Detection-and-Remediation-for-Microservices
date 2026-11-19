
import os
import sys
import time
import sqlite3
import logging
from datetime import datetime, timezone

# =============================================================================
# PROJECT PATH
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

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from anomaly_detector.metric_collector import (
    get_latest_metrics
)

# =============================================================================
# CONFIGURATION
# =============================================================================

WAIT_MINUTES = 3

MAX_NORMAL_ERROR_RATE = 5.0

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_db_connection():
    return sqlite3.connect(INCIDENTS_DB)


# =============================================================================
# CHECK ORDER SERVICE RECOVERY
# =============================================================================

def check_order_recovery(metrics):

    if not metrics:
        log.warning(
            "No metrics available for order-service"
        )
        return "unknown"

    spike_active = float(
        metrics.get("spike_active", 0.0)
    )

    log.info(
        "order-service recovery check: spike_active=%.2f",
        spike_active
    )

    if spike_active <= 0:
        return "success"

    return "failure"


# =============================================================================
# CHECK PAYMENT SERVICE RECOVERY
# =============================================================================

def check_payment_recovery(metrics):

    if not metrics:
        log.warning(
            "No metrics available for payment-service"
        )
        return "unknown"

    error_rate = float(
        metrics.get("error_rate", 0.0)
    )

    burst_active = float(
        metrics.get("burst_active", 0.0)
    )

    error_recovered = (
        error_rate < MAX_NORMAL_ERROR_RATE
    )

    burst_recovered = (
        burst_active <= 0
    )

    log.info(
        "payment-service recovery check: "
        "error_rate=%.2f%%, burst_active=%.2f",
        error_rate,
        burst_active
    )

    if error_recovered and burst_recovered:
        return "success"

    if error_recovered or burst_recovered:
        return "partial"

    return "failure"


# =============================================================================
# CHECK INVENTORY SERVICE RECOVERY
# =============================================================================

def check_inventory_recovery(metrics):

    if not metrics:
        log.warning(
            "No metrics available for inventory-service"
        )
        return "unknown"

    overload_active = float(
        metrics.get("overload_active", 0.0)
    )

    log.info(
        "inventory-service recovery check: "
        "overload_active=%.2f",
        overload_active
    )

    if overload_active <= 0:
        return "success"

    return "failure"


# =============================================================================
# GENERIC SERVICE RECOVERY CHECK
# =============================================================================

def check_recovery(service_name):

    log.info(
        "Checking recovery for %s",
        service_name
    )

    try:
        metrics = get_latest_metrics(service_name)
    except Exception:
        log.exception(
            "Failed to retrieve metrics for %s",
            service_name
        )
        return "unknown"

    if not metrics:
        log.warning(
            "No usable metrics available for %s",
            service_name
        )
        return "unknown"

    log.info(
        "Latest metrics for %s: %s",
        service_name,
        metrics
    )

    try:
        if service_name == "order-service":
            return check_order_recovery(metrics)

        if service_name == "payment-service":
            return check_payment_recovery(metrics)

        if service_name == "inventory-service":
            return check_inventory_recovery(metrics)

        log.error(
            "Unknown service: %s",
            service_name
        )
        return "unknown"

    except (TypeError, ValueError, AttributeError) as exc:
        log.warning(
            "Invalid recovery metrics for %s: %s",
            service_name,
            exc
        )
        return "unknown"


# =============================================================================
# RECORD OUTCOME
# =============================================================================

def record_outcome(
    incident_id,
    outcome,
    time_to_recover
):
    # Do not persist an unavailable or unverified result as a
    # successful or failed remediation.
    if outcome == "unknown":
        log.warning(
            "Outcome for incident %s remains unverified",
            incident_id
        )
        return False

    status_by_outcome = {
        "success": "resolved",
        "partial": "remediated",
        "failure": "failed"
    }

    if outcome not in status_by_outcome:
        raise ValueError(
            f"Unsupported recovery outcome: {outcome}"
        )

    status = status_by_outcome[outcome]

    now = datetime.now(timezone.utc).isoformat()

    conn = get_db_connection()

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE incidents
            SET
                outcome = ?,
                time_to_recover = ?,
                resolved_at = ?,
                status = ?
            WHERE incident_id = ?
              AND outcome IS NULL
            """,
            (
                outcome,
                time_to_recover,
                now,
                status,
                incident_id
            )
        )

        conn.commit()

        if cursor.rowcount == 0:
            log.warning(
                "Incident %s was not updated. "
                "It may not exist or may already have an outcome.",
                incident_id
            )
            return False

        log.info(
            "Incident %s updated: outcome=%s, status=%s, "
            "time_to_recover=%.2fs",
            incident_id,
            outcome,
            status,
            time_to_recover
        )

        return True

    except Exception:
        conn.rollback()
        log.exception(
            "Failed to record outcome for %s",
            incident_id
        )
        raise

    finally:
        conn.close()


# =============================================================================
# MONITOR ONE INCIDENT
# =============================================================================

def monitor_outcome(
    incident_id,
    service_name,
    wait_minutes=WAIT_MINUTES
):
    log.info("=" * 70)
    log.info("OUTCOME MONITOR STARTED")
    log.info("Incident: %s", incident_id)
    log.info("Service: %s", service_name)
    log.info(
        "Waiting %s minutes before recovery check",
        wait_minutes
    )
    log.info("=" * 70)

    start_time = time.time()

    time.sleep(
        max(0, wait_minutes) * 60
    )

    outcome = check_recovery(service_name)

    elapsed = time.time() - start_time

    # If metrics are unavailable, preserve the incident for a later
    # verification attempt instead of recording a false failure.
    if outcome == "unknown":
        log.warning(
            "Outcome verification deferred for incident %s",
            incident_id
        )

        return {
            "incident_id": incident_id,
            "service_name": service_name,
            "outcome": "unknown",
            "time_to_recover": None
        }

    updated = record_outcome(
        incident_id,
        outcome,
        elapsed
    )

    log.info(
        "Outcome monitoring completed: %s -> %s",
        service_name,
        outcome
    )

    return {
        "incident_id": incident_id,
        "service_name": service_name,
        "outcome": outcome,
        "time_to_recover": elapsed,
        "recorded": updated
    }


# =============================================================================
# GET PENDING OUTCOME CHECKS
# =============================================================================

def get_pending_outcome_checks():

    conn = get_db_connection()

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                incident_id,
                service_name,
                created_at
            FROM incidents
            WHERE outcome IS NULL
              AND status IN (
                  'remediated',
                  'resolved',
                  'failure',
                  'failed'
              )
            ORDER BY created_at ASC
            """
        )

        return cursor.fetchall()

    except sqlite3.Error:
        log.exception(
            "Failed to retrieve pending outcome checks"
        )
        return []

    finally:
        conn.close()


# =============================================================================
# PROCESS ALL PENDING INCIDENTS
# =============================================================================

def process_pending_outcomes(
    wait_minutes=WAIT_MINUTES
):
    pending = get_pending_outcome_checks()

    if not pending:
        log.info("No pending outcome checks")
        return []

    log.info(
        "Found %d pending outcome checks",
        len(pending)
    )

    results = []

    for incident_id, service_name, created_at in pending:
        try:
            result = monitor_outcome(
                incident_id=incident_id,
                service_name=service_name,
                wait_minutes=wait_minutes
            )

            results.append(result)

        except Exception:
            log.exception(
                "Outcome monitoring failed for incident %s",
                incident_id
            )

    return results


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":

    print("\n" + "=" * 70)
    print("OUTCOME MONITOR TEST")
    print("=" * 70)

    services = [
        "order-service",
        "payment-service",
        "inventory-service"
    ]

    for service_name in services:

        print(f"\nTesting: {service_name}")

        try:
            metrics = get_latest_metrics(service_name)

            print("Latest metrics:", metrics)

            result = check_recovery(service_name)

            print("Recovery result:", result)

        except Exception as exc:
            print(f"ERROR: {exc}")

    print("\n" + "=" * 70)
    print("OUTCOME MONITOR TEST COMPLETE")
    print("=" * 70 + "\n")