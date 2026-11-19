
import os
import sqlite3
import logging
from datetime import datetime, timezone


# ============================================================
# PATHS AND CONFIGURATION
# ============================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PROFILES_DB = os.path.join(
    BASE_DIR, "anomaly_detector", "profiles.db"
)

INCIDENTS_DB = os.path.join(
    BASE_DIR, "incident_manager", "incidents.db"
)

DEFAULT_THRESHOLD = 0.60
THRESHOLD_MIN = 0.30
THRESHOLD_MAX = 0.90

ADJUST_EVERY = 10
FP_RATE_LIMIT = 0.30
THRESHOLD_STEP = 0.10

# Only terminal incidents with recorded outcomes are eligible.
COMPLETED_STATUSES = (
    "resolved",
    "recovered",
    "failed",
    "auto_healed",
    "healed",
)

FALSE_POSITIVE_OUTCOMES = {
    "false_positive",
}

FAILURE_OUTCOMES = {
    "failure",
    "failed",
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger(__name__)


# ============================================================
# DATABASE INITIALIZATION
# ============================================================

def init_threshold_db():
    """Initialize threshold tables and migrate existing databases."""

    os.makedirs(os.path.dirname(PROFILES_DB), exist_ok=True)

    with sqlite3.connect(PROFILES_DB) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                service_name TEXT PRIMARY KEY,
                sample_count INTEGER DEFAULT 0,
                updated_at TEXT,
                threshold REAL DEFAULT 0.60
            )
        """)

        profile_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(profiles)"
            ).fetchall()
        }

        if "threshold" not in profile_columns:
            conn.execute("""
                ALTER TABLE profiles
                ADD COLUMN threshold REAL DEFAULT 0.60
            """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS threshold_state (
                service_name TEXT PRIMARY KEY,
                last_processed_created_at TEXT,
                last_processed_incident_id TEXT
            )
        """)

        state_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(threshold_state)"
            ).fetchall()
        }

        if "last_processed_created_at" not in state_columns:
            conn.execute("""
                ALTER TABLE threshold_state
                ADD COLUMN last_processed_created_at TEXT
            """)

        if "last_processed_incident_id" not in state_columns:
            conn.execute("""
                ALTER TABLE threshold_state
                ADD COLUMN last_processed_incident_id TEXT
            """)

        conn.commit()


# ============================================================
# THRESHOLD HELPERS
# ============================================================

def clamp_threshold(threshold):
    """Keep a threshold within its permitted range."""

    return max(
        THRESHOLD_MIN,
        min(THRESHOLD_MAX, float(threshold)),
    )


def get_current_threshold(service_name):
    """Read the stored threshold for a service."""

    try:
        init_threshold_db()

        with sqlite3.connect(PROFILES_DB) as conn:
            row = conn.execute("""
                SELECT threshold
                FROM profiles
                WHERE service_name = ?
            """, (service_name,)).fetchone()

        if row is None or row[0] is None:
            return DEFAULT_THRESHOLD

        return clamp_threshold(row[0])

    except (sqlite3.Error, TypeError, ValueError) as exc:
        log.error(
            "Failed to read threshold for %s: %s",
            service_name,
            exc,
        )
        return DEFAULT_THRESHOLD


def save_threshold(service_name, threshold):
    """Update the threshold without overwriting other profile data."""

    init_threshold_db()

    threshold = clamp_threshold(threshold)
    now = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(PROFILES_DB) as conn:
        conn.execute("""
            INSERT INTO profiles (
                service_name,
                sample_count,
                updated_at,
                threshold
            )
            VALUES (?, 0, ?, ?)
            ON CONFLICT(service_name)
            DO UPDATE SET
                threshold = excluded.threshold,
                updated_at = excluded.updated_at
        """, (service_name, now, threshold))

        conn.commit()


# ============================================================
# ADAPTATION CURSOR
# ============================================================

def get_last_processed_cursor(service_name):
    """
    Return the timestamp and incident ID of the last processed incident.

    The incident ID breaks ties when multiple incidents share a timestamp.
    """

    init_threshold_db()

    with sqlite3.connect(PROFILES_DB) as conn:
        row = conn.execute("""
            SELECT
                last_processed_created_at,
                last_processed_incident_id
            FROM threshold_state
            WHERE service_name = ?
        """, (service_name,)).fetchone()

    if not row:
        return None, None

    return row[0], row[1]


def get_last_processed_time(service_name):
    """Compatibility helper for existing callers."""

    timestamp, _ = get_last_processed_cursor(service_name)
    return timestamp


def save_last_processed_time(
    service_name,
    timestamp,
    incident_id=None,
):
    """Save the adaptation cursor after processing a complete batch."""

    init_threshold_db()

    with sqlite3.connect(PROFILES_DB) as conn:
        conn.execute("""
            INSERT INTO threshold_state (
                service_name,
                last_processed_created_at,
                last_processed_incident_id
            )
            VALUES (?, ?, ?)
            ON CONFLICT(service_name)
            DO UPDATE SET
                last_processed_created_at =
                    excluded.last_processed_created_at,
                last_processed_incident_id =
                    excluded.last_processed_incident_id
        """, (service_name, timestamp, incident_id))

        conn.commit()


# ============================================================
# COMPLETED INCIDENTS
# ============================================================

def get_incidents_for_adjustment(service_name):
    """
    Fetch up to ten unprocessed, completed incidents.

    On first use, the latest ten eligible incidents are selected.
    Subsequent batches use the saved timestamp and incident ID cursor.

    Returned rows:
        outcome, human_reject_reason, status, created_at, incident_id
    """

    if not os.path.exists(INCIDENTS_DB):
        log.warning(
            "Incidents database not found: %s",
            INCIDENTS_DB,
        )
        return []

    last_timestamp, last_incident_id = (
        get_last_processed_cursor(service_name)
    )

    try:
        with sqlite3.connect(INCIDENTS_DB) as conn:
            if last_timestamp:
                # The ID tie-breaker prevents incidents with the same
                # created_at timestamp from being skipped.
                rows = conn.execute("""
                    SELECT
                        outcome,
                        human_reject_reason,
                        status,
                        created_at,
                        incident_id
                    FROM incidents
                    WHERE service_name = ?
                      AND outcome IS NOT NULL
                      AND LOWER(TRIM(status)) IN (
                          'resolved',
                          'recovered',
                          'failed',
                          'auto_healed',
                          'healed'
                      )
                      AND (
                          created_at > ?
                          OR (
                              created_at = ?
                              AND incident_id > ?
                          )
                      )
                    ORDER BY created_at ASC, incident_id ASC
                    LIMIT ?
                """, (
                    service_name,
                    last_timestamp,
                    last_timestamp,
                    last_incident_id or "",
                    ADJUST_EVERY,
                )).fetchall()

            else:
                # Process the latest historical batch chronologically.
                rows = conn.execute("""
                    SELECT
                        outcome,
                        human_reject_reason,
                        status,
                        created_at,
                        incident_id
                    FROM incidents
                    WHERE service_name = ?
                      AND outcome IS NOT NULL
                      AND LOWER(TRIM(status)) IN (
                          'resolved',
                          'recovered',
                          'failed',
                          'auto_healed',
                          'healed'
                      )
                    ORDER BY created_at DESC, incident_id DESC
                    LIMIT ?
                """, (
                    service_name,
                    ADJUST_EVERY,
                )).fetchall()

                rows = list(reversed(rows))

        return rows

    except sqlite3.Error as exc:
        log.error(
            "Failed to read incidents for %s: %s",
            service_name,
            exc,
        )
        return []


# ============================================================
# ADJUST THRESHOLD
# ============================================================

def adjust_threshold(service_name):
    """
    Adjust the threshold after ten new completed incidents.

    Explicit false-positive feedback can increase the threshold.
    Remediation failures are reported separately and do not change
    the anomaly-detection threshold.
    """

    log.info(
        "Checking adaptive threshold for %s",
        service_name,
    )

    rows = get_incidents_for_adjustment(service_name)
    current_threshold = get_current_threshold(service_name)

    if len(rows) < ADJUST_EVERY:
        log.info(
            "%s: waiting for a complete batch (%d/%d). "
            "Threshold remains %.2f",
            service_name,
            len(rows),
            ADJUST_EVERY,
            current_threshold,
        )

        return {
            "service": service_name,
            "adjusted": False,
            "old_threshold": current_threshold,
            "new_threshold": current_threshold,
            "threshold": current_threshold,
            "incident_count": len(rows),
            "reason": "Waiting for 10 new completed incidents",
        }

    false_positives = 0
    remediation_failures = 0

    for outcome, reject_reason, status, created_at, incident_id in rows:
        normalized_outcome = (
            str(outcome or "").strip().lower()
        )

        normalized_reason = (
            str(reject_reason or "").strip().lower()
        )

        normalized_status = (
            str(status or "").strip().lower()
        )

        if (
            normalized_outcome in FALSE_POSITIVE_OUTCOMES
            or normalized_reason == "false_positive"
        ):
            false_positives += 1

        if (
            normalized_outcome in FAILURE_OUTCOMES
            or normalized_status == "failed"
        ):
            remediation_failures += 1

    total_incidents = len(rows)

    false_positive_rate = false_positives / total_incidents
    failure_rate = remediation_failures / total_incidents

    new_threshold = current_threshold
    adjustment_reason = "No adjustment required"

    # Increasing the threshold makes the ML score test less sensitive.
    if false_positive_rate > FP_RATE_LIMIT:
        new_threshold = clamp_threshold(
            current_threshold + THRESHOLD_STEP
        )
        adjustment_reason = (
            "False-positive rate exceeded 30%; "
            "threshold increased"
        )

    adjusted = new_threshold != current_threshold

    if adjusted:
        save_threshold(service_name, new_threshold)

    # Save the cursor even when no adjustment is necessary. This prevents
    # the same batch from being processed repeatedly.
    last_row = rows[-1]

    save_last_processed_time(
        service_name,
        last_row[3],
        last_row[4],
    )

    log.info(
        "%s | threshold %.2f -> %.2f | "
        "false positives=%d/%d (%.2f) | "
        "remediation failures=%d/%d (%.2f) | %s",
        service_name,
        current_threshold,
        new_threshold,
        false_positives,
        total_incidents,
        false_positive_rate,
        remediation_failures,
        total_incidents,
        failure_rate,
        adjustment_reason,
    )

    return {
        "service": service_name,
        "adjusted": adjusted,
        "old_threshold": current_threshold,
        "new_threshold": new_threshold,
        "threshold": new_threshold,
        "incident_count": total_incidents,
        "false_positives": false_positives,
        "false_positive_rate": round(false_positive_rate, 4),
        "remediation_failures": remediation_failures,
        "failure_rate": round(failure_rate, 4),
        "reason": adjustment_reason,
        "adjusted_at": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# ADJUST ALL SERVICES
# ============================================================

def adjust_all_thresholds():
    """Adjust thresholds for all configured services."""

    try:
        from .per_service_detector import SERVICES
    except ImportError:
        from per_service_detector import SERVICES

    results = []

    for service_name in SERVICES:
        try:
            results.append(adjust_threshold(service_name))
        except Exception as exc:
            log.exception(
                "Threshold adjustment failed for %s",
                service_name,
            )
            results.append({
                "service": service_name,
                "adjusted": False,
                "reason": str(exc),
            })

    return results


# ============================================================
# PRINT SUMMARY
# ============================================================

def print_summary(results):
    print("\n" + "=" * 75)
    print("ADAPTIVE THRESHOLD SUMMARY")
    print("=" * 75)

    for result in results:
        print(
            f"{result.get('service', 'unknown'):20} | "
            f"threshold={result.get('threshold', DEFAULT_THRESHOLD):.2f} | "
            f"adjusted={str(result.get('adjusted', False)):5} | "
            f"{result.get('reason', '')}"
        )

    print("=" * 75)


# ============================================================
# STANDALONE EXECUTION
# ============================================================

if __name__ == "__main__":
    init_threshold_db()
    print_summary(adjust_all_thresholds())