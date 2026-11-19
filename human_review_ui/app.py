import os
import sys
import sqlite3
import logging
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
)


# ============================================================
# CONFIGURATION
# ============================================================

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

REMEDIATION_DIR = os.path.join(
    BASE_DIR,
    "remediation_executor"
)

if REMEDIATION_DIR not in sys.path:
    sys.path.insert(0, REMEDIATION_DIR)


# ============================================================
# STREAMLIT CONFIG
# ============================================================

st.set_page_config(
    page_title="Operations Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# CONSTANTS
# ============================================================

SERVICES = [
    "order-service",
    "payment-service",
    "inventory-service",
]

SUCCESS_VALUES = {
    "success",
    "successful",
    "resolved",
    "recovered",
    "auto_healed",
    "healed",
    "human_resolved",
}

FAILURE_VALUES = {
    "failed",
    "failure",
    "error",
}

REMEDIATION_ACTIONS = {
    "sighup_worker",
    "inspect_logs_then_sighup",
    "cpu_throttle",
    "restart_container",
    "tier1_sighup",
    "tier1_inspect_logs_then_sighup",
    "tier1_cpu_throttle",
    "tier2_restart",
    "circuit_breaker",
}

STATUS_SUCCESS_VALUES = {
    "resolved",
    "recovered",
    "remediated",
    "auto_healed",
    "healed",
}

# Threshold adaptation configuration.
DEFAULT_THRESHOLD = 0.60
ADJUST_EVERY = 10
FP_RATE_LIMIT = 0.30
THRESHOLD_STEP = 0.10
THRESHOLD_MIN = 0.30
THRESHOLD_MAX = 0.90


def get_connection():
    """
    Open incidents database.
    """

    if not os.path.exists(INCIDENTS_DB):
        return None

    conn = sqlite3.connect(
        INCIDENTS_DB,
        check_same_thread=False
    )

    conn.row_factory = sqlite3.Row

    return conn


def get_profiles_connection():
    """
    Open detector profiles database.
    """

    if not os.path.exists(PROFILES_DB):
        return None

    conn = sqlite3.connect(
        PROFILES_DB,
        check_same_thread=False
    )

    conn.row_factory = sqlite3.Row

    return conn


def get_table_columns(conn, table_name):
    """
    Return column names for a SQLite table.
    """

    try:

        rows = conn.execute(
            f"PRAGMA table_info({table_name})"
        ).fetchall()

        return {
            row["name"]
            for row in rows
        }

    except Exception:

        return set()


def get_all_incidents():
    """
    Retrieve all incidents.
    """

    conn = get_connection()

    if conn is None:
        return []

    try:

        columns = get_table_columns(
            conn,
            "incidents"
        )

        if not columns:
            return []

        rows = conn.execute(
            """
            SELECT *
            FROM incidents
            ORDER BY created_at DESC
            """
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    except Exception as exc:

        logger.exception(
            "Failed to read incidents: %s",
            exc
        )

        return []

    finally:

        conn.close()


def get_pending_incidents():
    """
    Retrieve all pending incidents.
    """

    conn = get_connection()

    if conn is None:
        return []

    try:

        rows = conn.execute(
            """
            SELECT *
            FROM incidents
            WHERE status = 'pending'
            ORDER BY created_at ASC
            """
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    except Exception as exc:

        logger.exception(
            "Failed to read pending incidents: %s",
            exc
        )

        return []

    finally:

        conn.close()


def get_recent_incidents(limit=20):
    """
    Retrieve recent incidents.
    """

    conn = get_connection()

    if conn is None:
        return []

    try:

        rows = conn.execute(
            """
            SELECT *
            FROM incidents
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    except Exception as exc:

        logger.exception(
            "Failed to read recent incidents: %s",
            exc
        )

        return []

    finally:

        conn.close()


def get_incident(incident_id):
    """
    Retrieve one incident.
    """

    conn = get_connection()

    if conn is None:
        return None

    try:

        row = conn.execute(
            """
            SELECT *
            FROM incidents
            WHERE incident_id = ?
            """,
            (incident_id,)
        ).fetchone()

        return dict(row) if row else None

    except Exception as exc:

        logger.exception(
            "Failed to read incident %s: %s",
            incident_id,
            exc
        )

        return None

    finally:

        conn.close()


# ============================================================
# HUMAN RESOLUTION
# ============================================================

def mark_human_resolved(incident_id):
    """
    Mark a Tier 3 incident as manually resolved.

    This is NOT automatic remediation.

    Important:
    human_approved is intentionally NOT changed here.
    Manual resolution and approval of automatic remediation
    are different concepts.
    """

    conn = get_connection()

    if conn is None:
        return False, "Incident database not found."

    try:

        columns = get_table_columns(
            conn,
            "incidents"
        )

        if "incident_id" not in columns:
            return False, "Incident table is unavailable."

        now = datetime.now(
            timezone.utc
        ).isoformat()

        updates = {
            "status": "resolved",
            "outcome": "human_resolved",
            "action_taken": "human_resolution",
            "resolved_at": now,
        }

        # Calculate recovery time.
        if "time_to_recover" in columns:

            row = conn.execute(
                """
                SELECT created_at
                FROM incidents
                WHERE incident_id = ?
                """,
                (incident_id,)
            ).fetchone()

            if row and row["created_at"]:

                try:

                    created_text = str(
                        row["created_at"]
                    )

                    if created_text.endswith("Z"):
                        created_text = (
                            created_text[:-1]
                            + "+00:00"
                        )

                    created_dt = datetime.fromisoformat(
                        created_text
                    )

                    if created_dt.tzinfo is None:
                        created_dt = created_dt.replace(
                            tzinfo=timezone.utc
                        )

                    resolved_dt = datetime.fromisoformat(
                        now
                    )

                    recovery_seconds = (
                        resolved_dt - created_dt
                    ).total_seconds()

                    updates[
                        "time_to_recover"
                    ] = max(
                        0,
                        int(recovery_seconds)
                    )

                except Exception:

                    logger.exception(
                        "Could not calculate time_to_recover"
                    )

        if "reviewed_at" in columns:
            updates["reviewed_at"] = now

        set_parts = []
        values = []

        for column, new_value in updates.items():

            if column in columns:

                set_parts.append(
                    f"{column} = ?"
                )

                values.append(
                    new_value
                )

        if not set_parts:
            return False, "No writable incident fields found."

        values.append(
            incident_id
        )

        query = f"""
            UPDATE incidents
            SET {", ".join(set_parts)}
            WHERE incident_id = ?
        """

        cursor = conn.execute(
            query,
            values
        )

        # Deactivate anomaly episode if the table exists.
        episode_columns = get_table_columns(
            conn,
            "anomaly_episodes"
        )

        if (
            "service_name" in episode_columns
            and "active" in episode_columns
        ):

            incident_row = conn.execute(
                """
                SELECT service_name
                FROM incidents
                WHERE incident_id = ?
                """,
                (incident_id,)
            ).fetchone()

            if incident_row:

                conn.execute(
                    """
                    UPDATE anomaly_episodes
                    SET active = 0
                    WHERE service_name = ?
                    """,
                    (
                        incident_row[
                            "service_name"
                        ],
                    )
                )

        conn.commit()

        if cursor.rowcount == 0:
            return False, "Incident not found."

        logger.info(
            "Incident %s marked as human resolved",
            incident_id
        )

        return True, "Incident marked as human resolved."

    except Exception as exc:

        conn.rollback()

        logger.exception(
            "Failed to mark incident %s as human resolved: %s",
            incident_id,
            exc
        )

        return False, str(exc)

    finally:

        conn.close()


# ============================================================
# FALSE POSITIVE FEEDBACK
# ============================================================

def mark_false_positive(incident_id):
    """
    Record explicit human false-positive feedback.

    This becomes the negative label used by the evaluation
    and threshold-adaptation components.
    """

    conn = get_connection()

    if conn is None:
        return False, "Incident database not found."

    try:

        columns = get_table_columns(
            conn,
            "incidents"
        )

        now = datetime.now(
            timezone.utc
        ).isoformat()

        updates = {
            "status": "resolved",
            "outcome": "false_positive",
            "action_taken": "human_feedback",
            "human_reject_reason": "false_positive",
            "resolved_at": now,
        }

        if "reviewed_at" in columns:
            updates["reviewed_at"] = now

        set_parts = []
        values = []

        for column, new_value in updates.items():

            if column in columns:

                set_parts.append(
                    f"{column} = ?"
                )

                values.append(
                    new_value
                )

        if not set_parts:
            return False, "No writable incident fields found."

        values.append(
            incident_id
        )

        query = f"""
            UPDATE incidents
            SET {", ".join(set_parts)}
            WHERE incident_id = ?
        """

        cursor = conn.execute(
            query,
            values
        )

        conn.commit()

        if cursor.rowcount == 0:
            return False, "Incident not found."

        logger.info(
            "Incident %s marked as false positive",
            incident_id
        )

        return True, "False-positive feedback recorded."

    except Exception as exc:

        conn.rollback()

        logger.exception(
            "Failed to mark false positive: %s",
            exc
        )

        return False, str(exc)

    finally:

        conn.close()


# ============================================================
# PROFILE / THRESHOLD HELPERS
# ============================================================

def get_thresholds():
    """
    Read current anomaly thresholds from profiles DB.
    """

    thresholds = {
        service: DEFAULT_THRESHOLD
        for service in SERVICES
    }

    conn = get_profiles_connection()

    if conn is None:
        return thresholds

    try:

        columns = get_table_columns(
            conn,
            "profiles"
        )

        if (
            "service_name" not in columns
            or "threshold" not in columns
        ):
            return thresholds

        rows = conn.execute(
            """
            SELECT service_name, threshold
            FROM profiles
            """
        ).fetchall()

        for row in rows:

            service = row[
                "service_name"
            ]

            if service in thresholds:

                try:

                    thresholds[
                        service
                    ] = float(
                        row["threshold"]
                    )

                except (
                    TypeError,
                    ValueError
                ):
                    pass

        return thresholds

    except Exception as exc:

        logger.exception(
            "Failed to read thresholds: %s",
            exc
        )

        return thresholds

    finally:

        conn.close()


# ============================================================
# GENERIC VALUE HELPERS
# ============================================================

def value(record, key, default="—"):

    if not record:
        return default

    result = record.get(key)

    if result is None:
        return default

    if (
        isinstance(result, str)
        and not result.strip()
    ):
        return default

    return result


def normalized(value_text):

    if value_text is None:
        return ""

    return str(
        value_text
    ).strip().lower()


def is_success(value_text):

    return (
        normalized(value_text)
        in SUCCESS_VALUES
    )


def is_failure(value_text):

    return (
        normalized(value_text)
        in FAILURE_VALUES
    )


def format_score(score):

    if score in (
        None,
        "",
        "—"
    ):
        return "—"

    try:
        return f"{float(score):.4f}"

    except (
        TypeError,
        ValueError
    ):
        return str(score)


def format_threshold(threshold):

    try:
        return f"{float(threshold):.2f}"

    except (
        TypeError,
        ValueError
    ):
        return str(threshold)


def format_timestamp(timestamp):

    if timestamp in (
        None,
        "",
        "—"
    ):
        return "—"

    try:

        text = str(timestamp)

        if text.endswith("Z"):
            text = (
                text[:-1]
                + "+00:00"
            )

        dt = datetime.fromisoformat(
            text
        )

        if dt.tzinfo is not None:
            dt = dt.astimezone()

        return dt.strftime(
            "%d %b %Y, %I:%M:%S %p"
        )

    except Exception:

        return str(timestamp)


# ============================================================
# ROUTING HELPERS
# ============================================================

def get_routing_decision(incident):

    routing = incident.get(
        "routing_decision"
    )

    if routing not in (
        None,
        ""
    ):
        return str(routing)

    routing = incident.get(
        "routing"
    )

    if routing not in (
        None,
        ""
    ):
        return str(routing)

    return ""


def is_human_review(incident):

    routing = normalized(
        get_routing_decision(
            incident
        )
    )

    action = normalized(
        incident.get(
            "suggested_action"
        )
    )

    status = normalized(
        incident.get(
            "status"
        )
    )

    if status != "pending":
        return False

    if routing in {
        "human_review",
        "human-review",
        "tier3",
        "tier_3",
    }:
        return True

    if (
        "human" in action
        or "tier3" in action
        or "tier_3" in action
    ):
        return True

    return False


def is_auto_processing_pending(incident):

    return (
        normalized(
            incident.get("status")
        ) == "pending"
        and not is_human_review(
            incident
        )
    )


# ============================================================
# TIER HELPERS
# ============================================================

def get_remediation_tier(incident):

    tier1_outcome = normalized(
        incident.get(
            "tier1_outcome"
        )
    )

    tier2_outcome = normalized(
        incident.get(
            "tier2_outcome"
        )
    )

    tier1_attempted = normalized(
        incident.get(
            "tier1_attempted"
        )
    )

    tier2_attempted = normalized(
        incident.get(
            "tier2_attempted"
        )
    )

    action = normalized(
        incident.get(
            "action_taken"
        )
    )

    outcome = normalized(
        incident.get(
            "outcome"
        )
    )

    if outcome == "human_resolved":
        return "Human Resolved"

    if is_success(
        tier2_outcome
    ):
        return "Tier 2"

    if is_success(
        tier1_outcome
    ):
        return "Tier 1"

    if tier2_attempted in {
        "1",
        "true",
        "yes",
    }:
        return "Tier 2 attempted"

    if tier2_outcome:
        return "Tier 2 attempted"

    if tier1_attempted in {
        "1",
        "true",
        "yes",
    }:
        return "Tier 1 attempted"

    if tier1_outcome:
        return "Tier 1 attempted"

    if "circuit" in action:
        return "Tier 4"

    if (
        "recover" in action
        or "natural" in action
        or outcome == "recovered"
    ):
        return "Natural Recovery"

    if action in REMEDIATION_ACTIONS:
        return "Remediation"

    return "Not attempted"


def get_solved_by(incident):

    outcome = normalized(
        incident.get(
            "outcome"
        )
    )

    tier2_outcome = normalized(
        incident.get(
            "tier2_outcome"
        )
    )

    tier1_outcome = normalized(
        incident.get(
            "tier1_outcome"
        )
    )

    action = normalized(
        incident.get(
            "action_taken"
        )
    )

    if outcome == "human_resolved":
        return "Human Resolution"

    if outcome == "false_positive":
        return "False Positive Feedback"

    if outcome in {
        "recovered",
        "natural_recovery",
        "natural recovery",
    }:
        return "Natural Recovery"

    if is_success(
        tier2_outcome
    ):
        return "Tier 2 — Container Restart"

    if is_success(
        tier1_outcome
    ):
        return "Tier 1 — Automatic Remediation"

    if (
        "circuit" in action
        and is_success(outcome)
    ):
        return "Tier 4 — Circuit Breaker"

    if outcome in STATUS_SUCCESS_VALUES:
        return "Automatic Remediation"

    if is_failure(
        tier2_outcome
    ):
        return "Tier 2 Failed"

    if is_failure(
        tier1_outcome
    ):
        return "Tier 1 Failed"

    if action:
        return str(
            incident.get(
                "action_taken"
            )
        )

    return "—"


def get_action_label(action):

    labels = {
        "sighup_worker":
            "SIGHUP Worker",

        "inspect_logs_then_sighup":
            "Inspect Logs → SIGHUP",

        "cpu_throttle":
            "CPU Throttle",

        "restart_container":
            "Restart Container",

        "tier1_sighup":
            "Tier 1 — SIGHUP",

        "tier1_inspect_logs_then_sighup":
            "Tier 1 — Inspect Logs → SIGHUP",

        "tier1_cpu_throttle":
            "Tier 1 — CPU Throttle",

        "tier2_restart":
            "Tier 2 — Restart Container",

        "circuit_breaker":
            "Tier 4 — Circuit Breaker",

        "recovered_without_remediation":
            "Natural Recovery",

        "human_resolution":
            "Human Resolution",

        "human_feedback":
            "Human Feedback",
    }

    if action in labels:
        return labels[action]

    if action in (
        None,
        "",
        "—"
    ):
        return "—"

    return str(
        action
    ).replace(
        "_",
        " "
    ).title()


# ============================================================
# STATUS HELPERS
# ============================================================

def get_incident_status_summary(incidents):

    total = len(incidents)

    pending = sum(
        1
        for incident in incidents
        if normalized(
            incident.get("status")
        ) == "pending"
    )

    auto_healing = sum(
        1
        for incident in incidents
        if normalized(
            incident.get("status")
        ) == "auto_healing"
    )

    successful_outcomes = sum(
        1
        for incident in incidents
        if (
            normalized(
                incident.get("status")
            ) in STATUS_SUCCESS_VALUES
            or is_success(
                incident.get("outcome")
            )
        )
    )

    failed = sum(
        1
        for incident in incidents
        if (
            normalized(
                incident.get("status")
            ) in {
                "failed",
                "failure"
            }
            or is_failure(
                incident.get("outcome")
            )
        )
    )

    tier1_success = sum(
        1
        for incident in incidents
        if is_success(
            incident.get(
                "tier1_outcome"
            )
        )
    )

    tier2_success = sum(
        1
        for incident in incidents
        if is_success(
            incident.get(
                "tier2_outcome"
            )
        )
    )

    tier3_count = sum(
        1
        for incident in incidents
        if normalized(
            get_routing_decision(
                incident
            )
        ) in {
            "human_review",
            "human-review",
            "tier3",
            "tier_3",
        }
    )

    tier4_count = sum(
        1
        for incident in incidents
        if "circuit" in normalized(
            incident.get(
                "action_taken"
            )
        )
    )

    natural_recovery = sum(
        1
        for incident in incidents
        if (
            normalized(
                incident.get("outcome")
            ) == "recovered"
            and not is_success(
                incident.get(
                    "tier1_outcome"
                )
            )
            and not is_success(
                incident.get(
                    "tier2_outcome"
                )
            )
        )
    )

    human_resolved = sum(
        1
        for incident in incidents
        if normalized(
            incident.get("outcome")
        ) == "human_resolved"
    )

    false_positives = sum(
        1
        for incident in incidents
        if normalized(
            incident.get(
                "human_reject_reason"
            )
        ) in {
            "false_positive",
            "false positive",
            "false-positive",
            "fp",
        }
    )

    return {
        "total": total,
        "pending": pending,
        "auto_healing": auto_healing,
        "successful": successful_outcomes,
        "failed": failed,
        "tier1_success": tier1_success,
        "tier2_success": tier2_success,
        "tier3": tier3_count,
        "tier4": tier4_count,
        "natural_recovery": natural_recovery,
        "human_resolved": human_resolved,
        "false_positives": false_positives,
    }


def get_service_status(incidents, service):

    service_incidents = [
        incident
        for incident in incidents
        if incident.get(
            "service_name"
        ) == service
    ]

    if not service_incidents:

        return {
            "status": "No incidents",
            "incident": None,
        }

    latest = sorted(
        service_incidents,
        key=lambda item: str(
            item.get(
                "created_at"
            ) or ""
        ),
        reverse=True,
    )[0]

    status = normalized(
        latest.get(
            "status"
        )
    )

    outcome = normalized(
        latest.get(
            "outcome"
        )
    )

    if outcome == "human_resolved":
        display_status = "Human Resolved"

    elif status in STATUS_SUCCESS_VALUES:
        display_status = "Recovered"

    elif status == "auto_healing":
        display_status = "Auto-Healing"

    elif status == "pending":

        if is_human_review(
            latest
        ):
            display_status = "Human Review"
        else:
            display_status = "Pending"

    elif status in {
        "failed",
        "failure"
    }:
        display_status = "Failed"

    elif status:
        display_status = status.replace(
            "_",
            " "
        ).title()

    else:
        display_status = "Unknown"

    return {
        "status": display_status,
        "incident": latest,
    }


# ============================================================
# REMEDIATION HISTORY
# ============================================================

def has_remediation_event(incident):

    tier1_outcome = incident.get(
        "tier1_outcome"
    )

    tier2_outcome = incident.get(
        "tier2_outcome"
    )

    action = normalized(
        incident.get(
            "action_taken"
        )
    )

    outcome = normalized(
        incident.get(
            "outcome"
        )
    )

    status = normalized(
        incident.get(
            "status"
        )
    )

    tier1_attempted = normalized(
        incident.get(
            "tier1_attempted"
        )
    )

    tier2_attempted = normalized(
        incident.get(
            "tier2_attempted"
        )
    )

    if outcome == "human_resolved":
        return True

    if outcome == "false_positive":
        return True

    if is_success(
        tier1_outcome
    ):
        return True

    if is_success(
        tier2_outcome
    ):
        return True

    if is_failure(
        tier1_outcome
    ):
        return True

    if is_failure(
        tier2_outcome
    ):
        return True

    if tier1_attempted in {
        "1",
        "true",
        "yes",
    }:
        return True

    if tier2_attempted in {
        "1",
        "true",
        "yes",
    }:
        return True

    if action in REMEDIATION_ACTIONS:
        return True

    if outcome in {
        "recovered",
        "resolved",
        "auto_healed",
        "healed",
        "success",
    }:
        return True

    if status in STATUS_SUCCESS_VALUES:
        return True

    return False


def get_remediation_history(incidents):

    return [
        incident
        for incident in incidents
        if has_remediation_event(
            incident
        )
    ]


def build_history_dataframe(incidents):

    rows = []

    for incident in incidents:

        rows.append(
            {
                "Incident":
                    value(
                        incident,
                        "incident_id"
                    ),

                "Service":
                    value(
                        incident,
                        "service_name"
                    ),

                "Anomaly":
                    value(
                        incident,
                        "anomaly_type"
                    ),

                "Risk":
                    value(
                        incident,
                        "risk_level"
                    ),

                "Anomaly Score":
                    format_score(
                        incident.get(
                            "anomaly_score"
                        )
                    ),

                "Status":
                    value(
                        incident,
                        "status"
                    ),

                "Routing":
                    get_routing_decision(
                        incident
                    ) or "—",

                "Tier":
                    get_remediation_tier(
                        incident
                    ),

                "Suggested Action":
                    get_action_label(
                        incident.get(
                            "suggested_action"
                        )
                    ),

                "Action Taken":
                    get_action_label(
                        incident.get(
                            "action_taken"
                        )
                    ),

                "Solved By":
                    get_solved_by(
                        incident
                    ),

                "Tier 1 Outcome":
                    value(
                        incident,
                        "tier1_outcome"
                    ),

                "Tier 2 Outcome":
                    value(
                        incident,
                        "tier2_outcome"
                    ),

                "Outcome":
                    value(
                        incident,
                        "outcome"
                    ),

                "Created":
                    format_timestamp(
                        incident.get(
                            "created_at"
                        )
                    ),

                "Resolved":
                    format_timestamp(
                        incident.get(
                            "resolved_at"
                        )
                    ),
            }
        )

    return pd.DataFrame(
        rows
    )


# ============================================================
# EVALUATION HELPERS
# ============================================================

def get_completed_incidents(incidents, service=None):
    """
    Return completed incidents.

    An outcome is required because evaluation is based on
    completed/feedback-labelled incidents.
    """

    completed = []

    for incident in incidents:

        if service is not None:

            if incident.get(
                "service_name"
            ) != service:
                continue

        outcome = normalized(
            incident.get(
                "outcome"
            )
        )

        if outcome:
            completed.append(
                incident
            )

    completed.sort(
        key=lambda item: str(
            item.get(
                "created_at"
            ) or ""
        )
    )

    return completed


def is_explicit_false_positive(incident):
    """
    Only explicit human false-positive feedback is considered
    a negative label.
    """

    return normalized(
        incident.get(
            "human_reject_reason"
        )
    ) in {
        "false_positive",
        "false positive",
        "false-positive",
        "fp",
    }


def get_evaluation_label(incident):
    """
    Feedback-derived evaluation label.

    0 = explicitly marked false positive
    1 = otherwise treated as a real anomaly

    This is a proxy label because the system does not have
    independently verified ground truth.
    """

    if is_explicit_false_positive(
        incident
    ):
        return 0

    return 1


def calculate_evaluation_metrics(
    incidents,
    threshold
):
    """
    Calculate evaluation metrics using the supplied threshold.
    """

    valid = []

    for incident in incidents:

        score = incident.get(
            "anomaly_score"
        )

        if score in (
            None,
            ""
        ):
            continue

        try:

            score = float(
                score
            )

        except (
            TypeError,
            ValueError
        ):
            continue

        valid.append(
            (
                score,
                get_evaluation_label(
                    incident
                )
            )
        )

    if not valid:

        return {
            "samples": 0,
            "false_positive_rate": 0.0,
            "precision": 0.0,
            "recall": 0.0,
            "f1_score": 0.0,
            "tp": 0,
            "tn": 0,
            "fp": 0,
            "fn": 0,
        }

    y_true = [
        item[1]
        for item in valid
    ]

    y_pred = [
        1
        if item[0] < -threshold
        else 0
        for item in valid
    ]

    tn, fp, fn, tp = confusion_matrix(
        y_true,
        y_pred,
        labels=[0, 1]
    ).ravel()

    fpr = (
        fp / (fp + tn)
        if (fp + tn) > 0
        else 0.0
    )

    return {
        "samples": len(y_true),
        "false_positive_rate": fpr,
        "precision": precision_score(
            y_true,
            y_pred,
            zero_division=0
        ),
        "recall": recall_score(
            y_true,
            y_pred,
            zero_division=0
        ),
        "f1_score": f1_score(
            y_true,
            y_pred,
            zero_division=0
        ),
        "tp": int(tp),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
    }


def get_remediation_failure_rate(
    incidents
):
    """
    Calculate remediation failure rate.

    This is displayed for evaluation only.

    Remediation failure does NOT directly lower the detector
    threshold.
    """

    if not incidents:
        return 0.0

    failures = 0

    for incident in incidents:

        outcome = normalized(
            incident.get(
                "outcome"
            )
        )

        if outcome in {
            "failure",
            "failed",
        }:
            failures += 1

    return failures / len(
        incidents
    )


def get_false_positive_rate(
    incidents
):
    """
    Calculate explicit false-positive feedback rate.
    """

    if not incidents:
        return 0.0

    false_positives = sum(
        1
        for incident in incidents
        if is_explicit_false_positive(
            incident
        )
    )

    return (
        false_positives
        / len(incidents)
    )


def simulate_threshold_evolution(
    incidents
):
    """
    Simulate the same threshold adaptation logic used by
    adaptive_threshold.py.

    Every 10 completed incidents:

        FP rate > 30%
            -> threshold +0.10

        Otherwise
            -> unchanged

    Remediation failure is reported but does not lower
    the detector threshold.
    """

    if not incidents:
        return pd.DataFrame()

    ordered = sorted(
        incidents,
        key=lambda item: str(
            item.get(
                "created_at"
            ) or ""
        )
    )

    threshold = DEFAULT_THRESHOLD

    rows = []

    total = len(
        ordered
    )

    for end_index in range(
        ADJUST_EVERY,
        total + 1,
        ADJUST_EVERY
    ):

        batch = ordered[
            end_index - ADJUST_EVERY:
            end_index
        ]

        fp_rate = get_false_positive_rate(
            batch
        )

        failure_rate = get_remediation_failure_rate(
            batch
        )

        before = threshold

        if fp_rate > FP_RATE_LIMIT:

            threshold = min(
                threshold + THRESHOLD_STEP,
                THRESHOLD_MAX
            )

            adjustment = "+0.10"

        else:

            adjustment = "No change"

        rows.append(
            {
                "Incidents Processed":
                    end_index,

                "Batch Size":
                    len(batch),

                "False Positive Rate":
                    f"{fp_rate:.1%}",

                "Remediation Failure Rate":
                    f"{failure_rate:.1%}",

                "Previous Threshold":
                    f"{before:.2f}",

                "Adjustment":
                    adjustment,

                "Simulated Threshold":
                    f"{threshold:.2f}",
            }
        )

    return pd.DataFrame(
        rows
    )


def build_model_comparison(
    incidents,
    service,
    service_threshold
):
    """
    Compare:

    1. Per-Service Adaptive Threshold
    2. Global Fixed-Threshold Baseline

    The baseline uses the same anomaly scores but always
    applies 0.60.

    It is therefore a fixed-threshold baseline, not a
    separately trained global Isolation Forest.
    """

    service_metrics = calculate_evaluation_metrics(
        incidents,
        service_threshold
    )

    global_metrics = calculate_evaluation_metrics(
        incidents,
        DEFAULT_THRESHOLD
    )

    rows = [
        {
            "Model":
                "Per-Service Adaptive Threshold",

            "Threshold":
                service_threshold,

            "Samples":
                service_metrics["samples"],

            "False Positive Rate":
                service_metrics[
                    "false_positive_rate"
                ],

            "Precision":
                service_metrics[
                    "precision"
                ],

            "Recall":
                service_metrics[
                    "recall"
                ],

            "F1 Score":
                service_metrics[
                    "f1_score"
                ],

            "TP":
                service_metrics["tp"],

            "TN":
                service_metrics["tn"],

            "FP":
                service_metrics["fp"],

            "FN":
                service_metrics["fn"],
        },

        {
            "Model":
                "Global Fixed-Threshold Baseline",

            "Threshold":
                DEFAULT_THRESHOLD,

            "Samples":
                global_metrics["samples"],

            "False Positive Rate":
                global_metrics[
                    "false_positive_rate"
                ],

            "Precision":
                global_metrics[
                    "precision"
                ],

            "Recall":
                global_metrics[
                    "recall"
                ],

            "F1 Score":
                global_metrics[
                    "f1_score"
                ],

            "TP":
                global_metrics["tp"],

            "TN":
                global_metrics["tn"],

            "FP":
                global_metrics["fp"],

            "FN":
                global_metrics["fn"],
        }
    ]

    return pd.DataFrame(
        rows
    )


# ============================================================
# INCIDENT DETAIL
# ============================================================

def show_incident_details(
    incident,
    show_human_message=True
):

    incident_id = value(
        incident,
        "incident_id"
    )

    service = value(
        incident,
        "service_name"
    )

    anomaly = value(
        incident,
        "anomaly_type"
    )

    risk_level = value(
        incident,
        "risk_level"
    )

    risk_score = format_score(
        incident.get(
            "risk_score"
        )
    )

    anomaly_score = format_score(
        incident.get(
            "anomaly_score"
        )
    )

    status = value(
        incident,
        "status"
    )

    routing = (
        get_routing_decision(
            incident
        )
        or "—"
    )

    suggested_action = get_action_label(
        incident.get(
            "suggested_action"
        )
    )

    action_taken = get_action_label(
        incident.get(
            "action_taken"
        )
    )

    tier = get_remediation_tier(
        incident
    )

    solved_by = get_solved_by(
        incident
    )

    created = format_timestamp(
        incident.get(
            "created_at"
        )
    )

    resolved = format_timestamp(
        incident.get(
            "resolved_at"
        )
    )

    with st.container(
        border=True
    ):

        st.subheader(
            f"{service} — {anomaly}"
        )

        st.caption(
            f"Incident ID: {incident_id}"
        )

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Risk Level",
                risk_level
            )

        with col2:
            st.metric(
                "Risk Score",
                risk_score
            )

        with col3:
            st.metric(
                "Anomaly Score",
                anomaly_score
            )

        with col4:
            st.metric(
                "Status",
                status
            )

        st.divider()

        info1, info2, info3 = st.columns(3)

        with info1:

            st.write(
                "**Routing Decision**"
            )

            st.write(
                routing
            )

        with info2:

            st.write(
                "**Suggested Action**"
            )

            st.write(
                suggested_action
            )

        with info3:

            st.write(
                "**Tier**"
            )

            st.write(
                tier
            )

        st.divider()

        detail1, detail2 = st.columns(2)

        with detail1:

            st.write(
                "**Action Taken**"
            )

            st.write(
                action_taken
            )

            st.write(
                "**Solved By**"
            )

            st.write(
                solved_by
            )

        with detail2:

            st.write(
                "**Created**"
            )

            st.write(
                created
            )

            st.write(
                "**Resolved**"
            )

            st.write(
                resolved
            )

        reason = incident.get(
            "reason"
        )

        if reason not in (
            None,
            ""
        ):

            st.divider()

            st.write(
                "**Reason**"
            )

            st.info(
                str(reason)
            )

        tier1_outcome = incident.get(
            "tier1_outcome"
        )

        tier2_outcome = incident.get(
            "tier2_outcome"
        )

        if (
            tier1_outcome not in (
                None,
                ""
            )
            or tier2_outcome not in (
                None,
                ""
            )
        ):

            st.divider()

            st.write(
                "**Remediation Events**"
            )

            remediation1, remediation2 = st.columns(2)

            with remediation1:

                st.write(
                    "Tier 1"
                )

                if is_success(
                    tier1_outcome
                ):

                    st.success(
                        str(
                            tier1_outcome
                        )
                    )

                elif is_failure(
                    tier1_outcome
                ):

                    st.error(
                        str(
                            tier1_outcome
                        )
                    )

                elif tier1_outcome:

                    st.info(
                        str(
                            tier1_outcome
                        )
                    )

                else:

                    st.write(
                        "Not attempted"
                    )

            with remediation2:

                st.write(
                    "Tier 2"
                )

                if is_success(
                    tier2_outcome
                ):

                    st.success(
                        str(
                            tier2_outcome
                        )
                    )

                elif is_failure(
                    tier2_outcome
                ):

                    st.error(
                        str(
                            tier2_outcome
                        )
                    )

                elif tier2_outcome:

                    st.info(
                        str(
                            tier2_outcome
                        )
                    )

                else:

                    st.write(
                        "Not attempted"
                    )

        # ----------------------------------------------------
        # HUMAN REVIEW
        # ----------------------------------------------------

        if (
            show_human_message
            and is_human_review(
                incident
            )
        ):

            st.divider()

            st.warning(
                "Tier 3 — Human investigation required."
            )

            st.write(
                "Fix the underlying service issue manually. "
                "After confirming that the issue has been "
                "resolved, mark this incident as resolved."
            )

            st.caption(
                "This action records a human resolution. "
                "It does not execute automatic remediation."
            )

            button_key = (
                f"resolve_{incident_id}"
            )

            if st.button(
                "Mark as Resolved",
                key=button_key,
                use_container_width=True
            ):

                success, message = (
                    mark_human_resolved(
                        incident_id
                    )
                )

                if success:

                    st.success(
                        message
                    )

                    st.rerun()

                else:

                    st.error(
                        message
                    )

        elif is_auto_processing_pending(
            incident
        ):

            st.divider()

            st.info(
                "Pending automatic processing. "
                "The remediation pipeline is handled by "
                "main.py and remediation_executor."
            )


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.title(
        "Operations Controls"
    )

    st.write(
        "Service Operations"
    )

    st.divider()

    if st.button(
        "Refresh Dashboard",
        use_container_width=True
    ):
        st.rerun()

    st.divider()

    st.write(
        "**Services**"
    )

    for service in SERVICES:

        st.write(
            f"• {service}"
        )

    st.divider()

    st.caption(
        "Automatic remediation is controlled by "
        "the backend pipeline."
    )


# ============================================================
# LOAD DATA
# ============================================================

incidents = get_all_incidents()

pending_incidents = [
    incident
    for incident in incidents
    if normalized(
        incident.get(
            "status"
        )
    ) == "pending"
]

human_review_incidents = [
    incident
    for incident in pending_incidents
    if is_human_review(
        incident
    )
]

auto_processing_pending = [
    incident
    for incident in pending_incidents
    if is_auto_processing_pending(
        incident
    )
]

thresholds = get_thresholds()

stats = get_incident_status_summary(
    incidents
)

remediation_history = get_remediation_history(
    incidents
)

completed_incidents = get_completed_incidents(
    incidents
)


# ============================================================
# HEADER
# ============================================================

st.title(
    "Operations Dashboard"
)

st.write(
    "Service anomaly detection, risk assessment, "
    "automatic remediation, evaluation, and human review."
)

st.divider()


# ============================================================
# 1. PENDING INCIDENTS — FIRST
# ============================================================

st.header(
    "Pending Incidents"
)

if not pending_incidents:

    st.success(
        "No pending incidents."
    )

else:

    st.write(
        f"{len(pending_incidents)} pending incident(s) "
        "currently recorded."
    )

    if auto_processing_pending:

        st.subheader(
            "Pending Automatic Processing"
        )

        st.caption(
            "These incidents are not routed to Tier 3 "
            "human review."
        )

        for incident in auto_processing_pending:

            with st.expander(
                f"{value(incident, 'incident_id')} — "
                f"{value(incident, 'service_name')} — "
                f"{value(incident, 'anomaly_type')}",
                expanded=False
            ):

                show_incident_details(
                    incident,
                    show_human_message=False
                )

    if human_review_incidents:

        st.subheader(
            "Human Review Required"
        )

        st.caption(
            "These incidents reached Tier 3 and require "
            "human intervention."
        )

        for incident in human_review_incidents:

            with st.expander(
                f"{value(incident, 'incident_id')} — "
                f"{value(incident, 'service_name')} — "
                f"{value(incident, 'anomaly_type')}",
                expanded=True
            ):

                show_incident_details(
                    incident,
                    show_human_message=True
                )


st.divider()


# ============================================================
# 2. AUTO-HEALED & REMEDIATION HISTORY
# ============================================================

st.header(
    "Auto-Healed & Remediation History"
)

st.write(
    "Completed automatic remediation, attempted "
    "remediation, natural recovery, and human resolution."
)

if not remediation_history:

    st.info(
        "No remediation or recovery events recorded yet."
    )

else:

    history_df = build_history_dataframe(
        remediation_history
    )

    st.dataframe(
        history_df,
        use_container_width=True,
        hide_index=True,
    )

    csv_data = history_df.to_csv(
        index=False
    ).encode(
        "utf-8"
    )

    st.download_button(
        label="Download CSV",
        data=csv_data,
        file_name="operations_remediation_history.csv",
        mime="text/csv",
        use_container_width=False,
    )


st.divider()


# ============================================================
# 3. SYSTEM OVERVIEW
# ============================================================

st.header(
    "System Overview"
)

metric1, metric2, metric3, metric4, metric5 = st.columns(5)

with metric1:

    st.metric(
        "Services",
        len(SERVICES)
    )

with metric2:

    st.metric(
        "Total Incidents",
        stats["total"]
    )

with metric3:

    st.metric(
        "Pending",
        stats["pending"]
    )

with metric4:

    st.metric(
        "Successful Outcomes",
        stats["successful"]
    )

with metric5:

    st.metric(
        "Failed",
        stats["failed"]
    )


st.divider()


# ============================================================
# 4. SERVICE STATUS
# ============================================================

st.header(
    "Service Status"
)

st.caption(
    "Status represents the latest recorded incident "
    "for each service, not a live Prometheus health check."
)

service_columns = st.columns(
    len(SERVICES)
)

for column, service in zip(
    service_columns,
    SERVICES
):

    status_data = get_service_status(
        incidents,
        service
    )

    latest = status_data[
        "incident"
    ]

    with column:

        with st.container(
            border=True
        ):

            st.subheader(
                service
            )

            st.metric(
                "Latest Status",
                status_data[
                    "status"
                ]
            )

            if latest:

                st.write(
                    "**Latest Incident**"
                )

                st.write(
                    value(
                        latest,
                        "incident_id"
                    )
                )

                st.write(
                    "**Anomaly**"
                )

                st.write(
                    value(
                        latest,
                        "anomaly_type"
                    )
                )

                st.write(
                    "**Risk**"
                )

                st.write(
                    value(
                        latest,
                        "risk_level"
                    )
                )

                st.write(
                    "**Recorded**"
                )

                st.caption(
                    format_timestamp(
                        latest.get(
                            "created_at"
                        )
                    )
                )

            else:

                st.success(
                    "No incidents recorded"
                )


st.divider()


# ============================================================
# 5. CURRENT DETECTION THRESHOLDS
# ============================================================

st.header(
    "Current Detection Thresholds"
)

st.caption(
    "Current anomaly threshold stored in the detector "
    "profiles database."
)

threshold_columns = st.columns(
    len(SERVICES)
)

for column, service in zip(
    threshold_columns,
    SERVICES
):

    with column:

        with st.container(
            border=True
        ):

            st.subheader(
                service
            )

            st.metric(
                "Threshold",
                format_threshold(
                    thresholds.get(
                        service,
                        DEFAULT_THRESHOLD
                    )
                )
            )

            st.caption(
                "Current anomaly threshold"
            )


st.divider()


# ============================================================
# 6. REMEDIATION SUMMARY
# ============================================================

st.header(
    "Remediation Summary"
)

summary_columns = st.columns(6)

with summary_columns[0]:

    st.metric(
        "Tier 1 Successful",
        stats["tier1_success"]
    )

with summary_columns[1]:

    st.metric(
        "Tier 2 Successful",
        stats["tier2_success"]
    )

with summary_columns[2]:

    st.metric(
        "Tier 3 Human Review",
        stats["tier3"]
    )

with summary_columns[3]:

    st.metric(
        "Tier 4",
        stats["tier4"]
    )

with summary_columns[4]:

    st.metric(
        "Natural Recovery",
        stats["natural_recovery"]
    )

with summary_columns[5]:

    st.metric(
        "Human Resolved",
        stats["human_resolved"]
    )


st.divider()


# ============================================================
# 7. EVALUATION & ADAPTATION
# ============================================================

st.header(
    "Evaluation & Adaptation"
)

st.write(
    "Evaluation of detector behaviour, false-positive feedback, "
    "model comparison, and threshold adaptation."
)

# ------------------------------------------------------------
# Evaluation explanation
# ------------------------------------------------------------

with st.expander(
    "How the evaluation works",
    expanded=False
):

    st.write(
        "Completed incidents are evaluated using recorded "
        "human feedback and incident outcomes."
    )

    st.write(
        "An incident explicitly marked as "
        "'false_positive' is treated as a negative label. "
        "Other completed incidents are treated as real "
        "anomalies for this feedback-derived evaluation."
    )

    st.write(
        "Remediation failure is evaluated separately. "
        "A failed remediation does not automatically mean "
        "that the detector produced a false positive."
    )

    st.write(
        "Threshold adaptation occurs after every 10 completed "
        "incidents. If the explicit false-positive rate exceeds "
        "30%, the threshold is increased by 0.10, subject to "
        "the configured minimum and maximum."
    )


# ------------------------------------------------------------
# 7A. FALSE POSITIVE EVALUATION
# ------------------------------------------------------------

st.subheader(
    "False Positive Evaluation"
)

evaluation_total = len(
    completed_incidents
)

evaluation_fp = sum(
    1
    for incident in completed_incidents
    if is_explicit_false_positive(
        incident
    )
)

evaluation_fp_rate = (
    evaluation_fp / evaluation_total
    if evaluation_total > 0
    else 0.0
)

evaluation_failure_rate = get_remediation_failure_rate(
    completed_incidents
)

fp_col1, fp_col2, fp_col3, fp_col4 = st.columns(4)

with fp_col1:

    st.metric(
        "Completed Incidents",
        evaluation_total
    )

with fp_col2:

    st.metric(
        "False Positives",
        evaluation_fp
    )

with fp_col3:

    st.metric(
        "False Positive Rate",
        f"{evaluation_fp_rate:.1%}"
    )

with fp_col4:

    st.metric(
        "Remediation Failure Rate",
        f"{evaluation_failure_rate:.1%}"
    )


# ------------------------------------------------------------
# FEEDBACK TABLE
# ------------------------------------------------------------

if completed_incidents:

    feedback_rows = []

    for incident in completed_incidents:

        feedback_rows.append(
            {
                "Incident":
                    value(
                        incident,
                        "incident_id"
                    ),

                "Service":
                    value(
                        incident,
                        "service_name"
                    ),

                "Anomaly":
                    value(
                        incident,
                        "anomaly_type"
                    ),

                "Anomaly Score":
                    format_score(
                        incident.get(
                            "anomaly_score"
                        )
                    ),

                "Outcome":
                    value(
                        incident,
                        "outcome"
                    ),

                "Human Feedback":
                    (
                        "False Positive"
                        if is_explicit_false_positive(
                            incident
                        )
                        else "Real Anomaly / No FP Feedback"
                    ),
            }
        )

    feedback_df = pd.DataFrame(
        feedback_rows
    )

    st.dataframe(
        feedback_df,
        use_container_width=True,
        hide_index=True,
    )


# ------------------------------------------------------------
# RECORD FALSE-POSITIVE FEEDBACK
# ------------------------------------------------------------

st.write(
    "**Human Feedback**"
)

st.caption(
    "Use this only when a reviewer confirms that a detected "
    "incident was actually a false positive."
)

feedback_candidates = [
    incident
    for incident in completed_incidents
    if not is_explicit_false_positive(
        incident
    )
]

if feedback_candidates:

    feedback_options = {
        (
            f"{incident.get('incident_id')} — "
            f"{incident.get('service_name')} — "
            f"{incident.get('anomaly_type')}"
        ): incident.get(
            "incident_id"
        )
        for incident in feedback_candidates
    }

    selected_feedback = st.selectbox(
        "Incident to label",
        list(
            feedback_options.keys()
        ),
        key="false_positive_selector"
    )

    if st.button(
        "Mark Selected Incident as False Positive",
        key="mark_false_positive_button",
    ):

        selected_id = feedback_options[
            selected_feedback
        ]

        success, message = mark_false_positive(
            selected_id
        )

        if success:

            st.success(
                message
            )

            st.rerun()

        else:

            st.error(
                message
            )

else:

    st.info(
        "No incidents currently available for new "
        "false-positive feedback."
    )


st.divider()


# ------------------------------------------------------------
# 7B. MODEL COMPARISON
# ------------------------------------------------------------

st.subheader(
    "Model Comparison"
)

st.write(
    "Comparison of the service-specific adaptive threshold "
    "against a global fixed-threshold baseline."
)

comparison_service = st.selectbox(
    "Select service for evaluation",
    SERVICES,
    key="comparison_service"
)

service_incidents = get_completed_incidents(
    incidents,
    comparison_service
)

current_service_threshold = thresholds.get(
    comparison_service,
    DEFAULT_THRESHOLD
)

if service_incidents:

    comparison_df = build_model_comparison(
        service_incidents,
        comparison_service,
        current_service_threshold
    )

    display_comparison = comparison_df.copy()

    display_comparison[
        "Threshold"
    ] = display_comparison[
        "Threshold"
    ].map(
        lambda x: f"{float(x):.2f}"
    )

    display_comparison[
        "False Positive Rate"
    ] = display_comparison[
        "False Positive Rate"
    ].map(
        lambda x: f"{float(x):.1%}"
    )

    display_comparison[
        "Precision"
    ] = display_comparison[
        "Precision"
    ].map(
        lambda x: f"{float(x):.4f}"
    )

    display_comparison[
        "Recall"
    ] = display_comparison[
        "Recall"
    ].map(
        lambda x: f"{float(x):.4f}"
    )

    display_comparison[
        "F1 Score"
    ] = display_comparison[
        "F1 Score"
    ].map(
        lambda x: f"{float(x):.4f}"
    )

    st.dataframe(
        display_comparison,
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "The Global Fixed-Threshold Baseline uses a fixed "
        "threshold of 0.60. It is a baseline comparison, "
        "not a separately trained global Isolation Forest."
    )

else:

    st.info(
        f"No completed incidents are available for "
        f"{comparison_service} yet."
    )


st.divider()


# ------------------------------------------------------------
# 7C. THRESHOLD ADAPTATION
# ------------------------------------------------------------

st.subheader(
    "Threshold Adaptation"
)

st.write(
    "Current detector thresholds and simulated adaptation "
    "history based on false-positive feedback."
)

adapt_col1, adapt_col2, adapt_col3 = st.columns(3)

with adapt_col1:

    st.metric(
        "Adjustment Interval",
        f"{ADJUST_EVERY} incidents"
    )

with adapt_col2:

    st.metric(
        "False Positive Limit",
        f"{FP_RATE_LIMIT:.0%}"
    )

with adapt_col3:

    st.metric(
        "Threshold Step",
        f"+{THRESHOLD_STEP:.2f}"
    )


threshold_rows = []

for service in SERVICES:

    service_completed = get_completed_incidents(
        incidents,
        service
    )

    service_fp_rate = get_false_positive_rate(
        service_completed
    )

    threshold_rows.append(
        {
            "Service":
                service,

            "Completed Incidents":
                len(service_completed),

            "False Positive Rate":
                f"{service_fp_rate:.1%}",

            "Current Threshold":
                f"{thresholds.get(service, DEFAULT_THRESHOLD):.2f}",

            "Next Adjustment":
                (
                    "Eligible"
                    if len(service_completed)
                    >= ADJUST_EVERY
                    and len(service_completed)
                    % ADJUST_EVERY == 0
                    else
                    f"After next {ADJUST_EVERY}-incident checkpoint"
                ),
        }
    )

threshold_df = pd.DataFrame(
    threshold_rows
)

st.dataframe(
    threshold_df,
    use_container_width=True,
    hide_index=True,
)


# ------------------------------------------------------------
# THRESHOLD EVOLUTION
# ------------------------------------------------------------

st.write(
    "**Threshold Adaptation History / Simulation**"
)

adaptation_service = st.selectbox(
    "Select service",
    SERVICES,
    key="adaptation_service"
)

adaptation_incidents = get_completed_incidents(
    incidents,
    adaptation_service
)

evolution_df = simulate_threshold_evolution(
    adaptation_incidents
)

if evolution_df.empty:

    st.info(
        f"{adaptation_service} does not yet have "
        f"{ADJUST_EVERY} completed incidents in a "
        f"full evaluation batch."
    )

else:

    st.dataframe(
        evolution_df,
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "This table simulates the configured adaptation "
        "rule from the recorded incident history. It does "
        "not claim to reconstruct historical threshold values "
        "unless those values were stored at the time."
    )


st.divider()


# ============================================================
# 8. FULL INCIDENT HISTORY
# ============================================================

st.header(
    "Incident History"
)

recent_incidents = get_recent_incidents(
    limit=20
)

if not recent_incidents:

    st.info(
        "No incidents recorded."
    )

else:

    history_rows = []

    for incident in recent_incidents:

        history_rows.append(
            {
                "Incident":
                    value(
                        incident,
                        "incident_id"
                    ),

                "Service":
                    value(
                        incident,
                        "service_name"
                    ),

                "Anomaly":
                    value(
                        incident,
                        "anomaly_type"
                    ),

                "Risk":
                    value(
                        incident,
                        "risk_level"
                    ),

                "Status":
                    value(
                        incident,
                        "status"
                    ),

                "Routing":
                    get_routing_decision(
                        incident
                    ) or "—",

                "Tier":
                    get_remediation_tier(
                        incident
                    ),

                "Action Taken":
                    get_action_label(
                        incident.get(
                            "action_taken"
                        )
                    ),

                "Solved By":
                    get_solved_by(
                        incident
                    ),

                "Outcome":
                    value(
                        incident,
                        "outcome"
                    ),

                "Created":
                    format_timestamp(
                        incident.get(
                            "created_at"
                        )
                    ),

                "Resolved":
                    format_timestamp(
                        incident.get(
                            "resolved_at"
                        )
                    ),
            }
        )

    incident_history_df = pd.DataFrame(
        history_rows
    )

    st.dataframe(
        incident_history_df,
        use_container_width=True,
        hide_index=True,
    )


st.divider()


# ============================================================
# FOOTER
# ============================================================

st.caption(
    "Operations Dashboard — anomaly detection, "
    "risk assessment, automated recovery, evaluation, "
    "adaptation, and human review."
)

st.caption(
    f"Incidents DB: {INCIDENTS_DB}"
)

st.caption(
    f"Profiles DB: {PROFILES_DB}"
)