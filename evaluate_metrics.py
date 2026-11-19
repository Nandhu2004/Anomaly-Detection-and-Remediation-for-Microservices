
import os
import sqlite3
import pandas as pd

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(
    BASE_DIR, "incident_manager", "incidents.db"
)

INCIDENT_EXPORT = os.path.join(
    BASE_DIR, "incident_evaluation_data.csv"
)

SUMMARY_EXPORT = os.path.join(
    BASE_DIR, "incident_evaluation_summary.csv"
)


# ============================================================
# LOAD INCIDENTS
# ============================================================

def load_incidents():
    if not os.path.isfile(DB_PATH):
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}"
        )

    with sqlite3.connect(DB_PATH) as conn:
        tables = pd.read_sql_query(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            """,
            conn,
        )["name"].tolist()

        if "incidents" not in tables:
            raise ValueError(
                "The database does not contain an incidents table."
            )

        df = pd.read_sql_query(
            'SELECT * FROM "incidents"',
            conn,
        )

    if df.empty:
        raise ValueError("The incidents table contains no records.")

    return df


# ============================================================
# EXPORT INCIDENT RECORDS
# ============================================================

def export_incidents(df):
    columns = [
        "incident_id",
        "service_name",
        "anomaly_type",
        "anomaly_score",
        "risk_level",
        "risk_score",
        "routing_decision",
        "status",
        "suggested_action",
        "action_taken",
        "outcome",
        "human_approved",
        "tier1_attempted",
        "tier1_outcome",
        "tier2_attempted",
        "tier2_outcome",
        "created_at",
        "resolved_at",
        "time_to_recover",
        "reason",
    ]

    available = [col for col in columns if col in df.columns]
    df[available].to_csv(INCIDENT_EXPORT, index=False)

    print(f"Incident records exported: {INCIDENT_EXPORT}")
    print(f"Total incident records: {len(df)}")


# ============================================================
# INCIDENT SUMMARY
# ============================================================

def create_summary(df):
    summary_columns = [
        "service_name",
        "anomaly_type",
        "status",
        "routing_decision",
        "action_taken",
        "outcome",
    ]

    available = [
        col for col in summary_columns if col in df.columns
    ]

    summary = (
        df.groupby(available, dropna=False)
        .size()
        .reset_index(name="incident_count")
        .sort_values("incident_count", ascending=False)
    )

    summary.to_csv(SUMMARY_EXPORT, index=False)

    print(f"\nIncident summary exported: {SUMMARY_EXPORT}")
    print("\nIncident counts by service:")
    print(df["service_name"].value_counts(dropna=False).to_string())

    if "status" in df.columns:
        print("\nIncident counts by status:")
        print(df["status"].value_counts(dropna=False).to_string())

    if "action_taken" in df.columns:
        print("\nRemediation action counts:")
        print(
            df["action_taken"]
            .value_counts(dropna=False)
            .to_string()
        )

    if "outcome" in df.columns:
        print("\nRecorded outcome counts:")
        print(df["outcome"].value_counts(dropna=False).to_string())


# ============================================================
# EVALUATION REQUIREMENTS
# ============================================================

def report_evaluation_requirements(df):
    print("\n" + "=" * 60)
    print("CLASSIFICATION EVALUATION CHECK")
    print("=" * 60)

    print(
        "\nIncident records represent recorded incidents, "
        "not necessarily every detector observation."
    )

    print(
        "\nThe incidents table does not establish independent "
        "ground-truth labels for both normal and anomalous "
        "observations."
    )

    print(
        "\nPrecision, recall, F1-score, and accuracy cannot be "
        "reliably calculated from incident counts alone."
    )

    print(
        "\nTo calculate these metrics, collect matched observations "
        "containing:"
    )
    print("  1. service_name")
    print("  2. timestamp")
    print("  3. actual_label (0 = normal, 1 = anomaly)")
    print("  4. predicted_label (0 = normal, 1 = anomaly)")

    print(
        "\nDo not use remediation success or incident status as "
        "a substitute for the actual anomaly label."
    )

    print(
        "\nNo changes have been made to main.py or the database."
    )


# ============================================================
# MAIN
# ============================================================

def main():
    try:
        df = load_incidents()
        export_incidents(df)
        create_summary(df)
        report_evaluation_requirements(df)

    except (OSError, ValueError, sqlite3.Error) as exc:
        print(f"\nEvaluation error: {exc}")


if __name__ == "__main__":
    main()
