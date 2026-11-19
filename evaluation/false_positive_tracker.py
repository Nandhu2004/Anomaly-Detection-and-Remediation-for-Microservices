import sqlite3
import logging
import pandas as pd
import os
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
INCIDENTS_DB = "incident_manager/incidents.db"
RESULTS_DIR  = "evaluation/results"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ── Load incidents ────────────────────────────────────────────────────────────
def load_all_incidents() -> pd.DataFrame:
    """Loads all resolved incidents from incidents.db."""
    try:
        conn = sqlite3.connect(INCIDENTS_DB)
        df   = pd.read_sql_query("""
            SELECT
                incident_id,
                service_name,
                anomaly_score,
                outcome,
                human_approved,
                human_reject_reason,
                routing_decision,
                time_to_recover,
                created_at
            FROM incidents
            WHERE outcome IS NOT NULL
            ORDER BY created_at ASC
        """, conn)
        conn.close()
        return df
    except Exception as e:
        log.error(f"Error loading incidents: {e}")
        return pd.DataFrame()


# ── False positive rate over time ─────────────────────────────────────────────
def track_false_positive_rate(
    service_name: str,
    window_size: int = 10
) -> pd.DataFrame:
    """
    Calculates false positive rate in rolling windows of N incidents.
    Shows how FPR changes over time as the feedback mechanism calibrates.

    This directly answers RQ2: Does feedback-driven threshold calibration
    improve detection precision over time?
    """
    df = load_all_incidents()
    if df.empty:
        log.warning("No incident data found")
        return pd.DataFrame()

    # Filter to this service
    service_df = df[df["service_name"] == service_name].copy()
    if service_df.empty:
        log.warning(f"No incidents for {service_name}")
        return pd.DataFrame()

    service_df = service_df.reset_index(drop=True)

    # Label each incident as false positive or true anomaly
    service_df["is_false_positive"] = (
        service_df["human_reject_reason"] == "false_positive"
    ).astype(int)

    # Calculate rolling false positive rate
    results = []
    for i in range(window_size, len(service_df) + 1):
        window   = service_df.iloc[i - window_size:i]
        fp_count = window["is_false_positive"].sum()
        fp_rate  = fp_count / window_size

        results.append({
            "incident_number": i,
            "window":          f"{i-window_size+1}-{i}",
            "false_positives": int(fp_count),
            "fp_rate":         round(fp_rate, 4),
            "timestamp":       window.iloc[-1]["created_at"],
        })

    results_df = pd.DataFrame(results)

    if not results_df.empty:
        print(f"\nFalse Positive Rate Over Time — {service_name}")
        print(f"Window size: {window_size} incidents")
        print("-" * 55)
        print(f"{'Window':<15} {'FP Count':<12} {'FP Rate':<12}")
        print("-" * 40)
        for _, row in results_df.iterrows():
            trend = "↓" if row["fp_rate"] < 0.3 else "↑"
            print(
                f"{row['window']:<15} "
                f"{row['false_positives']:<12} "
                f"{row['fp_rate']:<12.0%} {trend}"
            )

        # Summary
        if len(results_df) >= 2:
            first_fp = results_df.iloc[0]["fp_rate"]
            last_fp  = results_df.iloc[-1]["fp_rate"]
            change   = last_fp - first_fp
            print(f"\nFirst window FP rate: {first_fp:.0%}")
            print(f"Last window FP rate:  {last_fp:.0%}")
            print(f"Change:               {change:+.0%}")
            if change < 0:
                print("→ FP rate DECREASED over time — feedback mechanism working")
            elif change > 0:
                print("→ FP rate INCREASED — threshold may need further calibration")
            else:
                print("→ FP rate STABLE")

    return results_df


# ── Remediation success rate ──────────────────────────────────────────────────
def track_remediation_success_rate(service_name: str) -> dict:
    """
    Tracks remediation success rate over time.
    Shows whether the system is getting better at fixing issues.
    """
    df = load_all_incidents()
    if df.empty:
        return {}

    service_df = df[df["service_name"] == service_name].copy()
    if service_df.empty:
        return {}

    total      = len(service_df)
    successes  = (service_df["outcome"] == "success").sum()
    partials   = (service_df["outcome"] == "partial").sum()
    failures   = (service_df["outcome"] == "failure").sum()

    auto_heal  = (service_df["routing_decision"] == "auto_heal").sum()
    human_rev  = (service_df["routing_decision"] == "human_review").sum()

    avg_mttd   = service_df["time_to_recover"].mean()

    result = {
        "service":              service_name,
        "total_incidents":      total,
        "success_count":        int(successes),
        "partial_count":        int(partials),
        "failure_count":        int(failures),
        "success_rate":         round(successes / total, 4) if total > 0 else 0,
        "auto_heal_count":      int(auto_heal),
        "human_review_count":   int(human_rev),
        "avg_time_to_recover":  round(avg_mttd, 2) if not pd.isna(avg_mttd) else 0,
    }

    print(f"\nRemediation Success Rate — {service_name}")
    print("-" * 45)
    print(f"Total incidents:        {total}")
    print(f"Successful:             {successes} ({result['success_rate']:.0%})")
    print(f"Partial:                {partials}")
    print(f"Failed:                 {failures}")
    print(f"Auto-healed:            {auto_heal}")
    print(f"Human reviewed:         {human_rev}")
    print(f"Avg time to recover:    {result['avg_time_to_recover']:.1f} mins")

    return result


# ── Full report ───────────────────────────────────────────────────────────────
def run_full_report(service_name: str = "payment-service"):
    """
    Runs the complete false positive tracking report.
    Covers RQ1 (FPR over time) and RQ3 (remediation success).
    """
    print("\n" + "="*60)
    print(f"FALSE POSITIVE TRACKER — {service_name}")
    print("="*60)

    df = load_all_incidents()
    if df.empty:
        print("\nNo incident data found.")
        print("Run the pipeline and generate incidents first.")
        return

    # FP rate over time
    fp_df = track_false_positive_rate(service_name)

    # Remediation success rate
    success = track_remediation_success_rate(service_name)

    # Save results
    os.makedirs(RESULTS_DIR, exist_ok=True)

    if not fp_df.empty:
        fp_path = f"{RESULTS_DIR}/false_positive_history.csv"
        fp_df.to_csv(fp_path, index=False)
        print(f"\nFP history saved to {fp_path}")

    if success:
        import json
        success_path = f"{RESULTS_DIR}/remediation_success.json"
        with open(success_path, "w") as f:
            json.dump(success, f, indent=2)
        print(f"Remediation results saved to {success_path}")

    print("="*60 + "\n")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_full_report("payment-service")