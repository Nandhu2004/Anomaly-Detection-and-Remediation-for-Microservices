import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging
import os

# =============================================================================
# CONFIGURATION
# =============================================================================

PROMETHEUS_URL = os.getenv(
    "PROMETHEUS_URL",
    "http://localhost:9090"
)

STEP_SECONDS = 15

LOOKBACK_MINS = 20

NEAREST_TOLERANCE_SECONDS = 8

SERVICE_CONFIG = {

    "order-service": {

        "features": [
            "cpu_usage",
            "latency_avg"
        ],

        "indicator": "spike_active",

        "queries": {

            "cpu_usage":
                "order_cpu_usage_percent",

            "latency_avg":
                "rate(order_service_request_latency_seconds_sum[1m]) "
                "/ "
                "rate(order_service_request_latency_seconds_count[1m])",

            "cpu_spike_active":
                "order_cpu_spike_active",

            "latency_spike_active":
                "order_latency_spike_active",
        }
    },


    # =========================================================================
    # PAYMENT SERVICE
    # =========================================================================

    "payment-service": {

        "features": [
            "error_rate",
            "latency_avg"
        ],

        "indicator": "burst_active",

        "queries": {

            # IMPORTANT:
            # This returns error rate as a PERCENTAGE from 0 to 100.
            #
            # Example:
            #   2% errors  -> 2.0
            #   10% errors -> 10.0
            #   85% errors -> 85.0
            #
            "error_rate":
                "payment_error_rate_percent",

            "latency_avg":
                "rate(payment_service_request_latency_seconds_sum[1m]) "
                "/ "
                "rate(payment_service_request_latency_seconds_count[1m])",

            "burst_active":
                "payment_error_burst_active",
        }
    },


    # =========================================================================
    # INVENTORY SERVICE
    # =========================================================================

    "inventory-service": {

        "features": [
            "cpu_usage",
            "latency_avg"
        ],

        "indicator": "overload_active",

        "queries": {

            "cpu_usage":
                "inventory_cpu_usage_percent",

            "latency_avg":
                "rate(inventory_service_request_latency_seconds_sum[1m]) "
                "/ "
                "rate(inventory_service_request_latency_seconds_count[1m])",

            "overload_active":
                "inventory_cpu_overload_active",
        }
    }
}


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)


# =============================================================================
# PROMETHEUS RANGE QUERY
# =============================================================================

def query_prometheus_range(
    query: str,
    start: datetime,
    end: datetime,
    step: int = STEP_SECONDS,
):

    url = f"{PROMETHEUS_URL}/api/v1/query_range"

    params = {
        "query": query,
        "start": start.timestamp(),
        "end": end.timestamp(),
        "step": step,
    }

    try:

        response = requests.get(
            url,
            params=params,
            timeout=10,
        )

        response.raise_for_status()

        data = response.json()

        if data.get("status") != "success":

            log.error(
                "Prometheus query failed: %s",
                data
            )

            return []

        results = (
            data.get("data", {})
            .get("result", [])
        )

        if not results:
            return []

        # Usually there is one result for our gauges/aggregated queries.
        # If multiple series are returned, merge their values.
        all_values = []

        for result in results:

            all_values.extend(
                result.get("values", [])
            )

        return all_values

    except Exception as e:

        log.error(
            "Prometheus request failed: %s",
            e
        )

        return []


# =============================================================================
# CONVERT PROMETHEUS VALUES
# =============================================================================

def values_to_dict(values):

    result = {}

    for timestamp, value in values:

        try:

            timestamp = float(timestamp)

            if value in ("NaN", "nan", "+Inf", "-Inf"):

                value = 0.0

            else:

                value = float(value)

            result[timestamp] = value

        except (
            ValueError,
            TypeError
        ):

            continue

    return result


# =============================================================================
# NEAREST TIMESTAMP HELPER
# =============================================================================

def get_nearest_value(
    values_dict,
    timestamp,
    tolerance_seconds=NEAREST_TOLERANCE_SECONDS,
):

    if not values_dict:
        return 0.0

    nearest_timestamp = min(
        values_dict.keys(),
        key=lambda ts: abs(ts - timestamp)
    )

    difference = abs(
        nearest_timestamp - timestamp
    )

    if difference <= tolerance_seconds:

        return values_dict[
            nearest_timestamp
        ]

    return 0.0


# =============================================================================
# COLLECT SERVICE METRICS
# =============================================================================

def collect_service_metrics(service_name):

    if service_name not in SERVICE_CONFIG:

        log.error(
            "Unknown service: %s",
            service_name
        )

        return pd.DataFrame()

    config = SERVICE_CONFIG[
        service_name
    ]

    log.info(
        "Collecting metrics for %s "
        "(last %s mins)",
        service_name,
        LOOKBACK_MINS
    )

    end = datetime.now(
        timezone.utc
    )

    start = (
        end
        - timedelta(
            minutes=LOOKBACK_MINS
        )
    )

    collected = {}

    # -------------------------------------------------------------------------
    # Query metrics
    # -------------------------------------------------------------------------

    for metric_name, query in config[
        "queries"
    ].items():

        log.info(
            "Querying %s...",
            metric_name
        )

        values = query_prometheus_range(
            query,
            start,
            end,
            STEP_SECONDS
        )

        metric_values = values_to_dict(
            values
        )

        collected[
            metric_name
        ] = metric_values

        log.info(
            "  %s: %s data points",
            metric_name,
            len(metric_values)
        )

    # -------------------------------------------------------------------------
    # Find timestamps
    # -------------------------------------------------------------------------

    feature_names = config[
        "features"
    ]

    primary_metric = feature_names[0]

    if not collected.get(
        primary_metric
    ):

        log.warning(
            "No %s data found for %s",
            primary_metric,
            service_name
        )

        return pd.DataFrame()

    timestamps = sorted(
        collected[
            primary_metric
        ].keys()
    )

    rows = []

    # -------------------------------------------------------------------------
    # Build common dataframe
    # -------------------------------------------------------------------------

    for timestamp in timestamps:

        row = {

            "timestamp":
                datetime.fromtimestamp(
                    timestamp,
                    tz=timezone.utc
                )
        }

        # ---------------------------------------------------------------------
        # Add primary features
        # ---------------------------------------------------------------------

        for feature_name in feature_names:

            row[
                feature_name
            ] = get_nearest_value(
                collected.get(
                    feature_name,
                    {}
                ),
                timestamp
            )

        # ---------------------------------------------------------------------
        # Add anomaly indicator
        # ---------------------------------------------------------------------

        indicator_name = config[
            "indicator"
        ]

        # ---------------------------------------------------------------------
        # ORDER SERVICE
        #
        # Consider the service anomalous if either CPU spike OR latency spike
        # indicator is active.
        # ---------------------------------------------------------------------

        if service_name == "order-service":

            cpu_spike = get_nearest_value(
                collected.get(
                    "cpu_spike_active",
                    {}
                ),
                timestamp
            )

            latency_spike = get_nearest_value(
                collected.get(
                    "latency_spike_active",
                    {}
                ),
                timestamp
            )

            row[
                "spike_active"
            ] = float(
                max(
                    cpu_spike,
                    latency_spike
                )
            )

        # ---------------------------------------------------------------------
        # PAYMENT / INVENTORY
        # ---------------------------------------------------------------------

        else:

            row[
                indicator_name
            ] = get_nearest_value(
                collected.get(
                    indicator_name,
                    {}
                ),
                timestamp
            )

        rows.append(row)

    # -------------------------------------------------------------------------
    # Create DataFrame
    # -------------------------------------------------------------------------

    df = pd.DataFrame(
        rows
    )

    log.info(
        "Collected %s time steps for %s",
        len(df),
        service_name
    )

    # -------------------------------------------------------------------------
    # Ensure numeric feature columns
    # -------------------------------------------------------------------------

    for feature_name in feature_names:

        if feature_name in df.columns:

            df[
                feature_name
            ] = pd.to_numeric(
                df[feature_name],
                errors="coerce"
            )

    # Replace invalid numeric values.
    df = df.replace(
        [float("inf"), float("-inf")],
        0.0
    )

    df = df.fillna(0.0)

    # -------------------------------------------------------------------------
    # Payment error-rate sanity check
    # -------------------------------------------------------------------------
    #
    # The PromQL query above already returns percentage values.
    # This check protects the detector from impossible values.
    #
    # A valid percentage must be between 0 and 100.
    #
    # We do NOT divide by 100 here.
    #
    # -------------------------------------------------------------------------

    if (
        service_name == "payment-service"
        and "error_rate" in df.columns
    ):

        invalid_mask = (
            (df["error_rate"] < 0)
            | (df["error_rate"] > 100)
        )

        invalid_count = int(
            invalid_mask.sum()
        )

        if invalid_count > 0:

            log.warning(
                "Payment error_rate contains %s "
                "values outside 0-100%%. "
                "Clipping them to valid percentage range.",
                invalid_count
            )

            df["error_rate"] = df[
                "error_rate"
            ].clip(
                lower=0.0,
                upper=100.0
            )

    # -------------------------------------------------------------------------
    # Print ranges
    # -------------------------------------------------------------------------

    if not df.empty:

        for feature_name in feature_names:

            log.info(
                "  %s range: %.4f - %.4f",
                feature_name,
                df[feature_name].min(),
                df[feature_name].max()
            )

        indicator_name = (
            "spike_active"
            if service_name == "order-service"
            else config["indicator"]
        )

        if indicator_name in df.columns:

            log.info(
                "  %s range: %.0f - %.0f",
                indicator_name,
                df[indicator_name].min(),
                df[indicator_name].max()
            )

    return df


# =============================================================================
# BACKWARD COMPATIBILITY
# =============================================================================

def collect_payment_service_metrics():

    return collect_service_metrics(
        "payment-service"
    )


# =============================================================================
# LATEST METRICS
# =============================================================================

def get_latest_metrics(service_name):

    df = collect_service_metrics(
        service_name
    )

    if df is None or df.empty:

        return None

    latest = df.iloc[-1]

    result = {

        column: float(
            latest[column]
        )

        for column in df.columns

        if column != "timestamp"
    }

    log.info(
        "Latest metrics for %s: %s",
        service_name,
        result
    )

    return result


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":

    print("\n" + "=" * 70)
    print("GENERIC METRIC COLLECTOR TEST")
    print("=" * 70)

    for service_name in SERVICE_CONFIG:

        print(
            f"\n{'=' * 70}"
        )

        print(
            f"TESTING: {service_name}"
        )

        print(
            f"{'=' * 70}"
        )

        df = collect_service_metrics(
            service_name
        )

        print(
            f"\nCollected rows: {len(df)}"
        )

        if not df.empty:

            print(
                "\nLast 5 rows:"
            )

            print(
                df.tail()
            )

            print(
                "\nSummary:"
            )

            print(
                df.describe()
            )

    print(
        "\n" + "=" * 70
    )

    print(
        "Metric collector test complete"
    )

    print(
        "=" * 70 + "\n"
    )