from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    Summary,
)


# =============================================================================
# HTTP REQUEST METRICS
# =============================================================================

REQUEST_COUNT = Counter(
    "payment_service_requests_total",
    "Total HTTP requests received",
    [
        "method",
        "endpoint",
        "status_code",
    ],
)


REQUEST_LATENCY = Histogram(
    "payment_service_request_latency_seconds",
    "HTTP request latency in seconds",
    ["endpoint"],
    buckets=[
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
    ],
)


# =============================================================================
# PAYMENT TRANSACTION METRICS
# =============================================================================

PAYMENT_SUCCESS_COUNT = Counter(
    "payment_transactions_success_total",
    "Successfully processed payments",
    [
        "payment_method",
        "currency",
    ],
)


PAYMENT_FAILURE_COUNT = Counter(
    "payment_transactions_failed_total",
    "Failed payment transactions",
    [
        "failure_reason",
        "payment_method",
    ],
)


PAYMENT_DECLINED_COUNT = Counter(
    "payment_transactions_declined_total",
    "Soft-declined payment transactions",
    ["decline_reason"],
)


# =============================================================================
# REFUND METRICS
# =============================================================================

REFUND_COUNT = Counter(
    "payment_refunds_total",
    "Refund transactions processed",
    ["status"],
)


# =============================================================================
# AIOPS BURST / ERROR METRICS
# =============================================================================

ERROR_BURST_ACTIVE = Gauge(
    "payment_error_burst_active",
    "1 when an error burst is currently active, 0 otherwise",
)


ERROR_RATE_GAUGE = Gauge(
    "payment_error_rate_percent",
    "Rolling error rate as a percentage of /payments requests",
)


# =============================================================================
# PAYMENT BUSINESS METRICS
# =============================================================================

PAYMENT_AMOUNT = Summary(
    "payment_transaction_amount_inr",
    "Distribution of payment amounts (normalised to INR)",
    ["payment_method"],
)


PAYMENTS_IN_FLIGHT = Gauge(
    "payment_transactions_in_flight",
    "Number of payment requests currently being processed",
)
