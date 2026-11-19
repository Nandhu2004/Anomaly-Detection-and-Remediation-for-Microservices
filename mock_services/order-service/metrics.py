from prometheus_client import Counter, Histogram, Gauge

REQUEST_COUNT = Counter(
    "order_service_requests_total",
    "Total HTTP requests received",
    ["method", "endpoint", "status_code"],
)

REQUEST_LATENCY = Histogram(
    "order_service_request_latency_seconds",
    "HTTP request latency in seconds",
    ["endpoint"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

ORDER_SUCCESS_COUNT = Counter(
    "order_transactions_success_total",
    "Successfully placed orders",
    ["order_type"],
)

ORDER_FAILURE_COUNT = Counter(
    "order_transactions_failed_total",
    "Failed order transactions",
    ["failure_reason"],
)

CPU_SPIKE_ACTIVE = Gauge(
    "order_cpu_spike_active",
    "1 when a CPU/latency spike is currently active, 0 otherwise",
)

CPU_USAGE_GAUGE = Gauge(
    "order_cpu_usage_percent",
    "Simulated CPU usage percentage",
)

LATENCY_SPIKE_ACTIVE = Gauge(
    "order_latency_spike_active",
    "1 when response times are spiking, 0 otherwise",
)

ORDERS_IN_FLIGHT = Gauge(
    "order_requests_in_flight",
    "Number of order requests currently being processed",
)

ORDER_PROCESSING_TIME = Histogram(
    "order_processing_duration_seconds",
    "Time taken to process an order end to end",
    ["order_type"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0],
)