from prometheus_client import Counter, Histogram, Gauge

# ── Request-level metrics ─────────────────────────────────────────────────────
REQUEST_COUNT = Counter(
    "inventory_service_requests_total",
    "Total HTTP requests received",
    ["method", "endpoint", "status_code"],
)

REQUEST_LATENCY = Histogram(
    "inventory_service_request_latency_seconds",
    "HTTP request latency in seconds",
    ["endpoint"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
)

# ── Inventory-specific metrics ────────────────────────────────────────────────
STOCK_CHECK_COUNT = Counter(
    "inventory_stock_checks_total",
    "Total stock check operations",
    ["result"],
)

BATCH_JOB_COUNT = Counter(
    "inventory_batch_jobs_total",
    "Total batch stock calculation jobs",
    ["status"],
)

# ── CPU metrics (primary anomaly signal for inventory-service) ────────────────
# Key insight: inventory-service NORMALLY has high CPU (50-80%)
# because it runs constant batch jobs.
# Anomaly = CPU reaches 90-99% (overload, not normal batch work)
# This is why a global model fails — it would flag normal 70% CPU as anomalous

CPU_USAGE_GAUGE = Gauge(
    "inventory_cpu_usage_percent",
    "Simulated CPU usage percentage — normally high due to batch processing",
)

CPU_OVERLOAD_ACTIVE = Gauge(
    "inventory_cpu_overload_active",
    "1 when CPU is in overload state 0 otherwise",
)

BATCH_JOB_ACTIVE = Gauge(
    "inventory_batch_job_active",
    "1 when a batch stock calculation is running 0 otherwise",
)

REQUESTS_IN_FLIGHT = Gauge(
    "inventory_requests_in_flight",
    "Number of requests currently being processed",
)

BATCH_DURATION = Histogram(
    "inventory_batch_duration_seconds",
    "Time taken to complete a batch stock calculation",
    buckets=[1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0],
)