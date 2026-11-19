import asyncio
import json
import os
import random
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from metrics import (
    REQUEST_COUNT,
    REQUEST_LATENCY,
    ORDER_SUCCESS_COUNT,
    ORDER_FAILURE_COUNT,
    CPU_SPIKE_ACTIVE,
    CPU_USAGE_GAUGE,
    LATENCY_SPIKE_ACTIVE,
    ORDERS_IN_FLIGHT,
    ORDER_PROCESSING_TIME,
)

# ── Config ────────────────────────────────────────────────────────────────────
SERVICE_NAME    = os.getenv("SERVICE_NAME", "order-service")
SERVICE_PORT    = int(os.getenv("SERVICE_A_PORT", "8001"))
RUSH_INTERVAL   = int(os.getenv("RUSH_CHECK_INTERVAL_SECONDS", "60"))
RUSH_PROB       = float(os.getenv("RUSH_PROBABILITY", "0.20"))
RUSH_CPU_MIN    = float(os.getenv("RUSH_CPU_MIN", "75.0"))
RUSH_CPU_MAX    = float(os.getenv("RUSH_CPU_MAX", "95.0"))
NORMAL_CPU_MIN  = float(os.getenv("NORMAL_CPU_MIN", "10.0"))
NORMAL_CPU_MAX  = float(os.getenv("NORMAL_CPU_MAX", "35.0"))


def log(level: str, event: str, **fields):
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE_NAME,
        "level": level,
        "event": event,
        **fields,
    }

    print(
        json.dumps(record),
        flush=True
    )


# =============================================================================
# ORDER RUSH STATE
# =============================================================================

class RushState:
    active: bool = False
    rush_id: str | None = None
    end_time: float = 0.0
    cpu_load: float = 0.0

    def current_cpu(self) -> float:

        if self.active:

            return min(
                100.0,
                self.cpu_load + random.uniform(-5.0, 5.0)
            )

        return max(
            0.0,
            random.uniform(
                NORMAL_CPU_MIN,
                NORMAL_CPU_MAX
            )
        )


rush = RushState()


# =============================================================================
# PERSISTENT HARD FAILURE
# =============================================================================
#
# This failure does NOT expire automatically.
#
# It remains active until the container/process is restarted.
#
# This is specifically for demonstrating:
#
#       anomaly
#          ↓
#       Tier 1
#          ↓
#       still anomalous
#          ↓
#       Tier 2
#          ↓
#       container restart
#          ↓
#       hard_failure state disappears
#
# No existing rush behavior is changed.
# =============================================================================

class HardFailureState:

    active: bool = False

    failure_id: str | None = None

    def current_cpu(self) -> float:

        if self.active:

            # Keep CPU consistently anomalous.
            return random.uniform(
                90.0,
                98.0
            )

        return random.uniform(
            NORMAL_CPU_MIN,
            NORMAL_CPU_MAX
        )


hard_failure = HardFailureState()


# =============================================================================
# BACKGROUND HARD-FAILURE METRIC UPDATER
# =============================================================================
#
# Prometheus needs fresh values even when nobody calls /orders.
#
# Therefore, while hard_failure is active, this task continuously publishes
# anomalous CPU/latency state.
#
# When the container is restarted, this Python process starts again and:
#
#     hard_failure.active = False
#
# automatically.
# =============================================================================

async def hard_failure_metric_updater():

    while True:

        if hard_failure.active:

            CPU_SPIKE_ACTIVE.set(1)
            LATENCY_SPIKE_ACTIVE.set(1)

            CPU_USAGE_GAUGE.set(
                hard_failure.current_cpu()
            )

        elif rush.active:

            CPU_SPIKE_ACTIVE.set(1)
            LATENCY_SPIKE_ACTIVE.set(1)

            CPU_USAGE_GAUGE.set(
                rush.current_cpu()
            )

        else:

            CPU_SPIKE_ACTIVE.set(0)
            LATENCY_SPIKE_ACTIVE.set(0)

            CPU_USAGE_GAUGE.set(
                random.uniform(
                    NORMAL_CPU_MIN,
                    NORMAL_CPU_MAX
                )
            )

        await asyncio.sleep(5)


# =============================================================================
# RUSH FUNCTIONS
# =============================================================================

def maybe_expire_rush():

    if (
        rush.active
        and time.time() > rush.end_time
    ):

        log(
            "info",
            "order_rush_ended",
            rush_id=rush.rush_id
        )

        rush.active = False
        rush.rush_id = None
        rush.cpu_load = 0.0

        # Do not turn metrics off if hard failure is active.
        if not hard_failure.active:

            CPU_SPIKE_ACTIVE.set(0)
            LATENCY_SPIKE_ACTIVE.set(0)


def start_rush(
    duration: int,
    source: str = "scheduled"
):

    rush.active = True

    rush.rush_id = (
        f"rush_{uuid.uuid4().hex[:8]}"
    )

    rush.end_time = (
        time.time()
        + duration
    )

    rush.cpu_load = random.uniform(
        RUSH_CPU_MIN,
        RUSH_CPU_MAX
    )

    CPU_SPIKE_ACTIVE.set(1)
    LATENCY_SPIKE_ACTIVE.set(1)

    log(
        "warning",
        "order_rush_started",
        rush_id=rush.rush_id,
        duration_seconds=duration,
        simulated_cpu=rush.cpu_load,
        source=source
    )


async def rush_scheduler():

    log(
        "info",
        "rush_scheduler_started",
        interval_seconds=RUSH_INTERVAL
    )

    while True:

        await asyncio.sleep(
            RUSH_INTERVAL
        )

        maybe_expire_rush()

        # Do not start a normal rush while the persistent failure
        # demonstration is active.
        if (
            not rush.active
            and not hard_failure.active
            and random.random() < RUSH_PROB
        ):

            duration = random.randint(
                30,
                120
            )

            start_rush(
                duration,
                source="scheduled"
            )


# =============================================================================
# ORDER PROCESSING
# =============================================================================

def get_processing_latency() -> float:

    # Persistent failure remains slow.
    if hard_failure.active:

        return random.uniform(
            5.0,
            8.0
        )

    # Existing rush behavior.
    if rush.active:

        return random.uniform(
            1.0,
            8.0
        )

    # Existing normal behavior.
    return random.uniform(
        0.05,
        0.3
    )


def should_fail_under_load() -> tuple[bool, str]:

    maybe_expire_rush()

    # Persistent failure produces failures.
    if hard_failure.active:

        return (
            True,
            "persistent_service_failure"
        )

    # Existing rush behavior.
    if (
        rush.active
        and random.random() < 0.10
    ):

        return (
            True,
            "processing_timeout"
        )

    # Existing rare failure behavior.
    if random.random() < 0.01:

        return (
            True,
            "inventory_lock_timeout"
        )

    return (
        False,
        ""
    )


# =============================================================================
# APPLICATION LIFESPAN
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    log(
        "info",
        "service_started",
        port=SERVICE_PORT,
        version="1.0.0"
    )

    rush_task = asyncio.create_task(
        rush_scheduler()
    )

    metric_task = asyncio.create_task(
        hard_failure_metric_updater()
    )

    try:

        yield

    finally:

        rush_task.cancel()
        metric_task.cancel()

        log(
            "info",
            "service_stopped"
        )


app = FastAPI(
    title="Order Service",
    version="1.0.0",
    lifespan=lifespan
)


# =============================================================================
# REQUEST OBSERVATION MIDDLEWARE
# =============================================================================

@app.middleware("http")
async def observe_request(
    request: Request,
    call_next
):

    request_id = str(
        uuid.uuid4()
    )

    request.state.request_id = (
        request_id
    )

    start = time.perf_counter()

    response = await call_next(
        request
    )

    duration = (
        time.perf_counter()
        - start
    )

    endpoint = request.url.path

    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=endpoint,
        status_code=str(
            response.status_code
        ),
    ).inc()

    REQUEST_LATENCY.labels(
        endpoint=endpoint
    ).observe(
        duration
    )

    # Keep the CPU metric fresh on requests too.
    if hard_failure.active:

        current_cpu = (
            hard_failure.current_cpu()
        )

    else:

        current_cpu = (
            rush.current_cpu()
        )

    CPU_USAGE_GAUGE.set(
        current_cpu
    )

    log(
        "info",
        "http_request",
        request_id=request_id,
        method=request.method,
        path=endpoint,
        status_code=response.status_code,
        duration_ms=round(
            duration * 1000,
            2
        ),
        cpu_usage=current_cpu,
        rush_active=rush.active,
        hard_failure_active=hard_failure.active
    )

    response.headers[
        "X-Request-ID"
    ] = request_id

    return response


# =============================================================================
# PROMETHEUS
# =============================================================================

@app.get("/metrics")
async def prometheus_metrics():

    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


# =============================================================================
# HEALTH
# =============================================================================

@app.get("/health")
async def health():

    if hard_failure.active:

        current_cpu = (
            hard_failure.current_cpu()
        )

    else:

        current_cpu = (
            rush.current_cpu()
        )

    return {

        "status":
            "healthy",

        "service":
            SERVICE_NAME,

        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "rush_active":
            rush.active,

        "rush_id":
            rush.rush_id,

        # IMPORTANT:
        # executor.py uses this field.
        #
        # Hard failure remains TRUE until the
        # container/process is restarted.
        "spike_active":
            (
                rush.active
                or hard_failure.active
            ),

        "hard_failure_active":
            hard_failure.active,

        "hard_failure_id":
            hard_failure.failure_id,

        "cpu_usage_pct":
            current_cpu,
    }


# =============================================================================
# CREATE ORDER
# =============================================================================

@app.post("/orders")
async def create_order(
    request: Request
):

    body = await request.json()

    request_id = getattr(
        request.state,
        "request_id",
        str(uuid.uuid4())
    )

    order_id = (
        f"ord_{uuid.uuid4().hex[:12]}"
    )

    customer_id = body.get(
        "customer_id",
        "unknown"
    )

    order_type = body.get(
        "order_type",
        "standard"
    )

    items = body.get(
        "items",
        []
    )

    log(
        "info",
        "order_initiated",
        request_id=request_id,
        order_id=order_id,
        customer_id=customer_id,
        order_type=order_type,
        item_count=len(items),
        rush_active=rush.active,
        hard_failure_active=hard_failure.active
    )

    ORDERS_IN_FLIGHT.inc()

    proc_start = time.perf_counter()

    latency = (
        get_processing_latency()
    )

    await asyncio.sleep(
        latency
    )

    ORDERS_IN_FLIGHT.dec()

    proc_duration = (
        time.perf_counter()
        - proc_start
    )

    ORDER_PROCESSING_TIME.labels(
        order_type=order_type
    ).observe(
        proc_duration
    )

    failed, reason = (
        should_fail_under_load()
    )

    if failed:

        ORDER_FAILURE_COUNT.labels(
            failure_reason=reason
        ).inc()

        log(
            "error",
            "order_failed",
            request_id=request_id,
            order_id=order_id,
            failure_reason=reason,
            processing_time_ms=round(
                proc_duration * 1000,
                2
            ),
            rush_active=rush.active,
            hard_failure_active=hard_failure.active
        )

        raise HTTPException(
            status_code=503,
            detail={
                "error":
                    "order_processing_failed",

                "reason":
                    reason,

                "order_id":
                    order_id,
            }
        )

    ORDER_SUCCESS_COUNT.labels(
        order_type=order_type
    ).inc()

    log(
        "info",
        "order_success",
        request_id=request_id,
        order_id=order_id,
        customer_id=customer_id,
        processing_time_ms=round(
            proc_duration * 1000,
            2
        ),
        rush_active=rush.active,
        hard_failure_active=hard_failure.active
    )

    return {

        "order_id":
            order_id,

        "status":
            "confirmed",

        "customer_id":
            customer_id,

        "order_type":
            order_type,

        "item_count":
            len(items),

        "processing_ms":
            round(
                proc_duration * 1000,
                2
            ),

        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }


# =============================================================================
# GET ORDER
# =============================================================================

@app.get("/orders/{order_id}")
async def get_order(
    order_id: str
):

    log(
        "info",
        "order_status_query",
        order_id=order_id
    )

    return {

        "order_id":
            order_id,

        "status":
            random.choice(
                [
                    "confirmed",
                    "processing",
                    "shipped"
                ]
            ),

        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }


# =============================================================================
# EXISTING RUSH INJECTION
# =============================================================================

@app.post("/admin/trigger-rush")
async def trigger_rush(
    duration_seconds: int = 60
):

    if rush.active:

        return {
            "message":
                "Rush already active",

            "rush_id":
                rush.rush_id
        }

    start_rush(
        duration_seconds,
        source="manual"
    )

    return {

        "message":
            f"Rush triggered for "
            f"{duration_seconds}s",

        "rush_id":
            rush.rush_id
    }


# =============================================================================
# EXISTING STOP-RUSH ENDPOINT
# =============================================================================

@app.post("/admin/stop-rush")
async def stop_rush():

    if not rush.active:

        return {
            "message":
                "No active rush"
        }

    log(
        "warning",
        "order_rush_stopped_manually",
        rush_id=rush.rush_id
    )

    rush.active = False
    rush.rush_id = None
    rush.cpu_load = 0.0

    # Only clear anomaly metrics if the persistent
    # failure is not active.
    if not hard_failure.active:

        CPU_SPIKE_ACTIVE.set(0)
        LATENCY_SPIKE_ACTIVE.set(0)

    return {
        "message":
            "Rush stopped"
    }


# =============================================================================
# NEW: PERSISTENT HARD-FAILURE INJECTION
# =============================================================================
#
# IMPORTANT:
#
# This does NOT have a stop endpoint.
#
# It remains active until the container is restarted.
#
# Therefore:
#
#     POST /admin/trigger-hard-failure
#
# should eventually produce:
#
#     Tier 1 attempted
#           ↓
#     Tier 1 does not recover
#           ↓
#     Tier 2 restart
#           ↓
#     process recreated
#           ↓
#     hard_failure.active = False
#           ↓
#     recovery
#
# =============================================================================

@app.post("/admin/trigger-hard-failure")
async def trigger_hard_failure():

    if hard_failure.active:

        return {

            "message":
                "Hard failure already active",

            "failure_id":
                hard_failure.failure_id
        }

    hard_failure.active = True

    hard_failure.failure_id = (
        f"failure_{uuid.uuid4().hex[:8]}"
    )

    CPU_SPIKE_ACTIVE.set(1)
    LATENCY_SPIKE_ACTIVE.set(1)

    log(
        "error",
        "persistent_failure_injected",
        failure_id=
            hard_failure.failure_id
    )

    return {

        "message":
            "Persistent failure injected",

        "failure_id":
            hard_failure.failure_id,

        "note":
            "Failure remains active until "
            "the service container is restarted."
    }


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=SERVICE_PORT,
        log_level="warning"
    )