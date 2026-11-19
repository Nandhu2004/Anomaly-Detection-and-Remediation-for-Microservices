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
    STOCK_CHECK_COUNT,
    BATCH_JOB_COUNT,
    CPU_USAGE_GAUGE,
    CPU_OVERLOAD_ACTIVE,
    BATCH_JOB_ACTIVE,
    REQUESTS_IN_FLIGHT,
    BATCH_DURATION,
)

# ── Config ────────────────────────────────────────────────────────────────────
SERVICE_NAME        = os.getenv("SERVICE_NAME", "inventory-service")
SERVICE_PORT        = int(os.getenv("SERVICE_C_PORT", "8003"))
BATCH_INTERVAL      = int(os.getenv("BATCH_INTERVAL_SECONDS", "45"))
OVERLOAD_PROB       = float(os.getenv("OVERLOAD_PROBABILITY", "0.15"))
NORMAL_CPU_MIN      = float(os.getenv("NORMAL_CPU_MIN", "50.0"))
NORMAL_CPU_MAX      = float(os.getenv("NORMAL_CPU_MAX", "80.0"))
OVERLOAD_CPU_MIN    = float(os.getenv("OVERLOAD_CPU_MIN", "90.0"))
OVERLOAD_CPU_MAX    = float(os.getenv("OVERLOAD_CPU_MAX", "99.0"))


def log(level: str, event: str, **fields):
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service":   SERVICE_NAME,
        "level":     level,
        "event":     event,
        **fields,
    }
    print(json.dumps(record), flush=True)


# ── Overload State ────────────────────────────────────────────────────────────
# inventory-service is the compute-intensive batch workload archetype.
# CRITICAL DISTINCTION from other services:
#   - Normal CPU = 50-80% (always high — constant batch processing)
#   - Anomalous CPU = 90-99% (overload — batch job stuck or too large)
#
# This is the clearest demonstration of why global models fail:
# A global model trained across all 3 services learns "normal CPU = ~35%"
# (average of order=22%, payment=30%, inventory=65%)
# So inventory's NORMAL 65% CPU looks anomalous to the global model.
# Per-service profiling learns inventory's own baseline — 65% is normal for it.

class OverloadState:
    active:      bool       = False
    overload_id: str | None = None
    end_time:    float      = 0.0
    cpu_level:   float      = 0.0

    def current_cpu(self) -> float:
        """Returns current simulated CPU including normal batch load."""
        if self.active:
            # Overload: very high CPU with jitter
            return min(100.0, self.cpu_level + random.uniform(-2.0, 2.0))
        else:
            # Normal: consistently HIGH CPU due to constant batch jobs
            # This is intentionally high — 50-80% is NORMAL for this service
            return random.uniform(NORMAL_CPU_MIN, NORMAL_CPU_MAX)


overload = OverloadState()


def maybe_expire_overload():
    if overload.active and time.time() > overload.end_time:
        log("info", "cpu_overload_ended",
            overload_id=overload.overload_id)
        overload.active      = False
        overload.overload_id = None
        overload.cpu_level   = 0.0
        CPU_OVERLOAD_ACTIVE.set(0)


def start_overload(duration: int, source: str = "scheduled"):
    overload.active      = True
    overload.overload_id = f"overload_{uuid.uuid4().hex[:8]}"
    overload.end_time    = time.time() + duration
    overload.cpu_level   = random.uniform(OVERLOAD_CPU_MIN, OVERLOAD_CPU_MAX)
    CPU_OVERLOAD_ACTIVE.set(1)
    log("warning", "cpu_overload_started",
        overload_id=overload.overload_id,
        duration_seconds=duration,
        simulated_cpu=overload.cpu_level,
        source=source)


async def batch_scheduler():
    """
    Periodically triggers batch stock calculations (normal behavior)
    and occasionally triggers CPU overload (anomalous behavior).
    """
    log("info", "batch_scheduler_started", interval_seconds=BATCH_INTERVAL)
    while True:
        await asyncio.sleep(BATCH_INTERVAL)
        maybe_expire_overload()

        # Normal batch job — always runs (this is why CPU is normally high)
        if not overload.active:
            batch_duration = random.uniform(3.0, 15.0)
            BATCH_JOB_ACTIVE.set(1)
            BATCH_JOB_COUNT.labels(status="started").inc()
            log("info", "batch_job_started",
                estimated_duration=batch_duration)
            await asyncio.sleep(min(batch_duration, 5.0))
            BATCH_JOB_ACTIVE.set(0)
            BATCH_JOB_COUNT.labels(status="completed").inc()
            BATCH_DURATION.observe(batch_duration)

        # Random overload trigger
        if not overload.active and random.random() < OVERLOAD_PROB:
            duration = random.randint(30, 90)
            start_overload(duration, source="scheduled")


def get_request_latency() -> float:
    """
    Returns processing latency.
    During overload, latency increases significantly due to CPU contention.
    Even during normal operation, inventory has higher latency than other
    services because of constant batch processing overhead.
    """
    if overload.active:
        return random.uniform(3.0, 15.0)   # severe latency during overload
    else:
        return random.uniform(0.2, 1.5)    # naturally higher latency (batch overhead)


def should_fail() -> tuple[bool, str]:
    """Batch failures during overload."""
    maybe_expire_overload()
    if overload.active and random.random() < 0.15:
        return True, "batch_processing_timeout"
    if random.random() < 0.05:
        return True, "stock_lock_conflict"
    return False, ""


@asynccontextmanager
async def lifespan(app: FastAPI):
    log("info", "service_started", port=SERVICE_PORT, version="1.0.0")
    task = asyncio.create_task(batch_scheduler())
    yield
    task.cancel()
    log("info", "service_stopped")


app = FastAPI(title="Inventory Service", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def observe_request(request: Request, call_next):
    request_id                = str(uuid.uuid4())
    request.state.request_id = request_id
    start                     = time.perf_counter()

    response = await call_next(request)

    duration    = time.perf_counter() - start
    endpoint    = request.url.path
    current_cpu = overload.current_cpu()

    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=endpoint,
        status_code=str(response.status_code),
    ).inc()
    REQUEST_LATENCY.labels(endpoint=endpoint).observe(duration)
    CPU_USAGE_GAUGE.set(current_cpu)

    log("info", "http_request",
        request_id=request_id,
        method=request.method,
        path=endpoint,
        status_code=response.status_code,
        duration_ms=round(duration * 1000, 2),
        cpu_usage=round(current_cpu, 1),
        overload_active=overload.active)

    response.headers["X-Request-ID"] = request_id
    return response


@app.get("/metrics")
async def prometheus_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {
        "status":           "healthy",
        "service":          SERVICE_NAME,
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "overload_active":  overload.active,
        "overload_id":      overload.overload_id,
        "cpu_usage_pct":    round(overload.current_cpu(), 1),
        "batch_job_active": BATCH_JOB_ACTIVE._value.get() == 1.0,
    }


@app.get("/inventory/{item_id}")
async def check_stock(item_id: str, request: Request):
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))

    log("info", "stock_check_initiated",
        request_id=request_id,
        item_id=item_id,
        overload_active=overload.active)

    REQUESTS_IN_FLIGHT.inc()

    latency = get_request_latency()
    await asyncio.sleep(latency)

    REQUESTS_IN_FLIGHT.dec()

    failed, reason = should_fail()
    if failed:
        STOCK_CHECK_COUNT.labels(result="failed").inc()
        log("error", "stock_check_failed",
            request_id=request_id,
            item_id=item_id,
            failure_reason=reason,
            overload_active=overload.active)
        raise HTTPException(status_code=503, detail={
            "error":   "stock_check_failed",
            "reason":  reason,
            "item_id": item_id,
        })

    stock_level = random.randint(0, 1000)
    STOCK_CHECK_COUNT.labels(result="success").inc()

    log("info", "stock_check_success",
        request_id=request_id,
        item_id=item_id,
        stock_level=stock_level)

    return {
        "item_id":     item_id,
        "stock_level": stock_level,
        "status":      "in_stock" if stock_level > 0 else "out_of_stock",
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }


@app.post("/inventory/batch-recalculate")
async def batch_recalculate(request: Request):
    """Manually triggers a batch stock recalculation."""
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))
    job_id     = f"batch_{uuid.uuid4().hex[:8]}"

    log("info", "batch_recalculate_started",
        request_id=request_id,
        job_id=job_id,
        overload_active=overload.active)

    BATCH_JOB_ACTIVE.set(1)
    BATCH_JOB_COUNT.labels(status="started").inc()

    duration = get_request_latency() * 3   # batch takes longer
    await asyncio.sleep(min(duration, 5.0))

    BATCH_JOB_ACTIVE.set(0)
    BATCH_JOB_COUNT.labels(status="completed").inc()
    BATCH_DURATION.observe(duration)

    return {
        "job_id":    job_id,
        "status":    "completed",
        "duration":  round(duration, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/admin/trigger-overload")
async def trigger_overload(duration_seconds: int = 60):
    """Manually triggers CPU overload — used by tests."""
    if overload.active:
        return {"message": "Overload already active", "overload_id": overload.overload_id}
    start_overload(duration_seconds, source="manual")
    return {
        "message":     f"Overload triggered for {duration_seconds}s",
        "overload_id": overload.overload_id,
    }


@app.post("/admin/stop-overload")
async def stop_overload():
    """Immediately ends an active overload."""
    if not overload.active:
        return {"message": "No active overload"}
    log("warning", "cpu_overload_stopped_manually", overload_id=overload.overload_id)
    overload.active      = False
    overload.overload_id = None
    overload.cpu_level   = 0.0
    CPU_OVERLOAD_ACTIVE.set(0)
    return {"message": "Overload stopped"}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=SERVICE_PORT, log_level="warning")