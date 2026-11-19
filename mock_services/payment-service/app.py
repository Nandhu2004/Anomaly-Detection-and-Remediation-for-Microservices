import asyncio
import json
import os
import random
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from metrics import (
    REQUEST_COUNT,
    REQUEST_LATENCY,
    PAYMENT_SUCCESS_COUNT,
    PAYMENT_FAILURE_COUNT,
    PAYMENT_DECLINED_COUNT,
    REFUND_COUNT,
    ERROR_BURST_ACTIVE,
    ERROR_RATE_GAUGE,
    PAYMENT_AMOUNT,
    PAYMENTS_IN_FLIGHT,
)


# =============================================================================
# CONFIGURATION
# =============================================================================

SERVICE_NAME = os.getenv(
    "SERVICE_NAME",
    "payment-service"
)

SERVICE_PORT = int(
    os.getenv(
        "SERVICE_B_PORT",
        "8002"
    )
)

BURST_INTERVAL = int(
    os.getenv(
        "BURST_CHECK_INTERVAL_SECONDS",
        "60"
    )
)

BURST_PROB = float(
    os.getenv(
        "BURST_PROBABILITY",
        "0.15"
    )
)

BURST_ERR_RATE = float(
    os.getenv(
        "BURST_ERROR_RATE",
        "0.85"
    )
)

NORMAL_ERR_RATE = float(
    os.getenv(
        "NORMAL_ERROR_RATE",
        "0.02"
    )
)

ERROR_RATE_WINDOW_SECONDS = 60


# =============================================================================
# LOGGING
# =============================================================================

def log(level: str, event: str, **fields):

    record = {
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),

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
# PAYMENT ERROR RATE TRACKING
# =============================================================================

# Each entry:
#
#     (timestamp, is_error)
#
# Example:
#
#     (1725961000.0, False)
#     (1725961001.0, True)
#
# ONLY /payments requests are stored here.

request_history = deque()


def cleanup_request_history():

    cutoff = (
        time.time()
        - ERROR_RATE_WINDOW_SECONDS
    )

    while request_history:

        timestamp, _ = request_history[0]

        if timestamp >= cutoff:
            break

        request_history.popleft()


def record_payment_result(
    is_error: bool
):

    request_history.append(
        (
            time.time(),
            is_error
        )
    )

    cleanup_request_history()


def calculate_error_rate() -> float:

    cleanup_request_history()

    if not request_history:
        return 0.0

    total_requests = len(
        request_history
    )

    error_requests = sum(
        1
        for _, is_error in request_history
        if is_error
    )

    if total_requests == 0:
        return 0.0

    return round(
        (
            error_requests
            / total_requests
        ) * 100,
        2
    )


def update_error_rate_metric():

    error_rate = calculate_error_rate()

    ERROR_RATE_GAUGE.set(
        error_rate
    )

    return error_rate


# =============================================================================
# BURST STATE
# =============================================================================

class BurstState:

    active = False

    burst_id = None

    end_time = 0.0

    total_ok = 0

    total_fail = 0


burst = BurstState()


# =============================================================================
# BURST MANAGEMENT
# =============================================================================

def maybe_expire_burst():

    if (
        burst.active
        and time.time() > burst.end_time
    ):

        log(
            "warning",
            "error_burst_ended",
            burst_id=burst.burst_id,
            total_failures_during_burst=
                burst.total_fail,
        )

        burst.active = False

        burst.burst_id = None

        ERROR_BURST_ACTIVE.set(0)


def start_burst(
    duration: int,
    source: str = "scheduled"
):

    burst.active = True

    burst.burst_id = (
        f"burst_{uuid.uuid4().hex[:8]}"
    )

    burst.end_time = (
        time.time()
        + duration
    )

    burst.total_ok = 0

    burst.total_fail = 0

    ERROR_BURST_ACTIVE.set(1)

    log(
        "warning",
        "error_burst_started",
        burst_id=burst.burst_id,
        duration_seconds=duration,
        configured_error_probability=
            BURST_ERR_RATE,
        source=source,
    )


async def burst_scheduler():

    log(
        "info",
        "burst_scheduler_started",
        interval_seconds=BURST_INTERVAL,
    )

    while True:

        await asyncio.sleep(
            BURST_INTERVAL
        )

        maybe_expire_burst()

        if (
            not burst.active
            and random.random()
            < BURST_PROB
        ):

            duration = random.randint(
                20,
                90
            )

            start_burst(
                duration,
                source="scheduled"
            )


# =============================================================================
# FAILURE DECISION
# =============================================================================

def should_fail():

    maybe_expire_burst()

    if burst.active:

        rate = BURST_ERR_RATE

    else:

        rate = NORMAL_ERR_RATE

    failed = (
        random.random() < rate
    )

    if not failed:
        return False, ""

    if burst.active:

        reason = random.choice([
            "gateway_timeout",
            "processor_unavailable",
            "upstream_service_error",
            "rate_limit_exceeded",
        ])

    else:

        reason = random.choice([
            "gateway_timeout",
            "fraud_detected",
            "3ds_authentication_failed",
            "card_declined_by_bank",
            "network_error",
        ])

    return True, reason


# =============================================================================
# APPLICATION LIFESPAN
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    log(
        "info",
        "service_started",
        port=SERVICE_PORT,
        version="1.0.0",
    )

    task = asyncio.create_task(
        burst_scheduler()
    )

    yield

    task.cancel()

    log(
        "info",
        "service_stopped"
    )


# =============================================================================
# FASTAPI
# =============================================================================

app = FastAPI(
    title="Payment Service",
    version="1.0.0",
    lifespan=lifespan,
)


# =============================================================================
# HTTP OBSERVABILITY MIDDLEWARE
# =============================================================================

@app.middleware("http")
async def observe_request(
    request: Request,
    call_next
):

    request_id = str(
        uuid.uuid4()
    )

    request.state.request_id = request_id

    start = time.perf_counter()

    try:

        response = await call_next(
            request
        )

    except Exception:

        duration = (
            time.perf_counter()
            - start
        )

        endpoint = request.url.path

        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=endpoint,
            status_code="500",
        ).inc()

        REQUEST_LATENCY.labels(
            endpoint=endpoint
        ).observe(
            duration
        )

        # -------------------------------------------------------------
        # Track failed /payments requests
        # -------------------------------------------------------------

        if (
            request.method == "POST"
            and endpoint == "/payments"
        ):

            record_payment_result(True)

            update_error_rate_metric()

        log(
            "error",
            "http_request_exception",
            request_id=request_id,
            method=request.method,
            path=endpoint,
            duration_ms=round(
                duration * 1000,
                2
            ),
            rolling_error_rate=
                calculate_error_rate(),
        )

        raise

    duration = (
        time.perf_counter()
        - start
    )

    endpoint = request.url.path

    status_code = response.status_code

    # -----------------------------------------------------------------
    # General HTTP request counter
    # -----------------------------------------------------------------

    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=endpoint,
        status_code=str(status_code),
    ).inc()

    # -----------------------------------------------------------------
    # General HTTP latency
    # -----------------------------------------------------------------

    REQUEST_LATENCY.labels(
        endpoint=endpoint
    ).observe(
        duration
    )

    # -----------------------------------------------------------------
    # Track /payments HTTP result
    #
    # IMPORTANT:
    # Do NOT record here if create_payment() already recorded the
    # application result. Otherwise every payment would be counted twice.
    # -----------------------------------------------------------------

    if (
        request.method == "POST"
        and endpoint == "/payments"
    ):

        # Only record here for unexpected HTTP failures that were
        # returned without create_payment() recording a result.
        #
        # Normal payment success/failure is already recorded inside
        # create_payment().
        if status_code >= 500:

            # The middleware is seeing an actual HTTP 5xx response.
            record_payment_result(True)

            update_error_rate_metric()

    # -----------------------------------------------------------------
    # Logging
    # -----------------------------------------------------------------

    log(
        "info",
        "http_request",
        request_id=request_id,
        method=request.method,
        path=endpoint,
        status_code=status_code,
        duration_ms=round(
            duration * 1000,
            2
        ),
        rolling_error_rate=
            calculate_error_rate(),
    )

    response.headers[
        "X-Request-ID"
    ] = request_id

    return response


# =============================================================================
# PROMETHEUS
# =============================================================================
@app.get("/metrics")
def metrics():

    # Keep the rolling payment error-rate gauge synchronized
    # with the current 60-second request history.
    update_error_rate_metric()

    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

# =============================================================================
# HEALTH
# =============================================================================

@app.get("/health")
async def health():

    return {
        "status": "healthy",

        "service": SERVICE_NAME,

        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "error_burst_active":
            burst.active,

        "burst_id":
            burst.burst_id,

        "error_rate_pct":
            calculate_error_rate(),

        "tracked_payment_requests":
            len(request_history),
    }


# =============================================================================
# CREATE PAYMENT
# =============================================================================

@app.post("/payments")
async def create_payment(
    request: Request
):

    body = await request.json()

    request_id = getattr(
        request.state,
        "request_id",
        str(uuid.uuid4())
    )

    txn_id = (
        f"txn_{uuid.uuid4().hex[:12]}"
    )

    amount = float(
        body.get(
            "amount",
            0
        )
    )

    currency = body.get(
        "currency",
        "INR"
    ).upper()

    merchant_id = body.get(
        "merchant_id",
        "unknown"
    )

    pay_method = body.get(
        "payment_method",
        "card"
    )

    # Make sure burst state is current.
    maybe_expire_burst()

    log(
        "info",
        "payment_initiated",
        request_id=request_id,
        transaction_id=txn_id,
        amount=amount,
        currency=currency,
        merchant_id=merchant_id,
        payment_method=pay_method,
        burst_active=burst.active,
    )

    PAYMENTS_IN_FLIGHT.inc()

    # -------------------------------------------------------------------------
    # Latency
    # -------------------------------------------------------------------------

    base_latency = random.uniform(
        0.05,
        0.30
    )

    if burst.active:

        base_latency += random.uniform(
            0.50,
            2.50
        )

    await asyncio.sleep(
        base_latency
    )

    PAYMENTS_IN_FLIGHT.dec()

    # -------------------------------------------------------------------------
    # PAYMENT FAILURE
    # -------------------------------------------------------------------------

    failed, reason = should_fail()

    if failed:

        burst.total_fail += 1

        # -------------------------------------------------------------
        # THIS IS THE IMPORTANT FIX
        # -------------------------------------------------------------

        record_payment_result(
            True
        )

        current_error_rate = (
            update_error_rate_metric()
        )

        PAYMENT_FAILURE_COUNT.labels(
            failure_reason=reason,
            payment_method=pay_method,
        ).inc()

        log(
            "error",
            "payment_failed",
            request_id=request_id,
            transaction_id=txn_id,
            amount=amount,
            currency=currency,
            merchant_id=merchant_id,
            failure_reason=reason,
            burst_active=burst.active,
            burst_id=burst.burst_id,
            rolling_error_rate=
                current_error_rate,
        )

        raise HTTPException(
            status_code=402,
            detail={
                "error":
                    "payment_failed",

                "reason":
                    reason,

                "transaction_id":
                    txn_id,

                "rolling_error_rate":
                    current_error_rate,
            },
        )

    # -------------------------------------------------------------------------
    # BUSINESS DECLINE
    #
    # HTTP 200, therefore not an HTTP error.
    # -------------------------------------------------------------------------

    if random.random() < 0.03:

        burst.total_ok += 1

        record_payment_result(
            False
        )

        current_error_rate = (
            update_error_rate_metric()
        )

        PAYMENT_DECLINED_COUNT.labels(
            decline_reason=
                "insufficient_funds"
        ).inc()

        log(
            "warning",
            "payment_declined",
            request_id=request_id,
            transaction_id=txn_id,
            amount=amount,
            reason="insufficient_funds",
            rolling_error_rate=
                current_error_rate,
        )

        return JSONResponse(
            status_code=200,
            content={
                "transaction_id":
                    txn_id,

                "status":
                    "declined",

                "amount":
                    amount,

                "currency":
                    currency,

                "message":
                    "Payment declined: insufficient funds",

                "timestamp":
                    datetime.now(
                        timezone.utc
                    ).isoformat(),
            },
        )

    # -------------------------------------------------------------------------
    # SUCCESS
    # -------------------------------------------------------------------------

    burst.total_ok += 1

    record_payment_result(
        False
    )

    current_error_rate = (
        update_error_rate_metric()
    )

    PAYMENT_SUCCESS_COUNT.labels(
        payment_method=pay_method,
        currency=currency,
    ).inc()

    PAYMENT_AMOUNT.labels(
        payment_method=pay_method
    ).observe(
        amount
    )

    log(
        "info",
        "payment_success",
        request_id=request_id,
        transaction_id=txn_id,
        amount=amount,
        currency=currency,
        merchant_id=merchant_id,
        payment_method=pay_method,
        rolling_error_rate=
            current_error_rate,
    )

    return {
        "transaction_id":
            txn_id,

        "status":
            "success",

        "amount":
            amount,

        "currency":
            currency,

        "message":
            "Payment processed successfully",

        "rolling_error_rate":
            current_error_rate,

        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }


# =============================================================================
# REFUND
# =============================================================================

@app.post("/payments/{txn_id}/refund")
async def refund_payment(
    txn_id: str,
    request: Request
):

    body = await request.json()

    request_id = getattr(
        request.state,
        "request_id",
        str(uuid.uuid4())
    )

    refund_id = (
        f"ref_{uuid.uuid4().hex[:12]}"
    )

    amount = float(
        body.get(
            "amount",
            0
        )
    )

    reason = body.get(
        "reason",
        "customer_request"
    )

    log(
        "info",
        "refund_initiated",
        request_id=request_id,
        transaction_id=txn_id,
        refund_id=refund_id,
        refund_amount=amount,
        reason=reason,
    )

    await asyncio.sleep(
        random.uniform(
            0.05,
            0.20
        )
    )

    if random.random() < 0.05:

        REFUND_COUNT.labels(
            status="failed"
        ).inc()

        log(
            "error",
            "refund_failed",
            request_id=request_id,
            transaction_id=txn_id,
            refund_id=refund_id,
            reason="transaction_not_found",
        )

        raise HTTPException(
            status_code=404,
            detail="Original transaction not found",
        )

    REFUND_COUNT.labels(
        status="success"
    ).inc()

    log(
        "info",
        "refund_success",
        request_id=request_id,
        transaction_id=txn_id,
        refund_id=refund_id,
        refund_amount=amount,
    )

    return {
        "refund_id":
            refund_id,

        "transaction_id":
            txn_id,

        "status":
            "refunded",

        "amount":
            amount,

        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }


# =============================================================================
# PAYMENT STATUS
# =============================================================================

@app.get("/payments/{txn_id}/status")
async def payment_status(
    txn_id: str
):

    log(
        "info",
        "payment_status_query",
        transaction_id=txn_id,
    )

    return {
        "transaction_id":
            txn_id,

        "status":
            random.choice([
                "success",
                "pending",
                "declined",
            ]),

        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }


# =============================================================================
# MANUAL BURST
# =============================================================================

@app.post("/admin/trigger-burst")
async def trigger_burst(
    duration_seconds: int = 30
):

    if burst.active:

        return {
            "message":
                "Burst already active",

            "burst_id":
                burst.burst_id,
        }

    start_burst(
        duration_seconds,
        source="manual"
    )

    return {
        "message":
            f"Burst triggered for {duration_seconds}s",

        "burst_id":
            burst.burst_id,
    }


# =============================================================================
# STOP BURST
# =============================================================================

@app.post("/admin/stop-burst")
async def stop_burst():

    if not burst.active:

        return {
            "message":
                "No active burst"
        }

    log(
        "warning",
        "error_burst_stopped_manually",
        burst_id=burst.burst_id,
    )

    burst.active = False

    burst.burst_id = None

    ERROR_BURST_ACTIVE.set(0)

    return {
        "message":
            "Burst stopped"
    }


# =============================================================================
# SERVER
# =============================================================================

if __name__ == "__main__":

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=SERVICE_PORT,
        log_level="warning",
    )
