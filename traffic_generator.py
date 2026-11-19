"""
traffic_generator.py — Sends continuous realistic traffic to all services
Run this BEFORE and DURING warmup to build proper normal baseline data.

Usage:
    python traffic_generator.py              # runs forever
    python traffic_generator.py --minutes 10 # runs for 10 minutes then stops
    python traffic_generator.py --service payment  # only payment-service
"""

import requests
import random
import time
import argparse
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
PAYMENT_URL   = "http://localhost:8002/payments"
ORDER_URL     = "http://localhost:8001/orders"
INVENTORY_URL = "http://localhost:8003/inventory"

MERCHANTS    = ["merchant_001", "merchant_002", "merchant_003", "merchant_004"]
CUSTOMERS    = ["cust_001", "cust_002", "cust_003", "cust_004", "cust_005"]
ITEMS        = ["item_A", "item_B", "item_C", "item_D", "item_E"]
CURRENCIES   = ["INR", "INR", "INR", "USD"]  # mostly INR
PAY_METHODS  = ["card", "card", "card", "upi", "netbanking"]
ORDER_TYPES  = ["standard", "standard", "express", "bulk"]


def send_payment() -> str:
    """Sends one realistic payment request."""
    try:
        r = requests.post(
            PAYMENT_URL,
            json={
                "amount":         round(random.uniform(100, 5000), 2),
                "currency":       random.choice(CURRENCIES),
                "merchant_id":    random.choice(MERCHANTS),
                "payment_method": random.choice(PAY_METHODS),
            },
            timeout=10,
        )
        return f"payment {r.status_code}"
    except Exception as e:
        return f"payment error: {e}"


def send_order() -> str:
    """Sends one realistic order request."""
    try:
        item_count = random.randint(1, 5)
        r = requests.post(
            ORDER_URL,
            json={
                "customer_id": random.choice(CUSTOMERS),
                "order_type":  random.choice(ORDER_TYPES),
                "items":       random.sample(ITEMS, min(item_count, len(ITEMS))),
            },
            timeout=15,
        )
        return f"order {r.status_code}"
    except Exception as e:
        return f"order error: {e}"


def send_inventory_check() -> str:
    """Sends one realistic inventory check."""
    try:
        item_id = random.choice(ITEMS)
        r = requests.get(
            f"{INVENTORY_URL}/{item_id}",
            timeout=15,
        )
        return f"inventory {r.status_code}"
    except Exception as e:
        return f"inventory error: {e}"


def run(service: str = "all", minutes: int = None):
    """
    Sends continuous traffic to build normal baseline.

    Why this matters:
    - Without traffic, error_rate gauge stays at its last value
    - The Isolation Forest needs to see NORMAL behavior (0-2% error rate)
    - Traffic makes payment-service process real requests
    - This fills Prometheus with genuine normal metric readings
    """
    start_time  = time.time()
    request_num = 0
    end_time    = (start_time + minutes * 60) if minutes else None

    print("\n" + "="*60)
    print("TRAFFIC GENERATOR — building normal baseline")
    print(f"Service: {service}")
    print(f"Duration: {'forever (Ctrl+C to stop)' if not minutes else f'{minutes} minutes'}")
    print("="*60)
    print()
    print("Sending requests every 1-2 seconds...")
    print("Watch main.py — error_rate should stay near 0-2%")
    print("Ctrl+C to stop\n")

    while True:
        try:
            # Check if time limit reached
            if end_time and time.time() > end_time:
                print(f"\nCompleted {minutes} minutes of traffic generation")
                print(f"Total requests sent: {request_num}")
                break

            request_num += 1
            now = datetime.now().strftime("%H:%M:%S")
            results = []

            # Send to requested services
            if service in ("all", "payment"):
                result = send_payment()
                results.append(result)

            if service in ("all", "order"):
                result = send_order()
                results.append(result)

            if service in ("all", "inventory"):
                result = send_inventory_check()
                results.append(result)

            # Print status every 10 requests
            if request_num % 10 == 0:
                print(f"  [{now}] Request #{request_num}: {' | '.join(results)}")

            # Random delay 1-2 seconds between requests
            # This simulates realistic traffic patterns
            time.sleep(random.uniform(1.0, 2.0))

        except KeyboardInterrupt:
            print(f"\n\nStopped after {request_num} requests")
            elapsed = round((time.time() - start_time) / 60, 1)
            print(f"Ran for {elapsed} minutes")
            print("\nNow check main.py — model should be training on clean data")
            break


def main():
    parser = argparse.ArgumentParser(description="Traffic generator for AIOps demo")
    parser.add_argument(
        "--service",
        choices=["payment", "order", "inventory", "all"],
        default="all",
        help="Which service to send traffic to",
    )
    parser.add_argument(
        "--minutes",
        type=int,
        default=None,
        help="How many minutes to run (default: forever)",
    )
    args = parser.parse_args()
    run(args.service, args.minutes)


if __name__ == "__main__":
    main()