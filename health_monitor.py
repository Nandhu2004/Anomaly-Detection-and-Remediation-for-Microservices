"""
health_monitor.py — Live health dashboard for all three services
Refreshes every 5 seconds automatically.

Usage:
    python health_monitor.py
    Press Ctrl+C to stop
"""

import requests
import time
import os
from datetime import datetime

SERVICES = [
    {
        "name":    "order-service",
        "url":     "http://localhost:8001/health",
        "port":    8001,
    },
    {
        "name":    "payment-service",
        "url":     "http://localhost:8002/health",
        "port":    8002,
    },
    {
        "name":    "inventory-service",
        "url":     "http://localhost:8003/health",
        "port":    8003,
    },
]

REFRESH_SECONDS = 5


def clear():
    os.system("cls" if os.name == "nt" else "clear")


def get_health(url: str) -> dict:
    try:
        r = requests.get(url, timeout=3)
        return r.json()
    except Exception:
        return None


def get_anomaly_info(name: str, data: dict) -> tuple:
    """Returns (anomaly_active, metric_value, anomaly_label)"""
    if "payment" in name:
        active = data.get("error_burst_active", False)
        metric = f"error_rate = {data.get('error_rate_pct', 0):.1f}%"
        label  = "ERROR BURST" if active else "normal"
    elif "order" in name:
        active = data.get("rush_active", False)
        metric = f"cpu = {data.get('cpu_usage_pct', 0):.1f}%"
        label  = "CPU + LATENCY SPIKE" if active else "normal"
    else:
        active = data.get("overload_active", False)
        metric = f"cpu = {data.get('cpu_usage_pct', 0):.1f}%"
        label  = "CPU OVERLOAD" if active else "normal (high cpu expected)"

    return active, metric, label


def render():
    clear()
    now = datetime.now().strftime("%H:%M:%S")

    print("=" * 65)
    print(f"  AIOps — Live Service Health Monitor     {now}")
    print(f"  Refreshing every {REFRESH_SECONDS}s — Press Ctrl+C to stop")
    print("=" * 65)
    print()

    any_anomaly = False

    for svc in SERVICES:
        data = get_health(svc["url"])

        if data is None:
            print(f"  {svc['name']:<25} ✗ NOT RUNNING")
            print()
            continue

        active, metric, label = get_anomaly_info(svc["name"], data)

        if active:
            any_anomaly = True
            status_icon = "⚠"
        else:
            status_icon = "✓"

        print(f"  {status_icon} {svc['name']}")
        print(f"    Status:   {data.get('status', 'unknown')}")
        print(f"    Metric:   {metric}")
        print(f"    Anomaly:  {label}")
        print()

    print("-" * 65)
    if any_anomaly:
        print("  ⚠  ANOMALY DETECTED — check main.py for pipeline response")
    else:
        print("  ✓  All services normal")

    print()
    print("  Quick commands:")
    print("    Prometheus:  http://localhost:9090")
    print("    Review UI:   http://localhost:8501")
    print("=" * 65)


def main():
    print("Starting health monitor...")
    time.sleep(1)

    while True:
        try:
            render()
            time.sleep(REFRESH_SECONDS)
        except KeyboardInterrupt:
            clear()
            print("\nHealth monitor stopped.\n")
            break


if __name__ == "__main__":
    main()