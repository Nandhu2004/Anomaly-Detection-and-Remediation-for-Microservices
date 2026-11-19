import subprocess
import docker
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def get_current_replica_count(service_name: str) -> int:
    """Returns how many containers are currently running for a service."""
    try:
        client     = docker.from_env()
        containers = client.containers.list(
            filters={"name": service_name, "status": "running"}
        )
        return len(containers)
    except Exception as e:
        log.error(f"Error getting replica count: {e}")
        return 1


def scale_up(service_name: str, incident_id: str) -> dict:
    """
    Scales a service up by one replica using Docker Compose.
    Used for sustained latency spikes caused by high load —
    more instances = more capacity to handle requests.

    Note: For payment-service this is rarely needed since its
    anomaly is error_burst not load — scale_up is more relevant
    for order-service latency spikes in future.
    """
    current = get_current_replica_count(service_name)
    target  = current + 1

    log.warning(
        f"SCALE UP | {service_name} | {incident_id} | "
        f"{current} → {target} replicas"
    )

    try:
        result = subprocess.run(
            [
                "docker", "compose", "up",
                "--scale", f"{service_name}={target}",
                "--no-recreate", "-d"
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            log.info(f"Scale up successful: {service_name} now at {target} replicas")
            return {
                "success":          True,
                "action":           "scale_up",
                "service":          service_name,
                "incident_id":      incident_id,
                "previous_replicas": current,
                "new_replicas":      target,
                "timestamp":        datetime.now(timezone.utc).isoformat(),
            }
        else:
            log.error(f"Scale up failed: {result.stderr}")
            return {
                "success": False,
                "error":   result.stderr,
            }

    except subprocess.TimeoutExpired:
        log.error("Scale up timed out")
        return {"success": False, "error": "timeout"}
    except Exception as e:
        log.error(f"Scale up error: {e}")
        return {"success": False, "error": str(e)}


def scale_down(service_name: str) -> dict:
    """
    Scales a service back down to 1 replica.
    Called after a rush/spike ends to clean up extra instances.
    """
    current = get_current_replica_count(service_name)

    if current <= 1:
        log.info(f"{service_name} already at 1 replica — no scale down needed")
        return {"success": True, "message": "already_at_minimum"}

    log.info(f"SCALE DOWN | {service_name} | {current} → 1 replica")

    try:
        result = subprocess.run(
            [
                "docker", "compose", "up",
                "--scale", f"{service_name}=1",
                "--no-recreate", "-d"
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            log.info(f"Scale down successful: {service_name} back to 1 replica")
            return {
                "success":           True,
                "action":            "scale_down",
                "service":           service_name,
                "previous_replicas": current,
                "new_replicas":      1,
                "timestamp":         datetime.now(timezone.utc).isoformat(),
            }
        else:
            return {"success": False, "error": result.stderr}

    except Exception as e:
        log.error(f"Scale down error: {e}")
        return {"success": False, "error": str(e)}


# ── Run standalone to test ────────────────────────────────────────────────────
if __name__ == "__main__":
    import json

    print("\n" + "="*60)
    print("SCALE SERVICE TEST")
    print("="*60)

    print("\n[CHECK] Current replica count for payment-service:")
    count = get_current_replica_count("payment-service")
    print(f"  Current replicas: {count}")

    print("\nScale up/down is mainly used for order-service latency spikes.")
    print("For payment-service error bursts, restart_container is used instead.")
    print("="*60 + "\n")