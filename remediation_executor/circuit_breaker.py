import docker
import logging
import sqlite3
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

INCIDENTS_DB = "incident_manager/incidents.db"


def connect_docker() -> docker.DockerClient:
    """Connects to Docker daemon."""
    try:
        client = docker.from_env()
        client.ping()
        return client
    except Exception as e:
        log.error(f"Cannot connect to Docker: {e}")
        return None


def activate_circuit_breaker(container_name: str, incident_id: str) -> dict:
    """
    Tier 4 — Circuit breaker — pauses the container completely.

    Why pause instead of stop:
    - Paused container keeps its state in memory (easier to resume)
    - All processes inside are frozen — no more requests processed
    - Callers get immediate connection refused (fast fail)
    - Other services stop waiting 30 seconds for timeouts
    - Prevents cascading failures to dependent services

    This is a last resort — only triggered when:
    - Tier 1 (SIGHUP) failed
    - Tier 2 (restart) failed
    - Human review could not resolve it
    """
    client = connect_docker()
    if client is None:
        return {"success": False, "error": "docker_unavailable"}

    try:
        container = client.containers.get(container_name)
        container.reload()

        if container.status == "paused":
            log.warning(f"{container_name} is already paused")
            return {
                "success":  True,
                "action":   "circuit_breaker_already_active",
                "container": container_name,
            }

        log.warning(
            f"CIRCUIT BREAKER ACTIVATED | {container_name} | {incident_id} | "
            f"pausing container to prevent cascading failures"
        )

        container.pause()
        container.reload()

        log.warning(
            f"CIRCUIT BREAKER ACTIVE | {container_name} | "
            f"status={container.status} | all traffic blocked"
        )

        # Record in incidents.db
        update_incident_circuit_breaker(incident_id, container_name)

        return {
            "success":    True,
            "action":     "circuit_breaker",
            "container":  container_name,
            "incident_id": incident_id,
            "new_status": container.status,
            "message":    "Container paused — unresolvable issue detected",
            "next_step":  "Manual investigation required — run reset_circuit_breaker() when ready",
            "timestamp":  datetime.now(timezone.utc).isoformat(),
        }

    except docker.errors.NotFound:
        log.error(f"Container {container_name} not found")
        return {"success": False, "error": "container_not_found"}
    except Exception as e:
        log.error(f"Circuit breaker failed: {e}")
        return {"success": False, "error": str(e)}


def reset_circuit_breaker(container_name: str) -> dict:
    """
    Resets (unpauses) a circuit-broken container after manual investigation.
    Called manually by a DevOps engineer after fixing the root cause.
    """
    client = connect_docker()
    if client is None:
        return {"success": False, "error": "docker_unavailable"}

    try:
        container = client.containers.get(container_name)
        container.reload()

        if container.status != "paused":
            log.info(f"{container_name} is not paused (status={container.status})")
            return {
                "success": True,
                "message": f"Container already running — status={container.status}",
            }

        log.info(f"Resetting circuit breaker for {container_name}")
        container.unpause()
        container.reload()

        log.info(f"Circuit breaker reset | {container_name} | status={container.status}")

        return {
            "success":    True,
            "action":     "circuit_breaker_reset",
            "container":  container_name,
            "new_status": container.status,
            "timestamp":  datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        log.error(f"Circuit breaker reset failed: {e}")
        return {"success": False, "error": str(e)}


def update_incident_circuit_breaker(incident_id: str, container_name: str):
    """Records circuit breaker activation in incidents.db."""
    try:
        conn   = sqlite3.connect(INCIDENTS_DB)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE incidents
            SET status      = 'circuit_breaker_active',
                resolved_at = ?
            WHERE incident_id = ?
        """, (
            datetime.now(timezone.utc).isoformat(),
            incident_id,
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"Error updating circuit breaker status: {e}")


def get_circuit_breaker_status(container_name: str) -> str:
    """Returns whether circuit breaker is currently active for a container."""
    client = connect_docker()
    if client is None:
        return "unknown"
    try:
        container = client.containers.get(container_name)
        container.reload()
        return "active" if container.status == "paused" else "inactive"
    except Exception:
        return "unknown"


# ── Run standalone to test ────────────────────────────────────────────────────
if __name__ == "__main__":
    import json

    print("\n" + "="*60)
    print("CIRCUIT BREAKER TEST — payment-service")
    print("="*60)

    print("\n[CHECK] Circuit breaker status:")
    status = get_circuit_breaker_status("payment-service")
    print(f"  Circuit breaker: {status}")

    print("\nThis is Tier 4 — last resort only.")
    print("It pauses the container to prevent cascading failures.")
    print("To test manually:")
    print("  activate_circuit_breaker('payment-service', 'INC_TEST')")
    print("  reset_circuit_breaker('payment-service')")
    print("="*60 + "\n")