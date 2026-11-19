
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import docker


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)


def connect_docker() -> Optional[docker.DockerClient]:
    """Connect to the Docker daemon and verify connectivity."""
    try:
        client = docker.from_env()
        client.ping()
        log.info("Docker daemon connected")
        return client
    except Exception as exc:
        log.error("Cannot connect to Docker daemon: %s", exc)
        return None


def read_container_logs(
    container_name: str,
    tail: int = 30
) -> str:
    """Read recent logs for incident diagnosis."""
    client = connect_docker()

    if client is None:
        return ""

    try:
        container = client.containers.get(container_name)
        logs = container.logs(
            tail=tail
        ).decode("utf-8", errors="replace")

        log.info(
            "Read %s log lines from %s",
            tail,
            container_name
        )
        return logs

    except docker.errors.NotFound:
        log.error("Container %s not found", container_name)
        return ""

    except Exception as exc:
        log.error(
            "Error reading logs from %s: %s",
            container_name,
            exc
        )
        return ""

    finally:
        client.close()


def diagnose_from_logs(logs: str) -> str:
    """Identify a known failure pattern in container logs."""
    if not logs:
        return "unknown"

    logs_lower = logs.lower()

    if "upstream connection refused" in logs_lower:
        return "external_dependency"

    if "memory" in logs_lower and "error" in logs_lower:
        return "memory_error"

    if (
        "worker" in logs_lower
        and (
            "exception" in logs_lower
            or "error" in logs_lower
        )
    ):
        return "worker_error"

    if "timeout" in logs_lower:
        return "timeout"

    if "rate limit" in logs_lower:
        return "rate_limit"

    return "general_error"


def send_sighup(container_name: str) -> Dict[str, Any]:
    """Attempt Tier 1 recovery by sending SIGHUP."""
    client = connect_docker()

    if client is None:
        return {
            "success": False,
            "action": "sighup_reload",
            "error": "docker_unavailable"
        }

    try:
        container = client.containers.get(container_name)
        container.reload()

        if container.status != "running":
            return {
                "success": False,
                "action": "sighup_reload",
                "error": "container_not_running",
                "status": container.status
            }

        log.warning(
            "TIER 1 RECOVERY | %s | sending SIGHUP",
            container_name
        )

        container.kill(signal="SIGHUP")

        return {
            "success": True,
            "action": "sighup_reload",
            "container": container_name,
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }

    except docker.errors.NotFound:
        log.error("Container %s not found", container_name)

        return {
            "success": False,
            "action": "sighup_reload",
            "error": "container_not_found"
        }

    except Exception as exc:
        log.exception(
            "SIGHUP failed for %s",
            container_name
        )

        return {
            "success": False,
            "action": "sighup_reload",
            "error": str(exc)
        }

    finally:
        client.close()


def tier1_recovery(
    container_name: str,
    incident_id: str
) -> Dict[str, Any]:
    """
    Tier 1 workflow:
    1. Read container logs.
    2. Diagnose the likely failure pattern.
    3. Avoid SIGHUP for known external dependency failures.
    4. Attempt SIGHUP for other diagnoses.
    5. Return a structured result for the executor.
    """
    log.info(
        "Starting Tier 1 recovery | container=%s | incident=%s",
        container_name,
        incident_id
    )

    logs = read_container_logs(container_name)
    diagnosis = diagnose_from_logs(logs)

    log.info(
        "Diagnosis | container=%s | diagnosis=%s",
        container_name,
        diagnosis
    )

    if diagnosis == "external_dependency":
        return {
            "success": False,
            "action": "escalate_to_human",
            "diagnosis": diagnosis,
            "incident_id": incident_id,
            "reason": (
                "External dependency failure detected; "
                "container recovery may not resolve it."
            ),
            "logs": logs[-500:]
        }

    result = send_sighup(container_name)

    result.update({
        "diagnosis": diagnosis,
        "incident_id": incident_id,
        "logs": logs[-200:]
    })

    return result


if __name__ == "__main__":
    import json

    container_name = "payment-service"
    incident_id = "INC_TEST_001"

    print("\nTIER 1 RECOVERY TEST")
    print("=" * 50)

    logs = read_container_logs(container_name, tail=10)
    print("\nRecent logs:")
    print(logs or "No logs available.")

    print("\nDiagnosis:")
    print(diagnose_from_logs(logs))

    print("\nRecovery result:")
    result = tier1_recovery(container_name, incident_id)
    print(json.dumps(result, indent=2))