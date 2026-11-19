
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import docker


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger(__name__)


def connect_docker() -> Optional[docker.DockerClient]:
    """Connect to the Docker daemon."""
    try:
        client = docker.from_env()
        client.ping()
        return client

    except Exception as exc:
        log.error("Cannot connect to Docker daemon: %s", exc)
        return None


def get_container_status(container_name: str) -> str:
    """Return the current container status."""
    client = connect_docker()

    if client is None:
        return "unknown"

    try:
        container = client.containers.get(container_name)
        container.reload()
        return container.status

    except docker.errors.NotFound:
        return "not_found"

    except Exception as exc:
        log.error("Error getting container status: %s", exc)
        return "unknown"

    finally:
        client.close()


def restart_container(
    container_name: str,
    incident_id: str
) -> Dict[str, Any]:
    """
    Tier 2:
    Restart the container and verify that Docker reports it running.
    """
    client = connect_docker()

    if client is None:
        return {
            "success": False,
            "action": "container_restart",
            "error": "docker_unavailable"
        }

    try:
        container = client.containers.get(container_name)
        container.reload()

        if container.status != "running":
            log.warning(
                "Cannot restart %s: status=%s",
                container_name,
                container.status
            )

            return {
                "success": False,
                "action": "container_restart",
                "container": container_name,
                "error": "container_not_running",
                "status": container.status
            }

        log.warning(
            "TIER 2 RESTART | container=%s | incident=%s",
            container_name,
            incident_id
        )

        restart_start = time.monotonic()

        container.restart(timeout=10)

        max_wait = 30
        poll_interval = 2
        deadline = time.monotonic() + max_wait

        while time.monotonic() < deadline:
            container.reload()

            if container.status == "running":
                duration = round(
                    time.monotonic() - restart_start,
                    1
                )

                log.info(
                    "RESTART COMPLETE | container=%s | "
                    "duration=%ss",
                    container_name,
                    duration
                )

                return {
                    "success": True,
                    "action": "container_restart",
                    "container": container_name,
                    "incident_id": incident_id,
                    "restart_duration": duration,
                    "new_status": container.status,
                    "timestamp": datetime.now(
                        timezone.utc
                    ).isoformat()
                }

            time.sleep(poll_interval)

        container.reload()

        return {
            "success": False,
            "action": "container_restart",
            "container": container_name,
            "incident_id": incident_id,
            "error": "container_did_not_return_to_running",
            "status": container.status
        }

    except docker.errors.NotFound:
        log.error("Container %s not found", container_name)

        return {
            "success": False,
            "action": "container_restart",
            "error": "container_not_found"
        }

    except Exception as exc:
        log.exception(
            "Restart failed for %s",
            container_name
        )

        return {
            "success": False,
            "action": "container_restart",
            "error": str(exc)
        }

    finally:
        client.close()


def stop_and_start(
    container_name: str,
    incident_id: str,
    wait_seconds: int = 60
) -> Dict[str, Any]:
    """Alternative recovery: stop, wait, start, and check status."""
    if wait_seconds < 0:
        return {
            "success": False,
            "action": "stop_wait_start",
            "error": "invalid_wait_seconds"
        }

    client = connect_docker()

    if client is None:
        return {
            "success": False,
            "action": "stop_wait_start",
            "error": "docker_unavailable"
        }

    try:
        container = client.containers.get(container_name)
        container.reload()

        if container.status != "running":
            return {
                "success": False,
                "action": "stop_wait_start",
                "container": container_name,
                "error": "container_not_running",
                "status": container.status
            }

        log.warning(
            "STOP-WAIT-START | container=%s | "
            "incident=%s | wait=%ss",
            container_name,
            incident_id,
            wait_seconds
        )

        container.stop(timeout=10)
        time.sleep(wait_seconds)
        container.start()

        deadline = time.monotonic() + 30

        while time.monotonic() < deadline:
            container.reload()

            if container.status == "running":
                return {
                    "success": True,
                    "action": "stop_wait_start",
                    "container": container_name,
                    "incident_id": incident_id,
                    "waited_sec": wait_seconds,
                    "new_status": container.status,
                    "timestamp": datetime.now(
                        timezone.utc
                    ).isoformat()
                }

            time.sleep(2)

        container.reload()

        return {
            "success": False,
            "action": "stop_wait_start",
            "container": container_name,
            "incident_id": incident_id,
            "error": "container_did_not_return_to_running",
            "status": container.status
        }

    except Exception as exc:
        log.exception("Stop-wait-start failed")

        return {
            "success": False,
            "action": "stop_wait_start",
            "error": str(exc)
        }

    finally:
        client.close()


if __name__ == "__main__":
    import json

    container_name = "payment-service"
    incident_id = "INC_TEST_002"

    print("\nTIER 2 RESTART TEST")
    print("=" * 50)

    status = get_container_status(container_name)
    print("Current status:", status)

    if status == "running":
        result = restart_container(
            container_name,
            incident_id
        )
        print(json.dumps(result, indent=2))
    else:
        print(
            "Container is not running. Start it with "
            "docker compose up -d payment-service"
        )