
# Per-Service Anomaly Detection and Remediation for Microservices

## Overview

This project implements a lightweight, closed-loop AIOps framework for detecting service-specific anomalies in microservices, assessing incident risks, and performing automated remediation with recovery verification.

The system maintains independent feature configurations and Isolation Forest models for each microservice, enabling service-specific anomaly detection and risk-aware incident handling.

The framework monitors three containerized microservices:

- **Order Service**
- **Payment Service**
- **Inventory Service**

## Key Features

- **Per-Service Anomaly Detection:** Independent Isolation Forest models and feature configurations for each microservice.
- **Real-Time Monitoring:** Prometheus-based metric collection at fixed intervals.
- **Risk Assessment:** Evaluates anomalies using severity, service criticality, and detection confidence.
- **Automated Remediation:** Executes tiered recovery actions through the Docker Engine API.
- **Human-in-the-Loop:** Escalates critical or uncertain incidents for manual review and approval.
- **Recovery Verification:** Monitors service health after remediation and records the outcome.
- **Adaptive Thresholds:** Adjusts service-specific detection thresholds using historical false-positive feedback.
- **Monitoring Dashboard:** Streamlit dashboard displays service status, incident history, remediation details, and CSV exports.

## System Architecture

The framework follows a closed-loop operational workflow:

1. **Metric Collection:** Prometheus scrapes metrics exposed by the microservices.
2. **Anomaly Detection:** The metric collector retrieves observations using PromQL and passes them to the corresponding Isolation Forest model.
3. **Risk Assessment:** Detected anomalies are evaluated based on severity, service criticality, and confidence.
4. **Incident Routing:** The policy engine determines whether an incident requires automated remediation or human review.
5. **Remediation:** The Docker API executes the permitted recovery actions.
6. **Outcome Verification:** The system checks service health and records remediation outcomes.
7. **Threshold Adaptation:** Recent false-positive feedback is used to recalibrate service-specific detection thresholds.

## Remediation Strategy

The framework uses a four-tier remediation policy:

| Tier | Action | Description |
|---|---|---|
| Tier 1 | Lightweight Recovery | Executes a less disruptive recovery action using SIGHUP. |
| Tier 2 | Container Restart | Restarts the affected container and verifies recovery. |
| Tier 3 | Human Review | Escalates critical, uncertain, or unresolved incidents for operator review. |
| Tier 4 | Circuit Breaker | Supports human-authorized isolation of critical, persistent failures. |

## Technology Stack

| Component | Technology |
|---|---|
| Programming Language | Python |
| Anomaly Detection | Scikit-learn (Isolation Forest) |
| Monitoring | Prometheus |
| Containerization | Docker |
| Container Management | Docker SDK for Python |
| Dashboard | Streamlit |
| Data Processing | Pandas |
| Database | SQLite |
| Query Language | PromQL |

## Project Requirements

- Python
- Docker Desktop or Docker Engine
- Prometheus
- Required Python libraries listed in `requirements.txt`

The framework is designed to run on a standard development machine without requiring a dedicated GPU.

## Installation and Setup

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/<repository-name>.git
cd <repository-name>
```

### 2. Create a Virtual Environment

```bash
python -m venv venv
```

Activate it on Windows:

```bash
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Start Docker

Ensure Docker Desktop or Docker Engine is running before starting the microservices and remediation components.

### 5. Configure Prometheus

Configure Prometheus to scrape the metrics endpoints of the three microservices.

Default configuration:

- Prometheus: `http://localhost:9090`
- Order Service: Port `8001`
- Payment Service: Port `8002`
- Inventory Service: Port `8003`
- Scrape Interval: `15 seconds`

### 6. Run the Application

Start the anomaly detection and remediation pipeline:

```bash
python main.py
```

### 7. Launch the Dashboard

```bash
streamlit run dashboard.py
```

Open the local URL displayed in the terminal to access the dashboard.

## Database

The framework uses SQLite for persistent storage:

- **profiles.db:** Stores service-specific detection profiles and configurations.
- **incidents.db:** Stores detected incidents, remediation actions, and outcomes.

## Testing

The system supports controlled fault injection to examine its behaviour under abnormal service conditions.

| Microservice | Fault Scenario | Monitored Metrics |
|---|---|---|
| Order | Traffic Rush | CPU Utilization and Latency |
| Payment | Error Burst | Error Rate and Latency |
| Inventory | Service Overload | CPU Utilization and Latency |

These scenarios demonstrate anomaly detection, incident routing, remediation, and recovery verification.
