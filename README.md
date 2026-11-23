# TraceWatch

TraceWatch is a Java-based observability and monitoring project that helps
track, trace, and analyze the behavior of backend services. It collects
metrics with Prometheus and runs its supporting infrastructure through
Docker Compose, so the whole stack can be started locally with a single command.

## Features
- Metrics collection and scraping with Prometheus
- Containerized infrastructure via Docker Compose
- Java backend service(s) exposing metrics and trace data
- Easy local setup for development and testing

## Tech Stack
- Java (Spring Boot)
- Prometheus
- Docker and Docker Compose

## Project Structure
trace-watch/
├── infra/
│   ├── docker-compose.yml
│   └── prometheus/
│       └── prometheus.yml
└── src/

## Getting Started
1. Clone the repo
2. Start the infrastructure:
   docker compose -f infra/docker-compose.yml up -d
3. Run the application
4. Open Prometheus at http://localhost:9090

## Roadmap
- Add dashboards (e.g., Grafana)
- Add distributed tracing
- Add alerting rules
