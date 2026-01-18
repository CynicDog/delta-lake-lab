# Spark-SDP Quickstart

An environment for **Spark Declarative Pipelines (SDP)** on Spark 4.1.

### 1. Start Environment

Spin up the container using Docker Compose:

```bash
docker-compose up -d
```

### 2. Access

Enter the container and navigate to the project directory and : 

```bash
cd /app/sdp
docker exec -it spark-sdp /bin/bash
```

### 3. Run Pipeline

trigger the execution:

```bash
spark-pipelines run
```