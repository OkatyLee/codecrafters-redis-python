# Redis Clone in Python

This repository contains a Redis-like server built for the
[CodeCrafters "Build Your Own Redis" challenge](https://codecrafters.io/challenges/redis).

The server is implemented in `app/main.py` and speaks the Redis RESP protocol
over TCP.

## Requirements

- Python 3.14+
- `uv` for local runs via the provided script
- Docker (optional, for containerized runs)

## Run Locally

Start the server with the helper script:

```sh
./your_program.sh
```

Run it directly with custom options:

```sh
uv run --quiet -m app.main --host 127.0.0.1 --port 6379
```

Available runtime flags:

- `--host` - interface to bind to
- `--port` / `-p` - TCP port to listen on
- `--replicaof` - start as a replica of `host:port`
- `--dir` - directory used for persisted files
- `--dbfilename` - RDB filename inside the data directory
- `--metrics-port` - port for the Prometheus `/metrics` endpoint, use `0` to disable

## Run Tests

```sh
uv run pytest
```

## Docker

Build the image:

```sh
docker build -t codecrafters-redis-python .
```

Run the server in a container:

```sh
docker run --rm -p 6379:6379 codecrafters-redis-python
```

The container starts the server with:

```sh
python -m app.main --host 0.0.0.0 --port 6379 --dir /data
```

Persist RDB files on the host:

```sh
docker run --rm -p 6379:6379 -v ${PWD}/tmp/files:/data codecrafters-redis-python
```

Run with custom arguments:

```sh
docker run --rm -p 6380:6380 codecrafters-redis-python --port 6380 --host 0.0.0.0
```

Inspect metrics locally:

```sh
curl http://127.0.0.1:9100/metrics
```

## Observability

Bring up the Redis clone, Prometheus, and Grafana together:

```sh
docker compose -f docker-compose.observability.yml up -d
```

Endpoints:

- Redis: `127.0.0.1:6379`
- Replica when started for replication tests: `127.0.0.1:6380`
- Metrics: `http://127.0.0.1:9100/metrics`
- Replica metrics when started: `http://127.0.0.1:9101/metrics`
- Prometheus: `http://127.0.0.1:9090`
- Grafana: `http://127.0.0.1:3000` with `admin` / `admin`

Grafana is pre-provisioned with:

- a Prometheus datasource
- a `Redis Clone Overview` dashboard

Stop the stack with:

```sh
docker compose -f docker-compose.observability.yml down
```

## Load Testing

Quick perf run with the observability stack already wired in:

```sh
./scripts/perf/run-benchmark.sh smoke
```

PowerShell:

```powershell
./scripts/perf/run-benchmark.ps1 -Scenario smoke
```

Available scenarios:

- `smoke` - short mixed run for a quick sanity check
- `read-heavy` - favors `PING`, `GET`, and `MGET`
- `write-heavy` - favors `SET`, `INCR`, `LPUSH`, and `RPUSH`
- `replication` - writes to the master and measures visibility on a replica
- `pubsub` - fans out `PUBLISH` traffic to many subscribers and measures delivery latency

You can also override runtime parameters:

```powershell
./scripts/perf/run-benchmark.ps1 -Scenario write-heavy -Clients 150 -Requests 80000 -DataSize 256
```

Replication example:

```powershell
./scripts/perf/run-benchmark.ps1 -Scenario replication -Clients 24 -Requests 1200 -DataSize 128
```

Pub/Sub example:

```powershell
./scripts/perf/run-benchmark.ps1 -Scenario pubsub -Clients 32 -Publishers 6 -Requests 1500 -DataSize 96
```

The scripts:

- ensure `app`, `prometheus`, and `grafana` are running
- start `app-replica` automatically for the replication scenario
- execute `redis-benchmark` from an isolated Docker container on the same Compose network
- execute dedicated Python perf drivers for replication and pub/sub scenarios
- let you inspect throughput and latency live in Grafana while the benchmark is running

## Project Structure

- `app/main.py` - server startup and CLI arguments
- `app/handlers.py` - client request handling
- `app/command_handlers/` - command implementations
- `tests/` - automated test suite

## CodeCrafters

Containerization is only for local development and deployment convenience.
CodeCrafters still uses its own runner configuration from `codecrafters.yml`
and `.codecrafters/run.sh`.
