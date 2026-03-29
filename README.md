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

## Project Structure

- `app/main.py` - server startup and CLI arguments
- `app/handlers.py` - client request handling
- `app/command_handlers/` - command implementations
- `tests/` - automated test suite

## CodeCrafters

Containerization is only for local development and deployment convenience.
CodeCrafters still uses its own runner configuration from `codecrafters.yml`
and `.codecrafters/run.sh`.
