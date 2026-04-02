import asyncio
import logging
import threading
from typing import cast
from unittest.mock import MagicMock, patch

import pytest  # pyright: ignore[reportMissingImports]

from app.config import ServerConfig
from app.handlers import handle_client
from app.metrics import render_metrics
from app.parser import RESPParser
from app.state import AppState
from app.storage import CacheStorage
import app.metrics as metrics_module


def make_app_state() -> AppState:
    return AppState(
        ServerConfig("127.0.0.1", 0),
        CacheStorage(),
        logging.getLogger(__name__),
    )


def metric_value(payload: bytes, metric_name: str, labels: dict[str, str] | None = None) -> float:
    lines = payload.decode("utf-8").splitlines()
    label_suffix = ""
    if labels:
        label_suffix = "{" + ",".join(f'{key}="{value}"' for key, value in labels.items()) + "}"

    target_prefix = f"{metric_name}{label_suffix} "
    for line in lines:
        if line.startswith(target_prefix):
            return float(line.split()[-1])
    return 0.0


async def start_server(app_state: AppState) -> asyncio.base_events.Server:
    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_client(reader, writer, app_state)

    return await asyncio.start_server(_handle, app_state.config.host, 0)


async def wait_for_metric(
    metric_name: str,
    expected_value: float,
    labels: dict[str, str] | None = None,
    timeout: float = 1.0,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if metric_value(render_metrics(), metric_name, labels) == expected_value:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"Timed out waiting for {metric_name} to become {expected_value}")


@pytest.mark.asyncio
async def test_metrics_track_connections_commands_and_protocol_errors():
    app_state = make_app_state()
    server = await start_server(app_state)
    addr = server.sockets[0].getsockname()

    baseline = render_metrics()
    base_clients = metric_value(baseline, "redis_connected_clients")
    base_protocol_errors = metric_value(baseline, "redis_protocol_error_total")
    base_ping_total = metric_value(
        baseline,
        "redis_commands_total",
        {"command": "ping", "status": "ok"},
    )
    base_ping_latency_count = metric_value(
        baseline,
        "redis_command_duration_seconds_count",
        {"command": "ping"},
    )

    reader, writer = await asyncio.open_connection(*addr)
    await asyncio.sleep(0)
    connected_snapshot = render_metrics()
    assert metric_value(connected_snapshot, "redis_connected_clients") == base_clients + 1

    writer.write(b"*1\r\n$4\r\nPING\r\n")
    await writer.drain()
    assert await reader.readline() == b"+PONG\r\n"

    writer.write(b"+oops\r\n")
    await writer.drain()
    assert await reader.readline() == b"-ERR Protocol error\r\n"

    writer.close()
    await writer.wait_closed()
    await wait_for_metric("redis_connected_clients", base_clients)

    final_snapshot = render_metrics()
    assert metric_value(final_snapshot, "redis_connected_clients") == base_clients
    assert metric_value(final_snapshot, "redis_protocol_error_total") == base_protocol_errors + 1
    assert metric_value(
        final_snapshot,
        "redis_commands_total",
        {"command": "ping", "status": "ok"},
    ) == base_ping_total + 1
    assert metric_value(
        final_snapshot,
        "redis_command_duration_seconds_count",
        {"command": "ping"},
    ) == base_ping_latency_count + 1

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_metrics_track_active_subscriptions_and_connected_replicas():
    app_state = make_app_state()
    server = await start_server(app_state)
    addr = server.sockets[0].getsockname()

    baseline = render_metrics()
    base_subscriptions = metric_value(baseline, "redis_active_subscriptions")
    base_replicas = metric_value(baseline, "redis_connected_replicas")
    base_clients = metric_value(baseline, "redis_connected_clients")

    replica_writer = cast(asyncio.StreamWriter, object())
    app_state.register_replica(None, replica_writer)
    assert metric_value(render_metrics(), "redis_connected_replicas") == base_replicas + 1

    reader, writer = await asyncio.open_connection(*addr)
    parser = RESPParser(reader)

    writer.write(b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n")
    await writer.drain()
    assert await parser.parse() == [b"subscribe", b"ch1", 1]
    assert metric_value(render_metrics(), "redis_active_subscriptions") == base_subscriptions + 1

    writer.write(b"*1\r\n$11\r\nUNSUBSCRIBE\r\n")
    await writer.drain()
    assert await parser.parse() == [b"unsubscribe", b"ch1", 0]
    assert metric_value(render_metrics(), "redis_active_subscriptions") == base_subscriptions

    app_state.unregister_replica(replica_writer)
    assert metric_value(render_metrics(), "redis_connected_replicas") == base_replicas

    writer.close()
    await writer.wait_closed()
    await wait_for_metric("redis_connected_clients", base_clients)
    server.close()
    await server.wait_closed()



@pytest.fixture(autouse=True)
def clear_started_ports():
    """Clear _STARTED_PORTS before each test."""
    metrics_module._STARTED_PORTS.clear()
    yield
    metrics_module._STARTED_PORTS.clear()


# 1. Successful startup — server starts and port is registered
def test_starts_server_and_registers_port():
    mock_server = MagicMock()
    with patch("app.metrics.ThreadingHTTPServer", return_value=mock_server) as mock_cls:
        metrics_module.start_metrics_server(9090)

    mock_cls.assert_called_once_with(("0.0.0.0", 9090), metrics_module._MetricsHandler)
    mock_server.serve_forever.assert_called_once()
    assert 9090 in metrics_module._STARTED_PORTS


# 2. Successful repeat call on the same port — does nothing
def test_does_not_start_twice_on_same_port():
    mock_server = MagicMock()
    with patch("app.metrics.ThreadingHTTPServer", return_value=mock_server) as mock_cls:
        metrics_module.start_metrics_server(9090)
        metrics_module.start_metrics_server(9090)

    mock_cls.assert_called_once()  


# 3. Port is busy (errno 98 / 10048) — logs warning and does not crash
@pytest.mark.parametrize("errno_code", [98, 10048])
def test_busy_port_is_skipped(errno_code):
    error = OSError(errno_code, "Address already in use")
    error.errno = errno_code

    with patch("app.metrics.ThreadingHTTPServer", side_effect=error):
        metrics_module.start_metrics_server(9090)  # 

    assert 9090 not in metrics_module._STARTED_PORTS


# 4. Another OSError (e.g., no permissions) — is raised
def test_other_oserror_is_raised():
    error = OSError(13, "Permission denied")
    error.errno = 13

    with patch("app.metrics.ThreadingHTTPServer", side_effect=error):
        with pytest.raises(OSError, match="Permission denied"):
            metrics_module.start_metrics_server(9090)


# 5. Thread is started as a daemon with the correct name
def test_thread_is_daemon_with_correct_name():
    threads_created = []
    original_init = threading.Thread.__init__

    def capture_thread(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        threads_created.append(self)

    mock_server = MagicMock()
    with patch("app.metrics.ThreadingHTTPServer", return_value=mock_server):
        with patch.object(threading.Thread, "__init__", capture_thread):
            with patch.object(threading.Thread, "start"):
                metrics_module.start_metrics_server(9091)

    assert len(threads_created) == 1
    assert threads_created[0].daemon is True
    assert threads_created[0].name == "metrics-server-9091"