from __future__ import annotations

from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import logging
import threading


log = logging.getLogger(__name__)

COMMAND_DURATION_BUCKETS = (0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0)
BGSAVE_DURATION_BUCKETS = (0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0)


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_float(value: float) -> str:
    return format(value, ".17g")


@dataclass
class HistogramState:
    bucket_counts: list[int]
    count: int = 0
    total: float = 0.0

    def observe(self, value: float, buckets: tuple[float, ...]) -> None:
        self.count += 1
        self.total += value
        for idx, upper_bound in enumerate(buckets):
            if value <= upper_bound:
                self.bucket_counts[idx] += 1
                return


@dataclass
class MetricsState:
    connected_clients: int = 0
    connected_replicas: int = 0
    protocol_error_total: int = 0
    active_subscriptions: int = 0
    background_tasks: int = 0
    commands_total: dict[tuple[str, str], int] = field(default_factory=dict)
    command_duration_seconds: dict[str, HistogramState] = field(default_factory=dict)
    bgsave_duration_seconds: HistogramState = field(
        default_factory=lambda: HistogramState([0] * len(BGSAVE_DURATION_BUCKETS))
    )


_STATE = MetricsState()
_LOCK = threading.Lock()
_STARTED_PORTS: set[int] = set()


def client_connected() -> None:
    with _LOCK:
        _STATE.connected_clients += 1


def client_disconnected() -> None:
    with _LOCK:
        _STATE.connected_clients = max(0, _STATE.connected_clients - 1)


def set_connected_replicas(count: int) -> None:
    with _LOCK:
        _STATE.connected_replicas = max(0, count)


def record_command(command: bytes, *, status: str, duration_seconds: float) -> None:
    command_name = command.decode("utf-8", errors="replace").lower()
    status_name = status.lower()
    with _LOCK:
        key = (command_name, status_name)
        _STATE.commands_total[key] = _STATE.commands_total.get(key, 0) + 1
        histogram = _STATE.command_duration_seconds.setdefault(
            command_name,
            HistogramState([0] * len(COMMAND_DURATION_BUCKETS)),
        )
        histogram.observe(duration_seconds, COMMAND_DURATION_BUCKETS)


def record_protocol_error() -> None:
    with _LOCK:
        _STATE.protocol_error_total += 1


def set_active_subscriptions(count: int) -> None:
    with _LOCK:
        _STATE.active_subscriptions = max(0, count)


def set_background_tasks(count: int) -> None:
    with _LOCK:
        _STATE.background_tasks = max(0, count)


def observe_bgsave_duration(duration_seconds: float) -> None:
    with _LOCK:
        _STATE.bgsave_duration_seconds.observe(duration_seconds, BGSAVE_DURATION_BUCKETS)


def _render_histogram(
    metric_name: str,
    help_text: str,
    buckets: tuple[float, ...],
    states: list[tuple[dict[str, str], HistogramState]],
) -> list[str]:
    lines = [
        f"# HELP {metric_name} {help_text}",
        f"# TYPE {metric_name} histogram",
    ]

    for labels, histogram in states:
        cumulative = 0
        label_parts = [f'{key}="{_escape_label(value)}"' for key, value in labels.items()]
        for upper_bound, count in zip(buckets, histogram.bucket_counts, strict=True):
            cumulative += count
            bucket_labels = label_parts + [f'le="{_format_float(upper_bound)}"']
            lines.append(f"{metric_name}_bucket{{{','.join(bucket_labels)}}} {cumulative}")

        inf_labels = label_parts + ['le="+Inf"']
        if label_parts:
            lines.append(f"{metric_name}_bucket{{{','.join(inf_labels)}}} {histogram.count}")
            lines.append(f"{metric_name}_count{{{','.join(label_parts)}}} {histogram.count}")
            lines.append(f"{metric_name}_sum{{{','.join(label_parts)}}} {_format_float(histogram.total)}")
        else:
            lines.append(f"{metric_name}_bucket{{le=\"+Inf\"}} {histogram.count}")
            lines.append(f"{metric_name}_count {histogram.count}")
            lines.append(f"{metric_name}_sum {_format_float(histogram.total)}")

    return lines


def render_metrics() -> bytes:
    with _LOCK:
        connected_clients = _STATE.connected_clients
        connected_replicas = _STATE.connected_replicas
        protocol_error_total = _STATE.protocol_error_total
        active_subscriptions = _STATE.active_subscriptions
        background_tasks = _STATE.background_tasks
        commands_total = dict(_STATE.commands_total)
        command_duration_seconds = {
            command: HistogramState(hist.bucket_counts.copy(), hist.count, hist.total)
            for command, hist in _STATE.command_duration_seconds.items()
        }
        bgsave_duration_seconds = HistogramState(
            _STATE.bgsave_duration_seconds.bucket_counts.copy(),
            _STATE.bgsave_duration_seconds.count,
            _STATE.bgsave_duration_seconds.total,
        )

    lines = [
        "# HELP redis_connected_clients Number of currently connected clients",
        "# TYPE redis_connected_clients gauge",
        f"redis_connected_clients {connected_clients}",
        "# HELP redis_connected_replicas Number of currently connected replicas",
        "# TYPE redis_connected_replicas gauge",
        f"redis_connected_replicas {connected_replicas}",
        "# HELP redis_protocol_error_total Total number of protocol errors",
        "# TYPE redis_protocol_error_total counter",
        f"redis_protocol_error_total {protocol_error_total}",
        "# HELP redis_active_subscriptions Total number of active Pub/Sub subscriptions",
        "# TYPE redis_active_subscriptions gauge",
        f"redis_active_subscriptions {active_subscriptions}",
        "# HELP redis_background_tasks Number of currently running background tasks",
        "# TYPE redis_background_tasks gauge",
        f"redis_background_tasks {background_tasks}",
        "# HELP redis_commands_total Total number of commands processed",
        "# TYPE redis_commands_total counter",
    ]

    for (command, status), count in sorted(commands_total.items()):
        lines.append(
            f'redis_commands_total{{command="{_escape_label(command)}",status="{_escape_label(status)}"}} {count}'
        )

    lines.extend(
        _render_histogram(
            "redis_command_duration_seconds",
            "Duration of commands in seconds",
            COMMAND_DURATION_BUCKETS,
            [({"command": command}, histogram) for command, histogram in sorted(command_duration_seconds.items())],
        )
    )
    lines.extend(
        _render_histogram(
            "redis_bgsave_duration_seconds",
            "Duration of BGSAVE operations in seconds",
            BGSAVE_DURATION_BUCKETS,
            [({}, bgsave_duration_seconds)],
        )
    )
    return ("\n".join(lines) + "\n").encode("utf-8")


class _MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        payload = render_metrics()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:
        return


def start_metrics_server(port: int) -> None:
    if port in _STARTED_PORTS:
        return

    try:
        server = ThreadingHTTPServer(("0.0.0.0", port), _MetricsHandler)
    except OSError as e:
        if e.errno in {98, 10048}:
            log.warning("Port %d busy, metrics server skipped", port)
            return
        raise

    thread = threading.Thread(target=server.serve_forever, daemon=True, name=f"metrics-server-{port}")
    thread.start()
    _STARTED_PORTS.add(port)
    log.info("Prometheus metrics available at http://0.0.0.0:%d/metrics", port)
