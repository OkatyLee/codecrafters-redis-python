import argparse
import asyncio
import logging
import tempfile
from unittest.mock import AsyncMock

from app.command_executor import encode_command_as_resp_array
import pytest  # pyright: ignore[reportMissingImports]

from app.config import ServerConfig
from app.handlers import handle_client
from app.main import main
from app.replication import replication_handshake_and_loop
from app.parser import RESPParser
from app.state import AppState
from app.storage import CacheStorage


class _SocketStub:
    def getsockname(self):
        return ("127.0.0.1", 6379)


class _ServerStub:
    def __init__(self):
        self.sockets = [_SocketStub()]
        self.serve_forever = AsyncMock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


def make_app_state(
    config: ServerConfig | None = None,
    storage: CacheStorage | None = None,
) -> AppState:
    return AppState(
        config or ServerConfig("127.0.0.1", 0),
        storage or CacheStorage(),
        logging.getLogger(__name__),
    )


async def send_command_and_read_response(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    command: bytes,
) -> bytes:
    writer.write(command)
    await writer.drain()
    return await reader.readline()


@pytest.mark.asyncio
async def test_main_starts_server_and_serves_forever(monkeypatch):
    server = _ServerStub()
    captured_callback = None

    monkeypatch.setattr(
        "app.main.parse_args",
        lambda: argparse.Namespace(
            host="localhost",
            port=6379,
            replicaof=None,
            dir="./test_path",
            dbfilename="test_file",
            metrics_port=0,
        ),
    )

    async def fake_start_server(callback, host, port, **kwargs):
        nonlocal captured_callback
        captured_callback = callback
        assert host == "localhost"
        assert port == 6379
        return server

    monkeypatch.setattr("app.main.asyncio.start_server", fake_start_server)

    await main()

    assert captured_callback is not None
    server.serve_forever.assert_awaited_once()


@pytest.mark.asyncio
async def test_replication_handshake_propagates_master_write_to_slave():
    handshake_seen = asyncio.Event()

    async def fake_master(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        parser = RESPParser(reader)

        assert await parser.parse() == [b"PING"]
        writer.write(b"+PONG\r\n")
        await writer.drain()

        assert await parser.parse() == [b"REPLCONF", b"listening-port", b"6380"]
        writer.write(b"+OK\r\n")
        await writer.drain()

        assert await parser.parse() == [b"REPLCONF", b"capa", b"psync2"]
        writer.write(b"+OK\r\n")
        await writer.drain()

        assert await parser.parse() == [b"PSYNC", b"?", b"-1"]
        writer.write(b"+FULLRESYNC test-replid 0\r\n$0\r\n")
        await writer.drain()

        handshake_seen.set()
        writer.write(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 6380, f"127.0.0.1:{master_addr[1]}")
    app_state = make_app_state(config)
    master_reader, master_writer = await asyncio.open_connection(*master_addr)

    await replication_handshake_and_loop(master_reader, master_writer, app_state, 6380)

    assert handshake_seen.is_set()
    assert app_state.storage.get(b"foo") == b"bar"

    master_server.close()
    await master_server.wait_closed()


@pytest.mark.asyncio
async def test_slave_write_is_not_propagated_back_to_master():
    handshake_seen = asyncio.Event()
    capture_finished = asyncio.Event()
    stop_master = asyncio.Event()
    replicated_payloads: list[bytes] = []

    async def fake_master(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        parser = RESPParser(reader)

        assert await parser.parse() == [b"PING"]
        writer.write(b"+PONG\r\n")
        await writer.drain()

        listening_port = await parser.parse()
        assert listening_port is not None
        assert isinstance(listening_port, list)
        assert listening_port[:2] == [b"REPLCONF", b"listening-port"]
        writer.write(b"+OK\r\n")
        await writer.drain()

        assert await parser.parse() == [b"REPLCONF", b"capa", b"psync2"]
        writer.write(b"+OK\r\n")
        await writer.drain()

        assert await parser.parse() == [b"PSYNC", b"?", b"-1"]
        writer.write(b"+FULLRESYNC test-replid 0\r\n$0\r\n")
        await writer.drain()

        handshake_seen.set()
        try:
            payload = await asyncio.wait_for(reader.read(1024), timeout=0.2)
        except TimeoutError:
            payload = b""
        replicated_payloads.append(payload)
        capture_finished.set()

        await stop_master.wait()
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 0, f"127.0.0.1:{master_addr[1]}")
    app_state = make_app_state(config)

    async def handle_client_with_config(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await handle_client(reader, writer, app_state)

    slave_server = await asyncio.start_server(handle_client_with_config, "127.0.0.1", 0)
    slave_addr = slave_server.sockets[0].getsockname()

    master_reader, master_writer = await asyncio.open_connection(*master_addr)
    replication_task = asyncio.create_task(
        replication_handshake_and_loop(master_reader, master_writer, app_state, slave_addr[1])
    )

    await asyncio.wait_for(handshake_seen.wait(), timeout=1)

    client_reader, client_writer = await asyncio.open_connection(*slave_addr)
    response = await send_command_and_read_response(
        client_reader,
        client_writer,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    )

    await asyncio.wait_for(capture_finished.wait(), timeout=1)

    assert response == b"+OK\r\n"
    assert replicated_payloads == [b""]

    client_writer.close()
    await client_writer.wait_closed()
    slave_server.close()
    await slave_server.wait_closed()
    stop_master.set()
    await replication_task
    master_server.close()
    await master_server.wait_closed()


@pytest.mark.asyncio
async def test_replica_reports_zero_offset_for_initial_getack():
    handshake_seen = asyncio.Event()
    ack_received = asyncio.Event()
    stop_master = asyncio.Event()

    async def fake_master(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        parser = RESPParser(reader)

        assert await parser.parse() == [b"PING"]
        writer.write(b"+PONG\r\n")
        await writer.drain()

        listening_port = await parser.parse()
        assert listening_port is not None
        assert isinstance(listening_port, list)
        assert listening_port[:2] == [b"REPLCONF", b"listening-port"]
        writer.write(b"+OK\r\n")
        await writer.drain()

        assert await parser.parse() == [b"REPLCONF", b"capa", b"psync2"]
        writer.write(b"+OK\r\n")
        await writer.drain()

        assert await parser.parse() == [b"PSYNC", b"?", b"-1"]
        writer.write(b"+FULLRESYNC test-replid 0\r\n$0\r\n")
        await writer.drain()

        handshake_seen.set()

        writer.write(encode_command_as_resp_array([b"REPLCONF", b"GETACK", b"*"]))
        await writer.drain()

        ack = await asyncio.wait_for(parser.parse(), timeout=1)
        assert ack == [b"REPLCONF", b"ACK", b"0"]
        ack_received.set()

        await stop_master.wait()
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 6380, f"127.0.0.1:{master_addr[1]}")
    app_state = make_app_state(config)
    master_reader, master_writer = await asyncio.open_connection(*master_addr)

    replication_task = asyncio.create_task(
        replication_handshake_and_loop(master_reader, master_writer, app_state, 6380)
    )

    await asyncio.wait_for(handshake_seen.wait(), timeout=1)
    await asyncio.wait_for(ack_received.wait(), timeout=1)

    stop_master.set()
    await replication_task
    master_server.close()
    await master_server.wait_closed()


# ---------- parse_args tests ----------

def test_parse_args_defaults(monkeypatch):
    monkeypatch.setattr("sys.argv", ["prog"])
    from app.main import parse_args
    args = parse_args()
    assert args.port == 6379
    assert args.host == "127.0.0.1"
    assert args.replicaof is None
    assert args.dbfilename == "dump.rdb"
    assert args.metrics_port == 9100


def test_parse_args_custom_values(monkeypatch):
    monkeypatch.setattr("sys.argv", [
        "prog",
        "--port", "7000",
        "--host", "0.0.0.0",
        "--replicaof", "127.0.0.1:6379",
        "--dir", "/tmp/redis",
        "--dbfilename", "mydb.rdb",
        "--metrics-port", "9200",
    ])
    from app.main import parse_args
    args = parse_args()
    assert args.port == 7000
    assert args.host == "0.0.0.0"
    assert args.replicaof == "127.0.0.1:6379"
    assert args.dir == "/tmp/redis"
    assert args.dbfilename == "mydb.rdb"
    assert args.metrics_port == 9200


def test_parse_args_short_port_flag(monkeypatch):
    monkeypatch.setattr("sys.argv", ["prog", "-p", "9999"])
    from app.main import parse_args
    args = parse_args()
    assert args.port == 9999


# ---------- main() startup tests ----------

@pytest.mark.asyncio
async def test_main_loads_rdb_on_startup(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        # Pre-populate an RDB file
        storage = CacheStorage()
        storage.set(b"preloaded", b"value")
        storage.save(tmpdir, "startup.rdb")

        server = _ServerStub()

        monkeypatch.setattr(
            "app.main.parse_args",
            lambda: argparse.Namespace(
                host="127.0.0.1", port=6379, replicaof=None,
                dir=tmpdir, dbfilename="startup.rdb", metrics_port=0,
            ),
        )
        monkeypatch.setattr("app.main.asyncio.start_server", AsyncMock(return_value=server))
        monkeypatch.setattr("app.main.CacheStorage", lambda: storage)

        await main()

        assert storage.get(b"preloaded") == b"value"


@pytest.mark.asyncio
async def test_main_missing_rdb_does_not_crash(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        server = _ServerStub()

        monkeypatch.setattr(
            "app.main.parse_args",
            lambda: argparse.Namespace(
                host="127.0.0.1", port=6379, replicaof=None,
                dir=tmpdir, dbfilename="missing.rdb", metrics_port=0,
            ),
        )
        monkeypatch.setattr("app.main.asyncio.start_server", AsyncMock(return_value=server))

        await main()  # must not raise


@pytest.mark.asyncio
async def test_main_invalid_replicaof_format_returns_early(monkeypatch, caplog):
    monkeypatch.setattr(
        "app.main.parse_args",
        lambda: argparse.Namespace(
            host="127.0.0.1", port=6380, replicaof="bad-format",
            dir="./data", dbfilename="dump.rdb", metrics_port=0,
        ),
    )

    await main()

    assert "Error: --replicaof must be in the format host:port" in caplog.text


@pytest.mark.asyncio
async def test_main_starts_metrics_server_when_enabled(monkeypatch):
    server = _ServerStub()
    metrics_calls: list[int] = []

    monkeypatch.setattr(
        "app.main.parse_args",
        lambda: argparse.Namespace(
            host="127.0.0.1",
            port=6379,
            replicaof=None,
            dir="./test_path",
            dbfilename="test_file",
            metrics_port=9100,
        ),
    )
    monkeypatch.setattr("app.main.start_metrics_server", lambda port: metrics_calls.append(port))
    monkeypatch.setattr("app.main.asyncio.start_server", AsyncMock(return_value=server))

    await main()

    assert metrics_calls == [9100]
    server.serve_forever.assert_awaited_once()


# ---------- replication_handshake_and_loop error-path tests ----------

@pytest.mark.asyncio
async def test_handshake_aborts_on_bad_ping_response():
    async def fake_master(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        parser = RESPParser(reader)
        await parser.parse()  # consume PING
        writer.write(b"+NOPE\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 6380, f"127.0.0.1:{master_addr[1]}")
    app_state = make_app_state(config)
    master_reader, master_writer = await asyncio.open_connection(*master_addr)

    # Must return without raising
    await replication_handshake_and_loop(master_reader, master_writer, app_state, 6380)

    master_server.close()
    await master_server.wait_closed()


@pytest.mark.asyncio
async def test_handshake_aborts_on_bad_replconf_listening_port_response():
    async def fake_master(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        parser = RESPParser(reader)
        await parser.parse()  # PING
        writer.write(b"+PONG\r\n")
        await writer.drain()
        await parser.parse()  # REPLCONF listening-port
        writer.write(b"-ERR\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 6380, f"127.0.0.1:{master_addr[1]}")
    app_state = make_app_state(config)
    master_reader, master_writer = await asyncio.open_connection(*master_addr)

    await replication_handshake_and_loop(master_reader, master_writer, app_state, 6380)

    master_server.close()
    await master_server.wait_closed()


@pytest.mark.asyncio
async def test_handshake_aborts_on_bad_replconf_capa_response():
    async def fake_master(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        parser = RESPParser(reader)
        await parser.parse()  # PING
        writer.write(b"+PONG\r\n")
        await writer.drain()
        await parser.parse()  # REPLCONF listening-port
        writer.write(b"+OK\r\n")
        await writer.drain()
        await parser.parse()  # REPLCONF capa
        writer.write(b"-ERR\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 6380, f"127.0.0.1:{master_addr[1]}")
    app_state = make_app_state(config)
    master_reader, master_writer = await asyncio.open_connection(*master_addr)

    await replication_handshake_and_loop(master_reader, master_writer, app_state, 6380)

    master_server.close()
    await master_server.wait_closed()


@pytest.mark.asyncio
async def test_handshake_aborts_on_bad_psync_response():
    async def fake_master(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        parser = RESPParser(reader)
        await parser.parse()
        writer.write(b"+PONG\r\n")
        await writer.drain()
        await parser.parse()
        writer.write(b"+OK\r\n")
        await writer.drain()
        await parser.parse()
        writer.write(b"+OK\r\n")
        await writer.drain()
        await parser.parse()  # PSYNC
        writer.write(b"+UNEXPECTED response\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 6380, f"127.0.0.1:{master_addr[1]}")
    app_state = make_app_state(config)
    master_reader, master_writer = await asyncio.open_connection(*master_addr)

    await replication_handshake_and_loop(master_reader, master_writer, app_state, 6380)

    master_server.close()
    await master_server.wait_closed()


@pytest.mark.asyncio
async def test_handshake_aborts_on_invalid_rdb_payload():
    async def fake_master(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        parser = RESPParser(reader)
        await parser.parse()
        writer.write(b"+PONG\r\n")
        await writer.drain()
        await parser.parse()
        writer.write(b"+OK\r\n")
        await writer.drain()
        await parser.parse()
        writer.write(b"+OK\r\n")
        await writer.drain()
        await parser.parse()  # PSYNC
        writer.write(b"+FULLRESYNC test-replid 0\r\n")
        # Send a RESP token instead of the raw RDB bulk transfer payload.
        writer.write(b"+not-a-bulk-string\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 6380, f"127.0.0.1:{master_addr[1]}")
    app_state = make_app_state(config)
    master_reader, master_writer = await asyncio.open_connection(*master_addr)

    await replication_handshake_and_loop(master_reader, master_writer, app_state, 6380)

    master_server.close()
    await master_server.wait_closed()


@pytest.mark.asyncio
async def test_handshake_handles_connection_error_gracefully(capsys):
    """If the master closes the connection mid-handshake, no exception bubbles up."""
    async def fake_master(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Close immediately without sending anything
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 6380, f"127.0.0.1:{master_addr[1]}")
    app_state = make_app_state(config)
    master_reader, master_writer = await asyncio.open_connection(*master_addr)

    # Must return cleanly without raising, regardless of which code-path is taken
    await replication_handshake_and_loop(master_reader, master_writer, app_state, 6380)

    master_server.close()
    await master_server.wait_closed()


@pytest.mark.asyncio
async def test_replica_replies_with_ack_on_getack():
    handshake_seen = asyncio.Event()
    ack_received = asyncio.Event()
    stop_master = asyncio.Event()

    async def fake_master(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        parser = RESPParser(reader)

        assert await parser.parse() == [b"PING"]
        writer.write(b"+PONG\r\n")
        await writer.drain()

        listening_port = await parser.parse()
        assert listening_port is not None
        assert isinstance(listening_port, list)
        assert listening_port[:2] == [b"REPLCONF", b"listening-port"]
        writer.write(b"+OK\r\n")
        await writer.drain()

        assert await parser.parse() == [b"REPLCONF", b"capa", b"psync2"]
        writer.write(b"+OK\r\n")
        await writer.drain()

        assert await parser.parse() == [b"PSYNC", b"?", b"-1"]
        writer.write(b"+FULLRESYNC test-replid 0\r\n$0\r\n")
        await writer.drain()

        handshake_seen.set()

        set_cmd = [b"SET", b"foo", b"bar"]
        getack_cmd = [b"REPLCONF", b"GETACK", b"*"]
        writer.write(encode_command_as_resp_array(set_cmd))
        writer.write(encode_command_as_resp_array(getack_cmd))
        await writer.drain()

        ack = await asyncio.wait_for(parser.parse(), timeout=1)
        expected_offset = len(encode_command_as_resp_array(set_cmd))
        assert ack == [b"REPLCONF", b"ACK", str(expected_offset).encode()]
        ack_received.set()

        await stop_master.wait()
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 6380, f"127.0.0.1:{master_addr[1]}")
    app_state = make_app_state(config)
    master_reader, master_writer = await asyncio.open_connection(*master_addr)

    replication_task = asyncio.create_task(
        replication_handshake_and_loop(master_reader, master_writer, app_state, 6380)
    )

    await asyncio.wait_for(handshake_seen.wait(), timeout=1)
    await asyncio.wait_for(ack_received.wait(), timeout=1)

    stop_master.set()
    await replication_task
    master_server.close()
    await master_server.wait_closed()
