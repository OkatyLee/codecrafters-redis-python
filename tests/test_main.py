import argparse
import asyncio
from importlib.util import _incompatible_extension_module_restrictions
from unittest.mock import AsyncMock

import pytest

import app.storage
from app.config import ServerConfig
from app.handlers import encode_command_as_resp_array, handle_client
from app.main import main, replication_handshake_and_loop
from app.parser import RESPParser


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


@pytest.fixture(autouse=True)
def reset_storage_singleton():
    original_instance = app.storage._storage_instance
    app.storage._storage_instance = None
    try:
        yield
    finally:
        app.storage._storage_instance = original_instance


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
        lambda: argparse.Namespace(host="localhost", port=6379, replicaof=None),
    )

    async def fake_start_server(callback, host, port):
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
        writer.write(b"+FULLRESYNC test-replid 0\r\n$0\r\n\r\n")
        await writer.drain()

        handshake_seen.set()
        writer.write(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 6380, f"127.0.0.1:{master_addr[1]}")
    master_reader, master_writer = await asyncio.open_connection(*master_addr)

    await replication_handshake_and_loop(master_reader, master_writer, config, 6380)

    assert handshake_seen.is_set()
    assert app.storage.get_storage().get(b"foo") == b"bar"

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
        writer.write(b"+FULLRESYNC test-replid 0\r\n$0\r\n\r\n")
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

    async def handle_client_with_config(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await handle_client(reader, writer, config)

    slave_server = await asyncio.start_server(handle_client_with_config, "127.0.0.1", 0)
    slave_addr = slave_server.sockets[0].getsockname()

    master_reader, master_writer = await asyncio.open_connection(*master_addr)
    replication_task = asyncio.create_task(
        replication_handshake_and_loop(master_reader, master_writer, config, slave_addr[1])
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
        writer.write(b"+FULLRESYNC test-replid 0\r\n$0\r\n\r\n")
        await writer.drain()

        handshake_seen.set()

        set_cmd = [b"SET", b"foo", b"bar"]
        getack_cmd = [b"REPLCONF", b"GETACK", b"*"]
        writer.write(encode_command_as_resp_array(set_cmd))
        writer.write(encode_command_as_resp_array(getack_cmd))
        await writer.drain()

        ack = await asyncio.wait_for(parser.parse(), timeout=1)
        expected_offset = len(encode_command_as_resp_array(set_cmd)) + len(
            encode_command_as_resp_array(getack_cmd)
        )
        assert ack == [b"REPLCONF", b"ACK", str(expected_offset).encode()]
        ack_received.set()

        await stop_master.wait()
        writer.close()
        await writer.wait_closed()

    master_server = await asyncio.start_server(fake_master, "127.0.0.1", 0)
    master_addr = master_server.sockets[0].getsockname()

    config = ServerConfig("127.0.0.1", 6380, f"127.0.0.1:{master_addr[1]}")
    master_reader, master_writer = await asyncio.open_connection(*master_addr)

    replication_task = asyncio.create_task(
        replication_handshake_and_loop(master_reader, master_writer, config, 6380)
    )

    await asyncio.wait_for(handshake_seen.wait(), timeout=1)
    await asyncio.wait_for(ack_received.wait(), timeout=1)

    stop_master.set()
    await replication_task
    master_server.close()
    await master_server.wait_closed()
