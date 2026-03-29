import asyncio
import logging
import tempfile

import pytest  # pyright: ignore[reportMissingImports]

from app.config import ServerConfig
from app.handlers import handle_client
from app.main import replication_handshake_and_loop
from app.parser import RESPParser
from app.state import AppState
from app.storage import CacheStorage


def make_app_state(config: ServerConfig) -> AppState:
    return AppState(config, CacheStorage(), logging.getLogger(__name__))


async def _start_server(app_state: AppState) -> asyncio.base_events.Server:
    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_client(reader, writer, app_state)

    return await asyncio.start_server(_handle, app_state.config.host, app_state.config.port)


async def _send_command_and_parse_response(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    command: bytes,
):
    writer.write(command)
    await writer.drain()
    return await RESPParser(reader).parse()


async def _wait_until(predicate, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Timed out waiting for condition")


@pytest.mark.asyncio
async def test_smoke_master_write_replicates_to_slave_and_wait_observes_ack():
    master_config = ServerConfig("127.0.0.1", 0)
    slave_config = ServerConfig("127.0.0.1", 0, "127.0.0.1:1")
    master_app_state = make_app_state(master_config)
    slave_app_state = make_app_state(slave_config)

    master_server = await _start_server(master_app_state)
    slave_server = await _start_server(slave_app_state)
    master_addr = master_server.sockets[0].getsockname()
    slave_addr = slave_server.sockets[0].getsockname()

    slave_config.replicaof = f"127.0.0.1:{master_addr[1]}"

    master_reader, master_writer = await asyncio.open_connection(*master_addr)
    replication_task = asyncio.create_task(
        replication_handshake_and_loop(master_reader, master_writer, slave_app_state, slave_addr[1])
    )
    await _wait_until(lambda: len(master_app_state.get_replicas()) == 1)

    master_client_reader, master_client_writer = await asyncio.open_connection(*master_addr)
    slave_client_reader, slave_client_writer = await asyncio.open_connection(*slave_addr)

    try:
        set_response = await _send_command_and_parse_response(
            master_client_reader,
            master_client_writer,
            b"*3\r\n$3\r\nSET\r\n$5\r\nsmoke\r\n$5\r\nvalue\r\n",
        )
        wait_response = await _send_command_and_parse_response(
            master_client_reader,
            master_client_writer,
            b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n500\r\n",
        )
        replicated_value = await _send_command_and_parse_response(
            slave_client_reader,
            slave_client_writer,
            b"*2\r\n$3\r\nGET\r\n$5\r\nsmoke\r\n",
        )

        assert set_response == "OK"
        assert wait_response == 1
        assert replicated_value == b"value"
    finally:
        master_client_writer.close()
        await master_client_writer.wait_closed()
        slave_client_writer.close()
        await slave_client_writer.wait_closed()
        master_writer.close()
        await master_writer.wait_closed()
        await replication_task
        slave_server.close()
        await slave_server.wait_closed()
        master_server.close()
        await master_server.wait_closed()


@pytest.mark.asyncio
async def test_smoke_save_restart_and_load_restores_written_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ServerConfig("127.0.0.1", 0, dir=tmpdir, dbfilename="smoke.rdb")
        app_state = make_app_state(config)
        server = await _start_server(app_state)
        addr = server.sockets[0].getsockname()

        reader, writer = await asyncio.open_connection(*addr)
        try:
            assert await _send_command_and_parse_response(
                reader,
                writer,
                b"*3\r\n$3\r\nSET\r\n$4\r\nkeep\r\n$4\r\ndata\r\n",
            ) == "OK"
            assert await _send_command_and_parse_response(
                reader,
                writer,
                b"*1\r\n$4\r\nSAVE\r\n",
            ) == "OK"
        finally:
            writer.close()
            await writer.wait_closed()
            server.close()
            await server.wait_closed()

        restarted_config = ServerConfig("127.0.0.1", 0, dir=tmpdir, dbfilename="smoke.rdb")
        restarted_app_state = make_app_state(restarted_config)
        restarted_app_state.storage.load(restarted_config.dir, restarted_config.dbfilename)

        restarted_server = await _start_server(restarted_app_state)
        restarted_addr = restarted_server.sockets[0].getsockname()
        restarted_reader, restarted_writer = await asyncio.open_connection(*restarted_addr)

        try:
            restored = await _send_command_and_parse_response(
                restarted_reader,
                restarted_writer,
                b"*2\r\n$3\r\nGET\r\n$4\r\nkeep\r\n",
            )
            assert restored == b"data"
        finally:
            restarted_writer.close()
            await restarted_writer.wait_closed()
            restarted_server.close()
            await restarted_server.wait_closed()
