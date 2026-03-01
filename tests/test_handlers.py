import asyncio

import pytest

from app.handlers import handle_client


@pytest.fixture(autouse=True)
def reset_storage_singleton():
    import app.storage

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

    first_line = await reader.readline()
    if first_line.startswith(b"$") and first_line != b"$-1\r\n":
        length = int(first_line[1:-2])
        payload = await reader.readexactly(length + 2)
        return first_line + payload
    return first_line


@pytest.mark.asyncio
async def test_handle_client_ping():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(reader, writer, b"*1\r\n$4\r\nPING\r\n")

    assert response == b"+PONG\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_handle_client_echo():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(reader, writer, b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")

    assert response == b"$5\r\nhello\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_handle_client_set_and_get():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    set_response = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
    )
    get_response = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
    )

    assert set_response == b"+OK\r\n"
    assert get_response == b"$5\r\nvalue\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_handle_client_get_missing_returns_null_bulk_string():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n",
    )

    assert response == b"$-1\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_handle_client_unknown_command():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(reader, writer, b"*1\r\n$3\r\nFOO\r\n")

    assert response == b"-ERR unknown command\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_handle_client_set_with_px_expires():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    set_response = await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$2\r\n50\r\n",
    )
    await asyncio.sleep(0.07)
    get_response = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
    )

    assert set_response == b"+OK\r\n"
    assert get_response == b"$-1\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
