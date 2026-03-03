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
    if first_line.startswith(b"*") and first_line != b"*-1\r\n":
        count = int(first_line[1:-2])
        result = first_line
        for _ in range(count):
            header = await reader.readline()
            result += header
            if header.startswith(b"$"):
                length = int(header[1:-2])
                payload = await reader.readexactly(length + 2)
                result += payload
        return result
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


@pytest.mark.asyncio
async def test_blpop_immediate_when_list_has_elements():
    """BLPOP returns immediately if the list already has data."""
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)

    # Push an element first
    lpush_resp = await send_command_and_read_response(
        reader, writer,
        b"*3\r\n$5\r\nLPUSH\r\n$5\r\nmykey\r\n$5\r\nhello\r\n",
    )
    assert lpush_resp == b":1\r\n"

    # BLPOP mykey 0
    blpop_resp = await send_command_and_read_response(
        reader, writer,
        b"*3\r\n$5\r\nBLPOP\r\n$5\r\nmykey\r\n$1\r\n0\r\n",
    )
    assert blpop_resp == b"*2\r\n$5\r\nmykey\r\n$5\r\nhello\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_blpop_timeout_returns_nil():
    """BLPOP with short timeout on empty list returns *-1."""
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)

    blpop_resp = await send_command_and_read_response(
        reader, writer,
        b"*3\r\n$5\r\nBLPOP\r\n$5\r\nmykey\r\n$3\r\n0.1\r\n",
    )
    assert blpop_resp == b"*-1\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_blpop_blocks_until_push():
    """BLPOP blocks and unblocks when another client pushes."""
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader1, writer1 = await asyncio.open_connection(*addr)
    reader2, writer2 = await asyncio.open_connection(*addr)

    async def do_blpop():
        return await send_command_and_read_response(
            reader1, writer1,
            b"*3\r\n$5\r\nBLPOP\r\n$5\r\nmykey\r\n$1\r\n5\r\n",
        )

    async def do_push():
        await asyncio.sleep(0.1)
        return await send_command_and_read_response(
            reader2, writer2,
            b"*3\r\n$5\r\nLPUSH\r\n$5\r\nmykey\r\n$5\r\nworld\r\n",
        )

    blpop_result, push_result = await asyncio.gather(do_blpop(), do_push())

    assert push_result == b":1\r\n"
    assert blpop_result == b"*2\r\n$5\r\nmykey\r\n$5\r\nworld\r\n"

    writer1.close()
    await writer1.wait_closed()
    writer2.close()
    await writer2.wait_closed()
    server.close()
    await server.wait_closed()
