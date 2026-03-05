import asyncio

import pytest

from app.handlers import handle_client
from app.parser import RESPParser


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


async def send_command_and_parse_response(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    command: bytes,
):
    writer.write(command)
    await writer.drain()
    parser = RESPParser(reader)
    return await parser.parse()


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


@pytest.mark.asyncio
async def test_list_commands_rpush_llen_lrange():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)

    rpush_resp = await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
    )
    llen_resp = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
    )
    lrange_resp = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n",
    )

    assert rpush_resp == b":3\r\n"
    assert llen_resp == b":3\r\n"
    assert lrange_resp == b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_lpop_single_and_count_modes():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)

    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
    )

    lpop_count_resp = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n$1\r\n2\r\n",
    )
    lpop_single_resp = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n",
    )
    lpop_empty_resp = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n",
    )

    assert lpop_count_resp == b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"
    assert lpop_single_resp == b"$1\r\nc\r\n"
    assert lpop_empty_resp == b"$-1\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_type_for_string_list_and_stream():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)

    await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$3\r\nSET\r\n$3\r\nstr\r\n$3\r\nval\r\n",
    )
    await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$1\r\nx\r\n",
    )
    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$4\r\nXADD\r\n$6\r\nstream\r\n$3\r\n1-1\r\n$1\r\nf\r\n$1\r\nv\r\n",
    )

    type_str = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$4\r\nTYPE\r\n$3\r\nstr\r\n",
    )
    type_list = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$4\r\nTYPE\r\n$4\r\nlist\r\n",
    )
    type_stream = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$4\r\nTYPE\r\n$6\r\nstream\r\n",
    )
    type_missing = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$4\r\nTYPE\r\n$7\r\nmissing\r\n",
    )

    assert type_str == b"+string\r\n"
    assert type_list == b"+list\r\n"
    assert type_stream == b"+stream\r\n"
    assert type_missing == b"+None\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xadd_handler_explicit_and_bytes_id():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)

    explicit_resp = await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$4\r\nXADD\r\n$6\r\nstream\r\n$3\r\n1-1\r\n$1\r\nf\r\n$1\r\nv\r\n",
    )
    bytes_id_resp = await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$4\r\nXADD\r\n$6\r\nstream\r\n$3\r\n1-2\r\n$1\r\ng\r\n$1\r\nw\r\n",
    )

    assert explicit_resp == b"+1-1\r\n"
    assert bytes_id_resp == b"+1-2\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_blpop_wrong_number_of_arguments_returns_error():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$5\r\nBLPOP\r\n$5\r\nmykey\r\n",
    )

    assert response == b"-ERR wrong number of arguments for 'blpop' command\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_set_with_ex_and_px_returns_error_and_connection_stays_open():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    bad_set_response = await send_command_and_read_response(
        reader,
        writer,
        b"*7\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n1\r\n$2\r\nPX\r\n$2\r\n10\r\n",
    )
    ping_response = await send_command_and_read_response(
        reader,
        writer,
        b"*1\r\n$4\r\nPING\r\n",
    )

    assert bad_set_response == b"-ERR syntax error. Only one of EX or PX is allowed\r\n"
    assert ping_response == b"+PONG\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xadd_rejects_zero_id_with_error_reply():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$4\r\nXADD\r\n$6\r\nstream\r\n$3\r\n0-0\r\n$1\r\nf\r\n$1\r\nv\r\n",
    )

    assert response == b"-ERR The ID specified in XADD must be greater than 0-0\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xadd_rejects_wrong_payload_arity_with_error_reply():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$4\r\nXADD\r\n$6\r\nstream\r\n$3\r\n1-1\r\n$5\r\nfield\r\n",
    )

    assert response == b"-ERR wrong number of arguments for 'xadd' command\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xadd_rejects_non_stream_key_type_with_error_reply():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$3\r\nSET\r\n$6\r\nstream\r\n$5\r\nvalue\r\n",
    )
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$4\r\nXADD\r\n$6\r\nstream\r\n$3\r\n1-1\r\n$1\r\nf\r\n$1\r\nv\r\n",
    )

    assert response == b"-ERR Expected a dictionary value for stream key\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xread_streams_returns_new_entries_after_id():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$3\r\n1-1\r\n$1\r\nf\r\n$1\r\nv\r\n",
    )

    response = await send_command_and_parse_response(
        reader,
        writer,
        b"*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n",
    )

    assert response == [[b"mystream", [["1-1", [b"f", b"v"]]]]]

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xread_block_streams_unblocks_on_new_entry():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader1, writer1 = await asyncio.open_connection(*addr)
    reader2, writer2 = await asyncio.open_connection(*addr)

    async def do_xread_block():
        return await send_command_and_parse_response(
            reader1,
            writer1,
            b"*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$3\r\n500\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n",
        )

    async def do_xadd_later():
        await asyncio.sleep(0.1)
        return await send_command_and_read_response(
            reader2,
            writer2,
            b"*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$1\r\nf\r\n$1\r\nv\r\n",
        )

    xread_response, xadd_response = await asyncio.gather(do_xread_block(), do_xadd_later())

    assert xadd_response.startswith(b"+")
    assert isinstance(xread_response, list)
    assert len(xread_response) > 0

    first_stream = xread_response[0]
    assert isinstance(first_stream, list)
    assert len(first_stream) == 2
    assert first_stream[0] == b"mystream"

    stream_entries = first_stream[1]
    assert isinstance(stream_entries, list)
    assert len(stream_entries) > 0

    first_entry = stream_entries[0]
    assert isinstance(first_entry, list)
    assert len(first_entry) == 2
    assert first_entry[1] == [b"f", b"v"]

    writer1.close()
    await writer1.wait_closed()
    writer2.close()
    await writer2.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xread_streams_with_dollar_returns_null_without_new_entries():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$3\r\n1-1\r\n$1\r\nf\r\n$1\r\nv\r\n",
    )

    response = await send_command_and_parse_response(
        reader,
        writer,
        b"*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n",
    )

    assert response is None

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xread_block_zero_streams_waits_until_new_entry():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader1, writer1 = await asyncio.open_connection(*addr)
    reader2, writer2 = await asyncio.open_connection(*addr)

    async def do_xread_block_zero():
        return await send_command_and_parse_response(
            reader1,
            writer1,
            b"*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$1\r\n0\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$1\r\n$\r\n",
        )

    async def do_xadd_later():
        await asyncio.sleep(0.1)
        return await send_command_and_read_response(
            reader2,
            writer2,
            b"*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$1\r\nf\r\n$1\r\nv\r\n",
        )

    xread_response, xadd_response = await asyncio.wait_for(
        asyncio.gather(do_xread_block_zero(), do_xadd_later()),
        timeout=2.0,
    )

    assert xadd_response.startswith(b"+")
    assert isinstance(xread_response, list)
    assert len(xread_response) > 0

    first_stream = xread_response[0]
    assert isinstance(first_stream, list)
    assert first_stream[0] == b"mystream"

    stream_entries = first_stream[1]
    assert isinstance(stream_entries, list)
    assert len(stream_entries) > 0

    first_entry = stream_entries[0]
    assert isinstance(first_entry, list)
    assert first_entry[1] == [b"f", b"v"]

    writer1.close()
    await writer1.wait_closed()
    writer2.close()
    await writer2.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xread_rejects_missing_streams_keyword_with_error_reply():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$5\r\nXREAD\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n",
    )

    assert response == b"-ERR syntax error\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xread_rejects_unbalanced_streams_and_ids_with_error_reply():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n",
    )

    assert response == b"-ERR Unbalanced XREAD list of streams\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xread_rejects_non_stream_key_type_with_error_reply():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$3\r\nSET\r\n$9\r\nnotstream\r\n$5\r\nvalue\r\n",
    )
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$9\r\nnotstream\r\n$3\r\n0-0\r\n",
    )

    assert response == b"-ERR Key b'notstream' is not a stream\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    
    
@pytest.mark.asyncio
async def test_incr_command_with_correct_key():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    set_response = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$1\r\n5\r\n",
    )
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$4\r\nINCR\r\n$3\r\nkey\r\n",
    )

    assert set_response == b"+OK\r\n" 
    assert response == b":6\r\n"
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    
    
@pytest.mark.asyncio
async def test_incr_command_with_unexising_key():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$4\r\nINCR\r\n$3\r\nkey\r\n",
    )

    assert response == b":1\r\n"
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    
@pytest.mark.asyncio
async def test_incr_command_with_non_integer_value():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\nnotint\r\n",
    )
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$4\r\nINCR\r\n$3\r\nkey\r\n",
    )
    print(response)
    assert response == b"-ERR Value at key is not an integer\r\n"
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_multi_exec_queues_and_executes_commands_in_order():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    multi_response = await send_command_and_read_response(
        reader,
        writer,
        b"*1\r\n$5\r\nMULTI\r\n",
    )
    queued_set_response = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    )
    queued_get_response = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
    )
    exec_response = await send_command_and_read_response(
        reader,
        writer,
        b"*1\r\n$4\r\nEXEC\r\n",
    )
    get_after_exec_response = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
    )

    assert multi_response == b"+OK\r\n"
    assert queued_set_response == b"+QUEUED\r\n"
    assert queued_get_response == b"+QUEUED\r\n"
    assert exec_response == b"*2\r\n+OK\r\n$3\r\nbar\r\n"
    assert get_after_exec_response == b"$3\r\nbar\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_discard_clears_transaction_queue_without_applying_changes():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    multi_response = await send_command_and_read_response(
        reader,
        writer,
        b"*1\r\n$5\r\nMULTI\r\n",
    )
    queued_set_response = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    )
    discard_response = await send_command_and_read_response(
        reader,
        writer,
        b"*1\r\n$7\r\nDISCARD\r\n",
    )
    get_after_discard_response = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
    )

    assert multi_response == b"+OK\r\n"
    assert queued_set_response == b"+QUEUED\r\n"
    assert discard_response == b"+OK\r\n"
    assert get_after_discard_response == b"$-1\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_exec_without_multi_returns_error():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*1\r\n$4\r\nEXEC\r\n",
    )

    assert response == b"-ERR EXEC without MULTI\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_discard_without_multi_returns_error():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*1\r\n$7\r\nDISCARD\r\n",
    )

    assert response == b"-ERR DISCARD without MULTI\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
