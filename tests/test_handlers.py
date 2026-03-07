import asyncio

import pytest

from app.config import ServerConfig
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


async def read_fullresync_and_rdb(reader: asyncio.StreamReader) -> tuple[bytes, bytes]:
    first_line = await reader.readline()
    bulk_header = await reader.readline()
    length = int(bulk_header[1:-2])
    payload = await reader.readexactly(length + 2)
    return first_line, bulk_header + payload


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


@pytest.mark.asyncio
async def test_master_propagates_set_to_multiple_replicas():
    config = ServerConfig("127.0.0.1", 0)

    async def handle_client_with_config(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await handle_client(reader, writer, config)

    server = await asyncio.start_server(handle_client_with_config, "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    replica1_reader, replica1_writer = await asyncio.open_connection(*addr)
    replica2_reader, replica2_writer = await asyncio.open_connection(*addr)
    client_reader, client_writer = await asyncio.open_connection(*addr)

    for replica_reader, replica_writer, port in (
        (replica1_reader, replica1_writer, 6380),
        (replica2_reader, replica2_writer, 6381),
    ):
        replconf_response = await send_command_and_read_response(
            replica_reader,
            replica_writer,
            f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(port))}\r\n{port}\r\n".encode(),
        )
        replica_writer.write(
            b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
        )
        await replica_writer.drain()
        fullresync_line, rdb_response = await read_fullresync_and_rdb(replica_reader)

        assert replconf_response == b"+OK\r\n"
        assert fullresync_line.startswith(b"+FULLRESYNC ")
        assert rdb_response.startswith(b"$")

    set_response = await send_command_and_read_response(
        client_reader,
        client_writer,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    )

    replica1_command = await send_command_and_read_response(replica1_reader, replica1_writer, b"")
    replica2_command = await send_command_and_read_response(replica2_reader, replica2_writer, b"")

    assert set_response == b"+OK\r\n"
    assert replica1_command == b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    assert replica2_command == b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"

    client_writer.close()
    await client_writer.wait_closed()
    replica1_writer.close()
    await replica1_writer.wait_closed()
    replica2_writer.close()
    await replica2_writer.wait_closed()
    server.close()
    await server.wait_closed()


# ---------------------------------------------------------------------------
# WAIT command tests
# ---------------------------------------------------------------------------

def _make_server_with_config(config: ServerConfig):
    """Helper: start a server and return (server, addr)."""
    import asyncio

    async def _handle(reader, writer):
        await handle_client(reader, writer, config)

    return _handle


async def _connect_replica(addr, config: ServerConfig) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Connect a replica via REPLCONF + PSYNC and return the (reader, writer) pair."""
    reader, writer = await asyncio.open_connection(*addr)
    # REPLCONF listening-port
    writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n9999\r\n")
    await writer.drain()
    assert await reader.readline() == b"+OK\r\n"
    # PSYNC
    writer.write(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
    await writer.drain()
    _, _ = await read_fullresync_and_rdb(reader)
    return reader, writer


@pytest.mark.asyncio
async def test_wait_zero_numreplicas_returns_zero():
    """WAIT 0 <timeout> should return 0 immediately."""
    config = ServerConfig("127.0.0.1", 0)
    server = await asyncio.start_server(_make_server_with_config(config), "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    client_reader, client_writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        client_reader, client_writer,
        b"*3\r\n$4\r\nWAIT\r\n$1\r\n0\r\n$3\r\n500\r\n",
    )
    assert response == b":0\r\n"

    client_writer.close()
    await client_writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_wait_no_replicas_returns_zero():
    """WAIT when no replicas are connected should return 0 immediately."""
    config = ServerConfig("127.0.0.1", 0)
    server = await asyncio.start_server(_make_server_with_config(config), "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    client_reader, client_writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        client_reader, client_writer,
        b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$1\r\n0\r\n",
    )
    assert response == b":0\r\n"

    client_writer.close()
    await client_writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_wait_no_writes_returns_replica_count_immediately():
    """If no writes have been made (offset=0), WAIT returns replica count right away."""
    config = ServerConfig("127.0.0.1", 0)
    server = await asyncio.start_server(_make_server_with_config(config), "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    replica_reader, replica_writer = await _connect_replica(addr, config)
    client_reader, client_writer = await asyncio.open_connection(*addr)

    response = await send_command_and_read_response(
        client_reader, client_writer,
        b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n100\r\n",
    )
    assert response == b":1\r\n"

    client_writer.close()
    await client_writer.wait_closed()
    replica_writer.close()
    await replica_writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_wait_timeout_zero_no_ack_returns_zero():
    """WAIT with timeout=0 and a write that has not been ACKed returns 0."""
    config = ServerConfig("127.0.0.1", 0)
    server = await asyncio.start_server(_make_server_with_config(config), "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    replica_reader, replica_writer = await _connect_replica(addr, config)
    client_reader, client_writer = await asyncio.open_connection(*addr)

    # Perform a write to advance master_repl_offset
    set_resp = await send_command_and_read_response(
        client_reader, client_writer,
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
    )
    assert set_resp == b"+OK\r\n"

    # Drain the SET command that was propagated to the replica
    await replica_reader.readexactly(len(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"))

    # WAIT with timeout=0 — replica won't reply to GETACK in time
    response = await send_command_and_read_response(
        client_reader, client_writer,
        b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$1\r\n0\r\n",
    )
    assert response == b":0\r\n"

    client_writer.close()
    await client_writer.wait_closed()
    replica_writer.close()
    await replica_writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_wait_replica_acks_in_time_returns_one():
    """WAIT returns 1 when a replica sends ACK within the timeout."""
    config = ServerConfig("127.0.0.1", 0)
    server = await asyncio.start_server(_make_server_with_config(config), "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    replica_reader, replica_writer = await _connect_replica(addr, config)
    client_reader, client_writer = await asyncio.open_connection(*addr)

    set_cmd = b"*3\r\n$3\r\nSET\r\n$2\r\nfn\r\n$3\r\nbar\r\n"
    set_resp = await send_command_and_read_response(client_reader, client_writer, set_cmd)
    assert set_resp == b"+OK\r\n"

    # Consume the propagated SET on the replica side
    propagated_set = b"*3\r\n$3\r\nSET\r\n$2\r\nfn\r\n$3\r\nbar\r\n"
    await replica_reader.readexactly(len(propagated_set))

    async def replica_respond_to_getack():
        """Read GETACK from master, reply with ACK carrying correct offset."""
        resp_parser = RESPParser(replica_reader)
        msg = await asyncio.wait_for(resp_parser.parse(), timeout=1)
        assert msg == [b"REPLCONF", b"GETACK", b"*"]
        # Replica ACKs with the number of bytes it received (SET cmd + GETACK cmd)
        from app.handlers import encode_command_as_resp_array
        set_len = len(encode_command_as_resp_array([b"SET", b"fn", b"bar"]))
        getack_len = len(encode_command_as_resp_array([b"REPLCONF", b"GETACK", b"*"]))
        ack_offset = set_len + getack_len
        ack = encode_command_as_resp_array([b"REPLCONF", b"ACK", str(ack_offset).encode()])
        replica_writer.write(ack)
        await replica_writer.drain()

    wait_task = asyncio.create_task(
        send_command_and_read_response(
            client_reader, client_writer,
            b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n500\r\n",
        )
    )
    await asyncio.sleep(0.05)
    await replica_respond_to_getack()

    response = await asyncio.wait_for(wait_task, timeout=2)
    assert response == b":1\r\n"

    client_writer.close()
    await client_writer.wait_closed()
    replica_writer.close()
    await replica_writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_wait_timeout_expires_when_replica_does_not_ack():
    """WAIT returns 0 when replica ignores GETACK and timeout expires."""
    config = ServerConfig("127.0.0.1", 0)
    server = await asyncio.start_server(_make_server_with_config(config), "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    replica_reader, replica_writer = await _connect_replica(addr, config)
    client_reader, client_writer = await asyncio.open_connection(*addr)

    set_resp = await send_command_and_read_response(
        client_reader, client_writer,
        b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n",
    )
    assert set_resp == b"+OK\r\n"

    # Drain propagated SET but do NOT reply to GETACK
    await replica_reader.readexactly(len(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n"))

    # WAIT with 100 ms timeout — replica silent, should time out and return 0
    response = await asyncio.wait_for(
        send_command_and_read_response(
            client_reader, client_writer,
            b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$3\r\n100\r\n",
        ),
        timeout=2,
    )
    assert response == b":0\r\n"

    client_writer.close()
    await client_writer.wait_closed()
    replica_writer.close()
    await replica_writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_wait_two_replicas_only_one_acks():
    """WAIT 2 500 returns 1 when only one of two replicas ACKs in time."""
    config = ServerConfig("127.0.0.1", 0)
    server = await asyncio.start_server(_make_server_with_config(config), "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    replica1_reader, replica1_writer = await _connect_replica(addr, config)
    replica2_reader, replica2_writer = await _connect_replica(addr, config)
    client_reader, client_writer = await asyncio.open_connection(*addr)

    set_cmd_raw = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n"
    set_resp = await send_command_and_read_response(client_reader, client_writer, set_cmd_raw)
    assert set_resp == b"+OK\r\n"

    # Both replicas drain the propagated SET
    propagated = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n"
    await replica1_reader.readexactly(len(propagated))
    await replica2_reader.readexactly(len(propagated))

    from app.handlers import encode_command_as_resp_array

    async def replica1_acks():
        parser = RESPParser(replica1_reader)
        msg = await asyncio.wait_for(parser.parse(), timeout=1)
        assert msg == [b"REPLCONF", b"GETACK", b"*"]
        set_len = len(encode_command_as_resp_array([b"SET", b"a", b"b"]))
        getack_len = len(encode_command_as_resp_array([b"REPLCONF", b"GETACK", b"*"]))
        ack = encode_command_as_resp_array(
            [b"REPLCONF", b"ACK", str(set_len + getack_len).encode()]
        )
        replica1_writer.write(ack)
        await replica1_writer.drain()

    # Replica 2 reads GETACK but never replies
    async def replica2_ignores():
        parser = RESPParser(replica2_reader)
        await asyncio.wait_for(parser.parse(), timeout=1)  # consume GETACK

    wait_task = asyncio.create_task(
        send_command_and_read_response(
            client_reader, client_writer,
            b"*3\r\n$4\r\nWAIT\r\n$1\r\n2\r\n$3\r\n300\r\n",
        )
    )
    await asyncio.sleep(0.05)
    await asyncio.gather(replica1_acks(), replica2_ignores())

    response = await asyncio.wait_for(wait_task, timeout=2)
    assert response == b":1\r\n"

    client_writer.close()
    await client_writer.wait_closed()
    replica1_writer.close()
    await replica1_writer.wait_closed()
    replica2_writer.close()
    await replica2_writer.wait_closed()
    server.close()
    await server.wait_closed()
