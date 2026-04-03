import asyncio
import logging
import os
import tempfile
from typing import cast

import pytest    # pyright: ignore[reportMissingImports]

from app.config import ServerConfig
from app.handlers import handle_client
from app.parser import RESPParser
from app.state import AppState
from app.storage import CacheStorage


def make_app_state(
    config: ServerConfig | None = None,
    storage: CacheStorage | None = None,
) -> AppState:
    return AppState(
        config or ServerConfig("127.0.0.1", 0),
        storage or CacheStorage(),
        logging.getLogger(__name__),
    )


async def start_test_server(
    config: ServerConfig | None = None,
    app_state: AppState | None = None,
) -> tuple[asyncio.base_events.Server, AppState]:
    state = app_state or make_app_state(config)

    async def _handle(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await handle_client(reader, writer, state)

    server = await asyncio.start_server(_handle, state.config.host, 0)
    return server, state


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


async def send_command_and_parse_n_responses(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    command: bytes,
    count: int,
):
    writer.write(command)
    await writer.drain()
    parser = RESPParser(reader)
    return [await parser.parse() for _ in range(count)]


async def read_fullresync_and_rdb(reader: asyncio.StreamReader) -> tuple[bytes, bytes]:
    first_line = await reader.readline()
    bulk_header = await reader.readline()
    length = int(bulk_header[1:-2])
    payload = await reader.readexactly(length)
    return first_line, bulk_header + payload


@pytest.mark.asyncio
async def test_handle_client_ping():
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
async def test_handle_client_set_with_lowercase_px_expires():
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    set_response = await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\npx\r\n$2\r\n50\r\n",
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
async def test_lrange_wrongtype_returns_error_for_string_and_sorted_set_keys():
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)

    await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$3\r\nSET\r\n$7\r\nstr_key\r\n$5\r\nvalue\r\n",
    )
    await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$4\r\nZADD\r\n$8\r\nzset_key\r\n$3\r\n1.0\r\n$3\r\nfoo\r\n",
    )

    wrongtype_on_string = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nLRANGE\r\n$7\r\nstr_key\r\n$1\r\n0\r\n$2\r\n-1\r\n",
    )
    wrongtype_on_zset = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nLRANGE\r\n$8\r\nzset_key\r\n$1\r\n0\r\n$2\r\n-1\r\n",
    )

    expected = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    assert wrongtype_on_string == expected
    assert wrongtype_on_zset == expected

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_lpop_single_and_count_modes():
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    assert type_missing == b"+none\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xadd_handler_explicit_and_bytes_id():
    server, app_state = await start_test_server()
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

    assert explicit_resp == b"$3\r\n1-1\r\n"
    assert bytes_id_resp == b"$3\r\n1-2\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_blpop_wrong_number_of_arguments_returns_error():
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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

    assert response == [[b"mystream", [[b"1-1", [b"f", b"v"]]]]]

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xrange_compares_stream_ids_numerically():
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$3\r\n2-0\r\n$1\r\nf\r\n$1\r\na\r\n",
    )
    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$4\r\n10-0\r\n$1\r\ng\r\n$1\r\nb\r\n",
    )

    response = await send_command_and_parse_response(
        reader,
        writer,
        b"*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$3\r\n2-0\r\n$4\r\n10-0\r\n",
    )

    assert response == [
        [b"2-0", [b"f", b"a"]],
        [b"10-0", [b"g", b"b"]],
    ]

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_handle_client_inline_ping():
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(reader, writer, b"PING\r\n")

    assert response == b"+PONG\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xread_compares_stream_ids_numerically():
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$3\r\n2-0\r\n$1\r\nf\r\n$1\r\na\r\n",
    )
    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$4\r\n10-0\r\n$1\r\ng\r\n$1\r\nb\r\n",
    )

    response = await send_command_and_parse_response(
        reader,
        writer,
        b"*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n$3\r\n2-0\r\n",
    )

    assert response == [[b"mystream", [[b"10-0", [b"g", b"b"]]]]]

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xread_block_streams_unblocks_on_new_entry():
    server, app_state = await start_test_server()
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
    assert xadd_response.startswith(b"$")
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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

    assert xadd_response.startswith(b"$")
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
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$5\r\nXREAD\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n",
    )

    assert response == b"-ERR wrong number of arguments for 'xread' command\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xread_rejects_unbalanced_streams_and_ids_with_error_reply():
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$8\r\nmystream\r\n",
    )

    assert response == b"-ERR wrong number of arguments for 'xread' command\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_xread_rejects_non_stream_key_type_with_error_reply():
    server, app_state = await start_test_server()
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
async def test_xrange_wrongtype_returns_error_for_string_and_sorted_set_keys():
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)

    await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$3\r\nSET\r\n$10\r\nnotstream1\r\n$5\r\nvalue\r\n",
    )
    await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$4\r\nZADD\r\n$10\r\nnotstream2\r\n$3\r\n1.0\r\n$3\r\nfoo\r\n",
    )

    wrongtype_on_string = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nXRANGE\r\n$10\r\nnotstream1\r\n$1\r\n-\r\n$1\r\n+\r\n",
    )
    wrongtype_on_zset = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nXRANGE\r\n$10\r\nnotstream2\r\n$1\r\n-\r\n$1\r\n+\r\n",
    )

    expected = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    assert wrongtype_on_string == expected
    assert wrongtype_on_zset == expected

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    
    
@pytest.mark.asyncio
async def test_incr_command_with_correct_key():
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    assert response == b"-ERR Value at key is not an integer\r\n"
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_multi_exec_queues_and_executes_commands_in_order():
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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
    server, app_state = await start_test_server()
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

    server, app_state = await start_test_server(config)
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
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(replica_reader.read(2), timeout=0.05)

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


async def _read_replica_command(reader: asyncio.StreamReader) -> list[bytes]:
    parsed = await RESPParser(reader).parse()
    assert isinstance(parsed, list)
    assert all(isinstance(part, bytes) for part in parsed)
    return cast(list[bytes], parsed)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("setup_commands", "command", "expected_response", "expected_replica_command"),
    [
        pytest.param((), [b"LPUSH", b"mylist", b"hello"], b":1\r\n", [b"LPUSH", b"mylist", b"hello"], id="lpush"),
        pytest.param((), [b"RPUSH", b"mylist", b"a", b"b"], b":2\r\n", [b"RPUSH", b"mylist", b"a", b"b"], id="rpush"),
        pytest.param(([b"RPUSH", b"mylist", b"a", b"b"],), [b"LPOP", b"mylist"], b"$1\r\na\r\n", [b"LPOP", b"mylist"], id="lpop"),
        pytest.param((), [b"XADD", b"mystream", b"1-1", b"f", b"v"], b"$3\r\n1-1\r\n", [b"XADD", b"mystream", b"1-1", b"f", b"v"], id="xadd"),
        pytest.param((), [b"INCR", b"counter"], b":1\r\n", [b"INCR", b"counter"], id="incr"),
        pytest.param(([b"SET", b"key", b"value"],), [b"DEL", b"key"], b"+OK\r\n", [b"DEL", b"key"], id="del"),
        pytest.param((), [b"ZADD", b"zset_key", b"1.0", b"foo"], b":1\r\n", [b"ZADD", b"zset_key", b"1.0", b"foo"], id="zadd"),
        pytest.param(([b"ZADD", b"zset_key", b"1.0", b"foo"],), [b"ZREM", b"zset_key", b"foo"], b":1\r\n", [b"ZREM", b"zset_key", b"foo"], id="zrem"),
        pytest.param((), [b"GEOADD", b"geo_key", b"13.361389", b"38.115556", b"foo"], b":1\r\n", [b"GEOADD", b"geo_key", b"13.361389", b"38.115556", b"foo"], id="geoadd"),
    ],
)
async def test_master_propagates_other_write_commands_to_replica(
    setup_commands,
    command,
    expected_response,
    expected_replica_command,
):
    from app.command_executor import encode_command_as_resp_array

    config = ServerConfig("127.0.0.1", 0)
    server = await asyncio.start_server(_make_server_with_config(config), "127.0.0.1", 0)
    addr = server.sockets[0].getsockname()

    replica_reader, replica_writer = await _connect_replica(addr, config)
    client_reader, client_writer = await asyncio.open_connection(*addr)

    try:
        for setup_command in setup_commands:
            setup_response = await send_command_and_read_response(
                client_reader,
                client_writer,
                encode_command_as_resp_array(setup_command),
            )
            assert not setup_response.startswith(b"-")
            assert await _read_replica_command(replica_reader) == list(setup_command)

        response = await send_command_and_read_response(
            client_reader,
            client_writer,
            encode_command_as_resp_array(command),
        )
        replicated_command = await _read_replica_command(replica_reader)

        assert response == expected_response
        assert replicated_command == expected_replica_command
    finally:
        client_writer.close()
        await client_writer.wait_closed()
        replica_writer.close()
        await replica_writer.wait_closed()
        server.close()
        await server.wait_closed()


# ---------------------------------------------------------------------------
# WAIT command tests
# ---------------------------------------------------------------------------

def _make_server_with_config(config: ServerConfig):
    """Helper: start a server and return (server, addr)."""
    app_state = make_app_state(config)

    async def _handle(reader, writer):
        await handle_client(reader, writer, app_state)

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
        from app.command_executor import encode_command_as_resp_array
        resp_parser = RESPParser(replica_reader)
        msg = await asyncio.wait_for(resp_parser.parse(), timeout=1)
        assert msg == [b"REPLCONF", b"GETACK", b"*"]
        # Replica ACKs with the number of bytes it received (SET cmd + GETACK cmd)
        
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


    async def replica1_acks():
        from app.command_executor import encode_command_as_resp_array
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


# ---------- SAVE / BGSAVE handler tests ----------

@pytest.mark.asyncio
async def test_handle_save_command_returns_ok():
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ServerConfig("127.0.0.1", 6379, dir=tmpdir, dbfilename="test.rdb")

        server, app_state = await start_test_server(config)
        addr = server.sockets[0].getsockname()

        reader, writer = await asyncio.open_connection(*addr)
        response = await send_command_and_read_response(reader, writer, b"*1\r\n$4\r\nSAVE\r\n")

        assert response == b"+OK\r\n"
        assert os.path.exists(os.path.join(tmpdir, "test.rdb"))

        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_handle_save_persists_and_load_restores():
    with tempfile.TemporaryDirectory() as tmpdir:
        app_state = AppState(
            ServerConfig("127.0.0.1", 6379, dir=tmpdir, dbfilename="test.rdb"),
            CacheStorage(),
            logging.getLogger(__name__)
        )

        async def handler(reader, writer):
            await handle_client(reader, writer, app_state)

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        addr = server.sockets[0].getsockname()

        reader, writer = await asyncio.open_connection(*addr)
        await send_command_and_read_response(reader, writer, b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
        await send_command_and_read_response(reader, writer, b"*1\r\n$4\r\nSAVE\r\n")

        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()

        restored_storage = CacheStorage()
        restored_storage.load(tmpdir, "test.rdb")
        assert restored_storage.get(b"hello") == b"world"


@pytest.mark.asyncio
async def test_handle_bgsave_command_returns_background_saving_started():
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ServerConfig("127.0.0.1", 6379, dir=tmpdir, dbfilename="bg.rdb")

        server, app_state = await start_test_server(config)
        addr = server.sockets[0].getsockname()

        reader, writer = await asyncio.open_connection(*addr)
        response = await send_command_and_read_response(reader, writer, b"*1\r\n$6\r\nBGSAVE\r\n")

        assert response == b"+Background saving started\r\n"
        await asyncio.sleep(0.1)
        assert os.path.exists(os.path.join(tmpdir, "bg.rdb"))

        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


# ---------- KEYS handler tests ----------

@pytest.mark.asyncio
async def test_handle_keys_star_returns_all_keys():
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    await send_command_and_parse_response(reader, writer, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n1\r\n")
    await send_command_and_parse_response(reader, writer, b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$1\r\n2\r\n")

    response = await send_command_and_parse_response(reader, writer, b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n")

    assert isinstance(response, list)
    assert set(response) == {b"foo", b"bar"}

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_handle_keys_pattern_filters():
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    await send_command_and_parse_response(reader, writer, b"*3\r\n$3\r\nSET\r\n$6\r\nfoobar\r\n$1\r\n1\r\n")
    await send_command_and_parse_response(reader, writer, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n2\r\n")
    await send_command_and_parse_response(reader, writer, b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$1\r\n3\r\n")

    response = await send_command_and_parse_response(reader, writer, b"*2\r\n$4\r\nKEYS\r\n$4\r\nfoo*\r\n")

    assert isinstance(response, list)
    assert set(response) == {b"foobar", b"foo"}

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_handle_keys_no_match_returns_empty_array():
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_parse_response(reader, writer, b"*2\r\n$4\r\nKEYS\r\n$4\r\nnope\r\n")

    assert response == []

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_default_user_is_auto_authenticated_with_nopass():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    whoami_response = await send_command_and_parse_response(
        reader,
        writer,
        b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n",
    )
    ping_response = await send_command_and_read_response(
        reader,
        writer,
        b"*1\r\n$4\r\nPING\r\n",
    )

    assert whoami_response == b"default"
    assert ping_response == b"+PONG\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_setting_default_password_requires_auth_for_new_connections_only():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()

    authed_reader, authed_writer = await asyncio.open_connection(*addr)

    setuser_response = await send_command_and_read_response(
        authed_reader,
        authed_writer,
        b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\ndefault\r\n$7\r\n>secret\r\n",
    )
    old_session_ping = await send_command_and_read_response(
        authed_reader,
        authed_writer,
        b"*1\r\n$4\r\nPING\r\n",
    )
    second_reader, second_writer = await asyncio.open_connection(*addr)
    noauth_response = await send_command_and_read_response(
        second_reader,
        second_writer,
        b"*1\r\n$4\r\nPING\r\n",
    )
    wrongpass_response = await send_command_and_read_response(
        second_reader,
        second_writer,
        b"*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n",
    )
    auth_response = await send_command_and_read_response(
        second_reader,
        second_writer,
        b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n",
    )
    whoami_response = await send_command_and_parse_response(
        second_reader,
        second_writer,
        b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n",
    )
    ping_after_auth = await send_command_and_read_response(
        second_reader,
        second_writer,
        b"*1\r\n$4\r\nPING\r\n",
    )

    assert setuser_response == b"+OK\r\n"
    assert old_session_ping == b"+PONG\r\n"
    assert noauth_response == b"-NOAUTH  Authentication required.\r\n"
    assert wrongpass_response == b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"
    assert auth_response == b"+OK\r\n"
    assert whoami_response == b"default"
    assert ping_after_auth == b"+PONG\r\n"

    authed_writer.close()
    await authed_writer.wait_closed()
    second_writer.close()
    await second_writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_acl_setuser_creates_user_and_auth_switches_session_user():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()

    admin_reader, admin_writer = await asyncio.open_connection(*addr)
    user_reader, user_writer = await asyncio.open_connection(*addr)

    setuser_response = await send_command_and_read_response(
        admin_reader,
        admin_writer,
        b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$5\r\nalice\r\n$7\r\n>wonder\r\n",
    )
    auth_response = await send_command_and_read_response(
        user_reader,
        user_writer,
        b"*3\r\n$4\r\nAUTH\r\n$5\r\nalice\r\n$6\r\nwonder\r\n",
    )
    whoami_response = await send_command_and_parse_response(
        user_reader,
        user_writer,
        b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n",
    )

    assert setuser_response == b"+OK\r\n"
    assert auth_response == b"+OK\r\n"
    assert whoami_response == b"alice"

    admin_writer.close()
    await admin_writer.wait_closed()
    user_writer.close()
    await user_writer.wait_closed()
    server.close()
    await server.wait_closed()


async def create_sorted_set(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    res1 = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$4\r\nZADD\r\n$8\r\nzset_key\r\n$3\r\n1.1\r\n$3\r\nfoo\r\n",
    )
    res2 = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$4\r\nZADD\r\n$8\r\nzset_key\r\n$3\r\n100\r\n$3\r\nfoo\r\n",
    )
    res3 = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$4\r\nZADD\r\n$8\r\nzset_key\r\n$3\r\n100\r\n$3\r\nbar\r\n",
    )
    res4 = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$4\r\nZADD\r\n$8\r\nzset_key\r\n$4\r\n20.0\r\n$3\r\nbaz\r\n",
    )
    res5 = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$4\r\nZADD\r\n$8\r\nzset_key\r\n$4\r\n30.1\r\n$3\r\ncaz\r\n",
    )
    res6 = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$4\r\nZADD\r\n$8\r\nzset_key\r\n$4\r\n40.2\r\n$3\r\npaz\r\n",
    )
    return res1, res2, res3, res4, res5, res6


@pytest.mark.asyncio
async def test_sorted_set_command_zadd_zrank():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)
    res1, res2, res3, res4, res5, res6 = await create_sorted_set(reader, writer)
    assert all(res == b":1\r\n" for res in [res1, res3, res4, res5, res6])
    assert res2 == b":0\r\n"
    zrank_res = [
        await send_command_and_read_response(
            reader,
            writer,
            f"*3\r\n$5\r\nZRANK\r\n$8\r\nzset_key\r\n$3\r\n{member}\r\n".encode(),
        )
        for member in ["baz", "caz", "paz", "bar", "foo"]
    ]
    assert zrank_res == [f":{i}\r\n".encode() for i in range(5)]
    zrank_res =await send_command_and_read_response(
            reader,
            writer,
            b"*3\r\n$5\r\nZRANK\r\n$8\r\nzset_key\r\n$16\r\nunexisted_member\r\n",
        )
    assert zrank_res == b"*-1\r\n"
    zrank_res =await send_command_and_read_response(
            reader,
            writer,
            b"*3\r\n$5\r\nZRANK\r\n$8\r\nzset_key\r\n$7\r\nmember0\r\n",
        )
    assert zrank_res == b"*-1\r\n"
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    

@pytest.mark.asyncio
async def test_sorted_set_command_zadd_zrange():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)
    results = await create_sorted_set(reader, writer)
    assert list(results) == [b':1\r\n', b':0\r\n', b':1\r\n', b':1\r\n', b':1\r\n', b':1\r\n']
    pos_result = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nZRANGE\r\n$8\r\nzset_key\r\n$1\r\n0\r\n$1\r\n5\r\n",
        )
    assert pos_result == b"*5\r\n$3\r\nbaz\r\n$3\r\ncaz\r\n$3\r\npaz\r\n$3\r\nbar\r\n$3\r\nfoo\r\n"
    out_of_bound_result = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nZRANGE\r\n$8\r\nzset_key\r\n$2\r\n10\r\n$2\r\n15\r\n",
        )
    assert out_of_bound_result == b"*0\r\n"
    right_out_of_bound_result = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nZRANGE\r\n$8\r\nzset_key\r\n$1\r\n3\r\n$2\r\n20\r\n",
    )
    assert right_out_of_bound_result == b"*2\r\n$3\r\nbar\r\n$3\r\nfoo\r\n"
    left_greater_right_result = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nZRANGE\r\n$8\r\nzset_key\r\n$2\r\n20\r\n$1\r\n3\r\n",
    )
    assert left_greater_right_result == b"*0\r\n"
    negative_index_res = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nZRANGE\r\n$8\r\nzset_key\r\n$1\r\n2\r\n$2\r\n-1\r\n"
    )
    assert negative_index_res == b"*3\r\n$3\r\npaz\r\n$3\r\nbar\r\n$3\r\nfoo\r\n"
    negative_left_out_of_bound_res = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nZRANGE\r\n$8\r\nzset_key\r\n$2\r\n-8\r\n$2\r\n-1\r\n"
    )
    assert negative_left_out_of_bound_res == b"*5\r\n$3\r\nbaz\r\n$3\r\ncaz\r\n$3\r\npaz\r\n$3\r\nbar\r\n$3\r\nfoo\r\n"
    neg_left_bound_lower_than_neg_right_bound = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nZRANGE\r\n$8\r\nzset_key\r\n$2\r\n-2\r\n$2\r\n-3\r\n"
    ) 
    assert neg_left_bound_lower_than_neg_right_bound == b"*0\r\n"
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    
    
@pytest.mark.asyncio
async def test_sorted_set_command_zcard():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)
    empty_zcard_result = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$5\r\nZCARD\r\n$8\r\nzset_key\r\n"
    )
    assert empty_zcard_result == b":0\r\n"
    results = await create_sorted_set(reader, writer)
    assert list(results) == [b':1\r\n', b':0\r\n', b':1\r\n', b':1\r\n', b':1\r\n', b':1\r\n']
    zcard_result = await send_command_and_read_response(
        reader,
        writer,
        b"*2\r\n$5\r\nZCARD\r\n$8\r\nzset_key\r\n"
    )
    assert zcard_result == b":5\r\n"
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    

@pytest.mark.asyncio
async def test_sorted_set_command_zscore():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)
    results = await create_sorted_set(reader, writer)
    assert list(results) == [b':1\r\n', b':0\r\n', b':1\r\n', b':1\r\n', b':1\r\n', b':1\r\n']
    zscore_result = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$6\r\nZSCORE\r\n$8\r\nzset_key\r\n$3\r\nfoo\r\n"
    )
    assert zscore_result == b"$5\r\n100.0\r\n"
    zscore_result = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$6\r\nZSCORE\r\n$8\r\nzset_key\r\n$11\r\nnonexistent\r\n"
    )
    assert zscore_result == b"$-1\r\n"
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    

@pytest.mark.asyncio
async def test_sorted_set_command_zrem():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)
    results = await create_sorted_set(reader, writer)
    assert list(results) == [b':1\r\n', b':0\r\n', b':1\r\n', b':1\r\n', b':1\r\n', b':1\r\n']
    zrem_result = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$4\r\nZREM\r\n$8\r\nzset_key\r\n$3\r\nfoo\r\n"
    )
    assert zrem_result == b":1\r\n"
    zrem_result = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$4\r\nZREM\r\n$8\r\nzset_key\r\n$11\r\nnonexistent\r\n"
    )
    assert zrem_result == b":0\r\n"
    
    zrem_result = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$4\r\nZREM\r\n$8\r\nzset_key\r\n$3\r\nbar\r\n$3\r\nbaz\r\n"
    )
    assert zrem_result == b":2\r\n"
    
    zrange_res = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$6\r\nZRANGE\r\n$8\r\nzset_key\r\n$1\r\n0\r\n$2\r\n-1\r\n"
    )
    assert zrange_res == b"*2\r\n$3\r\ncaz\r\n$3\r\npaz\r\n"
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()

    
# ------------------------------- GEOSPARTIAL TESTS -------------------------------


@pytest.mark.asyncio
async def test_geospatial_command_geoadd_geopos():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)
    geoadd_result = await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$6\r\nGEOADD\r\n$7\r\ngeo_key\r\n$9\r\n13.361389\r\n$9\r\n38.115556\r\n$3\r\nfoo\r\n"
    )
    assert geoadd_result == b":1\r\n"
    geopos_result = await send_command_and_parse_response(
        reader,
        writer,
        b"*3\r\n$6\r\nGEOPOS\r\n$7\r\ngeo_key\r\n$3\r\nfoo\r\n"
    )
    assert isinstance(geopos_result, list)
    assert len(geopos_result) == 1
    assert isinstance(geopos_result[0], list)
    assert all(isinstance(val, bytes) for val in geopos_result[0])
    actual_res = [float(geopos_result[0][0].decode()), float(geopos_result[0][1].decode())] # type: ignore
    assert actual_res == pytest.approx([13.361389, 38.115556], abs=1e-5)

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    
    
@pytest.mark.asyncio
async def test_geospatial_command_zadd_geopos():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)
    zadd_result = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$4\r\nZADD\r\n$8\r\nzset_key\r\n$16\r\n3663832614298053\r\n$3\r\nfoo\r\n"
    )
    assert zadd_result == b":1\r\n"
    geopos_result = await send_command_and_parse_response(
        reader,
        writer,
        b"*3\r\n$6\r\nGEOPOS\r\n$8\r\nzset_key\r\n$3\r\nfoo\r\n"
    )
    assert isinstance(geopos_result, list)
    assert len(geopos_result) == 1
    assert isinstance(geopos_result[0], list)
    assert all(isinstance(val, bytes) for val in geopos_result[0])
    actual_res = [float(geopos_result[0][0].decode()), float(geopos_result[0][1].decode())] # type: ignore
    assert actual_res == pytest.approx([2.294471561908722, 48.85846255040141], abs=1e-5)

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_geospatial_command_geopos_multiple_members_and_missing_member():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)

    first_add = await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$6\r\nGEOADD\r\n$7\r\ngeo_key\r\n$9\r\n13.361389\r\n$9\r\n38.115556\r\n$3\r\nfoo\r\n",
    )
    second_add = await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$6\r\nGEOADD\r\n$7\r\ngeo_key\r\n$9\r\n15.087269\r\n$9\r\n37.502669\r\n$3\r\nbar\r\n",
    )

    assert first_add == b":1\r\n"
    assert second_add == b":1\r\n"

    geopos_result = await send_command_and_parse_response(
        reader,
        writer,
        b"*5\r\n$6\r\nGEOPOS\r\n$7\r\ngeo_key\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$7\r\nmissing\r\n",
    )

    assert isinstance(geopos_result, list)
    assert len(geopos_result) == 3
    assert geopos_result[2] is None

    foo_coords = [float(geopos_result[0][0].decode()), float(geopos_result[0][1].decode())]  # type: ignore[index]
    bar_coords = [float(geopos_result[1][0].decode()), float(geopos_result[1][1].decode())]  # type: ignore[index]

    assert foo_coords == pytest.approx([13.361389, 38.115556], abs=1e-5)
    assert bar_coords == pytest.approx([15.087269, 37.502669], abs=1e-5)

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_geospatial_command_geopos_missing_key_returns_nulls():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)

    geopos_result = await send_command_and_parse_response(
        reader,
        writer,
        b"*4\r\n$6\r\nGEOPOS\r\n$7\r\ngeo_key\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
    )

    assert geopos_result == [None, None]

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_geospatial_command_geoadd_invalid_coordinates_returns_error():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)

    invalid_longitude_result = await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$6\r\nGEOADD\r\n$7\r\ngeo_key\r\n$3\r\n181\r\n$9\r\n38.115556\r\n$3\r\nfoo\r\n",
    )

    assert invalid_longitude_result == b"-ERR invalid longitude,latitude pair 181.0,38.115556\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_geospatial_command_geopos_wrongtype_returns_error_for_string_key():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)

    await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$3\r\nSET\r\n$7\r\nstr_key\r\n$5\r\nvalue\r\n",
    )

    wrongtype_result = await send_command_and_read_response(
        reader,
        writer,
        b"*3\r\n$6\r\nGEOPOS\r\n$7\r\nstr_key\r\n$3\r\nfoo\r\n",
    )

    expected = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    assert wrongtype_result == expected

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_geospatial_command_geodist():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)

    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$6\r\nGEOADD\r\n$7\r\ngeo_key\r\n$9\r\n13.361389\r\n$9\r\n38.115556\r\n$7\r\nPalermo\r\n",
    )
    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$6\r\nGEOADD\r\n$7\r\ngeo_key\r\n$9\r\n15.087269\r\n$9\r\n37.502669\r\n$7\r\nCatania\r\n",
    )

    geodist_result = await send_command_and_parse_response(
        reader,
        writer,
        b"*4\r\n$7\r\nGEODIST\r\n$7\r\ngeo_key\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n",
    )

    assert isinstance(geodist_result, bytes)
    assert float(geodist_result.decode()) == pytest.approx(166274.15157, abs=1e-2)

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_geospatial_command_geodist_missing_member_returns_null():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)

    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$6\r\nGEOADD\r\n$7\r\ngeo_key\r\n$9\r\n13.361389\r\n$9\r\n38.115556\r\n$7\r\nPalermo\r\n",
    )

    geodist_result = await send_command_and_read_response(
        reader,
        writer,
        b"*4\r\n$7\r\nGEODIST\r\n$7\r\ngeo_key\r\n$7\r\nPalermo\r\n$7\r\nMissing\r\n",
    )

    assert geodist_result == b"$-1\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_geospatial_command_geosearch_fromlonlat_byradius():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)

    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$6\r\nGEOADD\r\n$7\r\ngeo_key\r\n$9\r\n13.361389\r\n$9\r\n38.115556\r\n$7\r\nPalermo\r\n",
    )
    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$6\r\nGEOADD\r\n$7\r\ngeo_key\r\n$9\r\n15.087269\r\n$9\r\n37.502669\r\n$7\r\nCatania\r\n",
    )
    await send_command_and_read_response(
        reader,
        writer,
        b"*5\r\n$6\r\nGEOADD\r\n$7\r\ngeo_key\r\n$9\r\n12.496366\r\n$9\r\n41.902782\r\n$4\r\nRome\r\n",
    )

    geosearch_result = await send_command_and_parse_response(
        reader,
        writer,
        b"*8\r\n$9\r\nGEOSEARCH\r\n$7\r\ngeo_key\r\n$10\r\nFROMLONLAT\r\n$9\r\n13.361389\r\n$9\r\n38.115556\r\n$8\r\nBYRADIUS\r\n$3\r\n200\r\n$2\r\nkm\r\n",
    )

    assert geosearch_result == [b"Palermo", b"Catania"]

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_geospatial_command_geosearch_missing_key_returns_empty_array():
    config = ServerConfig("127.0.0.1", 0)

    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(*addr)

    geosearch_result = await send_command_and_parse_response(
        reader,
        writer,
        b"*8\r\n$9\r\nGEOSEARCH\r\n$7\r\ngeo_key\r\n$10\r\nFROMLONLAT\r\n$9\r\n13.361389\r\n$9\r\n38.115556\r\n$8\r\nBYRADIUS\r\n$3\r\n200\r\n$2\r\nkm\r\n",
    )

    assert geosearch_result == []

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    



