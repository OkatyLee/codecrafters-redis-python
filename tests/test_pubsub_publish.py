import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest  # pyright: ignore[reportMissingImports]

from app.command_handlers.pubsub import cmd_publish
from app.config import PubSubConfig, ServerConfig
from app.pubsub_client import SubscriberClient
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


def make_writer(*, closing: bool = False) -> MagicMock:
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.is_closing.return_value = closing
    writer.wait_closed = AsyncMock()
    return writer


async def wait_until(predicate, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Timed out waiting for condition")


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
async def test_subscribe_returns_confirmation_array():
    server, _ = await start_test_server(ServerConfig("127.0.0.1", 0))
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    try:
        response = await send_command_and_parse_response(
            reader,
            writer,
            b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n",
        )
        assert response == [b"subscribe", b"ch1", 1]
    finally:
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_publish_delivers_message_to_all_subscribers_and_returns_count():
    server, _ = await start_test_server(ServerConfig("127.0.0.1", 0))
    addr = server.sockets[0].getsockname()

    sub1_reader, sub1_writer = await asyncio.open_connection(*addr)
    sub2_reader, sub2_writer = await asyncio.open_connection(*addr)
    pub_reader, pub_writer = await asyncio.open_connection(*addr)

    try:
        assert await send_command_and_parse_response(
            sub1_reader,
            sub1_writer,
            b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n",
        ) == [b"subscribe", b"ch1", 1]
        assert await send_command_and_parse_response(
            sub2_reader,
            sub2_writer,
            b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n",
        ) == [b"subscribe", b"ch1", 1]

        publish_response = await send_command_and_parse_response(
            pub_reader,
            pub_writer,
            b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nch1\r\n$5\r\nhello\r\n",
        )
        pushed_message_1 = await RESPParser(sub1_reader).parse()
        pushed_message_2 = await RESPParser(sub2_reader).parse()

        assert publish_response == 2
        assert pushed_message_1 == [b"message", b"ch1", b"hello"]
        assert pushed_message_2 == [b"message", b"ch1", b"hello"]
    finally:
        sub1_writer.close()
        await sub1_writer.wait_closed()
        sub2_writer.close()
        await sub2_writer.wait_closed()
        pub_writer.close()
        await pub_writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_publish_to_missing_channel_returns_zero():
    server, _ = await start_test_server(ServerConfig("127.0.0.1", 0))
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    try:
        response = await send_command_and_parse_response(
            reader,
            writer,
            b"*3\r\n$7\r\nPUBLISH\r\n$7\r\nmissing\r\n$5\r\nhello\r\n",
        )
        assert response == 0
    finally:
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_ping_allowed_in_subscribe_mode():
    server, _ = await start_test_server(ServerConfig("127.0.0.1", 0))
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    try:
        subscribe_response = await send_command_and_parse_response(
            reader,
            writer,
            b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n",
        )
        ping_response = await send_command_and_parse_response(
            reader,
            writer,
            b"*2\r\n$4\r\nPING\r\n$3\r\nhey\r\n",
        )

        assert subscribe_response == [b"subscribe", b"ch1", 1]
        assert ping_response == [b"pong", b"hey"]
    finally:
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_regular_command_blocked_in_subscribe_mode():
    server, _ = await start_test_server(ServerConfig("127.0.0.1", 0))
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    try:
        subscribe_response = await send_command_and_parse_response(
            reader,
            writer,
            b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n",
        )
        blocked_response = await send_command_and_read_response(
            reader,
            writer,
            b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
        )

        assert subscribe_response == [b"subscribe", b"ch1", 1]
        assert blocked_response == b"-ERR Command not allowed in subscribe mode\r\n"
    finally:
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_unsubscribe_exits_subscribe_mode_and_stops_future_delivery():
    server, _ = await start_test_server(ServerConfig("127.0.0.1", 0))
    addr = server.sockets[0].getsockname()

    sub_reader, sub_writer = await asyncio.open_connection(*addr)
    pub_reader, pub_writer = await asyncio.open_connection(*addr)

    try:
        subscribe_response = await send_command_and_parse_response(
            sub_reader,
            sub_writer,
            b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n",
        )
        unsubscribe_response = await send_command_and_parse_response(
            sub_reader,
            sub_writer,
            b"*1\r\n$11\r\nUNSUBSCRIBE\r\n",
        )
        ping_response = await send_command_and_read_response(
            sub_reader,
            sub_writer,
            b"*1\r\n$4\r\nPING\r\n",
        )
        publish_response = await send_command_and_parse_response(
            pub_reader,
            pub_writer,
            b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nch1\r\n$5\r\nhello\r\n",
        )

        assert subscribe_response == [b"subscribe", b"ch1", 1]
        assert unsubscribe_response == [b"unsubscribe", b"ch1", 0]
        assert ping_response == b"+PONG\r\n"
        assert publish_response == 0
    finally:
        sub_writer.close()
        await sub_writer.wait_closed()
        pub_writer.close()
        await pub_writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_unsubscribe_without_subscribe_returns_zero_count():
    server, _ = await start_test_server(ServerConfig("127.0.0.1", 0))
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    try:
        response = await send_command_and_parse_response(
            reader,
            writer,
            b"*1\r\n$11\r\nUNSUBSCRIBE\r\n",
        )
        assert response == [b"unsubscribe", None, 0]
    finally:
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_publish_queue_overflow_disconnects_slow_subscriber_from_all_channels():
    app_state = make_app_state()
    app_state.pubsub_config = PubSubConfig(queue_maxsize=1, slow_subscriber_policy="disconnect")

    slow_subscriber = SubscriberClient(make_writer(), maxsize=1)
    slow_subscriber.queue.put_nowait(b"already-full")
    slow_subscriber.subscribed_channels.update({b"ch1", b"ch2"})
    app_state.pubsub[b"ch1"].add(slow_subscriber)
    app_state.pubsub[b"ch2"].add(slow_subscriber)

    ctx = MagicMock()
    ctx.app_state = app_state

    result = cmd_publish(ctx, [b"ch1", b"hello"])
    assert result.value == 0

    await wait_until(lambda: slow_subscriber not in app_state.pubsub.get(b"ch1", set()))
    assert slow_subscriber not in app_state.pubsub.get(b"ch2", set())
    slow_subscriber.writer.close.assert_called_once()


@pytest.mark.asyncio
async def test_publish_prunes_closed_subscriber_without_breaking_fast_subscribers():
    app_state = make_app_state()
    fast_subscriber = SubscriberClient(make_writer(), maxsize=2)
    closed_subscriber = SubscriberClient(make_writer(closing=True), maxsize=2)
    fast_subscriber.subscribed_channels.add(b"ch1")
    closed_subscriber.subscribed_channels.add(b"ch1")
    app_state.pubsub[b"ch1"].update({fast_subscriber, closed_subscriber})

    ctx = MagicMock()
    ctx.app_state = app_state

    result = cmd_publish(ctx, [b"ch1", b"hello"])
    assert result.value == 1
    assert fast_subscriber.queue.get_nowait() == b"*3\r\n$7\r\nmessage\r\n$3\r\nch1\r\n$5\r\nhello\r\n"

    await wait_until(lambda: closed_subscriber not in app_state.pubsub.get(b"ch1", set()))
