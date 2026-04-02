
import asyncio
from app.command_handlers.pubsub import cmd_publish
from app.commands import CommandContext
from app.config import PubSubConfig
from app.resp_types import IntegerType
import pytest
from unittest.mock import AsyncMock, MagicMock


def make_writer(buffer_size=0, closing=False):
    sw = MagicMock(spec=asyncio.StreamWriter)
    sw.is_closing.return_value = closing
    sw.transport.get_write_buffer_size.return_value = buffer_size
    sw.drain = AsyncMock()
    return sw

@pytest.fixture
def ctx():
    state = MagicMock()
    state.pubsub = {}
    state.pubsub_config = PubSubConfig()
    
    context = MagicMock()
    context.app_state = state
    return context


async def test_publish_fanout_all_subscribers(ctx):
    channel = b"news"
    writers = [make_writer() for _ in range(5)]
    ctx.app_state.pubsub[channel] = set(writers)

    result = await cmd_publish(ctx, [channel, b"hello"])

    assert result == IntegerType(5)
    for sw in writers:
        sw.write.assert_called_once()
        sw.drain.assert_awaited_once()


async def test_publish_slow_subscriber_evicted(ctx: CommandContext):
    channel = b"news"
    slow = make_writer(buffer_size=128 * 1024)  # > MAX_BUFFER
    fast = make_writer(buffer_size=0)
    ctx.app_state.pubsub[channel] = {slow, fast}

    await cmd_publish(ctx, [channel, b"hello"])

    fast.write.assert_called_once()   
    slow.write.assert_not_called()    
    slow.close.assert_called_once()   
    assert slow not in ctx.app_state.pubsub[channel]


async def test_evict_writer_removes_from_all_channels(ctx):
    sw = make_writer(buffer_size=128 * 1024)
    ctx.app_state.pubsub[b"ch1"] = {sw}
    ctx.app_state.pubsub[b"ch2"] = {sw}
    ctx.app_state.pubsub[b"ch3"] = {sw}

    await cmd_publish(ctx, [b"ch1", b"msg"])

    for channel in [b"ch1", b"ch2", b"ch3"]:
        assert sw not in ctx.app_state.pubsub.get(channel, set())