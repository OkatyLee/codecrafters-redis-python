
import asyncio

from app.commands import Arity, CommandContext, command
from app.config import PubSubConfig
from app.metrics import set_active_subscriptions
from app.parser import RESPError
from app.resp_types import (
    ArrayType,
    BaseRESPType,
    BulkStringType,
    IntegerType,
    NullBulkStringType,
    RawResponse,
)
from app import metrics

def _subscription_reply(kind: bytes, channel: bytes | None, count: int) -> bytes:
    elements: list[BaseRESPType] = [BulkStringType(kind)]
    if channel is None:
        elements.append(NullBulkStringType())
    else:
        elements.append(BulkStringType(channel))
    elements.append(IntegerType(count))
    return ArrayType(elements).encode()


@command(
    name=b"SUBSCRIBE",
    arity=Arity(1, None),
    flags={"pubsub"},
    allowed_in_subscribe=True
)
def cmd_subscribe(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    writer = ctx.exec_ctx.connection_writer
    if writer is None:
        raise RESPError("ERR write error")
    
    payload = b""
    for channel in args:
        ch = channel if isinstance(channel, bytes) else str(channel).encode()
        ctx.app_state.pubsub[ch].add(writer)
        ctx.session.subscribed_channels.add(ch)
        payload += _subscription_reply(
            b"subscribe",
            ch,
            len(ctx.session.subscribed_channels),
        )
    set_active_subscriptions(sum(len(subscribers) for subscribers in ctx.app_state.pubsub.values()))
    ctx.session.in_subscribed_mode = True
    return RawResponse(payload)


@command(
    name=b"UNSUBSCRIBE",
    arity=Arity(0, None),
    flags={"pubsub"},
    allowed_in_subscribe=True
)
def cmd_unsubscribe(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    targets = (
        args if args else list(ctx.session.subscribed_channels)
    )
    
    writer = ctx.exec_ctx.connection_writer
    if writer is None:
        raise RESPError("ERR write error")
    
    payload = b""
    for ch in targets:
        ctx.session.subscribed_channels.discard(ch)
        ctx.app_state.pubsub[ch].discard(writer)
        payload += _subscription_reply(
            b"unsubscribe",
            ch,
            len(ctx.session.subscribed_channels),
        )
    set_active_subscriptions(sum(len(subscribers) for subscribers in ctx.app_state.pubsub.values()))
    if not ctx.session.subscribed_channels:
        ctx.session.in_subscribed_mode = False
    raw_response = payload if payload else _subscription_reply(b"unsubscribe", None, 0)
    return RawResponse(raw_response)


@command(
    name=b"PUBLISH",
    arity=Arity(2, 2),
    flags={"readonly", "coroutine", "pubsub"}
)
async def cmd_publish(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    channel, message = args[0], args[1]
    subscribers = ctx.app_state.pubsub.get(channel, set())
    if not subscribers:
        return IntegerType(0)
    cfg = ctx.app_state.pubsub_config
    payload = ArrayType([BulkStringType(val) for val in [b"message", channel, message]]).encode()
    active = []
    for sw in list(subscribers):  
        if sw.is_closing():
            _evict_writer(sw, ctx.app_state.pubsub)
        elif sw.transport.get_write_buffer_size() >= cfg.max_write_buffer_bytes:
            if cfg.slow_subscriber_policy == "disconnect":
                _evict_writer(sw, ctx.app_state.pubsub)
        else:
            sw.write(payload)
            active.append(sw)
    
    if active:
        async with asyncio.TaskGroup() as tg:
            for sw in active:
                tg.create_task(_drain(sw, ctx.app_state.pubsub, cfg))

    return IntegerType(len(active))


def _evict_writer(writer: asyncio.StreamWriter, pubsub: dict):
    """Unsub writer from all channels. Returns num of unsubed channels"""
    writer.close()
    removed = 0
    for channel, writers in list(pubsub.items()):
        if writer in writers:
            removed += 1
            writers.discard(writer)
        if not writers:
            pubsub.pop(channel, None)
    if removed:
        total = sum(len(w) for w in pubsub.values())
        metrics.set_active_subscriptions(total)
    return removed
        

async def _drain(sub_writer: asyncio.StreamWriter, pubsub: dict, cfg: PubSubConfig):
    try:
        async with asyncio.timeout(cfg.drain_timeout_seconds):
            await sub_writer.drain()
    except (TimeoutError, ConnectionResetError, BrokenPipeError, OSError):
        if cfg.slow_subscriber_policy == "disconnect":
            _evict_writer(sub_writer, pubsub)