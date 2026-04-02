
import asyncio

from app.commands import Arity, CommandContext, command
from app.metrics import set_active_subscriptions
from app.parser import RESPError
from app.pubsub_client import (
    SubscriberClient,
    unsubscribe_client_everywhere,
)
from app.resp_types import (
    ArrayType,
    BaseRESPType,
    BulkStringType,
    IntegerType,
    NullBulkStringType,
    RawResponse,
)


def _schedule_subscriber_cleanup(sub: SubscriberClient, ctx: CommandContext) -> None:
    if getattr(sub, "_cleanup_scheduled", False):
        return
    sub._cleanup_scheduled = True
    asyncio.create_task(unsubscribe_client_everywhere(sub, ctx.app_state))

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
async def cmd_subscribe(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    writer = ctx.exec_ctx.connection_writer
    if writer is None:
        raise RESPError("ERR write error")
    sub = ctx.session.subscriber_client
    if sub is None:
        sub = SubscriberClient(writer, maxsize=ctx.app_state.pubsub_config.queue_maxsize)
        await sub.start_sender()
        ctx.session.subscriber_client = sub
    payload = b""
    for channel in args:
        ch = channel if isinstance(channel, bytes) else str(channel).encode()
        ctx.app_state.pubsub[ch].add(sub)
        sub.subscribed_channels.add(ch)
        payload += _subscription_reply(
            b"subscribe",
            ch,
            len(sub.subscribed_channels),
        )
    set_active_subscriptions(sum(len(subscribers) for subscribers in ctx.app_state.pubsub.values()))
    ctx.session.in_subscribed_mode = True
    await sub.queue.put(payload)
    return RawResponse(b"")


@command(
    name=b"UNSUBSCRIBE",
    arity=Arity(0, None),
    flags={"pubsub"},
    allowed_in_subscribe=True
)
def cmd_unsubscribe(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    writer = ctx.exec_ctx.connection_writer
    if writer is None:
        raise RESPError("ERR write error")

    sub = ctx.session.subscriber_client

    if sub is None or not sub.subscribed_channels:
        return RawResponse(_subscription_reply(b"unsubscribe", None, 0))
    targets = args if args else list(sub.subscribed_channels)
    
    payload = b""
    for ch in targets:
        sub.subscribed_channels.discard(ch)
        ctx.app_state.pubsub[ch].discard(sub)
        if not ctx.app_state.pubsub[ch]:
            ctx.app_state.pubsub.pop(ch, None)
        payload += _subscription_reply(
            b"unsubscribe",
            ch,
            len(sub.subscribed_channels),
        )
    set_active_subscriptions(sum(len(subscribers) for subscribers in ctx.app_state.pubsub.values()))
    
    if not sub.subscribed_channels:
        ctx.session.in_subscribed_mode = False
    raw_response = payload if payload else _subscription_reply(b"unsubscribe", None, 0)
    return RawResponse(raw_response)


@command(
    name=b"PUBLISH",
    arity=Arity(2, 2),
    flags={"pubsub"}
)
def cmd_publish(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    channel, message = args[0], args[1]
    subscribers = ctx.app_state.pubsub.get(channel, set())
    if not subscribers:
        return IntegerType(0)
    cfg = ctx.app_state.pubsub_config
    payload = ArrayType([BulkStringType(val) for val in [b"message", channel, message]]).encode()
    delivered = 0
    for sub in list(subscribers):  
        if sub.writer.is_closing():
            _schedule_subscriber_cleanup(sub, ctx)
            continue
        try:
            sub.queue.put_nowait(payload)
            delivered += 1
        except asyncio.QueueFull:
            if cfg.slow_subscriber_policy == "disconnect":
                _schedule_subscriber_cleanup(sub, ctx)

    return IntegerType(delivered)

