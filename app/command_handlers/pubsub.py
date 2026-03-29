
from app.commands import Arity, CommandContext, command
from app.parser import RESPError
from app.resp_types import (
    ArrayType,
    BaseRESPType,
    BulkStringType,
    IntegerType,
    NullBulkStringType,
    RawResponse,
)


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
    writer = ctx.exec_ctx.replica_writer
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
    
    writer = ctx.exec_ctx.replica_writer
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
    subscribers = list(ctx.app_state.pubsub.get(channel, set()))
    if subscribers:
        payload = ArrayType([BulkStringType(val) for val in [b"message", channel, message]]).encode()
        for sub_writer in subscribers:
            if not sub_writer.is_closing():
                sub_writer.write(payload)
                await sub_writer.drain()
    return IntegerType(len(subscribers))
