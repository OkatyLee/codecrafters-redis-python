

from app.commands import Arity, CommandContext, command
from app.parser import RawResponse


@command(
    name=b"SUBSCRIBE",
    arity=Arity(1, None),
    flags={"pubsub"},
    allowed_in_subscribe=True
)
def cmd_subscrive(ctx: CommandContext, args: list[bytes]) -> RawResponse:
    writer = ctx.exec_ctx.replica_writer
    assert writer is not None
    
    payload = b""
    for channel in args:
        ch = channel if isinstance(channel, bytes) else str(channel).encode()
        ctx.app_state.pubsub[ch].add(writer)
        ctx.session.subscribed_channels.add(ch)
        payload += ctx.parser.encode_array([b"subscribe", ch, len(ctx.session.subscribed_channels)])
    ctx.session.in_subscribed_mode = True
    return RawResponse(payload=payload)


@command(
    name=b"UNSUBSCRIBE",
    arity=Arity(0, None),
    flags={"pubsub"},
    allowed_in_subscribe=True
)
def cmd_unsubscribe(ctx: CommandContext, args: list[bytes]):
    targets = (
        args if args else list(ctx.session.subscribed_channels)
    )
    
    writer = ctx.exec_ctx.replica_writer
    assert writer is not None
    
    payload = b""
    for ch in targets:
        ctx.session.subscribed_channels.discard(ch)
        ctx.app_state.pubsub[ch].discard(writer)
        payload += ctx.parser.encode_array([b"unsubscribe", ch, len(ctx.session.subscribed_channels)])
    if not ctx.session.subscribed_channels:
        ctx.session.in_subscribed_mode = False
    raw_response = payload if payload else ctx.parser.encode_array([b"unsubscribe", None, 0])
    return RawResponse(payload=raw_response)


@command(
    name=b"PUBLISH",
    arity=Arity(2, 2),
    flags={"readonly", "coroutine", "pubsub"}
)
async def cmd_publish(ctx: CommandContext, args: list[bytes]) -> int:
    channel, message = args[0], args[1]
    subscribers = list(ctx.app_state.pubsub.get(channel, set()))
    if subscribers:
        payload = ctx.parser.encode_array([b"message", channel, message])
        for sub_writer in subscribers:
            if not sub_writer.is_closing():
                sub_writer.write(payload)
                await sub_writer.drain()
    return len(subscribers)
