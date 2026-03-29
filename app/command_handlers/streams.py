
import asyncio
from collections.abc import Sequence

from app.commands import Arity, CommandContext, command
from app.parser import RESPError, NullBulkString


@command(
    name=b"TYPE",
    arity=Arity(1, 1),
    flags={"readonly", "fast", "streams"}
)
def cmd_type(ctx: CommandContext, args: list[bytes]) -> str:
    key = args[0]
    return ctx.app_state.storage.type(key) or "none"


@command(
    name=b"XADD",
    arity=Arity(3, None),
    flags={"write", "streams"}
)
def cmd_xadd(ctx: CommandContext, args: list[bytes]) -> str:
    stream_key, stream_id, *payload = args
    return ctx.app_state.storage.xadd(stream_key, stream_id, payload)


@command(
    name=b"XRANGE",
    arity=Arity(3, 3),
    flags={"readonly", "streams"}
)
def cmd_xrange(ctx: CommandContext, args: list[bytes]) -> list[list[str | dict[bytes, bytes]]]:
    stream_key, start, end = args
    return ctx.app_state.storage.xrange(stream_key, start, end)


@command(
    name=b"XREAD",
    arity=Arity(3, None),
    flags={"readonly", "streams"}
)
async def cmd_xread(ctx: CommandContext, args: list[bytes]) -> list[list[bytes | str | list[list[str | dict[bytes, bytes]]]]] | NullBulkString:
    parse_index = 0
    block_timeout_seconds: float | None = None
    if args[0].upper() == b"BLOCK":
        block_ms = int(args[1])
        if block_ms < 0:
            raise RESPError("timeout is negative")
        block_timeout_seconds = block_ms / 1000
        parse_index = 2
    if parse_index >= len(args) or args[parse_index].upper() != b"STREAMS":
        raise RESPError("syntax error")
    keys_and_ids = args[parse_index + 1 :]
    if len(keys_and_ids) == 0 or len(keys_and_ids) % 2 != 0:
        raise RESPError("Unbalanced XREAD list of streams")
    
    def resolve_xread_start_ids(keys: Sequence[bytes], ids: Sequence[bytes]) -> list[str | bytes]:
        """Resolve XREAD starting IDs once (including ``$`` semantics)."""
        resolved_ids: list[str | bytes] = []
        for index, raw_id in enumerate(ids):
            if raw_id == b"$":
                entries = cmd_xrange(ctx, [keys[index], b"-", b"+"])
                if entries:
                    entry_id = entries[-1][0]
                    assert isinstance(entry_id, str)
                    resolved_ids.append(entry_id)
                else:
                    resolved_ids.append("0-0")
            else:
                resolved_ids.append(raw_id)
        return resolved_ids
    
    split_index = len(keys_and_ids) // 2
    keys = keys_and_ids[:split_index]
    ids = keys_and_ids[split_index:]
    resolved_ids = resolve_xread_start_ids(keys, ids)
    
    entries = ctx.app_state.storage.xread_streams(keys, resolved_ids)
    if entries:
        return entries
    elif block_timeout_seconds is None:
        return NullBulkString()
    elif block_timeout_seconds == 0:
        while True:
            await asyncio.sleep(0.05)
            entries = ctx.app_state.storage.xread_streams(keys, resolved_ids)
            if entries:
                return entries
    else:
        deadline = asyncio.get_running_loop().time() + block_timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
            entries = ctx.app_state.storage.xread_streams(keys, resolved_ids)
            if entries:
                return entries
        else:
            return NullBulkString()


