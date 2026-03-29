from app.commands import Arity, CommandContext, command
from app.resp_types import ArrayType, BaseRESPType, BulkStringType, IntegerType, NullArrayType, NullBulkStringType, NullBulkStringType


@command(
    name=b"RPUSH",
    arity=Arity(2, None),
    flags={"write", "lists"}
)
def cmd_rpush(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, *values = args
    return IntegerType(ctx.app_state.storage.rpush(key, *values))


@command(
    name=b"LPUSH",
    arity=Arity(2, None),
    flags={"write", "lists"}
)
def cmd_lpush(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, *values = args
    return IntegerType(ctx.app_state.storage.lpush(key, *values))


@command(
    name=b"LRANGE",
    arity=Arity(3, 3),
    flags={"readonly", "lists"}
)
def cmd_lrange(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, start, end = args
    result = ctx.app_state.storage.lrange(key, int(start), int(end))
    return ArrayType(
        [BulkStringType(value) for value in result]
    )


@command(
    name=b"LLEN",
    arity=Arity(1, 1),
    flags={"readonly", "lists"}
)
def cmd_llen(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, = args
    return IntegerType(ctx.app_state.storage.llen(key))


@command(
    name=b"LPOP",
    arity=Arity(1, 2),
    flags={"write", "lists"}
)
def cmd_lpop(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, *args = args
    count = int(args[0]) if args else None
    values = ctx.app_state.storage.lpop(key, count=count)
    if values is None:
        return NullBulkStringType()
    
    if count is None:
        return BulkStringType(values[0])
    
    return ArrayType([BulkStringType(value) for value in values])


@command(
    name=b"BLPOP",
    arity=Arity(2, None),
    flags={"write", "lists"}

)
async def cmd_blpop(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    *keys, timeout = args

    result = await ctx.app_state.storage.blpop(*keys, timeout=float(timeout))
    if result is None:
        return NullArrayType()
    key, value = result
    key_payload = key if isinstance(key, bytes) else str(key).encode()
    val_payload = value if isinstance(value, bytes) else str(value).encode()
    return ArrayType([BulkStringType(key_payload), BulkStringType(val_payload)])
