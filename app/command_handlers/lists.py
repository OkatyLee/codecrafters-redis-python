
from app.commands import Arity, CommandContext, NullArray, NullBulkString, command
from app.storage import get_storage


@command(
    name=b"RPUSH",
    arity=Arity(2, None),
    flags={"write"}
)
def cmd_rpush(ctx: CommandContext, args: list[bytes]) -> int:
    key, *values = args
    storage = get_storage()
    return storage.rpush(key, *values)
    

@command(
    name=b"LPUSH",
    arity=Arity(2, None),
    flags={"write"}

)
def cmd_lpush(ctx: CommandContext, args: list[bytes]) -> int:
    key, *values = args
    storage = get_storage()
    return storage.lpush(key, *values)


@command(
    name=b"LRANGE",
    arity=Arity(3, 3),
    flags={"readonly"}
)
def cmd_lrange(ctx: CommandContext, args: list[bytes]) -> list[bytes]:
    key, start, end = args
    storage = get_storage()
    return storage.lrange(key, int(start), int(end))


@command(
    name=b"LLEN",
    arity=Arity(1, 1),
    flags={"readonly"}
)
def cmd_llen(ctx: CommandContext, args: list[bytes]) -> int:
    key, = args
    storage = get_storage()
    return storage.llen(key)


@command(
    name=b"LPOP",
    arity=Arity(1, 2),
    flags={"write"}
)
def cmd_lpop(ctx: CommandContext, args: list[bytes]) -> bytes | list[bytes] | NullBulkString:
    key, *args = args
    count = int(args[0]) if args else None
    storage = get_storage()
    values = storage.lpop(key, count=count)
    if values is None:
        return NullBulkString()
    
    if count is None:
        return values[0]
    
    return values


@command(
    name=b"BLPOP",
    arity=Arity(2, None),
    flags={"write"}

)
async def cmd_blpop(ctx: CommandContext, args: list[bytes]) -> list[bytes] | NullArray:
    storage = get_storage()
    *keys, timeout = args

    result = await storage.blpop(*keys, timeout=float(timeout))
    if result is None:
        return NullArray()
    key, value = result
    key_payload = key if isinstance(key, bytes) else str(key).encode()
    val_payload = value if isinstance(value, bytes) else str(value).encode()
    return [key_payload, val_payload]
