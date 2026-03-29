from app.commands import Arity, CommandContext, command
from app.parser import RESPError, NullBulkString


@command(
    name=b"SET",
    arity=Arity(2, None),
    flags={"write", "strings"}
)
def cmd_set(ctx: CommandContext, args: list[bytes]) -> str:
    key, value, opt_args = args[0], args[1], args[2:]
    if len(opt_args) % 2 != 0:
        raise RESPError("syntax error")

    params = {
        option.upper(): option_value
        for option, option_value in zip(opt_args[::2], opt_args[1::2], strict=False)
    }
    unknown_options = [opt for opt in params if opt not in (b"EX", b"PX")]
    if unknown_options:
        raise RESPError("syntax error")
    if params.get(b"EX") and params.get(b"PX"):
        raise RESPError("syntax error. Only one of EX or PX is allowed")
    px = params.get(b"PX")
    if px is not None:
        px = int(px) / 1000
    ex = params.get(b"EX")
    if ex is not None:
        ex = int(ex)
    ttl = ex if ex is not None else px
    res = ctx.app_state.storage.set(key, value, ttl)
    if res: return "OK" 
    raise RESPError("Failed to set key")

    
@command(
    name=b"GET",
    arity=Arity(1, 1),
    flags={"readonly", "strings"}
)
def cmd_get(ctx: CommandContext, args: list[bytes]) -> bytes | NullBulkString:
    key = args[0]
    value = ctx.app_state.storage.get(key)
    if value is None:
        return NullBulkString()
    return value


@command(
    name=b"DEL",
    arity=Arity(1, 1),
    flags={"write", "strings"}
)
def cmd_del(ctx: CommandContext, args: list[bytes]) -> str:
    key = args[0]
    res = ctx.app_state.storage.delete(key)
    if res:
        return "OK"
    
    raise RESPError("Failed to delete key")


@command(
    name=b"INCR",
    arity=Arity(1, 1),
    flags={"write", "strings"}
)
def cmd_incr(ctx: CommandContext, args: list[bytes]) -> int:
    key = args[0]
    return ctx.app_state.storage.incr(key)
