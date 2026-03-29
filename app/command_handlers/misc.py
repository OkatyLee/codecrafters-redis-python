import asyncio

from app.commands import Arity, CommandContext, command
from app.parser import RESPError
from app.resp_types import ArrayType, BaseRESPType, BulkStringType, SimpleStringType


@command(
    name=b"CONFIG",
    arity=Arity(2, 2),
    flags={"readonly", "misc"}
)
def cmd_config(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    config = ctx.app_state.config
    method, attr = args[0], args[1]
    if method.upper() != b"GET":
        raise RESPError("ERR only CONFIG GET is supported")
    if not hasattr(config, attr.decode()):
        raise RESPError("ERR Invalid CONFIG attribute")
    return ArrayType([BulkStringType(val) for val in [attr, getattr(config, attr.decode()).encode()]])
    

@command(
    name=b"SAVE",
    arity=Arity(0, 0),
    flags={"readonly", "misc"}
)
def cmd_save(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    config = ctx.app_state.config
    success = ctx.app_state.storage.save(config.dir, config.dbfilename)
    if not success:
        raise RESPError("ERR Error saving data")
    return SimpleStringType("OK")


@command(
    name=b"BGSAVE",
    arity=Arity(0, 0),
    flags={"readonly", "coroutine", "misc"}
)
async def cmd_bgsave(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    config = ctx.app_state.config
    async def start_bgsave():
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, ctx.app_state.storage.save, config.dir, config.dbfilename)
    asyncio.create_task(start_bgsave())
    return SimpleStringType("Background saving started")


@command(
    name=b"KEYS",
    arity=Arity(1, 1),
    flags={"readonly", "misc"}
)
def cmd_keys(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    pattern = args[0] if args else b"*"
    return ArrayType([BulkStringType(key) for key in ctx.app_state.storage.keys(pattern)])


