import asyncio
import os

from app.commands import Arity, Arity, CommandContext, command
from app.parser import RESPError
from app.resp_types import BaseRESPType, BulkStringType, IntegerType, SimpleStringType, ArrayType, RawResponse


EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
EMPTY_RDB_BYTES = bytes.fromhex(EMPTY_RDB_HEX)


@command(
    name=b"INFO",
    arity=Arity(0, 1),
    flags={"readonly", "misc"}
)
def cmd_info(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    config = ctx.app_state.config
    if config is None:
        raise ValueError("ERR Config is None")
    role = b"role:" + config.role.encode()
    master_replid = (
        b"master_replid:" + config.master_perlid.encode()
        if config.master_perlid
        else b"master_replid:"
    )
    master_repl_offset = b"master_repl_offset:" + str(config.master_repl_offset).encode()
    return BulkStringType(role + b"\r\n" + master_replid + b"\r\n" + master_repl_offset)


@command(
    name=b"REPLCONF",
    arity=Arity(1, 2),
    flags={"readonly", "replication", "misc"}

)
def cmd_replconf(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    option = args[0].upper()
    
    match option:
        case b"LISTENING-PORT":
            replica_port = args[1]
            ctx.app_state.register_replica(int(replica_port), None)
            return SimpleStringType("OK")
        case b"GETACK":
            replica_offset = ctx.app_state.config.get_replica_offset()
            return ArrayType([BulkStringType(val) for val in [b"REPLCONF", b"ACK", str(replica_offset).encode()]])
        case b"ACK":
            offset = args[1]
            ctx.app_state.set_replica_ack_offset(ctx.exec_ctx.replica_writer, int(offset))
            return RawResponse(b"")
        case _:
            return SimpleStringType("OK")
        
        
        
@command(
    name=b"PSYNC",
    arity=Arity(2, 2),
    flags={"readonly", "replication", "misc"}
)
def cmd_psync(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    config = ctx.app_state.config
    if args != [b"?", b"-1"]:
        raise RESPError("ERR invalid arguments for PSYNC")
    ctx.app_state.register_replica(None, ctx.exec_ctx.replica_writer)
    repl_id = config.master_perlid
    offset = config.master_repl_offset
    rdb_path = os.path.join(config.dir, config.dbfilename)

    if os.path.exists(rdb_path):
        with open(rdb_path, "rb") as f:
            rdb_data = f.read()
    else:
        rdb_data = EMPTY_RDB_BYTES
    if rdb_data == EMPTY_RDB_BYTES:
        ctx.app_state.logger.info("RDB files not found. Using empty RDB data")
    payload = SimpleStringType(
        f"FULLRESYNC {repl_id} {offset}"
    ).encode() + BulkStringType(rdb_data).encode()
    return RawResponse(value=payload)
    
    
    
@command(
    name=b"WAIT",
    arity=Arity(2, 2),
    flags={"readonly", "replication", "coroutine", "misc"}
)
async def cmd_wait(ctx: CommandContext, args: list[bytes]) -> IntegerType:
    config = ctx.app_state.config
    numreplicas = int(args[0])
    timeout = int(args[1])
    
    async def handle_wait_command(ctx: CommandContext, numreplicas: int, timeout: int) -> int:
        config = ctx.app_state.config
        replicas = ctx.app_state.get_replicas()

        if numreplicas <= 0:
            return 0

        current_offset = config.master_repl_offset

        # No writes propagated yet — all connected replicas are trivially in sync.
        if current_offset == 0:
            return len(replicas)

        def count_acked() -> int:
            return sum(
                1 for r in ctx.app_state.get_replicas()
                if ctx.app_state.replica_ack_offsets.get(r, -1) >= current_offset
            )

        if count_acked() >= numreplicas:
            return count_acked()

        # Send GETACK directly — bypassing propagate_to_replicas so that
        # master_repl_offset is NOT inflated by this management command.
        getack_cmd = ArrayType([BulkStringType(val) for val in [b"REPLCONF", b"GETACK", b"*"]]).encode()
        stale: list[asyncio.StreamWriter] = []
        for replica in list(replicas):
            try:
                replica.write(getack_cmd)
                await replica.drain()
            except Exception:
                stale.append(replica)
        for s in stale:
            ctx.app_state.unregister_replica(s)

        deadline = asyncio.get_event_loop().time() + timeout / 1000
        while asyncio.get_event_loop().time() < deadline:
            acked = count_acked()
            if acked >= numreplicas:
                return acked
            await asyncio.sleep(0.01)

        return count_acked()
    response = await handle_wait_command(ctx, numreplicas, timeout)
    return IntegerType(response)


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


