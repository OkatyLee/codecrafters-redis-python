import asyncio
import os

from app.commands import Arity, Arity, CommandContext, command
from app.parser import RESPError
from app.storage import get_storage


EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
EMPTY_RDB_BYTES = bytes.fromhex(EMPTY_RDB_HEX)


@command(
    name=b"INFO",
    arity=Arity(0, 1),
    flags={"readonly"}
)
def cmd_info(ctx: CommandContext, args: list[bytes]) -> bytes:
    config = ctx.config
    if config is None:
        raise ValueError("Config is None")
    role = b"role:" + config.role.encode()
    master_replid = (
        b"master_replid:" + config.master_perlid.encode()
        if config.master_perlid
        else b"master_replid:"
    )
    master_repl_offset = b"master_repl_offset:" + str(config.master_repl_offset).encode()
    return role + b"\r\n" + master_replid + b"\r\n" + master_repl_offset


@command(
    name=b"REPLCONF",
    arity=Arity(1, 2),
    flags={"readonly", "replication"}

)
def cmd_replconf(ctx: CommandContext, args: list[bytes]) -> str | list[bytes] | bytes:
    option = args[0].upper()
    config = ctx.config
    
    match option:
        case b"LISTENING-PORT":
            replica_port = args[1]
            config.register_replica(int(replica_port), None)
            return "OK"
        case b"GETACK":
            replica_offset = config.get_replica_offset()
            return [b"REPLCONF", b"ACK", str(replica_offset).encode()]
        case b"ACK":
            offset = args[1]
            config.set_replica_ack_offset(ctx.exec_ctx.replica_writer, int(offset))
            return b""
        case _:
            return "OK"
        
        
        
@command(
    name=b"PSYNC",
    arity=Arity(2, 2),
    flags={"readonly", "replication", "noparse"}
)
def cmd_psync(ctx: CommandContext, args: list[bytes]) -> bytes:
    config = ctx.config
    if args != [b"?", b"-1"]:
        raise RESPError("invalid arguments for PSYNC")
    config.register_replica(None, ctx.exec_ctx.replica_writer)
    repl_id = config.master_perlid
    offset = config.master_repl_offset
    rdb_path = os.path.join(config.dir, config.dbfilename)

    if os.path.exists(rdb_path):
        with open(rdb_path, "rb") as f:
            rdb_data = f.read()
    else:
        rdb_data = EMPTY_RDB_BYTES
    if rdb_data == EMPTY_RDB_BYTES:
        print("RDB files not found. Using empty RDB data")
    return ctx.parser.encode_simple_string(
         f"FULLRESYNC {repl_id} {offset}"
     ) + ctx.parser.encode_bulk_string(rdb_data)
    
    
    
@command(
    name=b"WAIT",
    arity=Arity(2, 2),
    flags={"readonly", "replication", "coroutine"}
)
async def cmd_wait(ctx: CommandContext, args: list[bytes]) -> int:
    config = ctx.config
    numreplicas = int(args[0])
    timeout = int(args[1])
    
    async def handle_wait_command(ctx: CommandContext, numreplicas: int, timeout: int) -> int:
        config = ctx.config
        replicas = config.get_replicas()

        if numreplicas <= 0:
            return 0

        current_offset = config.master_repl_offset

        # No writes propagated yet — all connected replicas are trivially in sync.
        if current_offset == 0:
            return len(replicas)

        def count_acked() -> int:
            return sum(
                1 for r in config.get_replicas()
                if config.replica_ack_offsets.get(r, -1) >= current_offset
            )

        if count_acked() >= numreplicas:
            return count_acked()

        # Send GETACK directly — bypassing propagate_to_replicas so that
        # master_repl_offset is NOT inflated by this management command.
        getack_cmd = ctx.parser.encode_array([b"REPLCONF", b"GETACK", b"*"])
        stale: list[asyncio.StreamWriter] = []
        for replica in list(replicas):
            try:
                replica.write(getack_cmd)
                await replica.drain()
            except Exception:
                stale.append(replica)
        for s in stale:
            config.unregister_replica(s)

        deadline = asyncio.get_event_loop().time() + timeout / 1000
        while asyncio.get_event_loop().time() < deadline:
            acked = count_acked()
            if acked >= numreplicas:
                return acked
            await asyncio.sleep(0.01)

        return count_acked()
    response = await handle_wait_command(ctx, numreplicas, timeout)
    return response


@command(
    name=b"CONFIG",
    arity=Arity(2, 2),
    flags={"readonly"}
)
def cmd_config(ctx: CommandContext, args: list[bytes]) -> list[bytes]:
    config = ctx.config
    method, attr = args[0], args[1]
    if method.upper() != b"GET":
        raise RESPError("only CONFIG GET is supported")
    if not hasattr(config, attr.decode()):
        raise RESPError("Invalid CONFIG attribute")
    return [attr, getattr(config, attr.decode()).encode()]
    

@command(
    name=b"SAVE",
    arity=Arity(0, 0),
    flags={"readonly"}
)
def cmd_save(ctx: CommandContext, args: list[bytes]) -> str:
    config = ctx.config
    success = get_storage().save(config.dir, config.dbfilename)
    if not success:
        raise RESPError("Error saving data")
    return "OK"


@command(
    name=b"BGSAVE",
    arity=Arity(0, 0),
    flags={"readonly", "coroutine"}
)
async def cmd_bgsave(ctx: CommandContext, args: list[bytes]) -> str:
    config = ctx.config
    async def start_bgsave():
        loop = asyncio.get_running_loop()
        storage = get_storage()
        await loop.run_in_executor(None, storage.save, config.dir, config.dbfilename)
    asyncio.create_task(start_bgsave())
    return "Background saving started"


@command(
    name=b"KEYS",
    arity=Arity(1, 1),
    flags={"readonly"}
)
def cmd_keys(ctx: CommandContext, args: list[bytes]) -> list[bytes]:
    pattern = args[0] if args else b"*"
    return get_storage().keys(pattern)


@command(
    name=b"PUBLISH",
    arity=Arity(2, 2),
    flags={"readonly", "coroutine"}
)
async def cmd_publish(ctx: CommandContext, args: list[bytes]) -> int:
    channel, message = args[0], args[1]
    config = ctx.config
    subscribers = list(config.pubsub.get(channel, set()))
    if subscribers:
        payload = ctx.parser.encode_array([b"message", channel, message])
        for sub_writer in subscribers:
            if not sub_writer.is_closing():
                sub_writer.write(payload)
                await sub_writer.drain()
    return len(subscribers)


