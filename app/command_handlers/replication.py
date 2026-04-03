import asyncio
import os

from app.command_executor import encode_command_as_resp_array
from app.commands import Arity, CommandContext, command
from app.parser import RESPError
from app.resp_types import ArrayType, BaseRESPType, BulkStringType, IntegerType, RawResponse, SimpleStringType


EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
EMPTY_RDB_BYTES = bytes.fromhex(EMPTY_RDB_HEX)


def _encode_rdb_bulk_transfer(rdb_data: bytes) -> bytes:
    return b"$" + str(len(rdb_data)).encode() + b"\r\n" + rdb_data


@command(
    name=b"INFO",
    arity=Arity(0, 1),
    flags={"readonly", "misc", "replication"},
)
def cmd_info(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    config = ctx.app_state.config
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
    flags={"readonly", "replication", "misc"},
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
            return ArrayType(
                [
                    BulkStringType(b"REPLCONF"),
                    BulkStringType(b"ACK"),
                    BulkStringType(str(replica_offset).encode()),
                ]
            )
        case b"ACK":
            ctx.app_state.set_replica_ack_offset(ctx.exec_ctx.connection_writer, int(args[1]))
            return RawResponse(b"")
        case _:
            return SimpleStringType("OK")


@command(
    name=b"PSYNC",
    arity=Arity(2, 2),
    flags={"readonly", "replication", "misc"},
)
def cmd_psync(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    config = ctx.app_state.config
    if args != [b"?", b"-1"]:
        raise RESPError("ERR invalid arguments for PSYNC")

    ctx.app_state.register_replica(None, ctx.exec_ctx.connection_writer)
    rdb_path = os.path.join(config.dir, config.dbfilename)
    if os.path.exists(rdb_path):
        with open(rdb_path, "rb") as rdb_file:
            rdb_data = rdb_file.read()
    else:
        rdb_data = EMPTY_RDB_BYTES

    if rdb_data == EMPTY_RDB_BYTES:
        ctx.app_state.logger.info("RDB files not found. Using empty RDB data")

    payload = (
        SimpleStringType(f"FULLRESYNC {config.master_perlid} {config.master_repl_offset}").encode()
        + _encode_rdb_bulk_transfer(rdb_data)
    )
    return RawResponse(payload)


@command(
    name=b"WAIT",
    arity=Arity(2, 2),
    flags={"readonly", "replication", "coroutine", "misc"},
)
async def cmd_wait(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    numreplicas = int(args[0])
    timeout = int(args[1])
    config = ctx.app_state.config
    replicas = ctx.app_state.get_replicas()

    if numreplicas <= 0:
        return IntegerType(0)

    current_offset = config.master_repl_offset
    if current_offset == 0:
        return IntegerType(len(replicas))

    def count_acked() -> int:
        return sum(
            1
            for replica in ctx.app_state.get_replicas()
            if ctx.app_state.replica_ack_offsets.get(replica, -1) >= current_offset
        )

    if count_acked() >= numreplicas:
        return IntegerType(count_acked())

    getack_payload = encode_command_as_resp_array([b"REPLCONF", b"GETACK", b"*"])
    stale_replicas: list[asyncio.StreamWriter] = []
    for replica in list(replicas):
        try:
            replica.write(getack_payload)
            await replica.drain()
        except Exception:
            stale_replicas.append(replica)

    for stale_replica in stale_replicas:
        ctx.app_state.unregister_replica(stale_replica)

    deadline = asyncio.get_running_loop().time() + timeout / 1000
    while asyncio.get_running_loop().time() < deadline:
        acked = count_acked()
        if acked >= numreplicas:
            return IntegerType(acked)
        await asyncio.sleep(0.01)

    return IntegerType(count_acked())
