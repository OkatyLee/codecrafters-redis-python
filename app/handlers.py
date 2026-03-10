import asyncio
from collections.abc import Sequence
import os
from app.commands import COMMAND_WRITE_FLAGS, ExecCtx, redis_command
from app.config import ServerConfig
from app.parser import RESPError, RESPParser
from app.storage import get_storage

EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
EMPTY_RDB_BYTES = bytes.fromhex(EMPTY_RDB_HEX)


def encode_command_as_resp_array(command: Sequence[bytes]) -> bytes:
    payload = b"*" + str(len(command)).encode() + b"\r\n"
    for item in command:
        payload += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
    return payload


def build_exec_ctx(
    command: Sequence[bytes],
    config: ServerConfig | None,
    *,
    from_replication: bool,
    replica_writer: asyncio.StreamWriter | None = None,
) -> ExecCtx:
    return ExecCtx(
        from_replication=from_replication,
        raw_resp_command=encode_command_as_resp_array(command),
        propagate=lambda payload, cfg=config: propagate_to_replicas(cfg, payload),
        replica_writer=replica_writer,
    )


async def propagate_to_replicas(config: ServerConfig | None, payload: bytes) -> None:
    if config is None or config.role != "master" or not config.replica_writers:
        return

    config.increment_master_repl_offset(len(payload))

    stale_writers: list[asyncio.StreamWriter] = []
    for replica_writer in list(config.replica_writers):
        try:
            replica_writer.write(payload)
            await replica_writer.drain()
        except Exception:
            stale_writers.append(replica_writer)

    for stale_writer in stale_writers:
        config.unregister_replica(stale_writer)
        stale_writer.close()
        await stale_writer.wait_closed()


@redis_command(b"SET", is_write=True)
def handle_set_command(key: bytes, value: bytes, ttl: int | float | None = None) -> bool:
    storage = get_storage()
    return storage.set(key, value, ttl)


def handle_get_command(key: bytes) -> bytes | None:
    """Return the stored value for *key* or ``None`` if missing."""
    storage = get_storage()
    return storage.get(key)


@redis_command(b"LPUSH", is_write=True)
def handle_lpush_command(key: bytes, *values: bytes) -> int:
    """Insert *values* at the head of list *key* and return new length."""
    storage = get_storage()
    return storage.lpush(key, *values)


def handle_llen_command(key: bytes) -> int:
    """Return the length of the list stored at *key*."""
    storage = get_storage()
    return storage.llen(key)


@redis_command(b"LPOP", is_write=True)
def handle_lpop_command(key: bytes, count: int | None = None) -> list[bytes] | None:
    """Pop one or more elements from the head of list *key*."""
    storage = get_storage()
    return storage.lpop(key, count)


@redis_command(b"RPUSH", is_write=True)
def handle_rpush_command(key: bytes, *values: bytes) -> int:
    """Append *values* to the tail of list *key* and return new length."""
    storage = get_storage()
    return storage.rpush(key, *values)


def handle_lrange_command(key: bytes, start: int, end: int) -> list[bytes]:
    """Return a slice of the list at *key* from *start* to *end*."""
    storage = get_storage()
    return storage.lrange(key, start, end)


def handle_type_command(key: bytes) -> str | None:
    """Return the type of the value stored at *key*."""
    storage = get_storage()
    return storage.type(key)


def handle_xrange_command(
    key: bytes,
    start: str | bytes,
    end: str | bytes,
) -> list[list[str | dict[bytes, bytes]]]:
    """Handle the XRANGE command logic."""
    storage = get_storage()
    return storage.xrange(key, start, end)


def handle_xread_streams_command(
    keys: Sequence[bytes],
    ids: Sequence[str | bytes],
) -> list[list[bytes | str | list[list[str | dict[bytes, bytes]]]]]:
    """Handle the XREAD command logic for multiple streams."""
    storage = get_storage()
    return storage.xread_streams(keys, ids)


def resolve_xread_start_ids(keys: Sequence[bytes], ids: Sequence[bytes]) -> list[str | bytes]:
    """Resolve XREAD starting IDs once (including ``$`` semantics)."""
    resolved_ids: list[str | bytes] = []
    for index, raw_id in enumerate(ids):
        if raw_id == b"$":
            entries = handle_xrange_command(keys[index], b"-", b"+")
            if entries:
                entry_id = entries[-1][0]
                assert isinstance(entry_id, str)
                resolved_ids.append(entry_id)
            else:
                resolved_ids.append("0-0")
        else:
            resolved_ids.append(raw_id)
    return resolved_ids


def handle_info_command(args: list[bytes], config: ServerConfig | None = None) -> bytes:
    del args
    if config is None:
        return b"Error: Config is None"
    role = b"role:" + config.role.encode()
    master_replid = (
        b"master_replid:" + config.master_perlid.encode()
        if config.master_perlid
        else b"master_replid:"
    )
    master_repl_offset = b"master_repl_offset:" + str(config.master_repl_offset).encode()
    return role + b"\r\n" + master_replid + b"\r\n" + master_repl_offset


async def handle_blpop_command(*keys: bytes, timeout: float = 0) -> tuple[bytes, bytes] | None:
    """Async wrapper for storage.blpop."""
    if len(keys) < 1:
        raise ValueError("Expected at least 1 key for command BLPOP")
    storage = get_storage()
    result = await storage.blpop(*keys, timeout=timeout)
    if result is None:
        return None
    key, value = result
    key_payload = key if isinstance(key, bytes) else str(key).encode()
    val_payload = value if isinstance(value, bytes) else str(value).encode()
    return key_payload, val_payload


@redis_command(b"XADD", is_write=True)
def handle_xadd_command(key: bytes, stream_id: bytes, payload: list[bytes]) -> str:
    """Handle the XADD command logic."""
    storage = get_storage()
    return storage.xadd(key, stream_id, payload)


@redis_command(b"INCR", is_write=True)
def handle_incr_command(key: bytes) -> int:
    """Increment the integer value stored at *key* by 1."""
    storage = get_storage()
    return storage.incr(key)


@redis_command(b"DEL", is_write=True)
def handle_del_command(key: bytes) -> bool:
    storage = get_storage()
    return storage.delete(key)



def handle_replconf_getack_command(config: ServerConfig | None) -> int:
    assert config is not None
    return config.get_replica_offset()


def handle_replconf_ack_command(
    config: ServerConfig | None,
    replica_writer: asyncio.StreamWriter | None,
    offset: bytes,
) -> None:
    assert config is not None
    config.set_replica_ack_offset(replica_writer, int(offset))


async def handle_wait_command(config: ServerConfig, numreplicas: int, timeout: int) -> int:
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
    getack_cmd = encode_command_as_resp_array([b"REPLCONF", b"GETACK", b"*"])
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


def handle_save_command(config: ServerConfig) -> bool:
    """Handle the SAVE command logic."""
    storage = get_storage()
    return storage.save(config.dir, config.dbfilename)


async def handle_bgsave_command(config: ServerConfig) -> str:
    """Handle the BGSAVE command: save RDB in the background."""
    loop = asyncio.get_running_loop()
    storage = get_storage()
    await loop.run_in_executor(None, storage.save, config.dir, config.dbfilename)
    return "Background saving started"


def handle_keys_command(pattern: bytes = b"*") -> list[bytes]:
    """Return all keys in storage matching *pattern* (Redis glob syntax)."""
    storage = get_storage()
    return storage.keys(pattern)

def _normalize_command(data: Sequence[object]) -> list[bytes]:
    """Normalize parsed RESP array items to bytes and uppercase command name."""
    normalized = [item if isinstance(item, bytes) else str(item).encode() for item in data]
    normalized[0] = normalized[0].upper()
    return normalized


def _encode_raw_resp_array(responses: Sequence[bytes]) -> bytes:
    """Build RESP array from pre-encoded RESP elements."""
    return b"*" + str(len(responses)).encode() + b"\r\n" + b"".join(responses)


async def _execute_command(
    command: list[bytes],
    parser: RESPParser,
    config: ServerConfig | None,
    ctx: ExecCtx | None = None,
) -> bytes:
    if ctx is None:
        ctx = build_exec_ctx(command, config, from_replication=False)

    response = b""
    try:
        match command:
            case [b"PING"]:
                response = parser.encode_simple_string("PONG")
            case [b"ECHO", payload]:
                response = parser.encode_bulk_string(payload)
            case [b"SET", key, value, *args]:
                if len(args) % 2 != 0:
                    raise ValueError("syntax error")
                params = dict(zip(args[::2], args[1::2], strict=False))
                if params.get(b"EX") and params.get(b"PX"):
                    raise ValueError("syntax error. Only one of EX or PX is allowed")
                px = params.get(b"PX")
                if px is not None:
                    px = int(px) / 1000
                ex = params.get(b"EX")
                if ex is not None:
                    ex = int(ex)
                ttl = ex if ex is not None else px
                response = (
                    parser.encode_simple_string("OK")
                    if handle_set_command(key, value, ttl)
                    else parser.encode_simple_error("Failed to set key")
                )
            case [b"GET", key]:
                value = handle_get_command(key)
                if value is not None:
                    payload = value if isinstance(value, bytes) else str(value).encode()
                    response = parser.encode_bulk_string(payload)
                else:
                    response = parser.encode_null()
            case [b"DEL", key]:
                response = (
                    parser.encode_simple_string("OK")
                    if handle_del_command(key)
                    else parser.encode_simple_error("Failed to delete key")
                )
            case [b"RPUSH", key, *values]:
                if len(values) == 0:
                    raise ValueError("wrong number of arguments for 'rpush' command")
                response = parser.encode_integer(handle_rpush_command(key, *values))
            case [b"LPUSH", key, *values]:
                if len(values) == 0:
                    raise ValueError("wrong number of arguments for 'lpush' command")
                response = parser.encode_integer(handle_lpush_command(key, *values))
            case [b"LRANGE", key, start, end]:
                response = parser.encode_array(handle_lrange_command(key, int(start), int(end)))
            case [b"LLEN", key]:
                response = parser.encode_integer(handle_llen_command(key))
            case [b"LPOP", key, *args]:
                if len(args) > 1:
                    raise ValueError("wrong number of arguments for 'lpop' command")
                count = int(args[0]) if args else None
                values = handle_lpop_command(key, count=count)
                if values is None:
                    response = parser.encode_null()
                elif count is None:
                    payload = values[0] if isinstance(values[0], bytes) else str(values[0]).encode()
                    response = parser.encode_bulk_string(payload)
                else:
                    response = parser.encode_array(values)
            case [b"BLPOP", *keys, timeout]:
                if len(keys) == 0:
                    raise ValueError("wrong number of arguments for 'blpop' command")
                result = await handle_blpop_command(*keys, timeout=float(timeout))
                if result is None:
                    response = parser.encode_null_array()
                else:
                    response = parser.encode_array([result[0], result[1]])
            case [b"TYPE", key]:
                response = parser.encode_simple_string(str(handle_type_command(key)))
            case [b"XADD", stream_key, stream_id, *payload]:
                response = parser.encode_simple_string(handle_xadd_command(stream_key, stream_id, list(payload)))
            case [b"XRANGE", stream_key, start, end]:
                response = parser.encode_array(handle_xrange_command(stream_key, start, end))
            case [b"XREAD", *args]:
                if len(args) == 0:
                    raise ValueError("wrong number of arguments for 'xread' command")
                parse_index = 0
                block_timeout_seconds: float | None = None
                if len(args) >= 2 and args[0].upper() == b"BLOCK":
                    block_ms = int(args[1])
                    if block_ms < 0:
                        raise ValueError("timeout is negative")
                    block_timeout_seconds = block_ms / 1000
                    parse_index = 2
                if parse_index >= len(args) or args[parse_index].upper() != b"STREAMS":
                    raise ValueError("syntax error")
                keys_and_ids = args[parse_index + 1 :]
                if len(keys_and_ids) == 0 or len(keys_and_ids) % 2 != 0:
                    raise ValueError("Unbalanced XREAD list of streams")

                split_index = len(keys_and_ids) // 2
                keys = keys_and_ids[:split_index]
                ids = keys_and_ids[split_index:]
                resolved_ids = resolve_xread_start_ids(keys, ids)
                entries = handle_xread_streams_command(keys, resolved_ids)
                if entries:
                    response = parser.encode_array(entries)
                elif block_timeout_seconds is None:
                    response = parser.encode_null()
                elif block_timeout_seconds == 0:
                    while True:
                        await asyncio.sleep(0.05)
                        entries = handle_xread_streams_command(keys, resolved_ids)
                        if entries:
                            response = parser.encode_array(entries)
                            break
                else:
                    deadline = asyncio.get_running_loop().time() + block_timeout_seconds
                    while asyncio.get_running_loop().time() < deadline:
                        await asyncio.sleep(0.05)
                        entries = handle_xread_streams_command(keys, resolved_ids)
                        if entries:
                            response = parser.encode_array(entries)
                            break
                    else:
                        response = parser.encode_null()
            case [b"INCR", key]:
                response = parser.encode_integer(handle_incr_command(key))
            case [b"INFO", *args]:
                response = parser.encode_bulk_string(handle_info_command(list(args), config))
            case [b"REPLCONF", b"listening-port", replica_port]:
                assert config is not None
                config.register_replica(int(replica_port), None)
                response = parser.encode_simple_string("OK")
            case [b"REPLCONF", b"GETACK", *args]:
                assert config is not None
                del args
                replica_offset = handle_replconf_getack_command(config)
                response = parser.encode_array([b"REPLCONF", b"ACK", str(replica_offset).encode()])
            case [b"REPLCONF", b"ACK", offset]:
                assert config is not None
                handle_replconf_ack_command(config, ctx.replica_writer, offset)
                response = b""
            case [b"REPLCONF", *_]:
                response = parser.encode_simple_string("OK")
            case [b"PSYNC", b"?", b"-1"]:
                assert config is not None
                config.register_replica(None, ctx.replica_writer)
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
                response = parser.encode_simple_string(
                    f"FULLRESYNC {repl_id} {offset}"
                ) + parser.encode_bulk_string(rdb_data)
            case [b"WAIT", numreplicas, timeout]:
                assert config is not None
                response = parser.encode_integer(await handle_wait_command(config, int(numreplicas), int(timeout)))
            case [b"CONFIG", arg1, arg2]:
                assert arg1.upper() == b'GET'
                assert hasattr(config, arg2.decode())
                response = parser.encode_array([arg2, getattr(config, arg2.decode()).encode()])
            case [b"SAVE"]:
                assert config is not None
                is_success = handle_save_command(config)
                response = parser.encode_simple_string("OK") if is_success else parser.encode_simple_error("Error saving data")
            case [b"KEYS", pattern]:
                response = parser.encode_array(handle_keys_command(pattern))
            case [b"BGSAVE"]:
                assert config is not None
                asyncio.create_task(handle_bgsave_command(config))
                response = parser.encode_simple_string("Background saving started")
            case [_, *_]:
                response = parser.encode_simple_error("unknown command")
    except (ValueError, TypeError, RESPError, AssertionError) as e:
        message = str(e)
        if message.startswith("-ERR "):
            message = message[5:]
        response = parser.encode_simple_error(message)

    is_write = COMMAND_WRITE_FLAGS.get(command[0], False)
    if is_write and not ctx.from_replication and not response.startswith(b"-"):
        await ctx.propagate(ctx.raw_resp_command)

    return response


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    config: ServerConfig | None = None,
) -> None:
    addr = writer.get_extra_info("peername")
    print(f"Connected to client at {addr}")
    parser = RESPParser(reader)
    in_multi = False
    queued_commands: list[list[bytes]] = []
    try:
        while True:
            data = await parser.parse()
            if data is None:
                break
            if not isinstance(data, list) or len(data) == 0:
                response = parser.encode_simple_error("Protocol error")
                writer.write(response)
                await writer.drain()
                continue

            command = _normalize_command(data)

            match command:
                case [b"MULTI"]:
                    if in_multi:
                        response = parser.encode_simple_error("MULTI calls can not be nested")
                    else:
                        in_multi = True
                        queued_commands.clear()
                        response = parser.encode_simple_string("OK")
                case [b"EXEC"]:
                    if not in_multi:
                        response = parser.encode_simple_error("EXEC without MULTI")
                    else:
                        commands_to_execute = queued_commands.copy()
                        queued_commands.clear()
                        in_multi = False
                        responses: list[bytes] = []
                        for queued in commands_to_execute:
                            queued_ctx = build_exec_ctx(
                                queued,
                                config,
                                from_replication=False,
                                replica_writer=writer,
                            )
                            responses.append(
                                await _execute_command(queued, parser, config, queued_ctx)
                            )
                        response = _encode_raw_resp_array(responses)
                case [b"DISCARD"]:
                    if not in_multi:
                        response = parser.encode_simple_error("DISCARD without MULTI")
                    else:
                        queued_commands.clear()
                        in_multi = False
                        response = parser.encode_simple_string("OK")
                case _:
                    if in_multi:
                        queued_commands.append(command)
                        response = parser.encode_simple_string("QUEUED")
                    else:
                        ctx = build_exec_ctx(
                            command,
                            config,
                            from_replication=False,
                            replica_writer=writer,
                        )
                        response = await _execute_command(command, parser, config, ctx)

            writer.write(response)
            await writer.drain()
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        if config is not None and writer in config.replica_writers:
            config.replica_writers.remove(writer)
        writer.close()
        await writer.wait_closed()
