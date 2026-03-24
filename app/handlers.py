import asyncio
from collections.abc import Sequence
from hashlib import sha256
import os
from app.session import ClientSession
from app.commands import COMMAND_WRITE_FLAGS, ExecCtx, redis_command
from app.config import ServerConfig
from app.parser import RESPError, RESPParser
from app.storage import get_storage

EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
EMPTY_RDB_BYTES = bytes.fromhex(EMPTY_RDB_HEX)
ALLOWED_BEFORE_AUTH = {b"AUTH"}


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
    session: ClientSession | None = None
) -> ExecCtx:
    return ExecCtx(
        from_replication=from_replication,
        raw_resp_command=encode_command_as_resp_array(command),
        propagate=lambda payload, cfg=config: propagate_to_replicas(cfg, payload),
        replica_writer=replica_writer,
        session=session,
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


@redis_command(b"ZADD", is_write=True)
def handle_zadd_command(key: bytes, score: float, member: bytes) -> int:
    storage = get_storage()
    return storage.zadd(key, score, member)


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


async def handle_publish_command(config: ServerConfig, channel: bytes, message: bytes) -> int:
    """Publish *message* to all subscribers of *channel*.

    Returns the number of clients that received the message.
    """
    subscribers = list(config.pubsub.get(channel, set()))
    if subscribers:
        payload = encode_command_as_resp_array([b"message", channel, message])
        for sub_writer in subscribers:
            if not sub_writer.is_closing():
                sub_writer.write(payload)
    return len(subscribers)


def handle_keys_command(pattern: bytes = b"*") -> list[bytes]:
    """Return all keys in storage matching *pattern* (Redis glob syntax)."""
    storage = get_storage()
    return storage.keys(pattern)


def handle_zrank_command(key: bytes, member: bytes) -> int | None:
    storage = get_storage()
    return storage.zrank(key, member)


def handle_zrange_command(key: bytes, start: int, end: int) -> list[bytes]:
    storage = get_storage()
    return storage.zrange(key, start, end)


def handle_zcard_command(key: bytes) -> int:
    storage = get_storage()
    return storage.zcard(key)


def handle_zscore_command(key: bytes, member: bytes) -> float | None:
    storage = get_storage()
    return storage.zscore(key, member)


@redis_command(b"ZREM", is_write=True)
def handle_zrem_command(key: bytes, members: list[bytes]) -> int:
    storage = get_storage()
    return storage.zrem(key, members)


@redis_command(b"GEOADD", is_write=True)
def handle_geoadd_command(key: bytes, longitude: float, latitude: float, member: bytes) -> int:
    storage = get_storage()
    return storage.geoadd(key, longitude, latitude, member)


def handle_geopos_command(key: bytes, members: list[bytes]) -> list[list[bytes] | None]:
    storage = get_storage()
    return storage.geopos(key, members)


def handle_geodist_command(key: bytes, member1: bytes, member2: bytes) -> float | None:
    storage = get_storage()
    return storage.geodist(key, member1, member2)


def handle_geosearch_command(key: bytes, longitude: bytes, latitude: bytes, radius: bytes, unit: bytes) -> list[bytes]:
    storage = get_storage()
    return storage.geosearch(key, longitude, latitude, radius, unit)


def _normalize_command(data: Sequence[object]) -> list[bytes]:
    """Normalize parsed RESP array items to bytes and uppercase command name."""
    normalized = [item if isinstance(item, bytes) else str(item).encode() for item in data]
    normalized[0] = normalized[0].upper()
    return normalized


def _encode_raw_resp_array(responses: Sequence[bytes]) -> bytes:
    """Build RESP array from pre-encoded RESP elements."""
    return b"*" + str(len(responses)).encode() + b"\r\n" + b"".join(responses)


def _requires_auth(
    command: list[bytes],
    config: ServerConfig | None,
    session: ClientSession | None,
) -> bool:
    if config is None or session is None:
        return False
    if session.is_authenticated:
        return False
    return command[0].upper() not in ALLOWED_BEFORE_AUTH


def handle_acl_setuser_command(args: list[bytes], config: ServerConfig, parser: RESPParser) -> bytes:
    if len(args) != 2:
        return parser.encode_simple_error("wrong number of arguments for 'ACL SETUSER'")

    username, password = args[0], args[1]
    if not password.startswith(b'>'):
        
        return parser.encode_simple_error("invalid password format for 'ACL SETUSER'")

    user = config.ensure_acl_user(username.decode())
    hashed_password = sha256(password[1:]).digest()
    user.passwords.add(hashed_password)
    user.nopass = False
    return parser.encode_simple_string("OK")


def handle_acl_getuser_command(username: bytes, config: ServerConfig, parser: RESPParser) -> bytes:
    user = config.get_acl_user(username.decode())
    if user is None:
        return parser.encode_null_array()
    else:
        response_array: list[bytes | list[bytes]] = []
        response_array.append(b"flags")
        response_array.append([b"nopass"] if user.nopass else [])
        response_array.append(b"password")
        response_array.append([p for p in user.passwords] if len(user.passwords) > 0 else [])

        return parser.encode_array(response_array)
    

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
                params = {
                    option.upper(): option_value
                    for option, option_value in zip(args[::2], args[1::2], strict=False)
                }
                unknown_options = [opt for opt in params if opt not in (b"EX", b"PX")]
                if unknown_options:
                    raise ValueError("syntax error")
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
            case [b"PUBLISH", channel, message]:
                assert config is not None
                count = await handle_publish_command(config, channel, message)
                response = parser.encode_integer(count)
            case [b"ZADD", key, score, member]:
                is_existing = handle_zadd_command(key, float(score), member)
                response = parser.encode_integer(1 if not is_existing else 0)
            case [b"ZRANK", key, member]:
                rank = handle_zrank_command(key, member)
                response = parser.encode_integer(rank) if rank is not None else parser.encode_null_array()
            case [b"ZRANGE", key, start, end]:
                members = handle_zrange_command(key, int(start), int(end))
                response = parser.encode_array(members)
                print(response)
            case [b"ZCARD", key]:
                count = handle_zcard_command(key)
                response = parser.encode_integer(count)
            case [b"ZSCORE", key, member]:
                score = handle_zscore_command(key, member)
                response = parser.encode_bulk_string(str(score).encode()) if score is not None else parser.encode_null()
            case [b"ZREM", key, *members]:
                removed_count = handle_zrem_command(key, members)
                response = parser.encode_integer(removed_count)
            case [b"GEOADD", key, longitude, latitude, member]:
                num_added = handle_geoadd_command(key, float(longitude), float(latitude), member)
                response = parser.encode_integer(num_added)
            case [b"GEOPOS", key, *members]:
                positions = handle_geopos_command(key, members)
                print(positions)
                response = parser.encode_array(positions)
            case [b"GEODIST", key, member1, member2]:
                distance = handle_geodist_command(key, member1, member2)
                response = parser.encode_bulk_string(str(distance).encode()) if distance is not None else parser.encode_null()
            case [b"GEOSEARCH", key, *args]:
                search_mode = args[0]
                if search_mode.upper() != b'FROMLONLAT':
                    raise RESPError("only FROMLONLAT is supported")
                longitude, latitude = args[1], args[2]
                search_opt = args[3]
                if search_opt.upper() != b"BYRADIUS":
                    raise RESPError("only BYRADIUS is supported")
                radius, unit = args[4], args[5]
                members = handle_geosearch_command(key, longitude, latitude, radius, unit)
                response = parser.encode_array(members)
            case [b"ACL", *args]:
                assert config is not None
                assert ctx.session is not None
                assert len(args) > 0
                
                match args[0].upper():
                    
                    case b"WHOAMI":
                        username = ctx.session.current_username if ctx.session.current_username else "default" 
                        response = parser.encode_bulk_string(username.encode())
                        
                    case b"GETUSER":
                        response = handle_acl_getuser_command(args[1], config, parser)

                    case b"SETUSER":
                        response = handle_acl_setuser_command(args[1:], config, parser)

            case [b"AUTH", *args]:
                assert config is not None
                assert ctx.session is not None
                assert len(args) in [1, 2]
                
                username, password = (args[0], args[1]) if len(args) == 2 else (b"default", args[0])
                user = config.get_acl_user(username.decode())
                if user is None or not user.check_password(password):
                    response = parser.encode_simple_error("WRONGPASS invalid username-password pair or user is disabled.")
                else:
                    ctx.session.login(user.name)
                    response = parser.encode_simple_string("OK")

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
    if config is None:
        config = ServerConfig("127.0.0.1", 0)

    addr = writer.get_extra_info("peername")
    print(f"Connected to client at {addr}")
    parser = RESPParser(reader)
    default_user = config.get_acl_user("default")
    session = ClientSession.create(default_user)
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
            if _requires_auth(command, config, session):
                response = parser.encode_simple_error("NOAUTH  Authentication required.")
                writer.write(response)
                await writer.drain()
                continue
            
            match command:
                case [b"MULTI"]:
                    if session.in_multi:
                        response = parser.encode_simple_error("MULTI calls can not be nested")
                    else:
                        session.in_multi = True
                        session.queued_commands.clear()
                        response = parser.encode_simple_string("OK")
                case [b"EXEC"]:
                    if not session.in_multi:
                        response = parser.encode_simple_error("EXEC without MULTI")
                    else:
                        commands_to_execute = session.queued_commands.copy()
                        session.queued_commands.clear()
                        session.in_multi = False
                        responses: list[bytes] = []
                        for queued in commands_to_execute:
                            queued_ctx = build_exec_ctx(
                                queued,
                                config,
                                from_replication=False,
                                replica_writer=writer,
                                session=session
                            )
                            responses.append(
                                await _execute_command(queued, parser, config, queued_ctx)
                            )
                        response = _encode_raw_resp_array(responses)
                case [b"DISCARD"]:
                    if not session.in_multi:
                        response = parser.encode_simple_error("DISCARD without MULTI")
                    else:
                        session.queued_commands.clear()
                        session.in_multi = False
                        response = parser.encode_simple_string("OK")
                case [b"SUBSCRIBE", *channels]:
                    assert config is not None
                    if not channels:
                        response = parser.encode_simple_error("wrong number of arguments for 'subscribe' command")
                    else:
                        combined = b""
                        for channel in channels:
                            ch = channel if isinstance(channel, bytes) else str(channel).encode()
                            config.pubsub[ch].add(writer)
                            session.subscribed_channels.add(ch)
                            combined += parser.encode_array([b"subscribe", ch, len(session.subscribed_channels)])
                        session.in_subscribed_mode = True
                        response = combined
                case [b"UNSUBSCRIBE", *channels]:
                    assert config is not None
                    targets = (
                        [c if isinstance(c, bytes) else str(c).encode() for c in channels]
                        if channels else list(session.subscribed_channels)
                    )
                    combined = b""
                    for ch in targets:
                        session.subscribed_channels.discard(ch)
                        config.pubsub[ch].discard(writer)
                        combined += parser.encode_array([b"unsubscribe", ch, len(session.subscribed_channels)])
                    if not session.subscribed_channels:
                        session.in_subscribed_mode = False
                    response = combined if combined else parser.encode_array([b"unsubscribe", None, 0])
                case _:
                    if session.in_subscribed_mode:
                        if command[0] == b"PING":
                            msg = command[1] if len(command) > 1 else b""
                            response = parser.encode_array([b"pong", msg])
                        else:
                            response = parser.encode_simple_error(
                                "ERR Command not allowed in subscribe mode"
                            )
                    elif session.in_multi:  
                        session.queued_commands.append(command)
                        response = parser.encode_simple_string("QUEUED")
                    else:
                        ctx = build_exec_ctx(
                            command,
                            config,
                            from_replication=False,
                            replica_writer=writer,
                            session=session
                        )
                        response = await _execute_command(command, parser, config, ctx)

            writer.write(response)
            await writer.drain()
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        if config is not None:
            if writer in config.replica_writers:
                config.replica_writers.remove(writer)
            for ch in session.subscribed_channels:
                config.pubsub[ch].discard(writer)
        writer.close()
        await writer.wait_closed()
