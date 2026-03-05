import asyncio
from collections.abc import Sequence

from app.parser import RESPError, RESPParser
from app.storage import get_storage


def handle_set_command(key: bytes, value: bytes, ttl: int | float | None = None) -> bool:
    """Handle the SET command logic."""
    try:
        storage = get_storage()
        return storage.set(key, value, ttl)
    except Exception as e:
        print("Unexpected exception occured:", e)
        return False


def handle_get_command(key: bytes) -> bytes | None:
    """Return the stored value for *key* or ``None`` if missing."""
    storage = get_storage()
    return storage.get(key)


def handle_lpush_command(key: bytes, *values: bytes) -> int:
    """Insert *values* at the head of list *key* and return new length."""
    storage = get_storage()
    return storage.lpush(key, *values)


def handle_llen_command(key: bytes) -> int:
    """Return the length of the list stored at *key*."""
    storage = get_storage()
    return storage.llen(key)


def handle_lpop_command(key: bytes, count: int | None = None) -> list[bytes] | None:
    """Pop one or more elements from the head of list *key*."""
    storage = get_storage()
    return storage.lpop(key, count)


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


def handle_xadd_command(key: bytes, stream_id: bytes, payload: list[bytes]) -> str:
    """Handle the XADD command logic."""
    storage = get_storage()
    return storage.xadd(key, stream_id, payload)


def handle_incr_command(key: bytes) -> int:
    """Increment the integer value stored at *key* by 1."""
    storage = get_storage()
    return storage.incr(key)


def _normalize_command(data: list[bytes | str]) -> list[bytes]:
    """Normalize parsed RESP array items to bytes for command dispatch."""
    normalized: list[bytes] = []
    for item in data:
        if isinstance(item, bytes):
            normalized.append(item)
        else:
            normalized.append(str(item).encode())
    normalized[0] = normalized[0].upper()
    return normalized


def _encode_raw_resp_array(responses: Sequence[bytes]) -> bytes:
    """Build RESP array from pre-encoded RESP elements."""
    return b"*" + str(len(responses)).encode() + b"\r\n" + b"".join(responses)


async def _execute_command(command: list[bytes], parser: RESPParser) -> bytes:
    """Execute a single command and return RESP-encoded response."""
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
                if handle_set_command(key, value, ttl):
                    response = parser.encode_simple_string("OK")
                else:
                    response = parser.encode_simple_error("Failed to set key")
            case [b"GET", key]:
                value = handle_get_command(key)
                if value is not None:
                    payload = value if isinstance(value, bytes) else str(value).encode()
                    response = parser.encode_bulk_string(payload)
                else:
                    response = parser.encode_null()
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
            case [_, *_]:
                response = parser.encode_simple_error("unknown command")
    except (ValueError, TypeError, RESPError, AssertionError) as e:
        message = str(e)
        if message.startswith("-ERR "):
            message = message[5:]
        response = parser.encode_simple_error(message)
    return response


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Async client connection handler implementing a small RESP server."""
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
                        responses = []
                        for queued in commands_to_execute:
                            responses.append(await _execute_command(queued, parser))
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
                        response = await _execute_command(command, parser)

            writer.write(response)
            await writer.drain()
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
