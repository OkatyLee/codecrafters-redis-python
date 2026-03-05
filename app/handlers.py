import asyncio
from collections.abc import Sequence

from app.parser import RESPParser, RESPError
from app.storage import get_storage


def handle_set_command(key: bytes, value: bytes, ttl: int | float | None = None) -> bool:
    """Handle the SET command logic.

    Returns ``True`` on success and ``False`` if an exception occurred.
    """
    try:
        storage = get_storage()
        result = storage.set(key, value, ttl)
        return result
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
    """Pop one or more elements from the head of list *key*.

    Returns a list of popped elements or ``None`` when the key is missing.
    """
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
    """Handle the XRANGE command logic.

    Returns a list of (entry_id, entry) tuples.
    """
    storage = get_storage()
    return storage.xrange(key, start, end)

def handle_xread_streams_command(
    keys: Sequence[bytes],
    ids: Sequence[str | bytes],
) -> list[list[bytes | str | list[list[str | dict[bytes, bytes]]]]]:
    """Handle the XREAD command logic for multiple streams.

    Returns a list of (entry_id, entry) tuples.
    """
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
    """Async wrapper for storage.blpop.

    Accepts multiple keys and an optional timeout in seconds.
    """
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
    """Handle the XADD command logic.

    Returns the stream ID of the added entry.
    """
    storage = get_storage()
    return storage.xadd(key, stream_id, payload)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Async client connection handler implementing a small RESP server.

    The handler reads RESP commands using :class:`RESPParser`, dispatches a
    subset of Redis-like commands implemented in this project and writes RESP
    encoded replies to the client socket.
    """
    addr = writer.get_extra_info("peername")
    print(f"Connected to client at {addr}")
    parser = RESPParser(reader)
    try:
        while True:
            data = await parser.parse()
            if data is None:
                break
            
            assert isinstance(data, list)
            for i, val in enumerate(data):
                if not isinstance(val, bytes):
                    data[i] = str(val).encode()
            assert isinstance(data[0], bytes)
            data[0] = data[0].upper()
            print(data)
            response = b""
            try:
                match data:
                    case [b"PING"]:
                        response = parser.encode_simple_string("PONG")
                    case [b"ECHO", payload]:
                        assert isinstance(payload, bytes)
                        response = parser.encode_bulk_string(payload)
                    case [b"SET", key, value, *args]:
                        assert isinstance(key, bytes) and isinstance(value, bytes)
                        assert len(args) % 2 == 0
                        if not all(isinstance(arg, bytes) for arg in args):
                            raise TypeError("SET options must be bulk strings")
                        byte_args = [arg for arg in args if isinstance(arg, bytes)]
                        params = dict(zip(byte_args[::2], byte_args[1::2], strict=False))
                        if params.get(b"EX") and params.get(b"PX"):
                            raise ValueError("syntax error. Only one of EX or PX is allowed")
                        px = params.get(b"PX")
                        if px is not None:
                            px = int(px) / 1000
                        ex = params.get(b"EX")
                        if ex is not None:
                            ex = int(ex)
                        ttl = ex if ex is not None else px
                        flag = handle_set_command(key, value, ttl)
                        response = parser.encode_simple_string("OK") if flag else parser.encode_simple_error("Failed to set key")
                    case [b"GET", key]:
                        assert isinstance(key, bytes)
                        value = handle_get_command(key)
                        if value is not None:
                            payload = value if isinstance(value, bytes) else str(value).encode()
                            response = parser.encode_bulk_string(payload)
                        else:
                            response = parser.encode_null()
                    case [b"RPUSH", key, *values]:
                        assert len(values) > 0
                        assert isinstance(key, bytes)
                        if not all(isinstance(value, bytes) for value in values):
                            raise TypeError("RPUSH values must be bulk strings")
                        byte_values = [value for value in values if isinstance(value, bytes)]
                        length = handle_rpush_command(key, *byte_values)
                        response = parser.encode_integer(length)
                    case [b"LPUSH", key, *values]:
                        assert len(values) > 0
                        assert isinstance(key, bytes)
                        if not all(isinstance(value, bytes) for value in values):
                            raise TypeError("LPUSH values must be bulk strings")
                        byte_values = [value for value in values if isinstance(value, bytes)]
                        length = handle_lpush_command(key, *byte_values)
                        response = parser.encode_integer(length)
                    case [b"LRANGE", key, start, end]:
                        assert isinstance(key, bytes)
                        assert isinstance(start, bytes)
                        assert isinstance(end, bytes)
                        try:
                            start_int = int(start)
                            end_int = int(end)
                        except:
                            raise ValueError("Invalid start or end value")
                        values = handle_lrange_command(key, start_int, end_int)
                        response = parser.encode_array(values)
                    case [b"LLEN", key]:
                        assert isinstance(key, bytes)
                        length = handle_llen_command(key)
                        response = parser.encode_integer(length)
                    case [b"LPOP", key, *args]:
                        assert isinstance(key, bytes)
                        if len(args) > 1:
                            raise ValueError("wrong number of arguments for 'lpop' command")
                        count_arg: bytes | None = None
                        if len(args) > 0:
                            raw_count = args[0]
                            if not isinstance(raw_count, bytes):
                                raise TypeError("LPOP count must be a bulk string")
                            count_arg = raw_count
                        count = int(count_arg) if count_arg is not None else None
                        values = handle_lpop_command(key, count=count)
                        if not values is None:
                            if not count is None:
                                
                                response = parser.encode_array(values)
                            else:
                                payload = values[0] if isinstance(values[0], bytes) else str(values[0]).encode()
                                response = parser.encode_bulk_string(payload)
                        else:
                            response = parser.encode_null()
                    case [b"BLPOP", *keys, timeout]:
                        if len(keys) == 0:
                            raise ValueError("wrong number of arguments for 'blpop' command")
                        if not all(isinstance(key_name, bytes) for key_name in keys):
                            raise TypeError("BLPOP keys must be bulk strings")
                        typed_keys = [key_name for key_name in keys if isinstance(key_name, bytes)]
                        if not isinstance(timeout, (bytes, str, int, float)):
                            raise TypeError("BLPOP timeout has invalid type")
                        timeout = float(timeout)
                        result = await handle_blpop_command(*typed_keys, timeout=timeout)
                        if result is not None:
                            key_name, value = result
                            key_payload = key_name if isinstance(key_name, bytes) else str(key_name).encode()
                            val_payload = value if isinstance(value, bytes) else str(value).encode()
                            response = parser.encode_array([key_payload, val_payload])
                        else:
                            response = parser.encode_null_array()
                    case [b"TYPE", key]:
                        assert isinstance(key, bytes)
                        value_type = handle_type_command(key)
                        response = parser.encode_simple_string(str(value_type))
                    case [b"XADD", stream_key, stream_id, *payload]:
                        assert isinstance(stream_key, bytes)
                        assert isinstance(stream_id, bytes)
                        if not all(isinstance(item, bytes) for item in payload):
                            raise TypeError("XADD payload must be bulk strings")
                        byte_payload = [item for item in payload if isinstance(item, bytes)]
                        stream_id = handle_xadd_command(stream_key, stream_id, byte_payload)
                        response = parser.encode_simple_string(stream_id)
                    case [b"XRANGE", stream_key, start, end]:
                        assert isinstance(stream_key, bytes)
                        assert isinstance(start, (str, bytes))
                        assert isinstance(end, (str, bytes))
                        entries = handle_xrange_command(stream_key, start, end)
                        print(entries)
                        response = parser.encode_array(entries)
                    case [b"XREAD", *args]:
                        if len(args) == 0:
                            raise ValueError("wrong number of arguments for 'xread' command")
                        if not all(isinstance(arg, bytes) for arg in args):
                            raise TypeError("XREAD arguments must be bulk strings")

                        byte_args = [arg for arg in args if isinstance(arg, bytes)]
                        parse_index = 0
                        block_timeout_seconds: float | None = None

                        if len(byte_args) >= 2 and byte_args[0].upper() == b"BLOCK":
                            block_raw = byte_args[1]
                            block_ms = int(block_raw)
                            if block_ms < 0:
                                raise ValueError("timeout is negative")
                            block_timeout_seconds = block_ms / 1000
                            parse_index = 2

                        if parse_index >= len(byte_args) or byte_args[parse_index].upper() != b"STREAMS":
                            raise ValueError("syntax error")

                        keys_and_ids = byte_args[parse_index + 1 :]
                        if len(keys_and_ids) == 0 or len(keys_and_ids) % 2 != 0:
                            raise ValueError("Unbalanced XREAD list of streams")

                        split_index = len(keys_and_ids) // 2
                        keys = keys_and_ids[:split_index]
                        ids = keys_and_ids[split_index:]
                        resolved_ids = resolve_xread_start_ids(keys, ids)

                        entries = handle_xread_streams_command(keys, resolved_ids)
                        if entries:
                            response = parser.encode_array(entries)
                        elif block_timeout_seconds is not None:
                            if block_timeout_seconds == 0:
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
                        else:
                            response = parser.encode_null()
                    case [cmd, *_]:
                        response = parser.encode_simple_error("unknown command")
            except (ValueError, TypeError, RESPError, AssertionError) as e:
                message = str(e)
                if message.startswith("-ERR "):
                    message = message[5:]
                response = parser.encode_simple_error(message)
            writer.write(response)
            await writer.drain()
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        writer.close()
        await writer.wait_closed()