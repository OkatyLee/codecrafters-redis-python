import asyncio
from app.parser import RESPParser
from app.storage import get_storage


def check_argument_len(expected_len, command_name):
    """Decorator validating minimal positional arguments for command handlers.

    The decorated function is expected to receive positional arguments where
    the first N correspond to the command's parameters. This simple check
    raises ``ValueError`` if not enough positional arguments are supplied.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            if len(args) < expected_len:
                raise ValueError(f"Expected at least {expected_len} arguments for command {command_name}")
            return func(*args, **kwargs)

        return wrapper

    return decorator


@check_argument_len(2, "SET")
def handle_set_command(key: str, value: str, ttl=None) -> bool:
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


@check_argument_len(1, "GET")
def handle_get_command(key: str) -> str:
    """Return the stored value for *key* or ``None`` if missing."""
    storage = get_storage()
    return storage.get(key)


@check_argument_len(2, "LPUSH")
def handle_lpush_command(key: str, *values) -> int:
    """Insert *values* at the head of list *key* and return new length."""
    storage = get_storage()
    return storage.lpush(key, *values)


@check_argument_len(1, "LLEN")
def handle_llen_command(key: str) -> int:
    """Return the length of the list stored at *key*."""
    storage = get_storage()
    return storage.llen(key)


@check_argument_len(1, "LPOP")
def handle_lpop_command(key: str) -> str:
    """Pop one or more elements from the head of list *key*.

    Returns a list of popped elements or ``None`` when the key is missing.
    """
    storage = get_storage()
    return storage.lpop(key)


@check_argument_len(2, "RPUSH")
def handle_rpush_command(key: str, *values) -> int:
    """Append *values* to the tail of list *key* and return new length."""
    storage = get_storage()
    return storage.rpush(key, *values)


@check_argument_len(3, "LRANGE")
def handle_lrange_command(key: str, start: int, end: int) -> list:
    """Return a slice of the list at *key* from *start* to *end*."""
    storage = get_storage()
    return storage.lrange(key, start, end)


async def handle_blpop_command(*keys, timeout: float = 0):
    """Async wrapper for storage.blpop.

    Accepts multiple keys and an optional timeout in seconds.
    """
    if len(keys) < 1:
        raise ValueError("Expected at least 1 key for command BLPOP")
    storage = get_storage()
    return await storage.blpop(*keys, timeout=timeout)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
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
            print(data)
            response = b""
            command = data[0].upper()
            match command:
                case b"PING":
                    response = "+PONG\r\n".encode()
                case b"ECHO":
                    payload = data[1]
                    if isinstance(payload, str):
                        payload_bytes = payload.encode("utf-8")
                    else:
                        payload_bytes = payload
                    byte_len = len(payload_bytes)
                    header = f"${byte_len}\r\n".encode("utf-8")
                    response = header + payload_bytes + b"\r\n"
                case b"SET":
                    ex = None
                    px = None
                    suff = [x.upper() for x in data[3:]]
                    if b"EX" in suff:
                        ex = int(data[3 + suff.index(b"EX") + 1])
                    elif b"PX" in suff:
                        px = int(data[3 + suff.index(b"PX") + 1]) / 1000.0
                    if ex and px:
                        raise ValueError("-ERR syntax error. Only one of EX or PX is allowed")
                    ttl = ex or px
                    flag = handle_set_command(data[1], data[2], ttl)
                    response = b"+OK\r\n" if flag else b"-ERR Failed to set key\r\n"
                case b"GET":
                    value = handle_get_command(data[1])
                    if value is not None:
                        payload = value if isinstance(value, bytes) else str(value).encode()
                        response = b"$" + str(len(payload)).encode() + b"\r\n" + payload + b"\r\n"
                    else:
                        response = b"$-1\r\n"
                case b"RPUSH":
                    key = data[1]
                    values = data[2:]
                    length = handle_rpush_command(key, *values)
                    response = b":" + str(length).encode() + b"\r\n"
                case b"LPUSH":
                    key = data[1]
                    values = data[2:]
                    length = handle_lpush_command(key, *values)
                    response = b":" + str(length).encode() + b"\r\n"
                case b"LRANGE":
                    key = data[1]
                    start = int(data[2])
                    end = int(data[3])
                    values = handle_lrange_command(key, start, end)
                    response = b"*" + str(len(values)).encode() + b"\r\n"
                    for value in values:
                        payload = value if isinstance(value, bytes) else str(value).encode()
                        response += b"$" + str(len(payload)).encode() + b"\r\n" + payload + b"\r\n"
                case b"LLEN":
                    key = data[1]
                    length = handle_llen_command(key)
                    response = b":" + str(length).encode() + b"\r\n"
                case b"LPOP":
                    key = data[1]
                    count = int(data[2]) if len(data) > 2 else None
                    values = handle_lpop_command(key, count=count)
                    if not values is None:
                        if not count is None:
                            response = b"*" + str(len(values)).encode() + b"\r\n"
                            for value in values:
                                payload = value if isinstance(value, bytes) else str(value).encode()
                                response += b"$" + str(len(payload)).encode() + b"\r\n" + payload + b"\r\n"
                        else:
                            payload = values[0] if isinstance(values[0], bytes) else str(values[0]).encode()
                            response = b"$" + str(len(payload)).encode() + b"\r\n" + payload + b"\r\n"
                    else:
                        response = b"$-1\r\n"
                case b"BLPOP":
                    # BLPOP key [key ...] timeout
                    if len(data) < 3:
                        response = b"-ERR wrong number of arguments for 'blpop' command\r\n"
                    else:
                        timeout = float(data[-1])
                        keys = data[1:-1]
                        result = await handle_blpop_command(*keys, timeout=timeout)
                        if result is not None:
                            key_name, value = result
                            key_payload = key_name if isinstance(key_name, bytes) else str(key_name).encode()
                            val_payload = value if isinstance(value, bytes) else str(value).encode()
                            response = (b"*2\r\n"
                                        b"$" + str(len(key_payload)).encode() + b"\r\n" + key_payload + b"\r\n"
                                        b"$" + str(len(val_payload)).encode() + b"\r\n" + val_payload + b"\r\n")
                        else:
                            response = b"*-1\r\n"
                case _:
                    response = b"-ERR unknown command\r\n"
            writer.write(response)
            await writer.drain()
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        writer.close()
        await writer.wait_closed()