from __future__ import annotations

import asyncio


class RESPErrorReply(Exception):
    pass


def encode_command(*parts: bytes | str | int) -> bytes:
    normalized = [
        part if isinstance(part, bytes) else str(part).encode("utf-8")
        for part in parts
    ]
    payload = b"*" + str(len(normalized)).encode("ascii") + b"\r\n"
    for item in normalized:
        payload += b"$" + str(len(item)).encode("ascii") + b"\r\n" + item + b"\r\n"
    return payload


async def read_response(reader: asyncio.StreamReader):
    line = await reader.readline()
    if not line:
        raise ConnectionError("Connection closed")

    prefix = line[:1]
    if prefix == b"+":
        return line[1:-2]
    if prefix == b"-":
        raise RESPErrorReply(line[1:-2].decode("utf-8", errors="replace"))
    if prefix == b":":
        return int(line[1:-2])
    if prefix == b"$":
        length = int(line[1:-2])
        if length == -1:
            return None
        data = await reader.readexactly(length + 2)
        return data[:-2]
    if prefix == b"*":
        count = int(line[1:-2])
        if count == -1:
            return None
        return [await read_response(reader) for _ in range(count)]

    raise ValueError(f"Unsupported RESP prefix: {prefix!r}")


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * p
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight
