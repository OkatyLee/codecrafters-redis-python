"""RESP (Redis Serialization Protocol) parser utilities.

This module provides a minimal RESP parser used by the async server and
tests. It supports arrays, bulk strings, integers, simple strings and
errors in the subset required by the exercises.
"""

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass



type RESPPrimitive = bytes | str | int | None
type RESPValue = RESPPrimitive | list["RESPValue"] | dict[bytes | str, "RESPValue"]


class NullArray: pass
class NullBulkString: pass


@dataclass(slots=True)
class RawResponse:
    payload: bytes


class RESPError(Exception):
    """Exception raised when a RESP error reply (prefix ``-``) is parsed.

    The message of the exception is the textual error payload (without the
    leading hyphen).
    """


class RESPParser:
    """Parse RESP-encoded messages from an ``asyncio.StreamReader``.

    The parser returns Python-native types used in the tests:
    - ``list`` for RESP arrays
    - ``bytes`` for bulk strings
    - ``int`` for integers
    - ``str`` for simple strings

    Null bulk strings and null arrays are represented with ``None``. If a
    RESP error token is parsed the parser raises :class:`RESPError`.
    """

    def __init__(self, reader: asyncio.StreamReader):
        """Create a parser reading from *reader*.

        Args:
            reader: An ``asyncio.StreamReader`` from which RESP bytes are read.
        """
        self.reader = reader

    async def parse(self) -> RESPValue | None:
        """Read and decode the next RESP token from the stream.

        Returns the decoded Python value or ``None`` when EOF is reached.
        Raises :class:`RESPError` for RESP error replies and :class:`ValueError`
        for unknown prefixes.
        """
        data = await self.reader.readline()
        if not data:
            return None
        if not data.endswith(b"\r\n"):
            raise ConnectionError("Incomplete RESP packet received (missing CRLF)")
        prefix = data[:1]
        payload = data[1:-2]
        match prefix:
            case b"*":
                return await self._parse_array(count=int(payload))
            case b"$":
                return await self._parse_bulk_string(length=int(payload))
            case b":":
                return int(payload)
            case b"+":
                return payload.decode()
            case b"-":
                raise RESPError(payload.decode())
            case _:
                raise ValueError(f"Unknown RESP prefix: {prefix!r}")

    async def _parse_array(self, count: int) -> list[RESPValue] | None:
        """Parse a RESP array consisting of *count* elements.

        Returns a Python ``list`` or ``None`` for null arrays (``count == -1``).
        """
        if count == -1:
            return None
        if count < -1:
            raise ValueError("Invalid array length")
        result = []
        for _ in range(count):
            result.append(await self.parse())
        return result

    async def _parse_bulk_string(self, length: int) -> bytes | None:
        """Read a bulk string of ``length`` bytes and return the raw bytes.

        A length of ``-1`` represents a null bulk string and results in
        ``None``.
        """
        if length == -1:
            return None
        if length < -1:
            raise ValueError("Invalid array length")
        try:
            data = await self.reader.readexactly(length + 2)  # +2 for \r\n
        except asyncio.IncompleteReadError as e:
            raise ConnectionError("Connection closed mid-bulk-string") from e
        return data[:-2]
    
    def encode_bulk_string(self, value: bytes | None) -> bytes:
        """Encode a bulk string value to RESP format.

        Returns the RESP-encoded bytes for a bulk string value or null.
        """
        if value is None:
            return b"$-1\r\n"
        if isinstance(value, str):
            value = value.encode()
        return b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
    
    def encode_simple_string(self, value: str) -> bytes:
        """Encode a simple string value to RESP format.

        Returns the RESP-encoded bytes for a simple string value.
        """
        return f"+{value}\r\n".encode()
    
    def encode_integer(self, value: int) -> bytes:
        """Encode an integer value to RESP format.

        Returns the RESP-encoded bytes for an integer value.
        """
        return f":{value}\r\n".encode()
    
    def encode_null(self) -> bytes:
        """Encode a null value to RESP format.

        Returns the RESP-encoded bytes for a null value.
        """
        return b"$-1\r\n"
    
    def encode_array(self, values: Sequence[object] | None) -> bytes:
        """Encode an array of values to RESP format.

        Returns the RESP-encoded bytes for an array of values.
        """
        if values is None:
            return b"*-1\r\n"
        result = b"*" + str(len(values)).encode() + b"\r\n"
        for value in values:
            if isinstance(value, RawResponse):
                result += value.payload
                continue
            if isinstance(value, bytes):
                result += self.encode_bulk_string(value)
            elif isinstance(value, str):
                result += self.encode_simple_string(value)
            elif isinstance(value, int):
                result += self.encode_integer(value)
            elif isinstance(value, list):
                result += self.encode_array(value)
            elif isinstance(value, dict):
                val = [item for k, v in value.items() for item in (k, v)]
                result += self.encode_array(val)
            elif value is None:
                result += self.encode_null_array()
            elif isinstance(value, NullArray):
                result += self.encode_null_array()
            elif isinstance(value, NullBulkString):
                result += self.encode_null()
            else:
                raise TypeError(f"Unsupported value type: {type(value)}")
        return result

    def encode_null_array(self) -> bytes:
        """Encode a null array value to RESP format."""
        return b"*-1\r\n"
    
    def encode_simple_error(self, message: str) -> bytes:
        """Encode a simple error message to RESP format.

        Returns the RESP-encoded bytes for a simple error message.
        """
        if not message.startswith("ERR"):
            message = f"ERR {message}"
        return f"-{message}\r\n".encode()
