"""RESP (Redis Serialization Protocol) parser utilities.

This module provides a minimal RESP parser used by the async server and
tests. It supports arrays, bulk strings, integers, simple strings and
errors in the subset required by the exercises.
"""

import asyncio


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

    async def parse(self):
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

    async def _parse_array(self, count: int):
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

    async def _parse_bulk_string(self, length: int):
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