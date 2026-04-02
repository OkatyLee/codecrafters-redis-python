"""RESP (Redis Serialization Protocol) parser utilities.

This module provides a minimal RESP parser used by the async server and
tests. It supports arrays, bulk strings, integers, simple strings and
errors in the subset required by the exercises.
"""

import asyncio

type RESPPrimitive = bytes | str | int | None
type RESPValue = RESPPrimitive | list["RESPValue"] | dict[bytes | str, "RESPValue"]

RESP_PREFIXES = {b"*", b"$", b":", b"+", b"-"}


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
                inline_payload = data[:-2]
                if self._looks_like_inline_command(inline_payload):
                    return self._parse_inline_command(inline_payload)
                raise ValueError(f"ERR Unknown RESP prefix: {prefix!r}")

    async def _parse_array(self, count: int) -> list[RESPValue] | None:
        """Parse a RESP array consisting of *count* elements.

        Returns a Python ``list`` or ``None`` for null arrays (``count == -1``).
        """
        if count == -1:
            return None
        if count < -1:
            raise ValueError("ERR Invalid array length")
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
            raise ValueError("ERR Invalid bulk string length")
        try:
            data = await self.reader.readexactly(length + 2)  # +2 for \r\n
        except asyncio.IncompleteReadError as e:
            raise ConnectionError("ERR Connection closed mid-bulk-string") from e
        if data[-2:] != b"\r\n":
            raise ConnectionError("ERR Invalid bulk string terminator")
        return data[:-2]

    @staticmethod
    def _looks_like_inline_command(line: bytes) -> bool:
        stripped = line.lstrip(b" \t")
        if not stripped:
            return True
        first = stripped[:1]
        return first not in RESP_PREFIXES and chr(first[0]).isalpha()

    @staticmethod
    def _parse_inline_command(line: bytes) -> list[bytes]:
        tokens: list[bytes] = []
        current = bytearray()
        quote_char: int | None = None
        escaped = False
        token_started = False

        for byte in line:
            if escaped:
                current.append(byte)
                escaped = False
                token_started = True
                continue

            if byte == ord("\\"):
                escaped = True
                token_started = True
                continue

            if quote_char is not None:
                if byte == quote_char:
                    quote_char = None
                else:
                    current.append(byte)
                token_started = True
                continue

            if byte in (ord('"'), ord("'")):
                quote_char = byte
                token_started = True
                continue

            if byte in (ord(" "), ord("\t")):
                if token_started:
                    tokens.append(bytes(current))
                    current.clear()
                    token_started = False
                continue

            current.append(byte)
            token_started = True

        if quote_char is not None:
            raise ValueError("ERR Unterminated inline command")
        if escaped:
            raise ValueError("ERR Trailing escape in inline command")
        if token_started:
            tokens.append(bytes(current))
        return tokens
