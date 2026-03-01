import asyncio

class RESPError(Exception):
    pass

class RESPParser:
    """
    Parses RESP (Redis Serialization Protocol) messages.
    """
    def __init__(self, reader: asyncio.StreamReader):
        self.reader = reader 
    
    async def parse(self):
        data = await self.reader.readline()
        if not data:
            return None
        if not data.endswith(b'\r\n'):
            raise ConnectionError("Incomplete RESP packet received (missing CRLF)")
        print(f"Received data: {data}")
        prefix = data[:1]
        print(f"Received prefix: {prefix}")
        payload = data[1:-2]
        print(f"Received payload: {payload}")
        if prefix == b'*':
            return await self._parse_array(count=int(payload))
        elif prefix == b'$':
            return await self._parse_bulk_string(length=int(payload))
        elif prefix == b':':
            return int(payload)
        elif prefix == b'+':
            return payload.decode()
        elif prefix == b'-':
            raise RESPError(payload.decode())
        else:
            raise ValueError(f"Unknown RESP prefix: {prefix!r}")
        
    async def _parse_array(self, count: int):
        if count == -1: 
            return None
        if count < -1: 
            raise ValueError("Invalid array length")
        result = []
        for _ in range(count):
            result.append(await self.parse())
        return result
    
    async def _parse_bulk_string(self, length: int):
        if length == -1:
            return None
        if length < -1: 
            raise ValueError("Invalid array length")
        try:
            data = await self.reader.readexactly(length + 2)  # +2 for \r\n
        except asyncio.IncompleteReadError as e:
            raise ConnectionError("Connection closed mid-bulk-string") from e
        return data[:-2]