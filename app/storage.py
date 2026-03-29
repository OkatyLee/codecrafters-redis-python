import asyncio
import fnmatch
import os
import struct
import time as _time_module
from collections.abc import Sequence
from collections import defaultdict
from time import monotonic
from typing import Any

from app.types import ListType, SortedSetType, StreamType, StringType, TypeRegistry

type RedisKey = bytes | str
type StoredValue = Any
type ExpireAt = float | None
type StorageRecord = tuple[StoredValue, ExpireAt]

class CacheStorage:
    """In-memory Redis-like cache used by the test-suite.

    The storage keeps values together with an optional expiration timestamp
    (monotonic seconds). It also supports list operations and an async
    blocking pop (``blpop``) which uses :class:`asyncio.Event` objects to
    wake waiting coroutines when new items are pushed.
    """

    def __init__(self):
        """Create an empty cache instance.

        Internal structures:
        - ``_storage``: mapping of key -> (value_or_list, expire_at)
        - ``_list_waiters``: mapping of key -> list of :class:`asyncio.Event`
          objects used to notify ``blpop`` waiters when an element is pushed.
        """
        self._storage: dict[RedisKey, StorageRecord] = {}
        # key -> list of asyncio.Event for BLPOP waiters
        self._list_waiters: dict[RedisKey, list[asyncio.Event]] = defaultdict(list)
        self._list_type = ListType(self._storage, self._list_waiters)
        self._stream_type = StreamType(self.get, self.set)
        self._sset = SortedSetType(self.get, self.set)
        self._type_registry = TypeRegistry(self._list_type, self._stream_type, StringType())

    def set(self, key: RedisKey, value: StoredValue, ttl: int | float | None = None) -> bool:
        """Set *key* to *value* with an optional time-to-live *ttl* (seconds).

        Raises ``ValueError`` for non-positive ttl values. Returns ``True`` on
        success.
        """
        if ttl is not None and ttl <= 0:
            raise ValueError("-ERR invalid expire time in set")
        expire_at = None
        now = monotonic()
        if ttl is not None:
            expire_at = now + ttl
        self._storage[key] = (value, expire_at)
        return key in self._storage

    def get(self, key: RedisKey) -> StoredValue | None:
        """Return the value for *key* or ``None`` if missing or expired.

        Expired keys are removed from the storage.
        """
        if key not in self._storage:
            return None

        value, expire_at = self._storage[key]
        if expire_at is not None and monotonic() > expire_at:
            del self._storage[key]
            return None
        return value

    def delete(self, key: RedisKey) -> bool:
        """Delete the key and return True if it existed, False otherwise."""
        if key in self._storage:
            del self._storage[key]
            return True
        return False

    def lpush(self, key: RedisKey, *values: bytes) -> int:
        """Push *values* to the head of the list stored at *key*.

        Creates the list if it does not exist. Returns the new list length.
        """
        return self._list_type.lpush(key, *values)

    def rpush(self, key: RedisKey, *values: bytes) -> int:
        """Append *values* to the tail of the list stored at *key*.

        Creates the list if it does not exist. Returns the new list length.
        """
        return self._list_type.rpush(key, *values)

    def lrange(self, key: RedisKey, start: int, end: int) -> list[bytes]:
        """Return a slice of the list at *key* from *start* to *end*.

        Negative indices follow the same semantics as Python slicing and the
        end is inclusive (as in Redis semantics used by tests).
        """
        return self._list_type.lrange(key, start, end)

    def llen(self, key: RedisKey) -> int:
        """Return the length of the list stored at *key* (0 if missing)."""
        return self._list_type.llen(key)

    def lpop(self, key: RedisKey, count: int | None = None) -> list[bytes] | None:
        """Pop elements from the head of the list at *key*.

        If *count* is provided, returns a list of up to *count* elements,
        otherwise returns a single-element list with the popped value. Returns
        ``None`` if the key does not exist or the list is empty.
        """
        return self._list_type.lpop(key, count)

    def _notify_waiters(self, key: RedisKey) -> None:
        """Wake up all BLPOP waiters registered for *key*.

        Internal helper used after list push operations to signal blocking
        ``blpop`` callers.
        """
        self._list_type.notify_waiters(key)

    async def blpop(self, *keys: RedisKey, timeout: float = 0) -> tuple[RedisKey, bytes] | None:
        """Blocking list pop from the first non-empty list among *keys*.

        Behaviour:
        - If any of the provided keys already contain data the pop happens
          immediately and a ``(key, value)`` tuple is returned.
        - Otherwise the coroutine blocks until an element is pushed to any of
          the keys or the optional *timeout* elapses. ``timeout=0`` means
          block indefinitely.

        Returns ``(key, value)`` on success or ``None`` on timeout.
        """
        return await self._list_type.blpop(*keys, timeout=timeout)
                        
    def type(self, key: RedisKey) -> str | None:
        """Return the type of the value stored at *key*.

        Returns ``"list"`` if the key exists and is a list, otherwise ``None``.
        """
        if key not in self._storage:
            return None
        value, _ = self._storage[key]
        return self._type_registry.resolve_name(value)
    
    def xadd(self, key: RedisKey, stream_id: str | bytes, payload: list[bytes]) -> str:
        """Add an entry to a stream.

        Returns the stream ID of the added entry.
        """
        return self._stream_type.xadd(key, stream_id, payload)
    
    def xrange(self, key: RedisKey, start: str | bytes, end: str | bytes) -> list[list[str | dict[bytes, bytes]]]:
        """Return a range of entries from a stream.

        Returns a list of (entry_id, entry) tuples.
        """
        return self._stream_type.xrange(key, start, end)
    
    def xread_streams(
        self,
        keys: Sequence[RedisKey],
        ids: Sequence[str | bytes],
    ) -> list[list[RedisKey | list[list[str | dict[bytes, bytes]]]]]:
        """Read entries from multiple streams.

        Returns a list of (entry_id, entry) tuples.
        """
        return self._stream_type.xread_streams(keys, ids)
    
    def incr(self, key: RedisKey) -> int:
        """
        Increment the integer value stored at *key* by 1.
        """
        value = self.get(key)
        if value is None:
            self.set(key, b"1")
            return 1
        try:
            value = int(value)
        except (ValueError, TypeError):
            raise TypeError("Value at key is not an integer")
        value += 1
        self.set(key, str(value).encode())
        return value
    
    def keys(self, pattern: str | bytes = b"*") -> list[bytes]:
        """Return all non-expired keys matching *pattern* (Redis glob syntax)."""
        pat_str: str = pattern.decode(errors="replace") if isinstance(pattern, bytes) else str(pattern)
        now = monotonic()
        result: list[bytes] = []
        for key, (_, expire_at) in list(self._storage.items()):
            if expire_at is not None and expire_at < now:
                continue
            if isinstance(key, bytes):
                key_str: str = key.decode(errors="replace")
                key_bytes: bytes = key
            else:
                key_str = str(key)
                key_bytes = key_str.encode()
            if fnmatch.fnmatchcase(key_str, pat_str):
                result.append(key_bytes)
        return result

    def save(self, dir: str, dbfilename: str) -> bool:
        """Save the storage data to an RDB file."""
        path = os.path.join(dir, dbfilename)
        now_mono = monotonic()
        now_unix = _time_module.time()

        def encode_length(n: int) -> bytes:
            if n <= 0x3F:
                return bytes([n])
            elif n <= 0x3FFF:
                return bytes([0x40 | (n >> 8), n & 0xFF])
            else:
                return b'\x80' + struct.pack('>I', n)

        def encode_string(s: bytes) -> bytes:
            return encode_length(len(s)) + s

        entries: list[tuple[bytes, bytes, int | None]] = []
        for key, (value, expire_at) in list(self._storage.items()):
            if expire_at is not None and expire_at < now_mono:
                continue
            if not isinstance(value, bytes):
                continue
            expire_ms: int | None = None
            if expire_at is not None:
                expire_ms = int((now_unix + (expire_at - now_mono)) * 1000)
            if isinstance(key, str):
                key = key.encode()
            entries.append((key, value, expire_ms))

        buf = bytearray()
        buf += b'REDIS0011'

        buf += b'\xfa'
        buf += encode_string(b'redis-ver')
        buf += encode_string(b'6.0.16')

        buf += b'\xfe'
        buf += encode_length(0)

        buf += b'\xfb'
        buf += encode_length(len(entries))
        buf += encode_length(sum(1 for _, _, e in entries if e is not None))

        for key, value, expire_ms in entries:
            if expire_ms is not None:
                buf += b'\xfc'
                buf += struct.pack('<Q', expire_ms)
            buf += b'\x00'
            buf += encode_string(key)
            buf += encode_string(value)

        buf += b'\xff'
        buf += struct.pack('<Q', _crc64(bytes(buf)))

        os.makedirs(dir, exist_ok=True)
        with open(path, 'wb') as f:
            f.write(buf)

        return True

    def load(self, dir: str, dbfilename: str, rdb_data: bytes | None = None) -> bool:
        """Load storage data from an RDB file."""
        if rdb_data is None:
            path = os.path.join(dir, dbfilename)
            if not os.path.exists(path):
                return False

            with open(path, 'rb') as f:
                data = f.read()

            if len(data) < 9 or data[:5] != b'REDIS':
                return False
        else:
            data = rdb_data
        pos = 9
        now_mono = monotonic()
        now_unix = _time_module.time()

        def decode_length(p: int) -> tuple[int, int]:
            first = data[p]
            enc = (first & 0xC0) >> 6
            if enc == 0:
                return first & 0x3F, p + 1
            elif enc == 1:
                return ((first & 0x3F) << 8) | data[p + 1], p + 2
            elif enc == 2:
                return struct.unpack('>I', data[p+1:p+5])[0], p + 5
            else:
                special = first & 0x3F
                if special == 0:
                    return data[p + 1], p + 2
                elif special == 1:
                    return struct.unpack('<H', data[p+1:p+3])[0], p + 3
                elif special == 2:
                    return struct.unpack('<I', data[p+1:p+5])[0], p + 5
                raise ValueError(f"Unsupported length special encoding: {special}")

        def decode_string(p: int) -> tuple[bytes, int]:
            first = data[p]
            enc = (first & 0xC0) >> 6
            if enc == 3:
                special = first & 0x3F
                if special == 0:
                    return str(data[p + 1]).encode(), p + 2
                elif special == 1:
                    val = struct.unpack('<H', data[p+1:p+3])[0]
                    return str(val).encode(), p + 3
                elif special == 2:
                    val = struct.unpack('<I', data[p+1:p+5])[0]
                    return str(val).encode(), p + 5
                raise ValueError(f"Unsupported string special encoding: {special}")
            length, p = decode_length(p)
            return data[p:p+length], p + length

        while pos < len(data):
            opcode = data[pos]
            pos += 1

            if opcode == 0xFA:
                _, pos = decode_string(pos)
                _, pos = decode_string(pos)
            elif opcode == 0xFE:
                _, pos = decode_length(pos)
            elif opcode == 0xFB:
                _, pos = decode_length(pos)
                _, pos = decode_length(pos)
            elif opcode == 0xFF:
                break
            else:
                expire_ms: int | None = None
                value_type = opcode

                if opcode == 0xFC:
                    expire_ms = struct.unpack('<Q', data[pos:pos+8])[0]
                    pos += 8
                    value_type = data[pos]
                    pos += 1
                elif opcode == 0xFD:
                    expire_s = struct.unpack('<I', data[pos:pos+4])[0]
                    expire_ms = expire_s * 1000
                    pos += 4
                    value_type = data[pos]
                    pos += 1

                key, pos = decode_string(pos)

                if value_type == 0:
                    value, pos = decode_string(pos)
                    if expire_ms is not None:
                        expire_unix = expire_ms / 1000
                        if expire_unix <= now_unix:
                            continue
                        expire_at = now_mono + (expire_unix - now_unix)
                    else:
                        expire_at = None
                    self._storage[key] = (value, expire_at)

        return True
    
    def zadd(self, key: RedisKey, score: float, member: bytes) -> bool:
        """Adding a member to a sorted set"""
        return self._sset.zadd(key, score, member)
    
    def zrank(self, key: RedisKey, member: bytes) -> int | None:
        """Return the rank of member in the sorted set at key."""
        #TODO
        return self._sset.zrank(key, member)
    
    def zrange(self, key: RedisKey, start: int, end: int) -> list[bytes]:
        """Return a range of members in the sorted set at key."""
        return self._sset.zrange(key, start, end)
    
    def zcard(self, key: RedisKey) -> int:
        """Return the number of members in the sorted set at key."""
        return self._sset.zcard(key)
    
    def zscore(self, key: RedisKey, member: bytes) -> float | None:
        """Return the score of member in the sorted set at key."""
        return self._sset.zscore(key, member)

    def zrem(self, key: RedisKey, members: list[bytes]) -> int:
        """Remove members from the sorted set at key."""
        return self._sset.zrem(key, members)
    
    def geoadd(self, key: RedisKey, longitude: float, latitude: float, member: bytes) -> int:
        """Add a member to a geospatial index."""
        return self._sset.geoadd(key, longitude, latitude, member)
    
    def geopos(self, key: RedisKey, members: list[bytes]) -> list[list[bytes] | None]:
        """Return the positions of members in the geospatial index at key."""
        return self._sset.geopos(key, members)

    def geodist(self, key: RedisKey, member1: bytes, member2: bytes) -> float | None:
        """Return the distance between two members in the geospatial index at key."""
        return self._sset.geodist(key, member1, member2)

    def geosearch(self, key: RedisKey, longitude: bytes, latitude: bytes, radius: bytes, unit: bytes) -> list[bytes]:
        """Search for members in the geospatial index at key within a given radius."""
        return self._sset.geosearch(key, longitude, latitude, radius, unit)




def _crc64(data: bytes) -> int:
    """Compute CRC-64/JONES checksum as used by Redis."""
    _POLY = 0xad93d23594c935a9
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ _POLY
            else:
                crc >>= 1
    return crc
