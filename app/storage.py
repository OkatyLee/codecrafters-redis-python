import asyncio
from collections.abc import Sequence
from collections import defaultdict
from time import monotonic
from typing import Any

from app.types import ListType, StreamType, StringType, TypeRegistry

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

_storage_instance: CacheStorage | None = None
    
def get_storage() -> CacheStorage:
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = CacheStorage()
    return _storage_instance