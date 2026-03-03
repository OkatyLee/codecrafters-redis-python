import asyncio
from collections import defaultdict
from time import monotonic
from typing import Any, List

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
        self._storage: dict[str, tuple[Any, float | None]] = {}
        # key -> list of asyncio.Event for BLPOP waiters
        self._list_waiters: dict[str, list[asyncio.Event]] = defaultdict(list)

    def set(self, key, value, ttl=None) -> bool:
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

    def get(self, key):
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

    def lpush(self, key, *values) -> int:
        """Push *values* to the head of the list stored at *key*.

        Creates the list if it does not exist. Returns the new list length.
        """
        if key not in self._storage:
            self._storage[key] = ([], None)
        current_list, _ = self._storage[key]
        current_list[:0] = values[::-1]  # Insert at the beginning
        self._storage[key] = (current_list, None)
        self._notify_waiters(key)
        return len(current_list)

    def rpush(self, key, *values) -> int:
        """Append *values* to the tail of the list stored at *key*.

        Creates the list if it does not exist. Returns the new list length.
        """
        if key not in self._storage:
            self._storage[key] = ([], None)
        current_list, _ = self._storage[key]
        current_list.extend(values)
        self._storage[key] = (current_list, None)
        self._notify_waiters(key)
        return len(current_list)

    def lrange(self, key, start, end) -> list:
        """Return a slice of the list at *key* from *start* to *end*.

        Negative indices follow the same semantics as Python slicing and the
        end is inclusive (as in Redis semantics used by tests).
        """
        if key not in self._storage:
            return []
        current_list, _ = self._storage[key]
        if start < 0:
            start = max(0, len(current_list) + start)
        if end < 0:
            end = max(0, len(current_list) + end)
        end += 1
        return current_list[start:end]

    def llen(self, key) -> int:
        """Return the length of the list stored at *key* (0 if missing)."""
        if key not in self._storage:
            return 0
        current_list, _ = self._storage[key]
        return len(current_list)

    def lpop(self, key, count=None):
        """Pop elements from the head of the list at *key*.

        If *count* is provided, returns a list of up to *count* elements,
        otherwise returns a single-element list with the popped value. Returns
        ``None`` if the key does not exist or the list is empty.
        """
        if key not in self._storage:
            return None
        current_list, _ = self._storage[key]
        if not current_list:
            return None
        if count is not None:
            count = min(count, len(current_list))
            values = current_list[:count]
            del current_list[:count]
        else:
            values = [current_list.pop(0)]
        return values

    def _notify_waiters(self, key):
        """Wake up all BLPOP waiters registered for *key*.

        Internal helper used after list push operations to signal blocking
        ``blpop`` callers.
        """
        if key in self._list_waiters:
            for event in self._list_waiters[key]:
                event.set()

    async def blpop(self, *keys, timeout: float = 0):
        """Blocking list pop from the first non-empty list among *keys*.

        Behaviour:
        - If any of the provided keys already contain data the pop happens
          immediately and a ``(key, value)`` tuple is returned.
        - Otherwise the coroutine blocks until an element is pushed to any of
          the keys or the optional *timeout* elapses. ``timeout=0`` means
          block indefinitely.

        Returns ``(key, value)`` on success or ``None`` on timeout.
        """
        # First, check if any key already has elements (non-blocking path)
        for key in keys:
            if key in self._storage:
                current_list, _ = self._storage[key]
                if current_list:
                    value = current_list.pop(0)
                    return (key, value)

        # Blocking path: register waiters and wait for notification
        event = asyncio.Event()
        for key in keys:
            self._list_waiters[key].append(event)

        try:
            while True:
                event.clear()
                try:
                    if timeout > 0:
                        await asyncio.wait_for(event.wait(), timeout=timeout)
                    else:
                        await event.wait()
                except asyncio.TimeoutError:
                    return None

                # Check which key has data
                for key in keys:
                    if key in self._storage:
                        current_list, _ = self._storage[key]
                        if current_list:
                            value = current_list.pop(0)
                            return (key, value)
                # Spurious wakeup or another waiter consumed it; loop again
        finally:
            # Unregister waiters
            for key in keys:
                if key in self._list_waiters:
                    try:
                        self._list_waiters[key].remove(event)
                    except ValueError:
                        pass
                    if not self._list_waiters[key]:
                        del self._list_waiters[key]
    
_storage_instance = None    
    
def get_storage() -> CacheStorage:
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = CacheStorage()
    return _storage_instance