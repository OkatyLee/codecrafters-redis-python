import asyncio
from abc import ABC, abstractmethod
from collections.abc import Sequence
from time import time
from typing import Any, Callable

from app.parser import RESPError

type RedisKey = bytes | str
type ExpireAt = float | None
type Storage = dict[RedisKey, tuple[Any, ExpireAt]]
type Waiters = dict[RedisKey, list[asyncio.Event]]
type StreamEntry = tuple[str, dict[bytes, bytes]]


class ValueType(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def supports(self, value: Any) -> bool:
        raise NotImplementedError


class ListType(ValueType):
    def __init__(self, storage: Storage, list_waiters: Waiters):
        self._storage = storage
        self._list_waiters = list_waiters

    @property
    def name(self) -> str:
        return "list"

    def supports(self, value: Any) -> bool:
        return isinstance(value, list)

    def lpush(self, key: RedisKey, *values: bytes) -> int:
        if key not in self._storage:
            self._storage[key] = ([], None)
        current_list, _ = self._storage[key]
        current_list[:0] = values[::-1]
        self._storage[key] = (current_list, None)
        self.notify_waiters(key)
        return len(current_list)

    def rpush(self, key: RedisKey, *values: bytes) -> int:
        if key not in self._storage:
            self._storage[key] = ([], None)
        current_list, _ = self._storage[key]
        current_list.extend(values)
        self._storage[key] = (current_list, None)
        self.notify_waiters(key)
        return len(current_list)

    def lrange(self, key: RedisKey, start: int, end: int) -> list[bytes]:
        if key not in self._storage:
            return []
        current_list, _ = self._storage[key]
        if start < 0:
            start = max(0, len(current_list) + start)
        if end < 0:
            end = max(0, len(current_list) + end)
        end += 1
        return current_list[start:end]

    def llen(self, key: RedisKey) -> int:
        if key not in self._storage:
            return 0
        current_list, _ = self._storage[key]
        return len(current_list)

    def lpop(self, key: RedisKey, count: int | None = None) -> list[bytes] | None:
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

    def notify_waiters(self, key: RedisKey) -> None:
        if key in self._list_waiters:
            for event in self._list_waiters[key]:
                event.set()

    async def blpop(self, *keys: RedisKey, timeout: float = 0) -> tuple[RedisKey, bytes] | None:
        for key in keys:
            if key in self._storage:
                current_list, _ = self._storage[key]
                if current_list:
                    value = current_list.pop(0)
                    return (key, value)

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

                for key in keys:
                    if key in self._storage:
                        current_list, _ = self._storage[key]
                        if current_list:
                            value = current_list.pop(0)
                            return (key, value)
        finally:
            for key in keys:
                if key in self._list_waiters:
                    try:
                        self._list_waiters[key].remove(event)
                    except ValueError:
                        pass
                    if not self._list_waiters[key]:
                        del self._list_waiters[key]


class StreamType(ValueType):
    def __init__(
        self,
        get_value: Callable[[RedisKey], Any],
        set_value: Callable[[RedisKey, Any], bool],
    ):
        self._get_value = get_value
        self._set_value = set_value

    @property
    def name(self) -> str:
        return "stream"

    def supports(self, value: Any) -> bool:
        return isinstance(value, dict)

    def _decode_stream_id(self, stream_id: str | bytes) -> str:
        if isinstance(stream_id, bytes):
            return stream_id.decode()
        return str(stream_id)

    def _parse_id(self, stream_id: str) -> tuple[int, int]:
        timestamp, counter = stream_id.split("-", maxsplit=1)
        return int(timestamp), int(counter)

    def _last_stream_id(self, stream: dict[str, Any]) -> tuple[int, int]:
        last_id = stream.get("ID")
        if last_id is None and stream.get("entries"):
            last_id = stream["entries"][-1][0]
        if last_id is None:
            return (-1, -1)
        if isinstance(last_id, bytes):
            last_id = last_id.decode()
        return self._parse_id(last_id)

    def _next_autogen_id(self, requested_id: str, last_timestamp: int, last_counter: int) -> str:
        if requested_id == "*":
            timestamp = int(time() * 1000)
            if timestamp < last_timestamp:
                timestamp = last_timestamp
        else:
            timestamp = int(requested_id[:-2])

        if last_timestamp == timestamp:
            counter = last_counter + 1
        elif timestamp == 0:
            counter = 1
        else:
            counter = 0

        return f"{timestamp}-{counter}"

    def _resolve_stream_id(self, requested_id: str, last_timestamp: int, last_counter: int) -> str:
        if requested_id == "*" or requested_id.endswith("-*"):
            return self._next_autogen_id(requested_id, last_timestamp, last_counter)
        return requested_id

    def xadd(self, key: RedisKey, stream_id: str | bytes, payload: list[bytes]) -> str:
        requested_id = self._decode_stream_id(stream_id)

        if len(payload) == 0 or len(payload) % 2 != 0:
            raise RESPError("ERR wrong number of arguments for 'xadd' command")

        if self._get_value(key) is None:
            self._set_value(key, {})

        stream = self._get_value(key)
        if not isinstance(stream, dict):
            raise TypeError("Expected a dictionary value for stream key")

        stream.setdefault("entries", [])

        last_timestamp, last_counter = self._last_stream_id(stream)
        resolved_id = self._resolve_stream_id(requested_id, last_timestamp, last_counter)
        timestamp, counter = self._parse_id(resolved_id)

        if (timestamp, counter) <= (0, 0):
            raise RESPError("ERR The ID specified in XADD must be greater than 0-0")

        if (timestamp, counter) <= (last_timestamp, last_counter):
            raise RESPError("ERR The ID specified in XADD is equal or smaller than the target stream top item")

        entry: dict[bytes, bytes] = {
            payload[index]: payload[index + 1]
            for index in range(0, len(payload), 2)
        }
        stream["entries"].append((resolved_id, entry))
        stream["ID"] = resolved_id
        return resolved_id
    
    def xrange(self, key: RedisKey, start: str | bytes, end: str | bytes) -> list[list[str | dict[bytes, bytes]]]:
        if isinstance(start, bytes):
            start = start.decode()
        if isinstance(end, bytes):
            end = end.decode()
        if start == "-":
            start = ''
        if end == "+":
            end = ''
        stream = self._get_value(key)
        if not isinstance(stream, dict):
            return []

        entries = stream.get("entries", [])
        result = []
        for entry_id, entry in entries:
            if entry_id < start and start != '':
                continue
            if entry_id > end and end != '':
                break
            result.append([entry_id, entry])
        return result
    
    def xread_streams(
        self,
        keys: Sequence[RedisKey],
        ids: Sequence[str | bytes],
    ) -> list[list[RedisKey | list[list[str | dict[bytes, bytes]]]]]:
        if len(keys) != len(ids):
            raise ValueError("Unbalanced XREAD stream keys and ids")

        result: list[list[Any]] = []
        for index, key in enumerate(keys):
            raw_stream_id = ids[index]
            stream_id = raw_stream_id.decode() if isinstance(raw_stream_id, bytes) else str(raw_stream_id)
            stream = self._get_value(key)

            if stream is None:
                continue
            if not isinstance(stream, dict):
                raise TypeError(f"Key {key} is not a stream")

            entries: list[StreamEntry] = stream.get("entries", [])
            if stream_id == "$":
                stream_id = entries[-1][0] if entries else "0-0"

            stream_entries: list[list[str | dict[bytes, bytes]]] = []
            for entry_id, entry in entries:
                if entry_id > stream_id:
                    stream_entries.append([entry_id, entry])

            if stream_entries:
                result.append([key, stream_entries])

        return result


class StringType(ValueType):
    @property
    def name(self) -> str:
        return "string"

    def supports(self, value: Any) -> bool:
        return isinstance(value, (str, bytes, int, float, bool))


class TypeRegistry:
    def __init__(self, *types_: ValueType) -> None:
        self._types = list(types_)

    def resolve_name(self, value: Any) -> str | None:
        for value_type in self._types:
            if value_type.supports(value):
                return value_type.name
        return None
