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

WRONGTYPE_ERROR = "WRONGTYPE Operation against a key holding the wrong kind of value"


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

    def _get_existing_list(self, key: RedisKey) -> list[bytes] | None:
        if key not in self._storage:
            return None
        current_value, _ = self._storage[key]
        if not isinstance(current_value, list):
            raise TypeError(WRONGTYPE_ERROR)
        return current_value

    def lpush(self, key: RedisKey, *values: bytes) -> int:
        current_list = self._get_existing_list(key)
        if current_list is None:
            current_list = []
        current_list[:0] = values[::-1]
        self._storage[key] = (current_list, None)
        self.notify_waiters(key)
        return len(current_list)

    def rpush(self, key: RedisKey, *values: bytes) -> int:
        current_list = self._get_existing_list(key)
        if current_list is None:
            current_list = []
        current_list.extend(values)
        self._storage[key] = (current_list, None)
        self.notify_waiters(key)
        return len(current_list)

    def lrange(self, key: RedisKey, start: int, end: int) -> list[bytes]:
        current_list = self._get_existing_list(key)
        if current_list is None:
            return []
        if start < 0:
            start = max(0, len(current_list) + start)
        if end < 0:
            end = max(0, len(current_list) + end)
        end += 1
        return current_list[start:end]

    def llen(self, key: RedisKey) -> int:
        current_list = self._get_existing_list(key)
        if current_list is None:
            return 0
        return len(current_list)

    def lpop(self, key: RedisKey, count: int | None = None) -> list[bytes] | None:
        current_list = self._get_existing_list(key)
        if current_list is None:
            return None
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
            current_list = self._get_existing_list(key)
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
                    current_list = self._get_existing_list(key)
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

    def _is_stream_value(self, value: Any) -> bool:
        return isinstance(value, dict) and (not value or "entries" in value or "ID" in value)

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
        if stream is None:
            return []
        if not self._is_stream_value(stream):
            raise TypeError(WRONGTYPE_ERROR)

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


class SortedSetType(ValueType):
    
    def __init__(
        self,
        get_value: Callable[[RedisKey], Any],
        set_value: Callable[[RedisKey, Any], bool],
    ):
        self._get_value = get_value
        self._set_value = set_value
        
    @property
    def name(self): 
        return "zset"
    
    def supports(self, value):
        return isinstance(value, (set, dict))
        
    def zadd(self, key: RedisKey, score: float, member: bytes) -> bool:
        sorted_set = self._get_value(key)
        if not isinstance(sorted_set, dict):
            sorted_set = {}
        is_existed = member in sorted_set
        sorted_set[member] = score
        self._set_value(key, sorted_set)
        return is_existed

    def zrank(self, key: RedisKey, member: bytes) -> int | None:
        sorted_set = self._get_value(key)
        if not isinstance(sorted_set, dict):
            return None
        if member not in sorted_set:
            return None
        target_score = sorted_set[member]
        
        return sum(1 for m, s in sorted_set.items() if s < target_score or (s == target_score and m < member))

    def zrange(self, key: RedisKey, start: int, end: int) -> list[bytes]:
        sorted_set = self._get_value(key)
        if start < 0:
            start = max(len(sorted_set) + start, 0)
        if end < 0:
            end = max(len(sorted_set) + end, 0)
        if not isinstance(sorted_set, dict):
            return []
        members = list(sorted_set.keys())
        members.sort(key=lambda m: (sorted_set[m], m))
        return members[start:end+1]
    
    def zcard(self, key: RedisKey) -> int:
        sorted_set = self._get_value(key)
        if not isinstance(sorted_set, dict):
            return 0
        return len(sorted_set)
    
    def zscore(self, key: RedisKey, member: bytes) -> float | None:
        sorted_set = self._get_value(key)
        if not isinstance(sorted_set, dict):
            return None
        return sorted_set.get(member)

    def zrem(self, key: RedisKey, members: list[bytes]) -> int:
        sorted_set = self._get_value(key)
        if not isinstance(sorted_set, dict):
            return 0
        removed_count = 0
        for member in members:
            if member in sorted_set:
                del sorted_set[member]
                removed_count += 1
        self._set_value(key, sorted_set)
        return removed_count
    
    MIN_LATITUDE = -85.05112878
    MAX_LATITUDE = 85.05112878
    MIN_LONGITUDE = -180
    MAX_LONGITUDE = 180
    
    def geoadd(self, key: RedisKey, longitude: float, latitude: float, member: bytes) -> int:
        sorted_set = self._get_value(key)
        if sorted_set is None:
            sorted_set = {}
        if not isinstance(sorted_set, dict):
            raise TypeError(WRONGTYPE_ERROR)
        if abs(longitude) > self.MAX_LONGITUDE or abs(latitude) > self.MAX_LATITUDE:
            raise ValueError(f"invalid longitude,latitude pair {longitude},{latitude}")
        is_exiting = member in sorted_set
        sorted_set[member] = self._encode_geoloc(longitude, latitude)
        self._set_value(key, sorted_set)
        return 1 if not is_exiting else 0

    def geopos(self, key: RedisKey, members: list[bytes]) -> list[list[bytes] | None]:
        sorted_set = self._get_value(key)
        if sorted_set is None:
            return [None for _ in members]
        if not isinstance(sorted_set, dict):
            raise TypeError(WRONGTYPE_ERROR)
        scores = []
        for mem in members:
            raw_score = sorted_set.get(mem)
            if raw_score is None:
                scores.append(None)
                continue
            try:
                score = int(raw_score)
            except:
                score = None
            scores.append(score)
        return [
            [str(pos).encode() for pos in self._decode_to_geoloc(score)] 
            if score is not None else None
            for score in scores
        ]

    def geodist(self, key: RedisKey, member1: bytes, member2: bytes) -> float | None:
        sorted_set = self._get_value(key)
        if sorted_set is None:
            return None
        if not isinstance(sorted_set, dict):
            raise TypeError(WRONGTYPE_ERROR)
        score1 = sorted_set.get(member1)
        score2 = sorted_set.get(member2)
        if score1 is None or score2 is None:
            return None
        try:
            geo_code1 = int(score1)
            geo_code2 = int(score2)
        except (ValueError, TypeError):
            raise TypeError(WRONGTYPE_ERROR)
        lon1, lat1 = self._decode_to_geoloc(geo_code1)
        lon2, lat2 = self._decode_to_geoloc(geo_code2)
        # Simplified distance calculation (not accurate for real-world use)
        return self._haversine(lat1, lon1, lat2, lon2)

    def geosearch(self, key: RedisKey, longitude: bytes, latitude: bytes, radius: bytes, unit: bytes) -> list[bytes]:
        to_meters = {
                    b'km': 1000.0,
                    b'ft': 0.3048,
                    b'in': 0.0254,
                    b'mi': 1609.34,
                    b'cm': 0.01,
                    b'mm': 0.001,
                    b'm': 1.0
        }
        norm_radius = float(radius.decode()) * to_meters.get(unit.lower(), 1.0)
        res = []
        sorted_set = self._get_value(key)
        if sorted_set is None:
            return []
        if not isinstance(sorted_set, dict):
            raise TypeError(WRONGTYPE_ERROR)
        for member, score in sorted_set.items():
            if not isinstance(member, bytes):
                raise TypeError(WRONGTYPE_ERROR)
            try:
                lon2, lat2 = self._decode_to_geoloc(int(score))
            except (TypeError, ValueError):
                raise TypeError(WRONGTYPE_ERROR)
            lon1, lat1 = float(longitude.decode()), float(latitude.decode())
            dist = self._haversine(lat1, lon1, lat2, lon2)
            if dist < norm_radius:
                res.append(member)
        return res


    def _haversine(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        from math import radians, sin, cos, asin, sqrt
        R = 6372797.560856  # Earth radius in meters

        dLat = radians(lat2 - lat1)
        dLon = radians(lon2 - lon1)
        lat1 = radians(lat1)
        lat2 = radians(lat2)

        a = sin(dLat / 2)**2 + cos(lat1) * cos(lat2) * sin(dLon / 2)**2
        c = 2 * asin(sqrt(a))

        return R * c

    def _encode_geoloc(self, longitude: float, latitude: float) -> int:
        lon = self._interleave(longitude, self.MIN_LONGITUDE, self.MAX_LONGITUDE)
        lat = self._interleave(latitude, self.MIN_LATITUDE, self.MAX_LATITUDE)
        res = ""
        for i in range(26):
            res += lon[i] + lat[i]
        return int(res, 2)

    def _interleave(self, x: float, min_val: float, max_val: float) -> str:
        bits = ""
        for _ in range(26):
            mid = min_val + (max_val - min_val) / 2
            if x >= mid:
                bits += "1"
                min_val = mid
            else:
                bits += "0"
                max_val = mid
        return bits

    def _decode_to_geoloc(self, geo_code: int) -> list[float]:
        bits_geocode = bin(geo_code)[2:].zfill(52)  # Convert to 52-bit binary string
        latitude_bits = bits_geocode[1::2]  
        longitude_bits = bits_geocode[::2] 
        latitude = self._deinterleave(latitude_bits, self.MIN_LATITUDE, self.MAX_LATITUDE)
        longitude = self._deinterleave(longitude_bits, self.MIN_LONGITUDE, self.MAX_LONGITUDE)
        return [longitude, latitude]

    def _deinterleave(self, bits: str, min_val: float, max_val: float) -> float:
        for bit in bits:
            mid = (max_val + min_val) / 2
            if bit == "1":
                min_val = mid
            else:
                max_val = mid
        return (min_val + max_val) / 2



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
