
from app.commands import Arity, CommandContext, NullArray, NullBulkString, command
from app.parser import RESPError
from app.storage import get_storage


@command(
    name=b"ZADD",
    arity=Arity(3, 3),
    flags={"write"}
)
def cmd_zadd(ctx: CommandContext, args: list[bytes]) -> int:
    key, score, member = args

    is_existing = get_storage().zadd(key, float(score), member)
    return 1 if not is_existing else 0


@command(
    name=b"ZRANK",
    arity=Arity(2, 2),
    flags={"read"}
)
def cmd_zrank(ctx: CommandContext, args: list[bytes]) -> int | NullArray:
    key, member = args
    rank = get_storage().zrank(key, member)
    return rank if rank is not None else NullArray()


@command(
    name=b"ZRANGE",
    arity=Arity(3, 3),
    flags={"read"}
)
def cmd_zrange(ctx: CommandContext, args: list[bytes]) -> list[bytes]:
    key, start, end = args
    members = get_storage().zrange(key, int(start), int(end))
    return members


@command(
    name=b"ZCARD",
    arity=Arity(1, 1),
    flags={"read"}
)
def cmd_zcard(ctx: CommandContext, args: list[bytes]) -> int:
    key = args[0]
    count = get_storage().zcard(key)
    return count


@command(
    name=b"ZSCORE",
    arity=Arity(2, 2),
    flags={"read"}
)
def cmd_zscore(ctx: CommandContext, args: list[bytes]) -> bytes | NullBulkString:
    key, member = args
    score = get_storage().zscore(key, member)
    return str(score).encode() if score is not None else NullBulkString()


@command(
    name=b"ZREM",
    arity=Arity(2, None),
    flags={"write"}
)
def cmd_zrem(ctx: CommandContext, args: list[bytes]) -> int:
    key, members = args[0], args[1:]
    removed_count = get_storage().zrem(key, members)
    return removed_count


@command(
    name=b"GEOADD",
    arity=Arity(4, 4),
    flags={"write"}
)

def cmd_geoadd(ctx: CommandContext, args: list[bytes]) -> int:
    key, longitude, latitude, member = args
    num_added = get_storage().geoadd(key, float(longitude), float(latitude), member)
    return num_added


@command(
    name=b"GEOPOS",
    arity=Arity(2, None),
    flags={"readonly"}
)
def cmd_geopos(ctx: CommandContext, args: list[bytes]) -> list[list[bytes] | None]:
    key, members = args[0], args[1:]
    positions = get_storage().geopos(key, members)
    return positions


@command(
    name=b"GEODIST",
    arity=Arity(3, 3),
    flags={"readonly"}
)
def cmd_geodist(ctx: CommandContext, args: list[bytes]) -> bytes | NullBulkString:
    key, member1, member2 = args
    distance = get_storage().geodist(key, member1, member2)
    return str(distance).encode() if distance is not None else NullBulkString()


@command(
    name=b"GEOSEARCH",
    arity=Arity(7, 7),
    flags={"readonly"}
)
def cmd_geosearch(ctx: CommandContext, args: list[bytes]) -> list[bytes]:
    key, *args = args
    search_mode = args[0]
    if search_mode.upper() != b'FROMLONLAT':
        raise RESPError("only FROMLONLAT is supported")
    longitude, latitude = args[1], args[2]
    search_opt = args[3]
    if search_opt.upper() != b"BYRADIUS":
        raise RESPError("only BYRADIUS is supported")
    radius, unit = args[4], args[5]
    members = get_storage().geosearch(key, longitude, latitude, radius, unit)
    return members
