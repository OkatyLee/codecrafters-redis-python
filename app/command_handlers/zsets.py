
from app.commands import Arity, CommandContext, command
from app.parser import RESPError
from app.resp_types import ArrayType, BaseRESPType, BulkStringType, IntegerType, NullArrayType, NullBulkStringType


@command(
    name=b"ZADD",
    arity=Arity(3, 3),
    flags={"write", "zset"}
)
def cmd_zadd(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, score, member = args

    is_existing = ctx.app_state.storage.zadd(key, float(score), member)
    return IntegerType(1 if not is_existing else 0)


@command(
    name=b"ZRANK",
    arity=Arity(2, 2),
    flags={"read", "zset"}
)
def cmd_zrank(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, member = args
    rank = ctx.app_state.storage.zrank(key, member)
    return IntegerType(rank) if rank is not None else NullArrayType()


@command(
    name=b"ZRANGE",
    arity=Arity(3, 3),
    flags={"read", "zset"}
)
def cmd_zrange(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, start, end = args
    members = ctx.app_state.storage.zrange(key, int(start), int(end))
    return ArrayType([BulkStringType(member) for member in members])


@command(
    name=b"ZCARD",
    arity=Arity(1, 1),
    flags={"read", "zset"}
)
def cmd_zcard(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key = args[0]
    count = ctx.app_state.storage.zcard(key)
    return IntegerType(count)


@command(
    name=b"ZSCORE",
    arity=Arity(2, 2),
    flags={"read", "zset"}
)
def cmd_zscore(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, member = args
    score = ctx.app_state.storage.zscore(key, member)
    return BulkStringType(str(score).encode()) if score is not None else NullBulkStringType()


@command(
    name=b"ZREM",
    arity=Arity(2, None),
    flags={"write", "zset"}
)
def cmd_zrem(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, members = args[0], args[1:]
    removed_count = ctx.app_state.storage.zrem(key, members)
    return IntegerType(removed_count)


@command(
    name=b"GEOADD",
    arity=Arity(4, 4),
    flags={"write", "zset", "geospatial"}
)

def cmd_geoadd(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, longitude, latitude, member = args
    num_added = ctx.app_state.storage.geoadd(key, float(longitude), float(latitude), member)
    return IntegerType(num_added)


@command(
    name=b"GEOPOS",
    arity=Arity(2, None),
    flags={"readonly", "zset", "geospatial"}
)
def cmd_geopos(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, members = args[0], args[1:]
    positions = ctx.app_state.storage.geopos(key, members)
    positions = ArrayType([
        ArrayType([BulkStringType(pos[0]), BulkStringType(pos[1])]) if pos is not None else NullArrayType()
        for pos in positions

    ])
    return positions


@command(
    name=b"GEODIST",
    arity=Arity(3, 3),
    flags={"readonly", "zset", "geospatial"}
)
def cmd_geodist(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, member1, member2 = args
    distance = ctx.app_state.storage.geodist(key, member1, member2)
    return BulkStringType(str(distance).encode()) if distance is not None else NullBulkStringType()


@command(
    name=b"GEOSEARCH",
    arity=Arity(7, 7),
    flags={"readonly", "zset", "geospatial"}
)
def cmd_geosearch(ctx: CommandContext, args: list[bytes]) -> BaseRESPType:
    key, *args = args
    search_mode = args[0]
    if search_mode.upper() != b'FROMLONLAT':
        raise RESPError("ERR only FROMLONLAT is supported")
    longitude, latitude = args[1], args[2]
    search_opt = args[3]
    if search_opt.upper() != b"BYRADIUS":
        raise RESPError("ERR only BYRADIUS is supported")
    radius, unit = args[4], args[5]
    members = ctx.app_state.storage.geosearch(key, longitude, latitude, radius, unit)
    return ArrayType([BulkStringType(member) for member in members])
