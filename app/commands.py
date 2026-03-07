from dataclasses import dataclass
from typing import Awaitable, Callable
from asyncio import StreamWriter

type PropagateCallback = Callable[[bytes], Awaitable[None]]

COMMAND_WRITE_FLAGS: dict[bytes, bool] = {}


def redis_command(name: bytes, *, is_write: bool = False):
    """Register command metadata used by the dispatcher."""

    command_name = name.upper()

    def decorator(func):
        COMMAND_WRITE_FLAGS[command_name] = is_write
        return func

    return decorator


@dataclass(slots=True)
class ExecCtx:
    """Execution context for a single command dispatch."""

    from_replication: bool
    raw_resp_command: bytes
    propagate: PropagateCallback
    replica_writer: StreamWriter | None = None
