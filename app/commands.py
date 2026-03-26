from dataclasses import dataclass
from typing import Awaitable, Callable
from asyncio import StreamWriter

from app.config import ServerConfig
from app.parser import RESPParser, RESPValue
from app.session import ClientSession

type PropagateCallback = Callable[[bytes], Awaitable[None]]
type CommandHandler = Callable[["CommandContext", list[bytes]], Awaitable[object] | object]

COMMAND_WRITE_FLAGS: dict[bytes, bool] = {}

COMMANDS: dict[bytes, "CommandSpec"] = {}

class NullArray: pass
class NullBulkString: pass


@dataclass(slots=True)
class Arity:
    min_args: int
    max_args: int | None = None
    
    def matches(self, num_args: int) -> bool:
        if num_args < self.min_args:
            return False
        if self.max_args is not None and num_args > self.max_args:
            return False
        return True


@dataclass(slots=True)
class CommandSpec:
    name: bytes
    handler: CommandHandler
    arity: Arity
    flags: set[str]
    allowed_before_auth: bool = False
    allowed_in_subscribe: bool = False   
    
    
def command(
    name: bytes,
    *,
    arity: Arity,
    flags: set[str] | None = None,
    allowed_before_auth: bool = False,
    allowed_in_subscribe: bool = False,
):
    def decorator(func):
        command_name = name.upper()
        COMMANDS[command_name] = CommandSpec(
            name=command_name,
            handler=func,
            arity=arity,
            flags=flags or set(),
            allowed_before_auth=allowed_before_auth,
            allowed_in_subscribe=allowed_in_subscribe
        )
        return func

    return decorator


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
    session: ClientSession | None = None


@dataclass(slots=True)
class CommandContext:
    parser: RESPParser
    config: ServerConfig
    session: ClientSession
    exec_ctx: ExecCtx
