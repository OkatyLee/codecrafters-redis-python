from dataclasses import dataclass
from typing import Awaitable, Callable
from asyncio import StreamWriter

from app.parser import RESPParser
from app.resp_types import BaseRESPType
from app.session import ClientSession
from app.state import AppState

type PropagateCallback = Callable[[bytes], Awaitable[None]]
type CommandHandler = Callable[["CommandContext", list[bytes]], Awaitable[BaseRESPType] | BaseRESPType]

COMMANDS: dict[bytes, "CommandSpec"] = {}


@dataclass(slots=True)
class Arity:
    """
    Specification for the arity of a Redis command. \
    Attributes: \
        min_args (int): The minimum number of arguments the command accepts. \
        max_args (int | None): The maximum number of arguments the command accepts, or None if no limit.
    """
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
    """
    Specification for a Redis command.
    Attributes:
        name (bytes): The command name as bytes.
        handler (CommandHandler): The function that handles execution of this command.
        arity (Arity): The number of arguments the command accepts.
        flags (set[str]): A set of flags describing command properties (e.g., 'write', 'readonly').
        allowed_before_auth (bool): Whether the command can be executed before authentication. Defaults to False.
        allowed_in_subscribe (bool): Whether the command can be executed in subscribe mode. Defaults to False.
    """
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
    """
    Decorator to register a command handler.
    """
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


def get_command_spec(name: bytes) -> "CommandSpec | None":
    return COMMANDS.get(name.upper())

@dataclass(slots=True)
class ExecCtx:
    """
    Execution context for a single command dispatch.
    Fields:
        from_replication (bool): Flag indicating whether the command is from replication.
        raw_resp_command (bytes): The raw RESP command.
        propagate (PropagateCallback): Callback for propagating changes.
        connection_writer (StreamWriter | None): The writer for the connection.
        session (ClientSession | None): The client session.
    """

    from_replication: bool
    raw_resp_command: bytes
    propagate: PropagateCallback
    connection_writer: StreamWriter | None = None
    session: ClientSession | None = None


@dataclass(slots=True)
class CommandContext:
    """
    Context for executing a command.                    
    Attributes:                                             
        parser (RESPParser): The RESP parser.           
        app_state (AppState): The application state.    
        session (ClientSession): The client session.    
        exec_ctx (ExecCtx): The execution context.     
    """
    parser: RESPParser
    app_state: AppState
    session: ClientSession
    exec_ctx: ExecCtx

