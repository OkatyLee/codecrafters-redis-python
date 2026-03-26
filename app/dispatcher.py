
import asyncio

from app.commands import COMMANDS, CommandContext, ExecCtx, NullArray, NullBulkString
import app.command_handlers  # noqa: F401
from app.config import ServerConfig
from app.parser import RESPError, RESPParser
from app.session import ClientSession


def encode_command_result(parser: RESPParser, result: object) -> bytes:
    if result is None:
        return parser.encode_null_array()
    elif isinstance(result, str):
        return parser.encode_simple_string(result)
    elif isinstance(result, int):
        return parser.encode_integer(result)
    elif isinstance(result, bytes):
        return parser.encode_bulk_string(result)
    elif isinstance(result, (list, tuple)):
        return parser.encode_array(result)
    elif isinstance(result, NullArray):
        return parser.encode_null_array()
    elif isinstance(result, NullBulkString):
        return parser.encode_null()
    else:
        raise ValueError("Unsupported result type")


async def dispatch_command(
    command: list[bytes],
    parser: RESPParser,
    config: ServerConfig,
    session: ClientSession,
    exec_ctx: ExecCtx,
) -> bytes:
    name = command[0].upper()
    args = command[1:]
    
    spec = COMMANDS.get(name)
    
    if spec is None:
        return parser.encode_simple_error("unknown command")
    
    if not spec.allowed_before_auth and not session.is_authenticated:
        return parser.encode_simple_error("NOAUTH  Authentication required.")
    
    if session.in_subscribed_mode and not spec.allowed_in_subscribe:
        return parser.encode_simple_error("ERR Command not allowed in subscribe mode")
    
    if not spec.arity.matches(len(args)):
        return parser.encode_simple_error(
            f"wrong number of arguments for '{spec.name.decode().lower()}' command"
        )
    
    ctx = CommandContext(
        parser=parser,
        config=config,
        session=session,
        exec_ctx=exec_ctx,
    )
    try:
        result = spec.handler(ctx, args)
        if asyncio.iscoroutine(result):
            result = await result
        response = encode_command_result(parser, result) if "noparse" not in spec.flags else result
    except (ValueError, TypeError, RESPError, AssertionError) as e:
        response = parser.encode_simple_error(str(e))
        
    if not isinstance(response, bytes):
        raise ValueError(f"Error: Dispatched command {spec.name.decode()} must return bytes")
    if "write" in spec.flags and not exec_ctx.from_replication and not response.startswith(b"-"):
        await exec_ctx.propagate(exec_ctx.raw_resp_command)
    return response
