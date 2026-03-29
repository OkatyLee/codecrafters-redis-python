

from app.command_executor import execute_single_command
from app.commands import CommandContext, ExecCtx, get_command_spec
import app.command_handlers  # noqa: F401
from app.parser import RESPParser
from app.resp_types import BaseRESPType, SimpleErrorType, SimpleStringType
from app.session import ClientSession
from app.state import AppState





async def dispatch_command(
    command: list[bytes],
    parser: RESPParser,
    app_state: AppState,
    session: ClientSession,
    exec_ctx: ExecCtx,
) -> BaseRESPType:
    name = command[0].upper()
    args = command[1:]
    
    spec = get_command_spec(name)
    
    if spec is None:
        return SimpleErrorType("ERR unknown command")
    
    if not spec.allowed_before_auth and not session.is_authenticated:
        return SimpleErrorType("NOAUTH  Authentication required.")
    
    if session.in_subscribed_mode and not spec.allowed_in_subscribe:
        return SimpleErrorType("ERR Command not allowed in subscribe mode")
    
    if not spec.arity.matches(len(args)):
        return SimpleErrorType(
            f"ERR wrong number of arguments for '{spec.name.decode().lower()}' command"
        )
    
    if session.in_multi and "transaction" not in spec.flags:
        session.queued_commands.append(command)
        return SimpleStringType("QUEUED")
    
    ctx = CommandContext(
        parser=parser,
        app_state=app_state,
        session=session,
        exec_ctx=exec_ctx,
    )
    response = await execute_single_command(ctx, spec, args)

    if "write" in spec.flags and not exec_ctx.from_replication and not isinstance(response, SimpleErrorType):
        await exec_ctx.propagate(exec_ctx.raw_resp_command)
    return response
