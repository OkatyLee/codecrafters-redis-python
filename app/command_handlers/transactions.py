
from app.command_executor import build_exec_ctx, execute_single_command
from app.commands import Arity, CommandContext, command
from app.parser import RESPError, RawResponse


def _encode_raw_resp_array(responses: list[bytes]) -> bytes:
    return b"*" + str(len(responses)).encode() + b"\r\n" + b"".join(responses)


@command(
    name=b"MULTI",
    arity=Arity(0, 0),
    flags={"transaction"}
)
def cmd_multi(ctx: CommandContext, args: list[bytes]) -> str:

    if ctx.session.in_multi:
        raise RESPError("MULTI calls can not be nested")

    ctx.session.in_multi = True
    ctx.session.queued_commands.clear()
    return "OK"


@command(
    name=b"DISCARD",
    arity=Arity(0, 0),
    flags={"transaction"},
)
def cmd_discard(ctx: CommandContext, args: list[bytes]) -> str:
    if not ctx.session.in_multi:
        raise RESPError("DISCARD without MULTI")

    ctx.session.queued_commands.clear()
    ctx.session.in_multi = False
    return "OK"



@command(
    name=b"EXEC",
    arity=Arity(0, 0),
    flags={"transaction"}
)
async def cmd_exec(ctx: CommandContext, args: list[bytes]) -> RawResponse:
    if not ctx.session.in_multi:
        raise RESPError("EXEC without MULTI")

    commands_to_execute = ctx.session.queued_commands.copy()
    ctx.session.queued_commands.clear()
    ctx.session.in_multi = False
    responses: list[bytes] = []
    for queued in commands_to_execute:
        queued_ctx = build_exec_ctx(
            queued,
            ctx.app_state,
            from_replication=False,
            replica_writer=ctx.exec_ctx.replica_writer,
            session=ctx.session
        )
        responses.append(
            await execute_single_command(
                CommandContext(
                    parser=ctx.parser,
                    app_state=ctx.app_state,
                    session=ctx.session,
                    exec_ctx=queued_ctx,
                ),
                queued,
            )
        )
    return RawResponse(payload=_encode_raw_resp_array(responses))
