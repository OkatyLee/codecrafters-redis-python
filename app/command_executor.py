

import asyncio
from collections.abc import Sequence
import inspect

from app.commands import CommandContext, CommandSpec, ExecCtx
from app.parser import RESPError
from app.resp_types import BaseRESPType, SimpleErrorType
from app.session import ClientSession
from app.state import AppState


async def execute_single_command(
    ctx: CommandContext,
    spec: CommandSpec,
    args: list[bytes],
) -> BaseRESPType:
    handler = spec.handler
    
    try:
        result = handler(ctx, args)
        response: BaseRESPType = await result if inspect.isawaitable(result) else result
    except (ValueError, RESPError, TypeError, AssertionError) as e:
        response = SimpleErrorType(str(e))
    return response


def build_exec_ctx(
    command: Sequence[bytes],
    app_state: AppState,
    *,
    from_replication: bool,
    connection_writer: asyncio.StreamWriter | None = None,
    session: ClientSession | None = None
) -> ExecCtx:
    from app.replication import propagate_to_replicas

    return ExecCtx(
        from_replication=from_replication,
        raw_resp_command=encode_command_as_resp_array(command),
        propagate=lambda payload, state=app_state: propagate_to_replicas(state, payload),
        connection_writer=connection_writer,
        session=session,
    )

def encode_command_as_resp_array(command: Sequence[bytes]) -> bytes:
    payload = b"*" + str(len(command)).encode() + b"\r\n"
    for item in command:
        payload += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
    return payload
