

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
    return ExecCtx(
        from_replication=from_replication,
        raw_resp_command=encode_command_as_resp_array(command),
        propagate=lambda payload, state=app_state: propagate_to_replicas(state, payload),
        connection_writer=connection_writer,
        session=session,
    )
    
    
async def propagate_to_replicas(app_state: AppState | None, payload: bytes) -> None:
    if app_state is None or app_state.config.role != "master" or not app_state.replica_writers:
        return

    app_state.config.increment_master_repl_offset(len(payload))

    stale_writers: list[asyncio.StreamWriter] = []
    
    async def _write_drain(connection_writer: asyncio.StreamWriter, payload: bytes):
        try:
            connection_writer.write(payload)
            await connection_writer.drain()
        except Exception:
            stale_writers.append(connection_writer)
            
    async with asyncio.TaskGroup() as tg:
        for connection_writer in list(app_state.replica_writers):
            tg.create_task(_write_drain(connection_writer, payload))


    async def _unregister_stale_replica(stale_writer: asyncio.StreamWriter):
        app_state.unregister_replica(stale_writer)
        stale_writer.close()
        try:
            await stale_writer.wait_closed()
        except Exception:
            pass

    async with asyncio.TaskGroup() as tg:
        for stale_writer in stale_writers:
            tg.create_task(_unregister_stale_replica(stale_writer))
    

def encode_command_as_resp_array(command: Sequence[bytes]) -> bytes:
    payload = b"*" + str(len(command)).encode() + b"\r\n"
    for item in command:
        payload += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
    return payload
