

import asyncio
from collections.abc import Sequence
import inspect

from app.commands import COMMANDS, CommandContext, ExecCtx
from app.parser import RESPError, RESPParser
from app.resp_types import BaseRESPType, SimpleErrorType, RawResponse
from app.session import ClientSession
from app.state import AppState





def encode_command_result(parser: RESPParser, result: BaseRESPType) -> bytes:
    return result.encode()


async def execute_single_command(
    ctx: CommandContext,
    args: list[bytes],
) -> BaseRESPType:
    name, *args = args
    spec = COMMANDS.get(name.upper())
    
    if spec is None:
        return SimpleErrorType("ERR unknown command")
    
    try: 
        result = spec.handler(ctx, args)
        response: BaseRESPType = await result if inspect.isawaitable(result) else result
    except (ValueError, RESPError, TypeError, AssertionError) as e:
        response = SimpleErrorType(str(e))
    return response


def build_exec_ctx(
    command: Sequence[bytes],
    app_state: AppState,
    *,
    from_replication: bool,
    replica_writer: asyncio.StreamWriter | None = None,
    session: ClientSession | None = None
) -> ExecCtx:
    return ExecCtx(
        from_replication=from_replication,
        raw_resp_command=encode_command_as_resp_array(command),
        propagate=lambda payload, state=app_state: propagate_to_replicas(state, payload),
        replica_writer=replica_writer,
        session=session,
    )
    
    
async def propagate_to_replicas(app_state: AppState | None, payload: bytes) -> None:
    if app_state is None or app_state.config.role != "master" or not app_state.replica_writers:
        return

    app_state.config.increment_master_repl_offset(len(payload))

    stale_writers: list[asyncio.StreamWriter] = []
    
    async def _write_drain(replica_writer: asyncio.StreamWriter, payload: bytes):
        try:
            replica_writer.write(payload)
            await replica_writer.drain()
        except Exception:
            stale_writers.append(replica_writer)
            
    async with asyncio.TaskGroup() as tg:
        for replica_writer in list(app_state.replica_writers):
            tg.create_task(_write_drain(replica_writer, payload))


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
