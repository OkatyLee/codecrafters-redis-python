import asyncio
from collections.abc import Sequence
from app.command_executor import build_exec_ctx
from app.session import ClientSession
from app.commands import COMMANDS, ExecCtx
from app.dispatcher import dispatch_command
from app.parser import RESPParser
from app.state import AppState



# def encode_command_as_resp_array(command: Sequence[bytes]) -> bytes:
#     payload = b"*" + str(len(command)).encode() + b"\r\n"
#     for item in command:
#         payload += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
#     return payload


# def build_exec_ctx(
#     command: Sequence[bytes],
#     app_state: AppState,
#     *,
#     from_replication: bool,
#     replica_writer: asyncio.StreamWriter | None = None,
#     session: ClientSession | None = None
# ) -> ExecCtx:
#     return ExecCtx(
#         from_replication=from_replication,
#         raw_resp_command=encode_command_as_resp_array(command),
#         propagate=lambda payload, state=app_state: propagate_to_replicas(state, payload),
#         replica_writer=replica_writer,
#         session=session,
#     )


# async def propagate_to_replicas(app_state: AppState | None, payload: bytes) -> None:
#     if app_state is None or app_state.config.role != "master" or not app_state.replica_writers:
#         return

#     app_state.config.increment_master_repl_offset(len(payload))

#     stale_writers: list[asyncio.StreamWriter] = []
    
#     async def _write_drain(replica_writer: asyncio.StreamWriter, payload: bytes):
#         try:
#             replica_writer.write(payload)
#             await replica_writer.drain()
#         except Exception:
#             stale_writers.append(replica_writer)
            
#     async with asyncio.TaskGroup() as tg:
#         for replica_writer in list(app_state.replica_writers):
#             tg.create_task(_write_drain(replica_writer, payload))


#     async def _unregister_stale_replica(stale_writer: asyncio.StreamWriter):
#         app_state.unregister_replica(stale_writer)
#         stale_writer.close()
#         try:
#             await stale_writer.wait_closed()
#         except Exception:
#             pass

#     async with asyncio.TaskGroup() as tg:
#         for stale_writer in stale_writers:
#             tg.create_task(_unregister_stale_replica(stale_writer))



def _normalize_command(data: Sequence[object]) -> list[bytes]:
    """Normalize parsed RESP array items to bytes and uppercase command name."""
    normalized = [item if isinstance(item, bytes) else str(item).encode() for item in data]
    normalized[0] = normalized[0].upper()
    return normalized


def _encode_raw_resp_array(responses: Sequence[bytes]) -> bytes:
    """Build RESP array from pre-encoded RESP elements."""
    return b"*" + str(len(responses)).encode() + b"\r\n" + b"".join(responses)


    

async def _execute_command(
    command: list[bytes],
    parser: RESPParser,
    app_state: AppState,
    ctx: ExecCtx | None = None,
) -> bytes:
    assert ctx is not None

    if app_state is not None and ctx.session is not None and command[0].upper() in COMMANDS:
        return await dispatch_command(command, parser, app_state, ctx.session, ctx)
    else:
        return parser.encode_simple_error("unknown command")

async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    app_state: AppState,
) -> None:
    
    addr = writer.get_extra_info("peername")
    app_state.logger.info(f"Connected to client at {addr}")
    parser = RESPParser(reader)
    default_user = app_state.config.get_acl_user("default")
    session = ClientSession.create(default_user)
    try:
        while True:
            data = await parser.parse()
            if data is None:
                break
            if not isinstance(data, list) or len(data) == 0:
                response = parser.encode_simple_error("Protocol error")
                writer.write(response)
                await writer.drain()
                continue

            command = _normalize_command(data)
        
            ctx = build_exec_ctx(
                command,
                app_state,
                from_replication=False,
                replica_writer=writer,
                session=session
            )
            response = await dispatch_command(command, parser, app_state, session, ctx)

            writer.write(response)
            await writer.drain()
    except Exception as e:
        app_state.logger.error(f"Error handling client: {e}")
    finally:
        if app_state.config is not None:
            if writer in app_state.replica_writers:
                app_state.replica_writers.remove(writer)
            for ch in session.subscribed_channels:
                app_state.pubsub[ch].discard(writer)
        writer.close()
        await writer.wait_closed()
