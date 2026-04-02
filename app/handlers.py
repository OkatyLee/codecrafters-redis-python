import asyncio
from collections.abc import Sequence
import time
from app.command_executor import build_exec_ctx
from app.metrics import client_connected, client_disconnected, record_command, record_protocol_error, set_active_subscriptions
from app.resp_types import SimpleErrorType
from app.session import ClientSession
from app.dispatcher import dispatch_command
from app.parser import RESPParser
from app.state import AppState


def _normalize_command(data: Sequence[object]) -> list[bytes]:
    """Normalize parsed RESP array items to bytes and uppercase command name."""
    normalized = [item if isinstance(item, bytes) else str(item).encode() for item in data]
    normalized[0] = normalized[0].upper()
    return normalized


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    app_state: AppState,
) -> None:
    
    addr = writer.get_extra_info("peername")
    app_state.logger.info(f"Connected to client at {addr}")
    client_connected()
    parser = RESPParser(reader)
    default_user = app_state.config.get_acl_user("default")
    session = ClientSession.create(default_user)
    try:
        while True:
            data = await parser.parse()
            if data is None:
                break
            if not isinstance(data, list) or len(data) == 0:
                record_protocol_error()
                response = SimpleErrorType("ERR Protocol error")
                writer.write(response.encode())
                await writer.drain()
                continue

            command = _normalize_command(data)
            started_at = time.perf_counter()
        
            ctx = build_exec_ctx(
                command,
                app_state,
                from_replication=False,
                connection_writer=writer,
                session=session
            )
            response = await dispatch_command(command, parser, app_state, session, ctx)
            payload = response.encode()
            record_command(
                command[0],
                status="error" if payload.startswith(b"-") else "ok",
                duration_seconds=time.perf_counter() - started_at,
            )
            writer.write(payload)
            await writer.drain()
    except Exception as e:
        app_state.logger.error(f"Error handling client: {e}")
    finally:
        if app_state.config is not None:
            if writer in app_state.replica_writers:
                app_state.unregister_replica(writer)
            for ch in session.subscribed_channels:
                app_state.pubsub[ch].discard(writer)
            set_active_subscriptions(sum(len(subscribers) for subscribers in app_state.pubsub.values()))
        client_disconnected()
        writer.close()
        await writer.wait_closed()
