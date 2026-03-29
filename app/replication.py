import asyncio

from app.command_executor import build_exec_ctx, encode_command_as_resp_array
from app.dispatcher import dispatch_command
from app.parser import RESPParser
from app.persistence import load_rdb_snapshot
from app.session import ClientSession
from app.state import AppState


def _normalize_command(data: list[object]) -> list[bytes]:
    normalized = [item if isinstance(item, bytes) else str(item).encode() for item in data]
    normalized[0] = normalized[0].upper()
    return normalized


async def propagate_to_replicas(app_state: AppState | None, payload: bytes) -> None:
    if app_state is None or app_state.config.role != "master" or not app_state.replica_writers:
        return

    app_state.config.increment_master_repl_offset(len(payload))
    stale_writers: list[asyncio.StreamWriter] = []

    async def _write_drain(connection_writer: asyncio.StreamWriter) -> None:
        try:
            connection_writer.write(payload)
            await connection_writer.drain()
        except Exception:
            stale_writers.append(connection_writer)

    async with asyncio.TaskGroup() as tg:
        for connection_writer in list(app_state.replica_writers):
            tg.create_task(_write_drain(connection_writer))

    async def _unregister_stale_replica(stale_writer: asyncio.StreamWriter) -> None:
        app_state.unregister_replica(stale_writer)
        stale_writer.close()
        try:
            await stale_writer.wait_closed()
        except Exception:
            pass

    async with asyncio.TaskGroup() as tg:
        for stale_writer in stale_writers:
            tg.create_task(_unregister_stale_replica(stale_writer))


async def replication_handshake_and_loop(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    app_state: AppState,
    my_listening_port: int,
) -> None:
    parser = RESPParser(reader)
    replication_session = ClientSession.create(app_state.config.get_acl_user("default"))

    try:
        app_state.logger.info("Starting replication handshake")

        for command, expected_response, error_message in (
            ([b"PING"], "PONG", "Received unexpected response to PING from master"),
            (
                [b"REPLCONF", b"listening-port", str(my_listening_port).encode()],
                "OK",
                "Received unexpected response to REPLCONF listening-port",
            ),
            (
                [b"REPLCONF", b"capa", b"psync2"],
                "OK",
                "Received unexpected response to REPLCONF capa",
            ),
        ):
            writer.write(encode_command_as_resp_array(command))
            await writer.drain()
            response = await parser.parse()
            if response != expected_response:
                app_state.logger.error(error_message)
                return

        writer.write(encode_command_as_resp_array([b"PSYNC", b"?", b"-1"]))
        await writer.drain()
        response = await parser.parse()
        if not isinstance(response, str) or not response.startswith("FULLRESYNC "):
            app_state.logger.error("Received unexpected response to PSYNC")
            return

        rdb_payload = await parser.parse()
        if not isinstance(rdb_payload, bytes):
            app_state.logger.error("Received invalid RDB payload from master")
            return

        is_success = load_rdb_snapshot(app_state, rdb_payload)
        if is_success:
            app_state.logger.info("Replication handshake completed successfully")
        else:
            app_state.logger.error("Unexpected error during handshake")
            return

        while True:
            data = await parser.parse()
            if data is None:
                break
            if not isinstance(data, list) or len(data) == 0:
                continue

            command = _normalize_command(data) # type: ignore
            app_state.config.increment_replica_repl_offset(len(encode_command_as_resp_array(command)))
            exec_ctx = build_exec_ctx(
                command,
                app_state,
                from_replication=True,
                session=replication_session,
            )
            response = await dispatch_command(
                command,
                parser,
                app_state,
                replication_session,
                exec_ctx,
            )
            if command[:2] == [b"REPLCONF", b"GETACK"]:
                writer.write(response.encode())
                await writer.drain()
    except Exception as exc:
        app_state.logger.error(f"Replication error: {exc}")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError, OSError):
            app_state.logger.error("Error occurred while waiting for writer to close")
