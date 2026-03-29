import argparse
import asyncio
import logging

from app.command_executor import encode_command_as_resp_array
from app.config import ServerConfig
from app.dispatcher import dispatch_command
from app.handlers import build_exec_ctx, handle_client
from app.parser import RESPParser
from app.resp_types import ArrayType, BulkStringType
from app.session import ClientSession
from app.state import AppState
from app.storage import CacheStorage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Redis-like server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=6379,
        help="Port to listen on",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to listen on",
    )
    parser.add_argument(
        "--replicaof",
        default=None,
        help="Run as a replica server of the specified master",
    )
    parser.add_argument(
        "--dir",
        default="./tmp/files",
        help="Directory to store data",
    )
    parser.add_argument(
        "--dbfilename",
        default="dump.rdb",
        help="Filename to store database dump",
    )

    return parser.parse_args()


async def main() -> None:
    """Start server and, if needed, establish replica handshake."""
    args = parse_args()
    config = ServerConfig(args.host, args.port, args.replicaof, args.dir, args.dbfilename)
    logger = logging.getLogger(__name__)
    storage = CacheStorage()
    # Load persisted data from RDB file if it exists
    storage.load(config.dir, config.dbfilename)
    app_state = AppState(config, storage, logger)

    if config.replicaof:
        try:
            host, port_str = config.replicaof.split(":", 1)
            master_port = int(port_str)
        except (ValueError, IndexError):
            app_state.logger.error("Error: --replicaof must be in the format host:port")
            return

        app_state.logger.info(f"Connecting to master {host}:{master_port}")
        master_reader, master_writer = await asyncio.open_connection(host, master_port)
        asyncio.create_task(
            replication_handshake_and_loop(
                master_reader,
                master_writer,
                app_state,
                args.port,
            )
        )

    async def handle_client_wrapper(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_client(reader, writer, app_state)

    server = await asyncio.start_server(handle_client_wrapper, args.host, args.port)
    addr = server.sockets[0].getsockname()
    app_state.logger.info(f"Server started on {addr}")

    async with server:
        await server.serve_forever()


async def replication_handshake_and_loop(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    app_state: AppState,
    my_listening_port: int,
) -> None:
    """Perform replica handshake and apply write commands from master."""
    parser = RESPParser(reader)
    replication_session = ClientSession.create(app_state.config.get_acl_user("default"))
    try:
        app_state.logger.info("Starting replication handshake")

        writer.write(ArrayType([BulkStringType(b"PING")]).encode())
        await writer.drain()
        response = await parser.parse()
        if response != "PONG":
            app_state.logger.error("Received unexpected response to PING from master")
            return

        writer.write(
            ArrayType([BulkStringType(val) for val in [b"REPLCONF", b"listening-port", str(my_listening_port).encode()]])
            .encode()
        )
        
        await writer.drain()
        response = await parser.parse()
        if response != "OK":
            app_state.logger.error("Received unexpected response to REPLCONF listening-port")
            return

        writer.write(ArrayType([BulkStringType(val) for val in [b"REPLCONF", b"capa", b"psync2"]]).encode())
        await writer.drain()
        response = await parser.parse()
        if response != "OK":
            app_state.logger.error("Received unexpected response to REPLCONF capa")
            return

        writer.write(ArrayType([BulkStringType(val) for val in [b"PSYNC", b"?", b"-1"]]).encode())
        await writer.drain()
        response = await parser.parse()
        if not isinstance(response, str) or not response.startswith("FULLRESYNC "):
            app_state.logger.error("Received unexpected response to PSYNC")
            return

        rdb_payload = await parser.parse()
        if not isinstance(rdb_payload, bytes):
            app_state.logger.error("Received invalid RDB payload from master")
            return
        is_success = app_state.storage.load(app_state.config.dir, app_state.config.dbfilename, rdb_payload)
        app_state.logger.info("Replication handshake completed successfully" if is_success else "Unexpected error during handshake")
        while True:
            data = await parser.parse()
            if data is None:
                break
            if not isinstance(data, list) or len(data) == 0:
                continue
            command = [item if isinstance(item, bytes) else str(item).encode() for item in data]
            command[0] = command[0].upper()
            app_state.config.increment_replica_repl_offset(len(encode_command_as_resp_array(command)))
            ctx = build_exec_ctx(
                command,
                app_state,
                from_replication=True,
                session=replication_session,
            )
            response = await dispatch_command(command, parser, app_state, replication_session, ctx)
            if command[:2] == [b"REPLCONF", b"GETACK"] and response:
                writer.write(response.encode())
                await writer.drain()
    except Exception as e:
        app_state.logger.error(f"Replication error: {e}")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError, OSError):
            # Peer may drop/reset TCP connection during shutdown (common on Windows).
            app_state.logger.error("Error occurred while waiting for writer to close")
            pass


if __name__ == "__main__":
    asyncio.run(main())
