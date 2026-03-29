import argparse
import asyncio
import logging

from app.config import ServerConfig
from app.handlers import handle_client
from app.replication import replication_handshake_and_loop
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


if __name__ == "__main__":
    asyncio.run(main())
