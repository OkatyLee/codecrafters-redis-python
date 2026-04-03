import argparse
import asyncio
import logging
import signal
import sys

from app.config import PubSubConfig, ServerConfig
from app.handlers import handle_client
from app.metrics import start_metrics_server
from app.persistence import load_from_disk
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
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=9100,
        metavar="PORT",
        help="Port for Prometheus metrics server (0 to disable)",
    )


    return parser.parse_args()


async def main() -> None:
    """Start server and, if needed, establish replica handshake."""
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    if args.metrics_port:
        start_metrics_server(args.metrics_port)
    else:
        logger.info("Metrics server disabled")

    config = ServerConfig(args.host, args.port, args.replicaof, args.dir, args.dbfilename)
    storage = CacheStorage()
    pubsub_config = PubSubConfig()
    app_state = AppState(config, storage, logger, pubsub_config)
    # Load persisted data from RDB file if it exists
    load_from_disk(app_state)

    if config.replicaof:
        try:
            if " " in config.replicaof:
                host, port_str = config.replicaof.split(" ", 1)
            else:
                host, port_str = config.replicaof.split(":", 1)
            master_port = int(port_str)
        except (ValueError, IndexError):
            app_state.logger.error("Error: --replicaof must be in the format host:port or 'host port'")
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

    server = await asyncio.start_server(handle_client_wrapper, args.host, args.port, reuse_port=True)
    addr = server.sockets[0].getsockname()
    app_state.logger.info(f"Server started on {addr}")

    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, server.close)
            
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
