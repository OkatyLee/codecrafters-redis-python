import argparse
import asyncio

from app.config import ServerConfig
from app.handlers import _execute_command, build_exec_ctx, encode_command_as_resp_array, handle_client
from app.parser import RESPParser


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

    return parser.parse_args()


async def main() -> None:
    """Start server and, if needed, establish replica handshake."""
    args = parse_args()
    config = ServerConfig(args.host, args.port, args.replicaof)

    if config.replicaof:
        try:
            host, port_str = config.replicaof.split(":", 1)
            master_port = int(port_str)
        except (ValueError, IndexError):
            print("Error: --replicaof must be in the format host:port")
            return

        print(f"Connect to master {host}:{master_port}")
        master_reader, master_writer = await asyncio.open_connection(host, master_port)
        asyncio.create_task(
            replication_handshake_and_loop(
                master_reader,
                master_writer,
                config,
                args.port,
            )
        )

    async def handle_client_wrapper(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_client(reader, writer, config)

    server = await asyncio.start_server(handle_client_wrapper, args.host, args.port)
    addr = server.sockets[0].getsockname()
    print(f"Server started on {addr}")

    async with server:
        await server.serve_forever()


async def replication_handshake_and_loop(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    config: ServerConfig,
    my_listening_port: int,
) -> None:
    """Perform replica handshake and apply write commands from master."""
    parser = RESPParser(reader)
    try:
        print("Starting replication handshake")

        writer.write(parser.encode_array([b"PING"]))
        await writer.drain()
        response = await parser.parse()
        if response != "PONG":
            print("Received unexpected response to PING from master")
            return

        writer.write(
            parser.encode_array(
                [b"REPLCONF", b"listening-port", str(my_listening_port).encode()]
            )
        )
        await writer.drain()
        response = await parser.parse()
        if response != "OK":
            print("Received unexpected response to REPLCONF listening-port")
            return

        writer.write(parser.encode_array([b"REPLCONF", b"capa", b"psync2"]))
        await writer.drain()
        response = await parser.parse()
        if response != "OK":
            print("Received unexpected response to REPLCONF capa")
            return

        writer.write(parser.encode_array([b"PSYNC", b"?", b"-1"]))
        await writer.drain()
        response = await parser.parse()
        if not isinstance(response, str) or not response.startswith("FULLRESYNC "):
            print("Received unexpected response to PSYNC")
            return

        rdb_payload = await parser.parse()
        if not isinstance(rdb_payload, bytes):
            print("Received invalid RDB payload from master")
            return

        print("Replication handshake completed successfully")

        while True:
            data = await parser.parse()
            if data is None:
                break
            if not isinstance(data, list) or len(data) == 0:
                continue
            command = [item if isinstance(item, bytes) else str(item).encode() for item in data]
            command[0] = command[0].upper()
            config.increment_replica_repl_offset(len(encode_command_as_resp_array(command)))
            ctx = build_exec_ctx(command, config, from_replication=True)
            response = await _execute_command(command, parser, config, ctx)
            if command[:2] == [b"REPLCONF", b"GETACK"] and response:
                writer.write(response)
                await writer.drain()
    except Exception as e:
        print(f"Replication error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
