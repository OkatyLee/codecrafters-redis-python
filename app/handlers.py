import asyncio
from collections.abc import Sequence
from hashlib import sha256
import os
from app.session import ClientSession
from app.commands import COMMANDS, COMMAND_WRITE_FLAGS, CommandSpec, ExecCtx, redis_command
from app.config import ServerConfig
from app.dispatcher import dispatch_command
from app.parser import RESPError, RESPParser
from app.storage import get_storage



def encode_command_as_resp_array(command: Sequence[bytes]) -> bytes:
    payload = b"*" + str(len(command)).encode() + b"\r\n"
    for item in command:
        payload += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
    return payload


def build_exec_ctx(
    command: Sequence[bytes],
    config: ServerConfig | None,
    *,
    from_replication: bool,
    replica_writer: asyncio.StreamWriter | None = None,
    session: ClientSession | None = None
) -> ExecCtx:
    return ExecCtx(
        from_replication=from_replication,
        raw_resp_command=encode_command_as_resp_array(command),
        propagate=lambda payload, cfg=config: propagate_to_replicas(cfg, payload),
        replica_writer=replica_writer,
        session=session,
    )


async def propagate_to_replicas(config: ServerConfig | None, payload: bytes) -> None:
    if config is None or config.role != "master" or not config.replica_writers:
        return

    config.increment_master_repl_offset(len(payload))

    stale_writers: list[asyncio.StreamWriter] = []
    for replica_writer in list(config.replica_writers):
        try:
            replica_writer.write(payload)
            await replica_writer.drain()
        except Exception:
            stale_writers.append(replica_writer)

    for stale_writer in stale_writers:
        config.unregister_replica(stale_writer)
        stale_writer.close()
        await stale_writer.wait_closed()



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
    config: ServerConfig | None,
    ctx: ExecCtx | None = None,
) -> bytes:
    if ctx is None:
        ctx = build_exec_ctx(command, config, from_replication=False)

    if config is not None and ctx.session is not None and command[0].upper() in COMMANDS:
        return await dispatch_command(command, parser, config, ctx.session, ctx)
    else:
        return parser.encode_simple_error("unknown command")

async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    config: ServerConfig | None = None,
) -> None:
    if config is None:
        config = ServerConfig("127.0.0.1", 0)

    addr = writer.get_extra_info("peername")
    print(f"Connected to client at {addr}")
    parser = RESPParser(reader)
    default_user = config.get_acl_user("default")
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
            
            match command:
                case [b"MULTI"]:
                    if session.in_multi:
                        response = parser.encode_simple_error("MULTI calls can not be nested")
                    else:
                        session.in_multi = True
                        session.queued_commands.clear()
                        response = parser.encode_simple_string("OK")
                case [b"EXEC"]:
                    if not session.in_multi:
                        response = parser.encode_simple_error("EXEC without MULTI")
                    else:
                        commands_to_execute = session.queued_commands.copy()
                        session.queued_commands.clear()
                        session.in_multi = False
                        responses: list[bytes] = []
                        for queued in commands_to_execute:
                            queued_ctx = build_exec_ctx(
                                queued,
                                config,
                                from_replication=False,
                                replica_writer=writer,
                                session=session
                            )
                            responses.append(
                                await _execute_command(queued, parser, config, queued_ctx)
                            )
                        response = _encode_raw_resp_array(responses)
                case [b"DISCARD"]:
                    if not session.in_multi:
                        response = parser.encode_simple_error("DISCARD without MULTI")
                    else:
                        session.queued_commands.clear()
                        session.in_multi = False
                        response = parser.encode_simple_string("OK")
                case [b"SUBSCRIBE", *channels]:
                    assert config is not None
                    if not channels:
                        response = parser.encode_simple_error("wrong number of arguments for 'subscribe' command")
                    else:
                        combined = b""
                        for channel in channels:
                            ch = channel if isinstance(channel, bytes) else str(channel).encode()
                            config.pubsub[ch].add(writer)
                            session.subscribed_channels.add(ch)
                            combined += parser.encode_array([b"subscribe", ch, len(session.subscribed_channels)])
                        session.in_subscribed_mode = True
                        response = combined
                case [b"UNSUBSCRIBE", *channels]:
                    assert config is not None
                    targets = (
                        [c if isinstance(c, bytes) else str(c).encode() for c in channels]
                        if channels else list(session.subscribed_channels)
                    )
                    combined = b""
                    for ch in targets:
                        session.subscribed_channels.discard(ch)
                        config.pubsub[ch].discard(writer)
                        combined += parser.encode_array([b"unsubscribe", ch, len(session.subscribed_channels)])
                    if not session.subscribed_channels:
                        session.in_subscribed_mode = False
                    response = combined if combined else parser.encode_array([b"unsubscribe", None, 0])
                case _:
                    if session.in_subscribed_mode:
                        if command[0] == b"PING":
                            msg = command[1] if len(command) > 1 else b""
                            response = parser.encode_array([b"pong", msg])
                        else:
                            response = parser.encode_simple_error(
                                "ERR Command not allowed in subscribe mode"
                            )
                    elif session.in_multi:  
                        session.queued_commands.append(command)
                        response = parser.encode_simple_string("QUEUED")
                    else:
                        ctx = build_exec_ctx(
                            command,
                            config,
                            from_replication=False,
                            replica_writer=writer,
                            session=session
                        )
                        response = await _execute_command(command, parser, config, ctx)

            writer.write(response)
            await writer.drain()
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        if config is not None:
            if writer in config.replica_writers:
                config.replica_writers.remove(writer)
            for ch in session.subscribed_channels:
                config.pubsub[ch].discard(writer)
        writer.close()
        await writer.wait_closed()
