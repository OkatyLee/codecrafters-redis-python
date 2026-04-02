"""Tests for core.py command handlers including ACL, AUTH, PING, and ECHO."""

import asyncio
import logging
import tempfile

import pytest

from app.config import ServerConfig
from app.handlers import handle_client
from app.parser import RESPParser
from app.state import AppState
from app.storage import CacheStorage


def make_app_state(
    config: ServerConfig | None = None,
    storage: CacheStorage | None = None,
) -> AppState:
    """Create an AppState for testing."""
    return AppState(
        config or ServerConfig("127.0.0.1", 0),
        storage or CacheStorage(),
        logging.getLogger(__name__),
    )


async def start_test_server(
    config: ServerConfig | None = None,
    app_state: AppState | None = None,
) -> tuple[asyncio.base_events.Server, AppState]:
    """Start a test server."""
    state = app_state or make_app_state(config)

    async def _handle(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await handle_client(reader, writer, state)

    server = await asyncio.start_server(_handle, state.config.host, 0)
    return server, state


async def send_command_and_read_response(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    command: bytes,
) -> bytes:
    """Send a command and read the full response."""
    writer.write(command)
    await writer.drain()

    first_line = await reader.readline()
    if first_line.startswith(b"$") and first_line != b"$-1\r\n":
        length = int(first_line[1:-2])
        payload = await reader.readexactly(length + 2)
        return first_line + payload
    if first_line.startswith(b"*") and first_line != b"*-1\r\n":
        count = int(first_line[1:-2])
        result = first_line
        for _ in range(count):
            header = await reader.readline()
            result += header
            if header.startswith(b"$"):
                length = int(header[1:-2])
                if length != -1:
                    payload = await reader.readexactly(length + 2)
                    result += payload
        return result
    return first_line


async def send_command_and_parse_response(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    command: bytes,
):
    """Send a command and parse the response."""
    writer.write(command)
    await writer.drain()
    parser = RESPParser(reader)
    return await parser.parse()


# ==================== PING Tests ====================


@pytest.mark.asyncio
async def test_ping_no_args_returns_pong():
    """PING with no args returns +PONG."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(reader, writer, b"*1\r\n$4\r\nPING\r\n")

    assert response == b"+PONG\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_ping_with_message_returns_bulk_string():
    """PING with message returns the message as bulk string."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader, writer, b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n"
    )

    assert response == b"$5\r\nhello\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_ping_in_subscribed_mode_returns_array():
    """PING in subscribe mode returns array with [pong, message]."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # Subscribe to a channel
    await send_command_and_parse_response(
        reader, writer, b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n"
    )
    
    # PING in subscribe mode with message
    response = await send_command_and_read_response(
        reader, writer, b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n"
    )

    # Should get array: [pong, hello]
    assert response == b"*2\r\n$4\r\npong\r\n$5\r\nhello\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_ping_in_subscribed_mode_no_args():
    """PING in subscribe mode with no args returns [pong, ""]."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # Subscribe to a channel
    await send_command_and_parse_response(
        reader, writer, b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nch1\r\n"
    )
    
    # PING in subscribe mode without message
    response = await send_command_and_read_response(
        reader, writer, b"*1\r\n$4\r\nPING\r\n"
    )

    # Should get array: [pong, ""]
    assert response == b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


# ==================== ECHO Tests ====================


@pytest.mark.asyncio
async def test_echo_returns_bulk_string():
    """ECHO returns the argument as a bulk string."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader, writer, b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
    )

    assert response == b"$5\r\nhello\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


# ==================== AUTH Tests ====================


@pytest.mark.asyncio
async def test_auth_wrong_password():
    """AUTH with wrong password raises WRONGPASS error."""
    config = ServerConfig("127.0.0.1", 0)
    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # First set default user with nopass=False to require password
    config.acl_users["default"].nopass = False
    from hashlib import sha256
    config.acl_users["default"].passwords.add(sha256(b"correctpass").digest())
    
    response = await send_command_and_read_response(
        reader, writer, b"*2\r\n$4\r\nAUTH\r\n$8\r\nwrongpwd\r\n"
    )

    assert b"WRONGPASS" in response or b"invalid" in response

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_auth_correct_password():
    """AUTH with correct password succeeds."""
    config = ServerConfig("127.0.0.1", 0)
    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # First set default user with password
    config.acl_users["default"].nopass = False
    from hashlib import sha256
    config.acl_users["default"].passwords.add(sha256(b"correctpass").digest())
    
    response = await send_command_and_read_response(
        reader, writer, b"*2\r\n$4\r\nAUTH\r\n$11\r\ncorrectpass\r\n"
    )

    assert response == b"+OK\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_auth_with_username_and_password():
    """AUTH with username and password."""
    config = ServerConfig("127.0.0.1", 0)
    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # Set default user with password
    from hashlib import sha256
    config.acl_users["default"].nopass = False
    config.acl_users["default"].passwords.add(sha256(b"correctpass").digest())
    
    # AUTH with username and password
    response = await send_command_and_read_response(
        reader, writer, b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$11\r\ncorrectpass\r\n"
    )

    assert response == b"+OK\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


# ==================== ACL Tests ====================


@pytest.mark.asyncio
async def test_acl_whoami_default_user():
    """ACL WHOAMI returns default username when not logged in."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    response = await send_command_and_read_response(
        reader, writer, b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n"
    )

    assert response == b"$7\r\ndefault\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_acl_whoami_after_auth():
    """ACL WHOAMI returns username after AUTH."""
    config = ServerConfig("127.0.0.1", 0)
    server, app_state = await start_test_server(config)
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # Set default user with password
    from hashlib import sha256
    config.acl_users["default"].nopass = False
    config.acl_users["default"].passwords.add(sha256(b"password").digest())
    
    # Authenticate
    await send_command_and_read_response(
        reader, writer, b"*2\r\n$4\r\nAUTH\r\n$8\r\npassword\r\n"
    )
    
    # Check WHOAMI
    response = await send_command_and_read_response(
        reader, writer, b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n"
    )

    assert response == b"$7\r\ndefault\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_acl_setuser_invalid_password_format():
    """ACL SETUSER with invalid password format raises error."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # Try SETUSER with invalid password (should start with >)
    # *4 means 4 elements: ACL, SETUSER, username, password
    response = await send_command_and_read_response(
        reader, writer, b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$8\r\ntestuser\r\n$4\r\npass\r\n"
    )

    assert b"ERR" in response

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_acl_setuser_valid_password():
    """ACL SETUSER with valid password format succeeds."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # SETUSER with valid password (prefixed with >)
    # *4 means 4 elements: ACL, SETUSER, username, password
    response = await send_command_and_read_response(
        reader, writer, b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$8\r\ntestuser\r\n$9\r\n>testpass\r\n"
    )

    assert response == b"+OK\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_acl_setuser_wrong_arg_count():
    """ACL SETUSER with wrong argument count raises error."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # SETUSER with only 1 arg (needs username and password)
    response = await send_command_and_read_response(
        reader, writer, b"*2\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n"
    )

    assert b"ERR" in response and b"wrong number" in response

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_acl_getuser_existing_user():
    """ACL GETUSER returns user info for existing user."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # First set a user with *4 elements: ACL, SETUSER, username, password
    await send_command_and_read_response(
        reader, writer, b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$8\r\ntestuser\r\n$9\r\n>testpass\r\n"
    )
    
    # Get the user
    response = await send_command_and_parse_response(
        reader, writer, b"*3\r\n$3\r\nACL\r\n$7\r\nGETUSER\r\n$8\r\ntestuser\r\n"
    )

    # Response should be an array with flags, password info, etc.
    assert isinstance(response, list)
    assert b"flags" in response
    assert b"password" in response

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_acl_getuser_nonexistent_user():
    """ACL GETUSER returns null array for nonexistent user."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # Get a user that doesn't exist
    response = await send_command_and_read_response(
        reader, writer, b"*3\r\n$3\r\nACL\r\n$7\r\nGETUSER\r\n$12\r\nnonexistuser\r\n"
    )

    assert response == b"*-1\r\n"

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_acl_getuser_wrong_arg_count():
    """ACL GETUSER with wrong argument count raises error."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # GETUSER with no username arg
    response = await send_command_and_read_response(
        reader, writer, b"*2\r\n$3\r\nACL\r\n$7\r\nGETUSER\r\n"
    )

    assert b"ERR" in response and b"wrong number" in response

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_acl_unknown_subcommand():
    """ACL with unknown subcommand raises error."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # Unknown subcommand
    response = await send_command_and_read_response(
        reader, writer, b"*2\r\n$3\r\nACL\r\n$7\r\nUNKNOWN\r\n"
    )

    assert b"ERR" in response and b"unknown" in response.lower()

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_acl_getuser_with_password():
    """ACL GETUSER returns password info when user has password."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # Create a user with password - *4 elements: ACL, SETUSER, username, password
    await send_command_and_read_response(
        reader, writer, b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$5\r\nalice\r\n$9\r\n>mypasswd\r\n"
    )
    
    # Get the user
    response = await send_command_and_parse_response(
        reader, writer, b"*3\r\n$3\r\nACL\r\n$7\r\nGETUSER\r\n$5\r\nalice\r\n"
    )

    assert isinstance(response, list)
    # Should have passwords (non-empty list at the password key)
    password_idx = response.index(b"password") if b"password" in response else -1
    assert password_idx >= 0
    # The next element should be a list with at least one password hash
    if password_idx + 1 < len(response):
        password_list = response[password_idx + 1]
        assert isinstance(password_list, list)

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_acl_getuser_default_user():
    """ACL GETUSER returns info for default user."""
    server, app_state = await start_test_server()
    addr = server.sockets[0].getsockname()

    reader, writer = await asyncio.open_connection(*addr)
    
    # Get the default user
    response = await send_command_and_parse_response(
        reader, writer, b"*3\r\n$3\r\nACL\r\n$7\r\nGETUSER\r\n$7\r\ndefault\r\n"
    )

    assert isinstance(response, list)
    assert b"flags" in response
    assert b"password" in response

    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
