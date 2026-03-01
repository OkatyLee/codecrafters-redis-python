from unittest.mock import AsyncMock

import pytest

from app.handlers import handle_client
from app.main import main


class _SocketStub:
    def getsockname(self):
        return ("127.0.0.1", 6379)


class _ServerStub:
    def __init__(self):
        self.sockets = [_SocketStub()]
        self.serve_forever = AsyncMock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


@pytest.mark.asyncio
async def test_main_starts_server_and_serves_forever(monkeypatch):
    server = _ServerStub()

    async def fake_start_server(callback, host, port):
        assert callback is handle_client
        assert host == "localhost"
        assert port == 6379
        return server

    monkeypatch.setattr("app.main.asyncio.start_server", fake_start_server)

    await main()

    server.serve_forever.assert_awaited_once()
