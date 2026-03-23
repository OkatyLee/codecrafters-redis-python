import asyncio

import pytest  # pyright: ignore[reportMissingImports]

from app.parser import RESPError, RESPParser


def make_reader(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


@pytest.mark.asyncio
async def test_parser_array():
    reader = make_reader(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
    parser = RESPParser(reader)
    result = await parser.parse()

    assert result == [b"ECHO", b"hello"]


@pytest.mark.asyncio
async def test_parser_integer():
    parser = RESPParser(make_reader(b":123\r\n"))

    assert await parser.parse() == 123


@pytest.mark.asyncio
async def test_parser_simple_string():
    parser = RESPParser(make_reader(b"+OK\r\n"))

    assert await parser.parse() == "OK"


@pytest.mark.asyncio
async def test_parser_null_bulk_string():
    parser = RESPParser(make_reader(b"$-1\r\n"))

    assert await parser.parse() is None


@pytest.mark.asyncio
async def test_parser_error_reply_raises_resp_error():
    parser = RESPParser(make_reader(b"-ERR wrong type\r\n"))

    with pytest.raises(RESPError, match="ERR wrong type"):
        await parser.parse()


@pytest.mark.asyncio
async def test_parser_invalid_prefix_raises_value_error():
    parser = RESPParser(make_reader(b"?hello\r\n"))

    with pytest.raises(ValueError, match="Unknown RESP prefix"):
        await parser.parse()


@pytest.mark.asyncio
async def test_parser_missing_crlf_raises_connection_error():
    parser = RESPParser(make_reader(b"+OK\n"))

    with pytest.raises(ConnectionError, match="Incomplete RESP packet"):
        await parser.parse()


@pytest.mark.asyncio
async def test_parser_incomplete_bulk_string_raises_connection_error():
    reader = asyncio.StreamReader()
    reader.feed_data(b"$5\r\nabc")
    reader.feed_eof()
    parser = RESPParser(reader)

    with pytest.raises(ConnectionError, match="Connection closed mid-bulk-string"):
        await parser.parse()
