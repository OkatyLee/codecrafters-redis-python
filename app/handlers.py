import asyncio
from app.parser import RESPParser
from app.storage import get_storage

def handle_set_command(key: str, value: str, ttl=None) -> bool:
    try:
        storage = get_storage()
        storage.set(key, value, ttl)
        return True
    except Exception as e:
        print("Unexpected exception occured:", e)
        return False
    
def handle_get_command(key: str) -> str:
    storage = get_storage()
    return storage.get(key)

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info('peername')
    print(f'Connected to client at {addr}')
    parser = RESPParser(reader)
    try:
        while True:
            data = await parser.parse()
            if data is None:
                break
            print(data)
            response = b''
            command = data[0].upper()
            match command:
                case b"PING":
                    response = "+PONG\r\n".encode()
                case b"ECHO":
                    payload = data[1]
                    response = b"$" + str(len(payload)).encode() + b"\r\n" + payload + b"\r\n"
                case b'SET':
                    ttl = None
                    if len(data) > 3 and data[3].upper() == b"EX":
                        ttl = int(data[4])
                    elif len(data) > 3 and data[3].upper() == b"PX":
                        ttl = int(data[4]) / 1000.0
                    flag = handle_set_command(data[1], data[2], ttl)
                    response = b"+OK\r\n" if flag else b"-ERR\r\n"
                case b'GET':
                    value = handle_get_command(data[1])
                    if value is not None:
                        payload = value if isinstance(value, bytes) else str(value).encode()
                        response = b"$" + str(len(payload)).encode() + b"\r\n" + payload + b"\r\n"
                    else:
                        response = b"$-1\r\n"
                case _:
                    response = b"-ERR unknown command\r\n"
            writer.write(response)
            await writer.drain()
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        writer.close()
        await writer.wait_closed()