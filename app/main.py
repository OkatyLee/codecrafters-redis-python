import asyncio
from app.handlers import handle_client


async def main() -> None:
    """Start the asyncio TCP server bound to localhost:6379.

    The function prints the bound address and then awaits
    ``server.serve_forever()``. Tests patch ``asyncio.start_server`` so no
    actual network access is required during unit tests.
    """
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    server = await asyncio.start_server(handle_client, "localhost", 6379)

    addr = server.sockets[0].getsockname()
    print(f"Server started on {addr}")

    async with server:
        await server.serve_forever()



if __name__ == "__main__":
    asyncio.run(main())
