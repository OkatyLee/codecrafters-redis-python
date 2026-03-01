import asyncio
from app.handlers import handle_client

async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    
    addr = server.sockets[0].getsockname()
    print(f"Server started on {addr}")
    
    async with server:
        await server.serve_forever()
                
    


if __name__ == "__main__":
    asyncio.run(main())
