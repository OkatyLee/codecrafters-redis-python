import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.accept()
    while True:
        client_socket, _ = server_socket.accept()
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            client_socket.sendall(b"+PONG\r\n")
        client_socket.close()
    


if __name__ == "__main__":
    main()
