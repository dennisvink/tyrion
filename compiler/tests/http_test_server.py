#!/usr/bin/env python3
import argparse
import socket
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port-file", required=True)
    parser.add_argument("--requests", type=int, default=1)
    args = parser.parse_args()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        Path(args.port_file).write_text(str(port), encoding="ascii")

        for _ in range(args.requests):
            connection, _ = server.accept()
            with connection:
                request = b""
                while b"\r\n\r\n" not in request:
                    chunk = connection.recv(4096)
                    if not chunk:
                        break
                    request += chunk
                if not request.startswith(b"GET /winter HTTP/1.1\r\n"):
                    raise RuntimeError(f"unexpected request: {request!r}")
                if b"Host: 127.0.0.1:" not in request:
                    raise RuntimeError(f"missing Host header: {request!r}")
                body = b"The North remembers"
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
                    b"X-Tyrion: capable\r\n"
                    b"Connection: close\r\n\r\n" + body
                )
                connection.sendall(response)


if __name__ == "__main__":
    main()
