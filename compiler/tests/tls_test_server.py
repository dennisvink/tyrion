#!/usr/bin/env python3

import argparse
import socket
import ssl


DEFAULT_BODY = b"The encrypted North knows"


def response_for(body, chunked=False):
    if chunked:
        encoded = bytearray(
            b"HTTP/1.1 200 OK\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"X-Tyrion: encrypted\r\n"
            b"Connection: close\r\n"
            b"\r\n"
        )
        offset = 0
        while offset < len(body):
            chunk = body[offset : offset + 4096]
            encoded.extend(format(len(chunk), "x").encode("ascii"))
            encoded.extend(b"\r\n")
            encoded.extend(chunk)
            encoded.extend(b"\r\n")
            offset += len(chunk)
        encoded.extend(b"0\r\n\r\n")
        return bytes(encoded)
    return (
    b"HTTP/1.1 200 OK\r\n"
    + b"Content-Length: "
    + str(len(body)).encode("ascii")
    + b"\r\n"
    b"X-Tyrion: encrypted\r\n"
    b"Connection: close\r\n"
    b"\r\n"
    + body
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--port-file", required=True)
    parser.add_argument("--connections", type=int, required=True)
    parser.add_argument("--body-bytes", type=int, default=0)
    parser.add_argument("--chunked", action="store_true")
    args = parser.parse_args()
    body = DEFAULT_BODY
    if args.body_bytes > 0:
        body = b"A" * args.body_bytes
    response = response_for(body, args.chunked)

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(args.cert, args.key)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen()
        with open(args.port_file, "w", encoding="ascii") as output:
            output.write(str(server.getsockname()[1]))

        accepted = 0
        while accepted < args.connections:
            connection, _ = server.accept()
            accepted += 1
            try:
                with context.wrap_socket(connection, server_side=True) as secure:
                    request = bytearray()
                    while b"\r\n\r\n" not in request:
                        chunk = secure.recv(4096)
                        if not chunk:
                            break
                        request.extend(chunk)
                    secure.sendall(response)
            except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
                connection.close()


if __name__ == "__main__":
    main()
