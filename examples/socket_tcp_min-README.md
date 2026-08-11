# TCP Client

[`socket_tcp_min.ty`](socket_tcp_min.ty) opens a TCP connection to
`example.com`, writes a minimal HTTP request, reads one response chunk, and
closes the socket.

The executable requires outbound network access:

```sh
./build/tyrionic --build ./examples/socket_tcp_min.ty --out ./build/socket_tcp_min
./build/socket_tcp_min
```

