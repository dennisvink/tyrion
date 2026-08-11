# Terminal Chess

[`terminal_chess.ty`](terminal_chess.ty) is an interactive terminal chess
program. Board state, rendering, legal move validation, check detection, turns,
promotion, and terminal event handling are implemented in Tyrion.

Compile and start it from an interactive terminal:

```sh
./build/tyrionic --build ./examples/terminal_chess.ty --out ./build/terminal_chess
./build/terminal_chess
```

Use the arrow keys to move the cursor, Enter to select or move a piece, and
`q` to quit.
