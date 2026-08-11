# Tyrion

Tyrion is a self-hosted programming language that compiles `.ty` source into
standalone native binaries. The repository includes the compiler, interpreter,
built-in modules, examples, and native extension ABI.

## Tools

- `tyrionic` compiles Tyrion programs.
- `tyrion` interprets Tyrion programs.
- [`tyrionc`](https://github.com/dennisvink/tyrionc) bootstraps the compiler.

## Install

```sh
brew tap dennisvink/tyrion
brew install tyrion
```

## Use

```sh
tyrion app.ty
tyrionic --build app.ty --out app
./app
```

## Bootstrap

Clone the language and bootstrap repositories next to each other:

```sh
git clone https://github.com/dennisvink/tyrionc.git
git clone https://github.com/dennisvink/tyrion.git

make -C tyrionc
mkdir -p tyrion/build
tyrion/compiler/build_native_packages.sh

./tyrionc/build/tyrionc \
  --exec-call ./tyrion/tyrionc.ty \
  build_native \
  ./tyrion/tyrionc.ty \
  ./tyrion/build/c1

./tyrion/build/c1 \
  --build ./tyrion/tyrionc.ty \
  --out ./tyrion/build/c2

./tyrion/build/c2 \
  --build ./tyrion/tyrionc.ty \
  --out ./tyrion/build/tyrionic
```

## License

Copyright (c) 2026 [Dennis Vink](https://drvink.com/).

Licensed under the [MIT License](LICENSE).
