# Libelay

⚠️Warning: Not implemented yet⚠️

_"liberty" / "liberation" + "nostr relay"_
[lih-beh-lay]

_When it comes to Nostr, it implies freedom._

_No one can interfere with this freedom._

_Yes... even "libc"._

<img width="400" alt="Image" src="https://github.com/user-attachments/assets/00f6766e-7240-437b-bd59-e01ff868d01d" />

A high-performance, single-binary Nostr relay written in C.

## Build

```shell
git clone git@github.com:Hakkadaikon/libelay.git
cd libelay

# debug build
just debug-build

# release build
just release-build

./build/libelay
```

## Support

### Features

#### WebSocket

- opcode
    - 0x0 (continuation)   : No
    - 0x1 (text)           : Yes (Interpret with user callbacks)
    - 0x2 (binary)         : Yes (Interpret with user callbacks)
    - 0x8 (close)          : Yes
    - 0x9 (ping)           : No
    - 0xA (pong)           : Yes (When a ping is received, a pong is sent back.)
- TLS Support            : No
- Sub protocol           : No (Sec-WebSocket-Protocol)
- Extensions             : No (Sec-WebSocket-Extensions)
- Compression / Decode   : No

#### NIPS

- [] NIP-01
- ...

### Platform

- Linux : Ubuntu (24.04) x86_64 only

## Dependencies (develop)

- [clang-format](https://github.com/llvm/llvm-project/tree/main/clang/tools/clang-format)
- [cmake](https://github.com/Kitware/CMake)
- [googletest](https://github.com/google/googletest)
- [just](https://github.com/casey/just)
- [mado](git@github.com:akiomik/mado.git)

## Author

Hakkadaikon
