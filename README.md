# onion-relay

⚠️Warning: Not implemented yet⚠️

Nostr onion reley

![icon](https://github.com/user-attachments/assets/6747414b-d35f-4e1e-80ca-be8c039a4055)

## Usage

### Build

```shell
# library build (host/release)
# output : build/lib/
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build

# library build (host/debug)
# output : build/lib/
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build


# library build (nix/release)
# output : result/lib/
nix build

# library build (nix/debug)
# output : result/lib/
nix build .#debug

# sample build (debug)
# LDLIB : build/lib/
make BUILD=debug -C examples/echoback

# sample build (release)
# LDLIB : build/lib/
make BUILD=release -C examples/echoback

# musl build with example/echoback (release & x86/64 & linux only)
./shell/musl_build.sh
```

### Install

```shell
# default install dir:
# - /usr/local/lib/libwsserver.a
# - /usr/local/include/websocket.h
sudo cmake --install build
```

### Format

```shell
# Formatting source code (use clang-format)
./format.sh
```

### Test

```shell
cd tests
make test
```

### Run

```shell
# Example: echoback server
docker compose build --no-cache
docker compose up

# Static analysis (use clang-tidy)
./shell/static_analysis.sh
```

## Support

### Features

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

### Platform

- Linux : Ubuntu (22.04, 24.04)

## Dependencies

- [clang-format](https://github.com/llvm/llvm-project/tree/main/clang/tools/clang-format)
- [cmake](https://github.com/Kitware/CMake)
- [googletest](https://github.com/google/googletest)
- [just](https://github.com/casey/just)

## Author

Hakkadaikon
