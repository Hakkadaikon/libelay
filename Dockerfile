FROM debian:bookworm-slim AS build-dev
WORKDIR /opt/relay
COPY . /opt/relay
RUN \
  apt update && \
  apt install -y \
  build-essential \
  cmake \
  make
RUN \
  rm -rf build && \
  cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
  cmake --build build

FROM scratch AS runtime
WORKDIR /opt/websocket

COPY --from=build-dev /opt/relay/build/relay /opt/relay/bin/

ENTRYPOINT ["/opt/relay/bin/relay"]
