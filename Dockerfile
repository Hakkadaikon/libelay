FROM debian:bookworm-slim AS build-dev
WORKDIR /opt/libelay
COPY . /opt/libelay
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
WORKDIR /opt/libelay

COPY --from=build-dev /opt/libelay/build/libelay /opt/libelay/bin/

ENTRYPOINT ["/opt/libelay/bin/libelay"]
