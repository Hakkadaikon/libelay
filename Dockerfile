FROM debian:bookworm-slim AS build-dev
WORKDIR /opt/relay
COPY . /opt/relay
RUN \
  apt update && \
  apt install -y \
  cmake \
  make \
  just
RUN \
  just release-build

FROM scratch AS runtime
WORKDIR /opt/websocket

COPY --from=build-dev /opt/relay/build/relay /opt/relay/bin/

ENTRYPOINT ["/opt/relay/bin/relay"]
