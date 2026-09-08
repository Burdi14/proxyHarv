# proxyfarm multi-stage build → distroless/static non-root (§9.5, T-019).
#
# Pins (§15 — do not bump without a task):
#   sing-box 1.13.21  (exec'd as an external binary, GPL — never linked)
#   static curl 8.22.0 (stunnel/static-curl, glibc static)
#
# build:  docker build -f docker/Dockerfile --build-arg TARGETBIN=worker -t proxyfarm/worker .

FROM golang:1.23.12-alpine AS build
ARG TARGETBIN=worker
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# build all binaries once in one layer: shared module+stdlib cache
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags "-s -w" \
      -o /out/fetcher ./cmd/fetcher && \
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags "-s -w" \
      -o /out/coordinator ./cmd/coordinator && \
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags "-s -w" \
      -o /out/api ./cmd/api && \
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags "-s -w" \
      -o /out/worker ./cmd/worker

FROM alpine:3.20 AS tools
ARG TARGETARCH
ARG SINGBOX_VERSION=1.13.21
RUN apk add --no-cache curl xz tar
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) SB_SHA=24f9ef8e7234e13e71e74c3598a4164c5fe07b7b67ccc6e96cf68b54789f72cd; \
             CURL_SHA=d74460e43c0e6eaf40ec1fdda92c53dee94da8a6e6db066711541ada5aab0fa6; \
             SB_ARCH=amd64; CURL_ARCH=x86_64 ;; \
      arm64) SB_SHA=3e30b876c9a93c19e503e2a2d6249cf05e6a26766553d4b61e1daf48223f304f; \
             CURL_SHA=fa4de50f80fb2fbbf77a7bf8385891b6cb1a501a2e28b1ed18cf25e11fd66b66; \
             SB_ARCH=arm64; CURL_ARCH=aarch64 ;; \
      *) echo "unsupported arch $TARGETARCH"; exit 1 ;; \
    esac; \
    curl -sfL -o /tmp/singbox.tgz \
      "https://github.com/SagerNet/sing-box/releases/download/v${SINGBOX_VERSION}/sing-box-${SINGBOX_VERSION}-linux-${SB_ARCH}.tar.gz"; \
    echo "${SB_SHA}  /tmp/singbox.tgz" | sha256sum -c -; \
    tar -xzf /tmp/singbox.tgz -C /tmp; \
    mv /tmp/sing-box-${SINGBOX_VERSION}-linux-${SB_ARCH}/sing-box /out-sing-box; \
    curl -sfL -o /tmp/curl.tar.xz \
      "https://github.com/stunnel/static-curl/releases/download/8.22.0/curl-linux-${CURL_ARCH}-glibc-8.22.0.tar.xz"; \
    echo "${CURL_SHA}  /tmp/curl.tar.xz" | sha256sum -c -; \
    mkdir -p /tmp/curlx && tar -xJf /tmp/curl.tar.xz -C /tmp/curlx; \
    find /tmp/curlx -name curl -type f -exec cp {} /out-curl \; ; \
    chmod 0755 /out-sing-box /out-curl; \
    /out-curl --version | head -1; /out-sing-box version | head -1

FROM gcr.io/distroless/static-debian12:nonroot
ARG TARGETBIN=worker
LABEL org.opencontainers.image.title="proxyfarm/${TARGETBIN}" \
      org.opencontainers.image.source="https://github.com/Burdi14/proxyfarm"
COPY --from=build --chown=root:root --chmod=0755 /out/${TARGETBIN} /proxyfarm
COPY --from=tools --chown=root:root --chmod=0755 /out-sing-box /usr/local/bin/sing-box
COPY --from=tools --chown=root:root --chmod=0755 /out-curl /usr/local/bin/curl
COPY web /web
USER nonroot:nonroot
ENTRYPOINT ["/proxyfarm"]
