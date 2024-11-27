#syntax=docker/dockerfile:1.12

#
# base installs required dependencies and runs go mod download to cache dependencies
#
FROM --platform=${BUILDPLATFORM} docker.io/golang:1.23 AS base
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    build-essential \
    curl \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

#
# build creates all needed binaries
#
FROM --platform=${BUILDPLATFORM} base AS build
WORKDIR /src
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    GOOS=linux CGO_ENABLED=1 \
    go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -v -trimpath -o /tmp/matrix/ ./cmd/...


#
# Builds the Matrix image containing all required binaries
#
FROM gcr.io/distroless/static:nonroot

USER 65532:65532
EXPOSE 8008
EXPOSE 8448

LABEL org.opencontainers.image.title="Matrix"
LABEL org.opencontainers.image.description="Matrix messaging server written in Golang"
LABEL org.opencontainers.image.source="https://github.com/antinvestor/matrix"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.vendor="Ant Investor Ltd"

COPY --from=build /tmp/matrix/create-account /usr/bin/create-account
COPY --from=build /tmp/matrix/generate-config /usr/bin/generate-config
COPY --from=build /tmp/matrix/generate-keys /usr/bin/generate-keys
COPY --from=build /tmp/matrix/matrix /usr/bin/matrix

VOLUME /etc/matrix
WORKDIR /etc/matrix

#HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
#    CMD [ "/usr/bin/matrix", "--healthcheck" ]

ENTRYPOINT ["/usr/bin/matrix"]
