#
# base installs required dependencies and runs go mod download to cache dependencies
#
FROM --platform=${BUILDPLATFORM} docker.io/golang:1.24 AS base
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
    GOOS=linux CGO_ENABLED=0 \
    go build -v -trimpath -o /out/ ./cmd/...


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

COPY --from=build /out/create-account /usr/bin/create-account
COPY --from=build /out/generate-config /usr/bin/generate-config
COPY --from=build /out/generate-keys /usr/bin/generate-keys
COPY --from=build /out/matrix /usr/bin/matrix

VOLUME /etc/matrix
WORKDIR /etc/matrix

ENTRYPOINT ["/usr/bin/matrix"]
