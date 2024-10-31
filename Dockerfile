#syntax=docker/dockerfile:1.11

#
# base installs required dependencies and runs go mod download to cache dependencies
#
FROM --platform=${BUILDPLATFORM} docker.io/golang:1.23-alpine AS base
RUN apk --update --no-cache add bash build-base curl git

#
# build creates all needed binaries
#
FROM --platform=${BUILDPLATFORM} base AS build
WORKDIR /src
ARG TARGETOS
ARG TARGETARCH
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    USERARCH=`go env GOARCH` \
    GOARCH="$TARGETARCH" \
    GOOS="linux" \
    CGO_ENABLED=$([ "$TARGETARCH" = "$USERARCH" ] && echo "1" || echo "0") \
    go build -v -trimpath -o /out/ ./cmd/...


#
# Builds the Dendrite image containing all required binaries
#
FROM alpine:latest
RUN apk --update --no-cache add curl
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
EXPOSE 8008 8448

