#syntax=docker/dockerfile:1.13

FROM golang:1.23-bookworm AS build

# we will dump the binaries and config file to this location to ensure any local untracked files
# that come from the COPY . . file don't contaminate the build
RUN mkdir /matrix

# Utilise Docker caching when downloading dependencies, this stops us needlessly
# downloading dependencies every time.
ARG CGO
RUN --mount=target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=${CGO} go build -o /matrix ./cmd/generate-config && \
    CGO_ENABLED=${CGO} go build -o /matrix ./cmd/generate-keys && \
    CGO_ENABLED=${CGO} go build -o /matrix/matrix ./cmd/matrix && \
    CGO_ENABLED=${CGO} go build -cover -covermode=atomic -o /matrix/matrix-cover -coverpkg "github.com/antinvestor/..." ./cmd/matrix && \
    cp build/scripts/complement-cmd.sh /complement-cmd.sh

WORKDIR /matrix
RUN ./generate-keys --private-key matrix_key.pem

ENV SERVER_NAME=localhost
ENV API=0
ENV COVER=0
EXPOSE 8008 8448


# At runtime, generate TLS cert based on the CA now mounted at /ca
# At runtime, replace the SERVER_NAME with what we are told
CMD ./generate-keys --keysize 1024 --server $SERVER_NAME \
    --tls-cert server.crt --tls-key server.key --tls-authority-cert \
    /complement/ca/ca.crt --tls-authority-key /complement/ca/ca.key && \
    ./generate-config -server $SERVER_NAME --ci --cache_uri "redis://matrix:s3cr3t@localhost:6379/0" \
    --database_uri "postgres://matrix:s3cr3t@localhost:5432/matrix?sslmode=disable" > matrix.yaml && \
    # Bump max_open_conns up here in the global database config
    sed -i 's/max_open_conns:.*$/max_open_conns: 1990/g' matrix.yaml && \
    cp /complement/ca/ca.crt /usr/local/share/ca-certificates/ && \
    update-ca-certificates && exec /complement-cmd.sh