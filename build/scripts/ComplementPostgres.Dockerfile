FROM docker.io/golang:1.23 AS build

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash curl git gcc libc6-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set up working directory
RUN mkdir /matrix
WORKDIR /matrix

# Copy source code
COPY . .

# Build binaries
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=${CGO} go build -v -trimpath -o /matrix ./cmd/generate-config && \
    CGO_ENABLED=${CGO} go build -v -trimpath -o /matrix ./cmd/generate-keys && \
    CGO_ENABLED=${CGO} go build -v -trimpath -o /matrix ./cmd/matrix && \
    CGO_ENABLED=${CGO} go build -v -trimpath -cover -covermode=atomic -o /matrix/matrix-cover \
    -coverpkg "github.com/antinvestor/..." ./cmd/matrix

# Copy scripts
RUN cp build/scripts/complement-cmd.sh /matrix/complement-cmd.sh

# Generate keys
RUN /matrix/generate-keys --private-key /matrix/matrix_key.pem

# Set environment variables
ENV SERVER_NAME=localhost
ENV API=0
ENV COVER=0

ENV QUEUE_URI="nats://matrix:s3cr3t@queuestore:4221"
ENV CACHE_URI="redis://matrix:s3cr3t@cachestore:6378"
ENV DATABASE_URI="postgres://matrix:s3cr3t@datastore:5431/matrix?sslmode=disable"

# Expose ports
EXPOSE 8008 8448

# Run the application
CMD ["sh", "-c", "/matrix/generate-keys --keysize 1024 --server $SERVER_NAME \
    --tls-cert /matrix/server.crt --tls-key /matrix/server.key --tls-authority-cert \
    /complement/ca/ca.crt --tls-authority-key /complement/ca/ca.key && \
    /matrix/generate-config -server $SERVER_NAME --ci --cache_uri $CACHE_URI \
    --database_uri $DATABASE_URI --queue_uri $QUEUE_URI > /matrix/matrix.yaml && \
    sed -i 's/max_open_conns:.*$/max_open_conns: 1990/g' /matrix/matrix.yaml && \
    cp /complement/ca/ca.crt /usr/local/share/ca-certificates/ && \
    update-ca-certificates && exec /matrix/complement-cmd.sh"]