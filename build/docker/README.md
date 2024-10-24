# Docker images

These are Docker images for Dendrite!

They can be found on Docker Hub:

- [antinvestor/matrix](https://hub.docker.com/r/antinvestor/matrix) for monolith deployments

## Dockerfile

The `Dockerfile` is a multistage file which can build Dendrite. From the root of the Dendrite
repository, run:

```
docker build . -t antinvestor/matrix
```

## Compose file

There is one sample `docker-compose` files:

- `docker-compose.yml` which runs a Dendrite deployment with Postgres

## Configuration

The `docker-compose` files refer to the `/etc/matrix` volume as where the
runtime config should come from. The mounted folder must contain:

- `matrix.yaml` configuration file (based on one of the sample config files)
- `matrix_key.pem` server key, as generated using `cmd/generate-keys`
- `server.crt` certificate file
- `server.key` private key file for the above certificate

To generate keys:

```
docker run --rm --entrypoint="" \
  -v $(pwd)/runtime/config:/etc/matrix \
  antinvestor/matrix:latest \
  /usr/bin/generate-keys \
  -private-key /etc/matrix/matrix_key.pem \
  -tls-cert /etc/matrix/server.crt \
  -tls-key /etc/matrix/server.key
```

The key files will now exist in your current working directory, and can be mounted into place.

## Starting Dendrite

Create your config based on the [`dendrite-sample.yaml`](https://github.com/antinvestor/matrix/blob/main/dendrite-sample.yaml) sample configuration file.

Then start the deployment:

```
docker-compose -f docker-compose.yml up
```

## Building the images

The `build/docker/images-build.sh` script will build the base image, followed by
all of the component images.

The `build/docker/images-push.sh` script will push them to Docker Hub (subject
to permissions).

If you wish to build and push your own images, rename `antinvestor/matrix` to
the name of another Docker Hub repository in `images-build.sh` and `images-push.sh`.
