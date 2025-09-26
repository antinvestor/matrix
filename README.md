# Matrix

[![Build status](https://github.com/antinvestor/matrix/actions/workflows/matrix.yml/badge.svg?event=push)](https://github.com/antinvestor/matrix/actions/workflows/matrix.yml)

Matrix server written in Golang.

It intends to provide an **efficient**, **reliable** and **scalable** matrix server service

- Efficient: A small memory footprint with good baseline performance
- Reliable: Implements the Matrix specification as written, using the
  [same test suite](https://github.com/matrix-org/sytest) as Synapse as well as
  a [brand new Go test suite](https://github.com/matrix-org/complement).
- Scalable: can run on multiple machines and eventually scale to massive server deployments.

If you have further questions, please take a look at [our FAQ](docs/FAQ.md) or join us in:

This project is a fork of Dendrite : https://github.com/matrix-org/dendrite

## Requirements

See the [Planning your Installation](https://matrix-org.github.io/dendrite/installation/planning) page for
more information on requirements.

To build Matrix, you will need Go 1.24 or later.

For a usable federating Matrix deployment, you will also need:

- A domain name (or subdomain)
- A valid TLS certificate issued by a trusted authority for that domain
- SRV records or a well-known file pointing to your deployment

The [Federation Tester](https://federationtester.matrix.org) can be used to verify your deployment.

## Get started

If you wish to build a fully-federating matrix instance,
see [the Installation documentation](https://matrix-org.github.io/dendrite/installation).
For running in Docker, see [build/docker](build/docker).

The following instructions are enough to get matrix started as a non-federating test deployment using self-signed
certificates and SQLite databases:

```bash
$ git clone https://github.com/antinvestor/matrix
$ cd dendrite
$ go build -o bin/ ./cmd/...

# Generate a Matrix signing key for federation (required)
$ ./bin/generate-keys --private-key matrix_key.pem

# Generate a self-signed certificate (optional, but a valid TLS certificate is normally
# needed for Matrix federation/clients to work properly!)
$ ./bin/generate-keys --tls-cert server.crt --tls-key server.key

# Copy and modify the config file - you'll need to set a server name and paths to the keys
# at the very least, along with setting up the database connection strings.
$ cp dendrite-sample.yaml matrix.yaml

# Build and run the server:
$ ./bin/dendrite --tls-cert server.crt --tls-key server.key --config matrix.yaml

# Create an user account (add -admin for an admin user).
# Specify the localpart only, e.g. 'alice' for '@alice:domain.com'
$ ./bin/create-account --config matrix.yaml --username alice
```

Then point your favourite Matrix client at `http://localhost:8008` or `https://localhost:8448`.

This means Matrix support amongst others:

- Core room functionality (creating rooms, invites, auth rules)
- Room versions 1 to 12 supported
- Backfilling locally and via federation
- Accounts, profiles and devices
- Published room lists
- Typing
- Media APIs
- Redaction
- Tagging
- Context
- E2E keys and device lists
- Receipts
- Push
- Guests
- User Directory
- Presence
- Fulltext search

## Contributing

We would be grateful for any help on any issues you can contribute to.

We are prioritising features that will benefit massive deployments rather than homeservers.
More effort is required on features including :
  - OpenID, 
  - Guests 
  - Admin APIs
  - AS API 

If you're new to the project, see our
[Contributing page](https://matrix-org.github.io/dendrite/development/contributing) to get up to speed, then
look for [Good First Issues](https://github.com/antinvestor/matrix/labels/good%20first%20issue). If you're
familiar with the project, look for [Help Wanted](https://github.com/antinvestor/matrix/labels/help-wanted)
issues.
