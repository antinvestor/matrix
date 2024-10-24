#!/usr/bin/env bash

TAG=${1:-latest}

echo "Pushing tag '${TAG}'"

docker push antinvestor/matrix:${TAG}
