ENV_LOCAL_TEST=\
  TESTING_DATABASE_URI=postgres://matrix:s3cr3t@localhost:5431/matrix?sslmode=disable \
  POSTGRES_PASSWORD=s3cr3t \
  POSTGRES_DB=matrix \
  POSTGRES_HOST=localhost \
  POSTGRES_USER=matrix \
  TESTING_CACHE_URI=redis://matrix:s3cr3t@localhost:6378 \
  TESTING_QUEUE_URI=nats://matrix:s3cr3t@localhost:4221

SERVICE		?= $(shell basename `go list`)
VERSION		?= $(shell git describe --tags --always --dirty --match=v* 2> /dev/null || cat $(PWD)/.version 2> /dev/null || echo v0)
PACKAGE		?= $(shell go list)
PACKAGES	?= $(shell go list ./...)
FILES		?= $(shell find . -type f -name '*.go' -not -path "./vendor/*")



default: generate-grpc format build

help:   ## show this help
	@echo 'usage: make [target] ...'
	@echo ''
	@echo 'targets:'
	@egrep '^(.+)\:\ .*##\ (.+)' ${MAKEFILE_LIST} | sed 's/:.*##/#/' | column -t -c 2 -s '#'

format:
	find . -name '*.go' -not -path './.git/*' -exec sed -i '/^import (/,/^)/{/^$$/d}' {} +
	find . -name '*.go' -not -path './.git/*' -exec goimports -w {} +
	golangci-lint run --fix

clean:  ## go clean
	go clean

fmt:    ## format the go source files
	go fmt ./...

vet:    ## run go vet on the source files
	go vet ./...

doc:    ## generate godocs and start a local documentation webserver on port 8085
	godoc -http=:8085 -index

.PHONY: build generate-grpc

generate-grpc:
	@echo "Generating gRPC code..."
	@cd apis && ./generate.sh
	@echo "gRPC code generation completed"

# this command will start docker components that we set in docker-compose.yml
docker-setup: ## sets up docker container images
	docker compose up -d --remove-orphans --force-recreate

pg_wait:
	@count=0; \
	until  nc -z localhost 5431; do \
	  if [ $$count -gt 30 ]; then echo "can't wait forever for pg"; exit 1; fi; \
	    sleep 1; echo "waiting for postgresql" $$count; count=$$(($$count+1)); done; \
	    sleep 5;

# shutting down docker components
docker-stop: ## stops all docker containers
	docker compose down


# this command will run all tests in the repo
# INTEGRATION_TEST_SUITE_PATH is used to run specific tests in Golang,
# if it's not specified it will run all tests
tests: ## runs all system tests
	$(ENV_LOCAL_TEST) \
	go test ./... -v -run=$(INTEGRATION_TEST_SUITE_PATH)  -coverprofile=coverage.out;\
	RETURNCODE=$$?;\
	if [ "$$RETURNCODE" -ne 0 ]; then\
		echo "unit tests failed with error code: $$RETURNCODE" >&2;\
		exit 1;\
	fi;\
	go tool cover -html=coverage.out -o coverage.html

build: clean fmt vet docker-setup pg_wait tests docker-stop ## run all preliminary steps and tests the setup
