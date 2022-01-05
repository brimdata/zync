export GO111MODULE=on

LDFLAGS = -X github.com/brimdata/zync/cmd/zync/version.version=$(VERSION)
VERSION = $(shell git describe --always --dirty --tags)

vet:
	@go vet -copylocks ./...

fmt:
	@res=$$(go fmt ./...); \
	if [ -n "$${res}" ]; then \
		echo "go fmt failed on these files:"; echo "$${res}"; echo; \
		exit 1; \
	fi

test-unit:
	@go test -short ./...

test-system: build
	@go test -v -tags=system ./tests -args PATH=$(shell pwd)/dist

build:
	@mkdir -p dist
	@go build -ldflags='$(LDFLAGS)' -o dist ./cmd/...

install:
	@go install -ldflags='$(LDFLAGS)' ./cmd/...

clean:
	@rm -rf dist

localzq:
	@go mod edit -replace=github.com/brimsec/zq=../zq

.PHONY: vet test-unit test-system clean build localzq
