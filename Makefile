export GO111MODULE=on

LDFLAGS = -X github.com/brimdata/zync/cmd/zync/version.version=$(VERSION)
VERSION = $(shell git describe --always --dirty --tags)

fmt:
	@res=$$(go fmt ./...); \
	if [ -n "$${res}" ]; then \
		echo "go fmt failed on these files:"; echo "$${res}"; echo; \
		exit 1; \
	fi

tidy:
	@go mod tidy
	@git diff --exit-code -- go.mod go.sum

vet:
	@go vet ./...

test-unit:
	@go test -short ./...

test-system: build deps/zed
	@ZTEST_PATH='$(PWD)/dist:$(PWD)/deps:$(PATH)' go test -tags=ztests ./ztests

deps/zed: go.mod
	@GOBIN="$(CURDIR)/deps" go install \
		github.com/brimdata/zed/cmd/zed@$$(go list -f {{.Version}} -m github.com/brimdata/zed)

build:
	@mkdir -p dist
	@go build -ldflags='$(LDFLAGS)' -o dist ./cmd/...

install:
	@go install -ldflags='$(LDFLAGS)' ./cmd/...

clean:
	@rm -rf dist

localzq:
	@go mod edit -replace=github.com/brimsec/zq=../zq

.PHONY: vet tidy test-unit test-system clean build localzq
