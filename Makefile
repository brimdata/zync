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

tidy:
	@go mod tidy
	@git diff --exit-code -- go.mod go.sum

test-unit:
	@go test -short ./...

test-system: build deps/zed
	@ZTEST_PATH='$(PWD)/dist:$(PWD)/deps:$(PATH)' go test -tags=ztests ./ztests

zed_version = $(shell go list -f {{.Version}} -m github.com/brimdata/zed)

.PHONY: deps/zed
deps/zed: deps/zed-$(zed_version)
	@ln -fs $(<F) $@

deps/zed-$(zed_version):
	@mkdir -p $(@D)
	@echo 'module deps' > $@.mod
	@go get -modfile=$@.mod github.com/brimdata/zed@$(zed_version)
	@go build -mod=mod -modfile=$@.mod -o $@ github.com/brimdata/zed/cmd/zed

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
