.PHONY: all build fmt vet test bench clean openapi-check

GO ?= go
GOMEMLIMIT ?= 1800MiB
GOGC ?= 80
BINARY_NAME ?= notification-daemon

all: fmt vet test build

build:
	GOMEMLIMIT=$(GOMEMLIMIT) GOGC=$(GOGC) $(GO) build -trimpath -o bin/$(BINARY_NAME) ./cmd/notification-daemon

fmt:
	gofmt -s -w .

vet:
	$(GO) vet ./...

test:
	GOMEMLIMIT=$(GOMEMLIMIT) GOGC=$(GOGC) $(GO) test -v -race -coverprofile=coverage.txt ./cmd/... ./internal/... ./test/unit/...

bench:
	GOMEMLIMIT=$(GOMEMLIMIT) GOGC=$(GOGC) $(GO) test -bench=. -benchmem ./test/unit/...

openapi-check:
	@test -f api/openapi/openapi.yaml
	@test -f .ai/doc-api.md

clean:
	rm -rf bin/ coverage.txt coverage.html

