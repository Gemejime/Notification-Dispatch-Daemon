.PHONY: all build fmt vet test bench docker-build helm-lint test-performance soak-test clean openapi-check

GO ?= go
GOMEMLIMIT ?= 1800MiB
GOGC ?= 80
BINARY_NAME ?= notification-daemon
IMAGE_TAG ?= rag-notification-dispatch-daemon:latest

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

test-performance:
	GOMEMLIMIT=$(GOMEMLIMIT) GOGC=$(GOGC) $(GO) test -v -timeout=10m ./test/performance/...

soak-test:
	GOMEMLIMIT=$(GOMEMLIMIT) GOGC=$(GOGC) $(GO) test -v -run=TestSyntheticInMemoryHighDensitySoak -timeout=15m ./test/performance/...

docker-build:
	docker build -f deployments/docker/Dockerfile -t $(IMAGE_TAG) .

helm-lint:
	@if command -v helm >/dev/null 2>&1; then \
		helm lint deployments/helm/rag-notification-dispatch-daemon; \
	else \
		echo "helm binary not in PATH, cannot lint helm chart" >&2; \
		exit 1; \
	fi

openapi-check:
	@test -f api/openapi/openapi.yaml
	@test -f .ai/doc-api.md

clean:
	rm -rf bin/ coverage.txt coverage.html


