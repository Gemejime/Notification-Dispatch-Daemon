# Multi-stage distroless build for rag-notification-dispatch-daemon
# Stage 1: Build binary using official Go 1.26 toolchain
FROM golang:1.26-bookworm AS builder

ARG TARGETARCH

WORKDIR /build

# Copy dependency manifests first for build caching
COPY go.mod go.sum* ./
RUN go mod download

# Copy source code tree
COPY . .

# Compile statically linked binary with stripped debug symbols and path trimming
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
    -trimpath \
    -ldflags="-s -w -X main.version=1.0.0" \
    -o /build/bin/notification-daemon \
    ./cmd/notification-daemon

# Stage 2: Minimal attack-surface runtime using static distroless
FROM gcr.io/distroless/static:nonroot

LABEL maintainer="rag-distributed-systems-team" \
      service="rag-notification-dispatch-daemon" \
      version="1.0.0" \
      description="Production-grade high-density SSE notification daemon"

WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /build/bin/notification-daemon /app/notification-daemon

# Runtime environment variables per ADRs and architectural ceiling
ENV GOMEMLIMIT=1800MiB \
    GOGC=80 \
    PORT=8080

# Expose HTTP / SSE serving and metrics port
EXPOSE 8080

# Enforce unprivileged user execution (UID:GID 65532:65532)
USER nonroot:nonroot

# Entrypoint executes compiled Go daemon directly (zero-shell runtime)
ENTRYPOINT ["/app/notification-daemon"]

