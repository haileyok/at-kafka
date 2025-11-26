# Build stage
FROM golang:1.25.4-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o /build/bin/atkafka ./cmd/atkafka

# Runtime stage
FROM alpine:latest

# Install ca-certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Create non-root user
RUN addgroup -g 1000 atkafka && \
    adduser -D -u 1000 -G atkafka atkafka

# Copy binary from builder
COPY --from=builder /build/bin/atkafka /app/atkafka

# Change ownership
RUN chown -R atkafka:atkafka /app

# Switch to non-root user
USER atkafka

# Expose metrics port (default from bluesky-social/go-util)
EXPOSE 2112

ENTRYPOINT ["/app/atkafka"]
