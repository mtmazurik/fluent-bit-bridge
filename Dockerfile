FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy everything and let Go figure it out
COPY . .

# Let Go handle all the module stuff
RUN go mod tidy

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o fluent-bit-bridge .

# Final stage - ultra-slim distroless
FROM gcr.io/distroless/static-debian12:latest

WORKDIR /

# Copy the binary from builder stage
COPY --from=builder /app/fluent-bit-bridge .

# Copy CA certificates for HTTPS connections
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Expose port
EXPOSE 8080

# Run the binary
CMD ["./fluent-bit-bridge"]