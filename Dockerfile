# Use the official Go image to build the application
FROM golang:1.22.4 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download and cache dependencies
RUN go mod download

# Copy the source code into the container
COPY . .

# Build the Go application
RUN CGO_ENABLED=0 GIN_MODE=release GOOS=linux GOARCH=amd64 go build --ldflags "-w -s" ./cmd/main.go

# Use a minimal base image to run the application
FROM gcr.io/distroless/base-debian10

# Set the working directory inside the container
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main /app/

# Expose the port the app runs on
EXPOSE 8088

# Copy the configuration files
COPY --from=builder /app/config.dev.yaml .
COPY --from=builder /app/config.kafka_test.yaml .
COPY --from=builder /app/config.production.yaml .

# Set the default environment variable
ENV APP_ENV=production

# Command to run the application
CMD ["/app/main"]
