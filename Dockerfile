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

# Build the Go app for multiple architectures
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GIN_MODE=release GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build --ldflags "-w -s" ./cmd/main.go

FROM debian:10 AS busybox

# Install busybox
RUN apt-get update && apt-get install -y busybox --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Use a minimal base image to run the application
FROM gcr.io/distroless/base-debian10

# Set the working directory inside the container
WORKDIR /app

# Copy the Go application from the builder stage
COPY --from=builder /app/main /app/

# Copy busybox from the busybox stage
COPY --from=busybox /bin/busybox /bin/sh
COPY --from=busybox /bin/busybox /bin/date

# Expose the port the app runs on
EXPOSE 8088

# Set the default environment variable
ENV APP_PATH=./config/config.yaml

# Command to run the application
CMD ["/app/main"]
