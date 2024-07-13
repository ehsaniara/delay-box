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

FROM debian:10 AS builder

# Install bash
RUN apt-get update && apt-get install -y bash --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Use a minimal base image to run the application
FROM gcr.io/distroless/base-debian10

# Copy bash from the builder image to the distroless image
COPY --from=builder /bin/bash /bin/bash
COPY --from=builder /lib/x86_64-linux-gnu/libtinfo.so.6 /lib/x86_64-linux-gnu/libtinfo.so.6
COPY --from=builder /lib/x86_64-linux-gnu/libdl.so.2 /lib/x86_64-linux-gnu/libdl.so.2
COPY --from=builder /lib/x86_64-linux-gnu/libc.so.6 /lib/x86_64-linux-gnu/libc.so.6
COPY --from=builder /lib64/ld-linux-x86-64.so.2 /lib64/ld-linux-x86-64.so.2

# Set the working directory inside the container
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main /app/

# Expose the port the app runs on
EXPOSE 8088

# Set the default environment variable
ENV APP_PATH=./config/config.yaml

# Command to run the application
CMD ["/app/main"]
