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
COPY --from=busybox /usr/bin/curl /usr/bin/curl
COPY --from=busybox /lib/x86_64-linux-gnu/libc.so.6 /lib/x86_64-linux-gnu/
COPY --from=busybox /lib/x86_64-linux-gnu/libssl.so.1.1 /lib/x86_64-linux-gnu/
COPY --from=busybox /lib/x86_64-linux-gnu/libcrypto.so.1.1 /lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/libnghttp2.so.14 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/libidn2.so.0 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/librtmp.so.1 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/libssh2.so.1 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/libpsl.so.5 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/libunistring.so.2 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/libgnutls.so.30 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/libhogweed.so.4 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/libnettle.so.6 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/libgmp.so.10 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/libgcrypt.so.20 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /usr/lib/x86_64-linux-gnu/libgpg-error.so.0 /usr/lib/x86_64-linux-gnu/
COPY --from=busybox /lib/x86_64-linux-gnu/libz.so.1 /lib/x86_64-linux-gnu/

# Expose the port the app runs on
EXPOSE 8088

# Set the default environment variable
ENV APP_PATH=./config/config.yaml

# Command to run the application
CMD ["/app/main"]
