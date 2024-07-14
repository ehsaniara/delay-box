FROM ghcr.io/ehsaniara/delay-box:latest AS builder
# Install curl
FROM debian:10 AS curl-installer

RUN apt-get update && apt-get install -y curl --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Create the final image for amd64 architecture
FROM scratch AS amd64
WORKDIR /app
COPY --from=builder /app/main /app/
COPY --from=curl-installer /usr/bin/curl /usr/bin/curl
COPY --from=curl-installer /lib/x86_64-linux-gnu /lib/
COPY --from=curl-installer /usr/lib/x86_64-linux-gnu /usr/lib/

# Expose the port the app runs on
EXPOSE 8088

# Set the default environment variable
ENV APP_PATH=./config/config.yaml

# Command to run the application
CMD ["/app/main"]

# Create the final image for arm64 architecture
FROM scratch AS arm64
WORKDIR /app
COPY --from=builder /app/main /app/
COPY --from=curl-installer /usr/bin/curl /usr/bin/curl
COPY --from=curl-installer /lib/aarch64-linux-gnu /lib/
COPY --from=curl-installer /usr/lib/aarch64-linux-gnu /usr/lib/

# Expose the port the app runs on
EXPOSE 8088

# Set the default environment variable
ENV APP_PATH=./config/config.yaml

# Command to run the application
CMD ["/app/main"]

# Create the final multi-arch image
FROM ${TARGETARCH}
