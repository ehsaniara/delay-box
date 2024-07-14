FROM ghcr.io/ehsaniara/delay-box:latest AS builder


# Install curl
FROM debian:10 AS curl-installer

RUN apt-get update && apt-get install -y curl --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Create the final image using scratch (minimal base image)
FROM scratch

# Set the working directory inside the container
WORKDIR /app

# Copy the Go application from the builder stage
COPY --from=builder /app/main /app/

# Copy curl binary and its dependencies
COPY --from=curl-installer /usr/bin/curl /usr/bin/curl

# Copy architecture-specific libraries using conditional logic
# ARG TARGETARCH
# COPY --from=curl-installer /lib/${TARGETARCH}-linux-gnu /lib/
# COPY --from=curl-installer /usr/lib/${TARGETARCH}-linux-gnu /usr/lib/

# Copy architecture-specific libraries using conditional logic
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        cp -r /lib/aarch64-linux-gnu /lib/; \
        cp -r /usr/lib/aarch64-linux-gnu /usr/lib/; \
    else \
        cp -r /lib/${TARGETARCH}-linux-gnu /lib/; \
        cp -r /usr/lib/${TARGETARCH}-linux-gnu /usr/lib/; \
    fi

# Copy curl binary and its dependencies
# COPY --from=curl-installer /usr/bin/curl /usr/bin/curl
# COPY --from=curl-installer /usr/lib/aarch64-linux-gnu /usr/lib/aarch64-linux-gnu
# COPY --from=curl-installer /lib/aarch64-linux-gnu /lib/aarch64-linux-gnu

# Expose the port the app runs on
EXPOSE 8088

# Set the default environment variable
ENV APP_PATH=./config/config.yaml

# Command to run the application
CMD ["/app/main"]
