# Install BusyBox and curl
FROM debian:10 AS busybox

RUN apt-get update && \
    apt-get install -y busybox --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Use a minimal base image to run the application
FROM ghcr.io/ehsaniara/delay-box:latest

# Set the working directory inside the container
WORKDIR /app

# Copy busybox from the busybox stage
COPY --from=busybox /bin/busybox /bin/sh
COPY --from=busybox /bin/busybox /bin/date

# Expose the port the app runs on
EXPOSE 8088

# Set the default environment variable
ENV APP_PATH=./config/config.yaml

# Command to run the application
CMD ["/app/main"]
