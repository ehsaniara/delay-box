# Use the official Go image to build the application
FROM golang:1.22.4 AS go_builder

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

# Install bash and find all dependencies
RUN apt-get update && apt-get install -y bash --no-install-recommends && \
    mkdir -p /bash-libs && \
    ldd /bin/bash | tr -s '[:blank:]' '\n' | grep '^/' | xargs -I{} cp --parents {} /bash-libs/ && \
    cp /bin/bash /bash-libs/ && \
    rm -rf /var/lib/apt/lists/*

# Use a minimal base image to run the application
FROM gcr.io/distroless/base-debian10

# Copy bash and all its dependencies from the builder image
COPY --from=builder /bash-libs/ /

# Set default shell to bash
SHELL ["/bin/bash", "-c"]

# Set the working directory inside the container
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=go_builder /app/main /app/

# Expose the port the app runs on
EXPOSE 8088

# Set the default environment variable
ENV APP_PATH=./config/config.yaml

# Command to run the application
CMD ["/app/main"]
