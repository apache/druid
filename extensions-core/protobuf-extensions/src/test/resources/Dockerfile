# Dockerfile for protobuf matching the project dependency

FROM alpine:3.22

# Install minimal dependencies for downloading and running protoc
RUN apk update && apk add --no-cache \
    wget \
    unzip \
    && rm -rf /var/cache/apk/*

# Download and install protoc 25.8 binary (matches Java protobuf 3.25.8)
WORKDIR /opt
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then ARCH="x86_64"; elif [ "$ARCH" = "aarch64" ]; then ARCH="aarch_64"; fi && \
    wget https://github.com/protocolbuffers/protobuf/releases/download/v25.8/protoc-25.8-linux-$ARCH.zip && \
    unzip protoc-25.8-linux-$ARCH.zip && \
    rm protoc-25.8-linux-$ARCH.zip readme.txt

# Update PATH to include protoc binary and set include path
ENV PATH="/opt/bin:${PATH}" \
    PROTOC_INCLUDE="/opt/include"

# Verify installation
RUN protoc --version

WORKDIR /workspace

# Default to shell if no command specified
CMD ["/bin/sh"]