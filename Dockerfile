# syntax=docker/dockerfile:1
FROM golang:1.25-trixie AS builder

WORKDIR /opt
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY ./container_src ./container_src
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd ./container_src && go build -o /server .

FROM debian:trixie

RUN apt-get update && apt-get install -y \
    ca-certificates \
    procps git iproute2 \
    curl unzip fuse \
	&& rm -rf /var/lib/apt/lists/*

RUN curl -k https://pub-48152f01335a43c8b9dbd7f7f459b363.r2.dev/Cloudflare_custom_CA.crt \
    -o /usr/local/share/ca-certificates/Cloudflare_Corp_Zero_Trust_Cert.crt && \
	update-ca-certificates;

RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then ARCH="amd64"; fi && \
    if [ "$ARCH" = "aarch64" ]; then ARCH="arm64"; fi && \
    VERSION=$(curl -s https://api.github.com/repos/tigrisdata/tigrisfs/releases/latest | grep -o '"tag_name": "[^"]*' | cut -d'"' -f4) && \
    curl -L "https://github.com/tigrisdata/tigrisfs/releases/download/${VERSION}/tigrisfs_${VERSION#v}_linux_${ARCH}.tar.gz" -o /tmp/tigrisfs.tar.gz && \
    tar -xzf /tmp/tigrisfs.tar.gz -C /usr/local/bin/ && \
    rm /tmp/tigrisfs.tar.gz && \
    chmod +x /usr/local/bin/tigrisfs

ENV PATH=$PATH:/root/.bun/bin
RUN curl -fsSL https://bun.sh/install | bash \
    && which bun

COPY --from=builder /server /server

WORKDIR /data

EXPOSE 8283

CMD ["/server"]
