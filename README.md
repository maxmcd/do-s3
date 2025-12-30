# DO-S3

A Cloudflare Container that uses a Durable Object for persistent storage. The `/data` directory is a FUSE mount that connects to a Durable Object over an [S3 API](https://github.com/maxmcd/do-s3/blob/main/worker/s3.ts).

## Architecture

```mermaid
graph TB
    User[Browser/User]

    subgraph "Cloudflare Worker"
        Worker[Worker Router]
        RR[React Router<br/>Web UI]
    end

    subgraph "Terminal Container DO"
        Container[Go HTTP Server<br/>PTY/Shell]
        FUSE[tigrisfs FUSE Mount<br/>/data]
    end

    subgraph "S3 Durable Object"
        S3[S3 API Handler<br/>JWT Auth]
        SQLite[(SQLite Storage<br/>Chunked Objects)]
        WS[WebSocket<br/>Activity Stream]
    end

    User <-->|HTTP/WS| Worker
    Worker <-->|"/ (web pages)"| RR
    Worker <-->|"/ws (terminal)"| Container
    Worker <-->|"/s3-logs-ws (S3 request logs)"| WS

    Container <-->|WebSocket<br/>PTY I/O| User
    Container <-.->|reads/writes<br/>files| FUSE
    FUSE <-->|"/s3-* (S3 API)"| S3

    S3 <-->|store/retrieve| SQLite

    style Container fill:#e1f5ff
    style S3 fill:#fff4e1
    style SQLite fill:#f0f0f0
```

## How It Works

1. **Web UI**: React Router frontend served by Cloudflare Worker
2. **Terminal Container**: Go server running in a Cloudflare Container with:
   - WebSocket-based PTY for terminal access
   - FUSE filesystem mounted at `/data` using tigrisfs
3. **S3 Durable Object**: Custom S3-compatible API that:
   - Stores objects in SQLite (chunked for large files)
   - Uses JWT for authentication/authorization
   - Broadcasts request activity via WebSocket
