import { DurableObject } from "cloudflare:workers";
import { verifyToken } from "./lib/jwt";

interface S3Object {
  bucket: string;
  key: string;
  data: ArrayBuffer;
  size: number;
  etag: string;
  last_modified: string;
  content_type: string;
}

const CHUNK_SIZE = 1024 * 1024; // 1MB chunks to stay under 2MB SQLite limit

// Helper functions to compute depth and parent for a key
function computeDepth(key: string): number {
  return (key.match(/\//g) || []).length;
}

function computeParent(key: string): string {
  // Strip trailing slash, then find last slash
  const trimmed = key.endsWith("/") ? key.slice(0, -1) : key;
  const lastSlash = trimmed.lastIndexOf("/");
  if (lastSlash === -1) {
    return "";
  }
  return trimmed.slice(0, lastSlash + 1);
}

// Migration function type - receives the sql instance to run queries
type MigrationFn = (sql: SqlStorage) => void;

// Migrations are run in order. Each migration is a function that receives the sql instance.
// Once a migration has been run, it should never be modified - add a new migration instead.
const MIGRATIONS: MigrationFn[] = [
  // Migration 0: Initial schema
  (sql) => {
    sql.exec(`CREATE TABLE IF NOT EXISTS objects (
      bucket TEXT NOT NULL,
      key TEXT NOT NULL,
      chunk_index INTEGER NOT NULL,
      size INTEGER NOT NULL,
      etag TEXT NOT NULL,
      last_modified TEXT NOT NULL,
      content_type TEXT NOT NULL,
      data BLOB NOT NULL,
      PRIMARY KEY (bucket, key, chunk_index)
    )`);
    sql.exec(`CREATE INDEX IF NOT EXISTS idx_objects_listing 
     ON objects (bucket, key) 
     WHERE chunk_index = 0`);
    sql.exec(`CREATE TABLE IF NOT EXISTS multipart_uploads (
      upload_id TEXT PRIMARY KEY,
      bucket TEXT NOT NULL,
      key TEXT NOT NULL,
      created_at TEXT NOT NULL,
      content_type TEXT NOT NULL
    )`);
    sql.exec(`CREATE TABLE IF NOT EXISTS multipart_parts (
      upload_id TEXT NOT NULL,
      part_number INTEGER NOT NULL,
      chunk_index INTEGER NOT NULL,
      size INTEGER NOT NULL,
      etag TEXT NOT NULL,
      data BLOB NOT NULL,
      PRIMARY KEY (upload_id, part_number, chunk_index)
    )`);
  },

  // Migration 1: Add depth and parent columns for efficient directory listing
  (sql) => {
    sql.exec(`ALTER TABLE objects ADD COLUMN depth INTEGER`);
    sql.exec(`ALTER TABLE objects ADD COLUMN parent TEXT`);
    sql.exec(`CREATE INDEX IF NOT EXISTS idx_objects_parent 
     ON objects (bucket, parent) 
     WHERE chunk_index = 0`);

    // Backfill existing rows with depth and parent values
    const result = sql.exec(
      `SELECT bucket, key FROM objects WHERE chunk_index = 0 AND (depth IS NULL OR parent IS NULL)`
    );
    for (const row of result) {
      const r = row as { bucket: string; key: string };
      const depth = computeDepth(r.key);
      const parent = computeParent(r.key);
      sql.exec(
        `UPDATE objects SET depth = ?, parent = ? WHERE bucket = ? AND key = ? AND chunk_index = 0`,
        depth,
        parent,
        r.bucket,
        r.key
      );
    }
  },
];

export class S3 extends DurableObject<Env> {
  sql: SqlStorage;
  secretsCache: Map<string, string[]> = new Map();
  webSocketClients: Set<WebSocket> = new Set();

  constructor(state: DurableObjectState, env: Env) {
    super(state, env);
    this.sql = state.storage.sql;
    this.runMigrations();
  }

  private runMigrations() {
    // Create migrations table if it doesn't exist
    this.sql.exec(`CREATE TABLE IF NOT EXISTS _migrations (
      version INTEGER PRIMARY KEY
    )`);

    // Get current migration version
    const result = this.sql.exec(
      `SELECT COALESCE(MAX(version), -1) as version FROM _migrations`
    );
    const rows = [...result] as any[];
    const currentVersion = rows[0]?.version ?? -1;

    // Run pending migrations
    for (let i = currentVersion + 1; i < MIGRATIONS.length; i++) {
      const migration = MIGRATIONS[i];
      migration(this.sql);

      // Record that this migration has been run
      this.sql.exec(`INSERT INTO _migrations (version) VALUES (?)`, i);
    }
  }

  async fetch(request: Request) {
    const url = new URL(request.url);
    const method = request.method;

    // Check for WebSocket upgrade request
    if (request.headers.get("Upgrade") === "websocket") {
      return this.handleWebSocketUpgrade(request);
    }

    // Parse path-style URL: /bucket/key or /bucket?list-type=2
    // We need to preserve trailing slashes for directory markers
    const pathParts = url.pathname.split("/").filter((p) => p.length > 0);

    if (pathParts.length === 0) {
      return this.errorResponse("NoSuchBucket", "No bucket specified", 404);
    }

    const bucket = pathParts[0];

    // JWT Authentication
    // Support two auth methods:
    // 1. Bearer token: "Authorization: Bearer <jwt>"
    // 2. AWS-style with JWT as accessKeyId: "Authorization: AWS4-HMAC-SHA256 Credential=<jwt>/..."
    const authHeader = request.headers.get("Authorization");
    if (!authHeader) {
      return this.errorResponse("Unauthorized", "Missing authorization", 401);
    }

    let token: string;
    if (authHeader.startsWith("Bearer ")) {
      // Direct bearer token
      token = authHeader.slice(7);
    } else if (authHeader.startsWith("AWS4-HMAC-SHA256")) {
      // Extract JWT from AWS signature Credential field
      // Format: AWS4-HMAC-SHA256 Credential=<jwt>/20231201/auto/s3/aws4_request, ...
      const credentialMatch = authHeader.match(/Credential=([^\/,]+)/);
      if (!credentialMatch) {
        return this.errorResponse(
          "Unauthorized",
          "Invalid AWS authorization format",
          401
        );
      }
      token = credentialMatch[1];
    } else {
      return this.errorResponse(
        "Unauthorized",
        "Unsupported authorization type",
        401
      );
    }

    // Dev mode: allow dummy credentials "foo" to bypass auth
    // TODO: Remove this before production
    if (token === "foo") {
      // Extract key while preserving trailing slashes
      const bucketPrefix = `/${bucket}`;
      let key = "";
      if (url.pathname.startsWith(bucketPrefix + "/")) {
        key = url.pathname.slice(bucketPrefix.length + 1);
        key = decodeURIComponent(key);
      }
      return this.handleRequest(bucket, key, method, request, url);
    }

    // Decode JWT to get computer name (without verification yet)
    const parts = token.split(".");
    if (parts.length !== 3) {
      return this.errorResponse("Unauthorized", "Invalid token format", 401);
    }

    const payloadJson = JSON.parse(atob(parts[1]));
    const terminalName = payloadJson.sub;

    if (!terminalName) {
      return this.errorResponse("Unauthorized", "Invalid token claims", 401);
    }

    // Use shared secret from environment (or hardcoded for demo)
    const secret = this.env.S3_JWT_SECRET || "demo-secret-change-in-production";
    const secrets = [secret];

    // Verify token with shared secret
    let payload;
    try {
      payload = await verifyToken(token, secrets);
    } catch (err) {
      return this.errorResponse("Unauthorized", "Invalid token", 401);
    }

    if (!payload) {
      return this.errorResponse(
        "Unauthorized",
        "Token verification failed",
        401
      );
    }

    // Verify token's bucket claim matches requested bucket
    if (payload.bucket !== bucket) {
      return this.errorResponse(
        "Forbidden",
        "Token not valid for this bucket",
        403
      );
    }

    // Extract key while preserving trailing slashes
    // S3 treats "foo" and "foo/" as different keys (file vs directory marker)
    const bucketPrefix = `/${bucket}`;
    let key = "";
    if (url.pathname.startsWith(bucketPrefix + "/")) {
      key = url.pathname.slice(bucketPrefix.length + 1);
      // Decode URL-encoded characters (e.g., %20 -> space)
      key = decodeURIComponent(key);
    } else if (url.pathname === bucketPrefix) {
      key = "";
    }

    const startTime = Date.now();
    const response = await this.handleRequest(
      bucket,
      key,
      method,
      request,
      url
    );
    const duration = Date.now() - startTime;
    // Broadcast request info to all connected WebSocket clients
    this.broadcastRequestInfo({
      method,
      path: url.pathname + url.search,
      status: response.status,
      duration,
      timestamp: new Date().toISOString(),
    });

    return response;
  }

  private handleWebSocketUpgrade(request: Request): Response {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.webSocketClients.add(server);

    server.accept();

    server.addEventListener("close", () => {
      this.webSocketClients.delete(server);
    });

    server.addEventListener("error", () => {
      this.webSocketClients.delete(server);
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  private broadcastRequestInfo(info: {
    method: string;
    path: string;
    status: number;
    duration: number;
    timestamp: string;
  }) {
    const message = JSON.stringify(info);

    // Remove closed connections
    const toRemove: WebSocket[] = [];
    for (const ws of this.webSocketClients) {
      try {
        ws.send(message);
      } catch (err) {
        toRemove.push(ws);
      }
    }

    for (const ws of toRemove) {
      this.webSocketClients.delete(ws);
    }
  }

  private async handleRequest(
    bucket: string,
    key: string,
    method: string,
    request: Request,
    url: URL
  ): Promise<Response> {
    // HEAD bucket (check if bucket exists)
    if (method === "HEAD" && !key) {
      return this.headBucket(bucket);
    }

    // GET bucket with ?uploads - ListMultipartUploads
    if (method === "GET" && !key && url.searchParams.has("uploads")) {
      return this.listMultipartUploads(bucket, url.searchParams);
    }

    // GET bucket (list objects) - supports both ListObjectsV2 and ListObjects
    if (method === "GET" && !key) {
      return this.listObjectsV2(bucket, url.searchParams);
    }

    // GetObject or HeadObject
    if ((method === "GET" || method === "HEAD") && key) {
      return this.getObject(bucket, key, method === "HEAD");
    }

    // Multipart upload operations
    if (key) {
      const uploadId = url.searchParams.get("uploadId");

      // CreateMultipartUpload (POST with ?uploads)
      if (method === "POST" && url.searchParams.has("uploads")) {
        return this.createMultipartUpload(bucket, key, request);
      }

      // UploadPart (PUT with ?uploadId&partNumber)
      if (method === "PUT" && uploadId && url.searchParams.has("partNumber")) {
        const partNumber = parseInt(url.searchParams.get("partNumber")!);
        return this.uploadPart(bucket, key, uploadId, partNumber, request);
      }

      // CompleteMultipartUpload (POST with ?uploadId)
      if (method === "POST" && uploadId) {
        return this.completeMultipartUpload(bucket, key, uploadId, request);
      }

      // AbortMultipartUpload (DELETE with ?uploadId)
      if (method === "DELETE" && uploadId) {
        return this.abortMultipartUpload(uploadId);
      }
    }

    // CopyObject (PUT with x-amz-copy-source header)
    const copySource = request.headers.get("x-amz-copy-source");
    if (method === "PUT" && key && copySource) {
      return this.copyObject(bucket, key, copySource);
    }

    // PutObject
    if (method === "PUT" && key) {
      return this.putObject(bucket, key, request);
    }

    // DeleteObject
    if (method === "DELETE" && key) {
      return this.deleteObject(bucket, key);
    }

    return this.errorResponse(
      "NotImplemented",
      "Operation not implemented",
      501
    );
  }

  private async putObject(
    bucket: string,
    key: string,
    request: Request
  ): Promise<Response> {
    try {
      const data = await request.arrayBuffer();
      const size = data.byteLength;
      const contentType =
        request.headers.get("content-type") || "application/octet-stream";

      // Generate ETag (MD5 hash)
      const hashBuffer = await crypto.subtle.digest("MD5", data);
      const hashArray = Array.from(new Uint8Array(hashBuffer));
      const etag = hashArray
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");

      const lastModified = new Date().toISOString();

      // Delete existing object if any
      this.sql.exec(
        `DELETE FROM objects WHERE bucket = ? AND key = ?`,
        bucket,
        key
      );

      // Store data in chunks
      const dataArray = new Uint8Array(data);
      const depth = computeDepth(key);
      const parent = computeParent(key);

      // Always insert chunk 0 with metadata (even if empty)
      this.sql.exec(
        `INSERT INTO objects (bucket, key, chunk_index, size, etag, last_modified, content_type, data, depth, parent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        bucket,
        key,
        0,
        size,
        etag,
        lastModified,
        contentType,
        size === 0
          ? new ArrayBuffer(0)
          : dataArray.slice(0, Math.min(CHUNK_SIZE, size)).buffer,
        depth,
        parent
      );

      // If file is larger than one chunk, store remaining chunks
      if (size > CHUNK_SIZE) {
        let chunkIndex = 1;
        for (let offset = CHUNK_SIZE; offset < size; offset += CHUNK_SIZE) {
          const chunkEnd = Math.min(offset + CHUNK_SIZE, size);
          const chunk = dataArray.slice(offset, chunkEnd);

          this.sql.exec(
            `INSERT INTO objects (bucket, key, chunk_index, size, etag, last_modified, content_type, data, depth, parent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            bucket,
            key,
            chunkIndex,
            0,
            "",
            "",
            "",
            chunk.buffer,
            null,
            null
          );
          chunkIndex++;
        }
      }

      return new Response(null, {
        status: 200,
        headers: {
          ETag: `"${etag}"`,
          "x-amz-request-id": crypto.randomUUID(),
        },
      });
    } catch (error) {
      console.error("PutObject error:", error);
      return this.errorResponse("InternalError", "Failed to store object", 500);
    }
  }

  private async copyObject(
    bucket: string,
    destKey: string,
    copySource: string
  ): Promise<Response> {
    try {
      // Parse copy source: /bucket/key or bucket/key
      let sourcePath = copySource;
      if (sourcePath.startsWith("/")) {
        sourcePath = sourcePath.slice(1);
      }

      // Extract bucket and key from source path
      const slashIndex = sourcePath.indexOf("/");
      if (slashIndex === -1) {
        return this.errorResponse(
          "InvalidArgument",
          "Invalid copy source",
          400
        );
      }

      const sourceBucket = sourcePath.slice(0, slashIndex);
      const sourceKey = decodeURIComponent(sourcePath.slice(slashIndex + 1));

      // For now, only support same-bucket copies
      if (sourceBucket !== bucket) {
        return this.errorResponse(
          "InvalidArgument",
          "Cross-bucket copy not supported",
          400
        );
      }

      // Get source object metadata and data
      const metaResult = this.sql.exec(
        `SELECT size, etag, content_type FROM objects WHERE bucket = ? AND key = ? AND chunk_index = 0`,
        sourceBucket,
        sourceKey
      );
      const metaRows = [...metaResult] as any[];

      if (metaRows.length === 0) {
        return this.errorResponse(
          "NoSuchKey",
          "The specified source key does not exist.",
          404
        );
      }

      const sourceMeta = metaRows[0];
      const lastModified = new Date().toISOString();

      // Delete existing destination object if any
      this.sql.exec(
        `DELETE FROM objects WHERE bucket = ? AND key = ?`,
        bucket,
        destKey
      );

      // Copy all chunks from source to destination
      const chunksResult = this.sql.exec(
        `SELECT chunk_index, data FROM objects WHERE bucket = ? AND key = ? ORDER BY chunk_index`,
        sourceBucket,
        sourceKey
      );
      const chunks = [...chunksResult] as any[];
      const depth = computeDepth(destKey);
      const parent = computeParent(destKey);

      for (const chunk of chunks) {
        if (chunk.chunk_index === 0) {
          // First chunk has metadata
          this.sql.exec(
            `INSERT INTO objects (bucket, key, chunk_index, size, etag, last_modified, content_type, data, depth, parent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            bucket,
            destKey,
            0,
            sourceMeta.size,
            sourceMeta.etag,
            lastModified,
            sourceMeta.content_type,
            chunk.data,
            depth,
            parent
          );
        } else {
          // Other chunks have empty metadata
          this.sql.exec(
            `INSERT INTO objects (bucket, key, chunk_index, size, etag, last_modified, content_type, data, depth, parent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            bucket,
            destKey,
            chunk.chunk_index,
            0,
            "",
            "",
            "",
            chunk.data,
            null,
            null
          );
        }
      }

      // Return CopyObjectResult XML
      const xml = `<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <LastModified>${lastModified}</LastModified>
  <ETag>"${this.escapeXml(sourceMeta.etag)}"</ETag>
</CopyObjectResult>`;

      return new Response(xml, {
        status: 200,
        headers: {
          "Content-Type": "application/xml",
          "x-amz-request-id": crypto.randomUUID(),
        },
      });
    } catch (error) {
      console.error("CopyObject error:", error);
      return this.errorResponse("InternalError", "Failed to copy object", 500);
    }
  }

  private async getObject(
    bucket: string,
    key: string,
    headOnly: boolean
  ): Promise<Response> {
    try {
      // Get metadata from chunk 0
      const result = this.sql.exec(
        `SELECT size, etag, last_modified, content_type FROM objects WHERE bucket = ? AND key = ? AND chunk_index = 0`,
        bucket,
        key
      );

      const rows = [...result];
      if (rows.length === 0) {
        return this.errorResponse(
          "NoSuchKey",
          "The specified key does not exist.",
          404
        );
      }

      const row = rows[0] as any;
      const headers: Record<string, string> = {
        "Content-Type": row.content_type,
        "Content-Length": row.size.toString(),
        ETag: `"${row.etag}"`,
        "Last-Modified": new Date(row.last_modified).toUTCString(),
        "x-amz-request-id": crypto.randomUUID(),
      };

      if (headOnly) {
        return new Response(null, { status: 200, headers });
      }

      // Read all chunks
      const chunksResult = this.sql.exec(
        `SELECT data FROM objects WHERE bucket = ? AND key = ? ORDER BY chunk_index`,
        bucket,
        key
      );
      const chunks = [...chunksResult] as any[];

      if (chunks.length === 0) {
        // Empty file
        return new Response(new ArrayBuffer(0), { status: 200, headers });
      }

      // Combine chunks
      const totalSize = row.size;
      const combinedData = new Uint8Array(totalSize);
      let offset = 0;
      for (const chunk of chunks) {
        const chunkData = new Uint8Array(chunk.data);
        combinedData.set(chunkData, offset);
        offset += chunkData.byteLength;
      }

      return new Response(combinedData.buffer, { status: 200, headers });
    } catch (error) {
      console.error("GetObject error:", error);
      return this.errorResponse(
        "InternalError",
        "Failed to retrieve object",
        500
      );
    }
  }

  private async deleteObject(bucket: string, key: string): Promise<Response> {
    try {
      this.sql.exec(
        `DELETE FROM objects WHERE bucket = ? AND key = ?`,
        bucket,
        key
      );

      return new Response(null, {
        status: 204,
        headers: {
          "x-amz-request-id": crypto.randomUUID(),
        },
      });
    } catch (error) {
      console.error("DeleteObject error:", error);
      return this.errorResponse(
        "InternalError",
        "Failed to delete object",
        500
      );
    }
  }

  private async headBucket(bucket: string): Promise<Response> {
    // For now, we'll just return success for any bucket
    // In a real implementation, you might want to check if the bucket has any objects
    return new Response(null, {
      status: 200,
      headers: {
        "x-amz-request-id": crypto.randomUUID(),
      },
    });
  }

  private async listObjectsV2(
    bucket: string,
    params: URLSearchParams
  ): Promise<Response> {
    try {
      const prefix = params.get("prefix") || "";
      const delimiter = params.get("delimiter") || "";
      const maxKeys = parseInt(params.get("max-keys") || "1000");
      const startAfter = params.get("start-after") || "";
      const continuationToken = params.get("continuation-token") || "";

      let objects: any[] = [];
      let commonPrefixes: string[] = [];
      let isTruncated = false;
      let nextContinuationToken = "";

      if (delimiter === "/") {
        // Optimized path for "/" delimiter using parent column
        // Query 1: Get CommonPrefixes (distinct parent directories at this level)
        const prefixDepth = computeDepth(prefix);
        const targetDepth = prefixDepth + 1;

        let prefixQuery = `SELECT DISTINCT parent FROM objects WHERE bucket = ? AND chunk_index = 0 AND depth >= ?`;
        const prefixQueryParams: any[] = [bucket, targetDepth];

        if (prefix) {
          prefixQuery += ` AND parent >= ? AND parent < ?`;
          prefixQueryParams.push(prefix);
          const prefixUpperBound =
            prefix.slice(0, -1) +
            String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1);
          prefixQueryParams.push(prefixUpperBound);
        }

        if (continuationToken || startAfter) {
          // For pagination, we need to skip past already-seen prefixes
          const marker = continuationToken || startAfter;
          prefixQuery += ` AND parent > ?`;
          prefixQueryParams.push(computeParent(marker) || marker);
        }

        prefixQuery += ` ORDER BY parent LIMIT ?`;
        prefixQueryParams.push(maxKeys + 1);

        const prefixResult = this.sql.exec(prefixQuery, ...prefixQueryParams);
        const prefixRows = [...prefixResult] as any[];

        // Filter to only include prefixes at exactly the target level
        for (const row of prefixRows) {
          if (
            row.parent &&
            row.parent.startsWith(prefix) &&
            computeDepth(row.parent) === targetDepth
          ) {
            if (!commonPrefixes.includes(row.parent)) {
              commonPrefixes.push(row.parent);
            }
          }
        }

        // Query 2: Get Contents (files directly at this level, where parent === prefix)
        let contentsQuery = `SELECT key, size, etag, last_modified FROM objects WHERE bucket = ? AND chunk_index = 0 AND parent = ?`;
        const contentsQueryParams: any[] = [bucket, prefix];

        if (continuationToken || startAfter) {
          contentsQuery += ` AND key > ?`;
          contentsQueryParams.push(continuationToken || startAfter);
        }

        contentsQuery += ` ORDER BY key LIMIT ?`;
        contentsQueryParams.push(maxKeys + 1);

        const contentsResult = this.sql.exec(
          contentsQuery,
          ...contentsQueryParams
        );
        objects = [...contentsResult] as any[];

        // Merge and sort results, apply maxKeys limit
        // S3 returns CommonPrefixes and Contents interleaved by sort order
        const allResults: {
          type: "prefix" | "content";
          value: string;
          data?: any;
        }[] = [];

        for (const cp of commonPrefixes) {
          allResults.push({ type: "prefix", value: cp });
        }
        for (const obj of objects) {
          allResults.push({ type: "content", value: obj.key, data: obj });
        }

        // Sort by value (key or prefix)
        allResults.sort((a, b) => a.value.localeCompare(b.value));

        // Apply maxKeys limit
        if (allResults.length > maxKeys) {
          isTruncated = true;
          allResults.splice(maxKeys);
          const lastItem = allResults[allResults.length - 1];
          nextContinuationToken = lastItem.value;
        }

        // Split back into commonPrefixes and objects
        commonPrefixes = [];
        objects = [];
        for (const item of allResults) {
          if (item.type === "prefix") {
            commonPrefixes.push(item.value);
          } else {
            objects.push(item.data);
          }
        }
      } else if (delimiter) {
        // Non-"/" delimiter: fall back to post-query filtering
        let query = `SELECT key, size, etag, last_modified FROM objects WHERE bucket = ? AND chunk_index = 0`;
        const queryParams: any[] = [bucket];

        if (prefix) {
          query += ` AND key >= ? AND key < ?`;
          queryParams.push(prefix);
          const prefixUpperBound =
            prefix.slice(0, -1) +
            String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1);
          queryParams.push(prefixUpperBound);
        }

        if (continuationToken || startAfter) {
          query += ` AND key > ?`;
          queryParams.push(continuationToken || startAfter);
        }

        query += ` ORDER BY key LIMIT ?`;
        // Fetch more than maxKeys since we'll be collapsing some into CommonPrefixes
        queryParams.push(maxKeys * 10 + 1);

        const result = this.sql.exec(query, ...queryParams);
        const rows = [...result] as any[];

        const seenPrefixes = new Set<string>();
        let count = 0;

        for (const row of rows) {
          if (count >= maxKeys) {
            isTruncated = true;
            break;
          }

          const keyAfterPrefix = row.key.slice(prefix.length);
          const delimiterIndex = keyAfterPrefix.indexOf(delimiter);

          if (delimiterIndex >= 0) {
            // This key contains the delimiter, extract CommonPrefix
            const commonPrefix =
              prefix + keyAfterPrefix.slice(0, delimiterIndex + 1);
            if (!seenPrefixes.has(commonPrefix)) {
              seenPrefixes.add(commonPrefix);
              commonPrefixes.push(commonPrefix);
              count++;
              nextContinuationToken = row.key;
            }
          } else {
            // Direct content at this level
            objects.push(row);
            count++;
            nextContinuationToken = row.key;
          }
        }
      } else {
        // No delimiter: simple listing
        let query = `SELECT key, size, etag, last_modified FROM objects WHERE bucket = ? AND chunk_index = 0`;
        const queryParams: any[] = [bucket];

        if (prefix) {
          query += ` AND key >= ? AND key < ?`;
          queryParams.push(prefix);
          const prefixUpperBound =
            prefix.slice(0, -1) +
            String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1);
          queryParams.push(prefixUpperBound);
        }

        if (continuationToken || startAfter) {
          query += ` AND key > ?`;
          queryParams.push(continuationToken || startAfter);
        }

        query += ` ORDER BY key LIMIT ?`;
        queryParams.push(maxKeys + 1);

        const result = this.sql.exec(query, ...queryParams);
        const rows = [...result] as any[];

        isTruncated = rows.length > maxKeys;
        objects = rows.slice(0, maxKeys);

        if (isTruncated && objects.length > 0) {
          nextContinuationToken = objects[objects.length - 1].key;
        }
      }

      // Build XML response
      const keyCount = objects.length + commonPrefixes.length;
      let xml = '<?xml version="1.0" encoding="UTF-8"?>\n';
      xml +=
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n';
      xml += `  <Name>${this.escapeXml(bucket)}</Name>\n`;
      xml += `  <Prefix>${this.escapeXml(prefix)}</Prefix>\n`;
      if (delimiter) {
        xml += `  <Delimiter>${this.escapeXml(delimiter)}</Delimiter>\n`;
      }
      xml += `  <KeyCount>${keyCount}</KeyCount>\n`;
      xml += `  <MaxKeys>${maxKeys}</MaxKeys>\n`;
      xml += `  <IsTruncated>${isTruncated}</IsTruncated>\n`;

      if (nextContinuationToken && isTruncated) {
        xml += `  <NextContinuationToken>${this.escapeXml(nextContinuationToken)}</NextContinuationToken>\n`;
      }

      for (const row of objects) {
        xml += "  <Contents>\n";
        xml += `    <Key>${this.escapeXml(row.key)}</Key>\n`;
        xml += `    <LastModified>${new Date(row.last_modified).toISOString()}</LastModified>\n`;
        xml += `    <ETag>"${this.escapeXml(row.etag)}"</ETag>\n`;
        xml += `    <Size>${row.size}</Size>\n`;
        xml += `    <StorageClass>STANDARD</StorageClass>\n`;
        xml += "  </Contents>\n";
      }

      for (const cp of commonPrefixes) {
        xml += "  <CommonPrefixes>\n";
        xml += `    <Prefix>${this.escapeXml(cp)}</Prefix>\n`;
        xml += "  </CommonPrefixes>\n";
      }

      xml += "</ListBucketResult>";

      return new Response(xml, {
        status: 200,
        headers: {
          "Content-Type": "application/xml",
          "x-amz-request-id": crypto.randomUUID(),
        },
      });
    } catch (error) {
      console.error("ListObjectsV2 error:", error);
      return this.errorResponse("InternalError", "Failed to list objects", 500);
    }
  }

  private async listMultipartUploads(
    bucket: string,
    params: URLSearchParams
  ): Promise<Response> {
    try {
      const prefix = params.get("prefix") || "";
      const keyMarker = params.get("key-marker") || "";
      const uploadIdMarker = params.get("upload-id-marker") || "";
      const maxUploads = parseInt(params.get("max-uploads") || "1000");

      let query = `SELECT upload_id, bucket, key, created_at FROM multipart_uploads WHERE bucket = ?`;
      const queryParams: any[] = [bucket];

      if (prefix) {
        // Use range query instead of LIKE to avoid special character escaping issues
        query += ` AND key >= ? AND key < ?`;
        queryParams.push(prefix);
        const prefixUpperBound =
          prefix.slice(0, -1) +
          String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1);
        queryParams.push(prefixUpperBound);
      }

      if (keyMarker) {
        if (uploadIdMarker) {
          // If both markers are set, start after the specific upload
          query += ` AND (key > ? OR (key = ? AND upload_id > ?))`;
          queryParams.push(keyMarker, keyMarker, uploadIdMarker);
        } else {
          query += ` AND key > ?`;
          queryParams.push(keyMarker);
        }
      }

      query += ` ORDER BY key, upload_id LIMIT ?`;
      queryParams.push(maxUploads + 1);

      const result = this.sql.exec(query, ...queryParams);
      const rows = [...result] as any[];

      const isTruncated = rows.length > maxUploads;
      const uploads = rows.slice(0, maxUploads);

      let nextKeyMarker = "";
      let nextUploadIdMarker = "";
      if (isTruncated && uploads.length > 0) {
        const lastUpload = uploads[uploads.length - 1];
        nextKeyMarker = lastUpload.key;
        nextUploadIdMarker = lastUpload.upload_id;
      }

      // Build XML response
      let xml = '<?xml version="1.0" encoding="UTF-8"?>\n';
      xml +=
        '<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n';
      xml += `  <Bucket>${this.escapeXml(bucket)}</Bucket>\n`;
      xml += `  <KeyMarker>${this.escapeXml(keyMarker)}</KeyMarker>\n`;
      xml += `  <UploadIdMarker>${this.escapeXml(uploadIdMarker)}</UploadIdMarker>\n`;
      xml += `  <MaxUploads>${maxUploads}</MaxUploads>\n`;
      xml += `  <IsTruncated>${isTruncated}</IsTruncated>\n`;

      if (prefix) {
        xml += `  <Prefix>${this.escapeXml(prefix)}</Prefix>\n`;
      }

      if (isTruncated) {
        xml += `  <NextKeyMarker>${this.escapeXml(nextKeyMarker)}</NextKeyMarker>\n`;
        xml += `  <NextUploadIdMarker>${this.escapeXml(nextUploadIdMarker)}</NextUploadIdMarker>\n`;
      }

      for (const row of uploads) {
        xml += "  <Upload>\n";
        xml += `    <Key>${this.escapeXml(row.key)}</Key>\n`;
        xml += `    <UploadId>${this.escapeXml(row.upload_id)}</UploadId>\n`;
        xml += `    <Initiated>${new Date(row.created_at).toISOString()}</Initiated>\n`;
        xml += `    <StorageClass>STANDARD</StorageClass>\n`;
        xml += "  </Upload>\n";
      }

      xml += "</ListMultipartUploadsResult>";

      return new Response(xml, {
        status: 200,
        headers: {
          "Content-Type": "application/xml",
          "x-amz-request-id": crypto.randomUUID(),
        },
      });
    } catch (error) {
      console.error("ListMultipartUploads error:", error);
      return this.errorResponse(
        "InternalError",
        "Failed to list multipart uploads",
        500
      );
    }
  }

  private async createMultipartUpload(
    bucket: string,
    key: string,
    request: Request
  ): Promise<Response> {
    try {
      const uploadId = crypto.randomUUID();
      const contentType =
        request.headers.get("content-type") || "application/octet-stream";
      const createdAt = new Date().toISOString();

      this.sql.exec(
        `INSERT INTO multipart_uploads (upload_id, bucket, key, created_at, content_type) VALUES (?, ?, ?, ?, ?)`,
        uploadId,
        bucket,
        key,
        createdAt,
        contentType
      );

      const xml = `<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>${this.escapeXml(bucket)}</Bucket>
  <Key>${this.escapeXml(key)}</Key>
  <UploadId>${uploadId}</UploadId>
</InitiateMultipartUploadResult>`;

      return new Response(xml, {
        status: 200,
        headers: {
          "Content-Type": "application/xml",
          "x-amz-request-id": crypto.randomUUID(),
        },
      });
    } catch (error) {
      console.error("CreateMultipartUpload error:", error);
      return this.errorResponse(
        "InternalError",
        "Failed to create multipart upload",
        500
      );
    }
  }

  private async uploadPart(
    bucket: string,
    key: string,
    uploadId: string,
    partNumber: number,
    request: Request
  ): Promise<Response> {
    try {
      const data = await request.arrayBuffer();
      const size = data.byteLength;

      // Generate ETag (MD5 hash)
      const hashBuffer = await crypto.subtle.digest("MD5", data);
      const hashArray = Array.from(new Uint8Array(hashBuffer));
      const etag = hashArray
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");

      // Delete existing chunks for this part if any
      this.sql.exec(
        `DELETE FROM multipart_parts WHERE upload_id = ? AND part_number = ?`,
        uploadId,
        partNumber
      );

      // Store data in chunks
      const dataArray = new Uint8Array(data);

      // Always insert chunk 0 with metadata (even if empty)
      this.sql.exec(
        `INSERT INTO multipart_parts (upload_id, part_number, chunk_index, size, etag, data) VALUES (?, ?, ?, ?, ?, ?)`,
        uploadId,
        partNumber,
        0,
        size,
        etag,
        size === 0
          ? new ArrayBuffer(0)
          : dataArray.slice(0, Math.min(CHUNK_SIZE, size)).buffer
      );

      // If part is larger than one chunk, store remaining chunks
      if (size > CHUNK_SIZE) {
        let chunkIndex = 1;
        for (let offset = CHUNK_SIZE; offset < size; offset += CHUNK_SIZE) {
          const chunkEnd = Math.min(offset + CHUNK_SIZE, size);
          const chunk = dataArray.slice(offset, chunkEnd);

          this.sql.exec(
            `INSERT INTO multipart_parts (upload_id, part_number, chunk_index, size, etag, data) VALUES (?, ?, ?, ?, ?, ?)`,
            uploadId,
            partNumber,
            chunkIndex,
            0,
            "",
            chunk.buffer
          );
          chunkIndex++;
        }
      }

      return new Response(null, {
        status: 200,
        headers: {
          ETag: `"${etag}"`,
          "x-amz-request-id": crypto.randomUUID(),
        },
      });
    } catch (error) {
      console.error("UploadPart error:", error);
      return this.errorResponse("InternalError", "Failed to upload part", 500);
    }
  }

  private async completeMultipartUpload(
    bucket: string,
    key: string,
    uploadId: string,
    request: Request
  ): Promise<Response> {
    try {
      // Get upload metadata
      const uploadResult = this.sql.exec(
        `SELECT content_type FROM multipart_uploads WHERE upload_id = ?`,
        uploadId
      );
      const uploads = [...uploadResult] as any[];

      if (uploads.length === 0) {
        return this.errorResponse(
          "NoSuchUpload",
          "The specified upload does not exist",
          404
        );
      }

      const contentType = uploads[0].content_type;

      // Get all parts metadata (chunk 0 only), ordered by part number
      const partsResult = this.sql.exec(
        `SELECT part_number, size, etag FROM multipart_parts WHERE upload_id = ? AND chunk_index = 0 ORDER BY part_number`,
        uploadId
      );
      const parts = [...partsResult] as any[];

      if (parts.length === 0) {
        return this.errorResponse("InvalidPart", "No parts were uploaded", 400);
      }

      // Calculate total size
      let totalSize = 0;
      for (const part of parts) {
        totalSize += part.size;
      }

      // Delete existing object if any
      this.sql.exec(
        `DELETE FROM objects WHERE bucket = ? AND key = ?`,
        bucket,
        key
      );

      // Copy part chunks to object chunks, re-indexing them
      let objectChunkIndex = 0;
      const lastModified = new Date().toISOString();
      const etag = `${crypto.randomUUID().replace(/-/g, "")}-${parts.length}`;
      const depth = computeDepth(key);
      const parent = computeParent(key);

      for (const part of parts) {
        const partChunksResult = this.sql.exec(
          `SELECT data FROM multipart_parts WHERE upload_id = ? AND part_number = ? ORDER BY chunk_index`,
          uploadId,
          part.part_number
        );
        const partChunks = [...partChunksResult] as any[];

        for (const chunk of partChunks) {
          // First chunk (index 0) has metadata, rest have empty strings
          if (objectChunkIndex === 0) {
            this.sql.exec(
              `INSERT INTO objects (bucket, key, chunk_index, size, etag, last_modified, content_type, data, depth, parent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              bucket,
              key,
              objectChunkIndex,
              totalSize,
              etag,
              lastModified,
              contentType,
              chunk.data,
              depth,
              parent
            );
          } else {
            this.sql.exec(
              `INSERT INTO objects (bucket, key, chunk_index, size, etag, last_modified, content_type, data, depth, parent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              bucket,
              key,
              objectChunkIndex,
              0,
              "",
              "",
              "",
              chunk.data,
              null,
              null
            );
          }
          objectChunkIndex++;
        }
      }

      // Clean up multipart upload data
      this.sql.exec(
        `DELETE FROM multipart_parts WHERE upload_id = ?`,
        uploadId
      );
      this.sql.exec(
        `DELETE FROM multipart_uploads WHERE upload_id = ?`,
        uploadId
      );

      const xml = `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://localhost:8787/${this.escapeXml(bucket)}/${this.escapeXml(key)}</Location>
  <Bucket>${this.escapeXml(bucket)}</Bucket>
  <Key>${this.escapeXml(key)}</Key>
  <ETag>"${etag}"</ETag>
</CompleteMultipartUploadResult>`;

      return new Response(xml, {
        status: 200,
        headers: {
          "Content-Type": "application/xml",
          "x-amz-request-id": crypto.randomUUID(),
        },
      });
    } catch (error) {
      console.error("CompleteMultipartUpload error:", error);
      return this.errorResponse(
        "InternalError",
        "Failed to complete multipart upload",
        500
      );
    }
  }

  private async abortMultipartUpload(uploadId: string): Promise<Response> {
    try {
      this.sql.exec(
        `DELETE FROM multipart_parts WHERE upload_id = ?`,
        uploadId
      );
      this.sql.exec(
        `DELETE FROM multipart_uploads WHERE upload_id = ?`,
        uploadId
      );

      return new Response(null, {
        status: 204,
        headers: {
          "x-amz-request-id": crypto.randomUUID(),
        },
      });
    } catch (error) {
      console.error("AbortMultipartUpload error:", error);
      return this.errorResponse(
        "InternalError",
        "Failed to abort multipart upload",
        500
      );
    }
  }

  private errorResponse(
    code: string,
    message: string,
    status: number
  ): Response {
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>${this.escapeXml(code)}</Code>
  <Message>${this.escapeXml(message)}</Message>
  <RequestId>${crypto.randomUUID()}</RequestId>
</Error>`;

    return new Response(xml, {
      status,
      headers: {
        "Content-Type": "application/xml",
        "x-amz-request-id": crypto.randomUUID(),
      },
    });
  }

  private escapeXml(unsafe: string): string {
    return unsafe
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&apos;");
  }
}
