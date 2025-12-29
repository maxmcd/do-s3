import { env } from "cloudflare:test";
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  ListMultipartUploadsCommand,
  CopyObjectCommand,
} from "@aws-sdk/client-s3";
import { describe, it, expect, beforeAll } from "vitest";
import { DOMParser } from "@xmldom/xmldom";
import { signToken } from "../lib/jwt";
import type { Computer } from "../computers";

// Polyfill DOMParser and Node constants for AWS SDK XML parsing
globalThis.DOMParser = DOMParser as any;
globalThis.Node = {
  ELEMENT_NODE: 1,
  TEXT_NODE: 3,
  CDATA_SECTION_NODE: 4,
  COMMENT_NODE: 8,
  DOCUMENT_NODE: 9,
} as any;

// Store test computer credentials
let testComputer: Computer;
let testSecret: string;

async function createS3ClientForBucket(doName: string): Promise<S3Client> {
  // Token for accessing this S3 DO instance
  // The bucket name in the token should match what's in the S3 path
  const token = await signToken(
    { sub: testComputer.slug, bucket: doName, expiresIn: 3600 },
    testSecret
  );

  const id = env.S3.idFromName(doName);
  const stub = env.S3.get(id);

  return new S3Client({
    endpoint: "http://test",
    region: "auto",
    credentials: {
      // HACK: Pass JWT token as accessKeyId!
      // The AWS SDK will include this in the Authorization header's Credential field
      // Format: "AWS4-HMAC-SHA256 Credential=<jwt>/20231201/auto/s3/aws4_request, ..."
      // Our S3 DO extracts the JWT from the Credential field
      accessKeyId: token,
      secretAccessKey: "not-used", // AWS SDK requires this but S3 DO ignores it
    },
    forcePathStyle: true, // Important: use path-style addressing (bucket in path)
    // Use stub.fetch as the request handler
    requestHandler: {
      handle: async (request: any) => {
        const query = request.query
          ? `?${new URLSearchParams(request.query).toString()}`
          : "";
        const url = `http://test${request.path}${query}`;

        // AWS SDK has already added Authorization header with JWT in Credential field
        // We just pass it through as-is - this tests the real AWS key hack!
        const fetchRequest = new Request(url, {
          method: request.method,
          headers: request.headers,
          body: request.body,
        });

        const response = await stub.fetch(fetchRequest);

        return {
          response: {
            statusCode: response.status,
            headers: Object.fromEntries(response.headers.entries()),
            body: response.body,
          },
        };
      },
    },
  });
}

describe("S3 with AWS SDK", () => {
  beforeAll(async () => {
    // Create a test computer with secrets
    const computersStub = env.COMPUTERS.get(env.COMPUTERS.idFromName("global"));
    const result = await computersStub.createComputer("Test Computer");

    if (!result.success || !result.computer) {
      throw new Error(`Failed to create test computer: ${result.error}`);
    }

    testComputer = result.computer;
    const secrets = JSON.parse(testComputer.secrets);
    testSecret = secrets[0];
  });

  it("can PUT and GET an object using AWS S3 SDK", async () => {
    const doName = "test-instance";
    const s3Client = await createS3ClientForBucket(doName);

    const bucket = doName; // Use DO name as bucket name for testing
    const key = "test-file.txt";
    const content = "Hello from AWS SDK!";

    // PUT object
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: key, Body: content })
    );

    // GET object
    const getResult = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: key })
    );

    const bodyText = await getResult.Body?.transformToString();
    expect(bodyText).toBe(content);
  });

  it("can PUT and GET an empty file", async () => {
    const doName = "empty-test-bucket";
    const s3Client = await createS3ClientForBucket(doName);

    const bucket = doName;
    const key = "empty.txt";

    const putResult = await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: key, Body: "" })
    );
    expect(putResult.$metadata.httpStatusCode).toBe(200);
    expect(putResult.ETag).toBeTruthy();

    const getResult = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: key })
    );
    expect(getResult.$metadata.httpStatusCode).toBe(200);
    expect(getResult.ContentLength).toBe(0);

    const bodyText = await getResult.Body?.transformToString();
    expect(bodyText).toBe("");
  });

  it("can HEAD an object", async () => {
    const doName = "head-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;
    const key = "test.txt";
    const content = "test content";

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: key, Body: content })
    );

    const headResult = await s3Client.send(
      new HeadObjectCommand({ Bucket: bucket, Key: key })
    );

    expect(headResult.$metadata.httpStatusCode).toBe(200);
    expect(headResult.ContentLength).toBe(content.length);
    expect(headResult.ETag).toBeTruthy();
  });

  it("can DELETE an object", async () => {
    const doName = "delete-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;
    const key = "delete-me.txt";

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: key, Body: "delete this" })
    );

    const deleteResult = await s3Client.send(
      new DeleteObjectCommand({ Bucket: bucket, Key: key })
    );
    expect(deleteResult.$metadata.httpStatusCode).toBe(204);

    await expect(
      s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }))
    ).rejects.toThrow();
  });

  it("can list objects", async () => {
    const doName = "list-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "file1.txt", Body: "data1" })
    );
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "file2.txt", Body: "data2" })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "dir/file3.txt",
        Body: "data3",
      })
    );

    const listResult = await s3Client.send(
      new ListObjectsV2Command({ Bucket: bucket })
    );

    expect(listResult.Contents).toHaveLength(3);
    expect(listResult.KeyCount).toBe(3);
    const keys = listResult.Contents?.map((obj) => obj.Key) || [];
    expect(keys.includes("file1.txt")).toBe(true);
    expect(keys.includes("file2.txt")).toBe(true);
    expect(keys.includes("dir/file3.txt")).toBe(true);
  });

  it("can list objects with prefix", async () => {
    const doName = "prefix-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "foo/a.txt", Body: "a" })
    );
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "foo/b.txt", Body: "b" })
    );
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "bar/c.txt", Body: "c" })
    );

    const listResult = await s3Client.send(
      new ListObjectsV2Command({ Bucket: bucket, Prefix: "foo/" })
    );

    const keys = listResult.Contents?.map((obj) => obj.Key) || [];
    expect(listResult.KeyCount).toBe(2);
    expect(keys.includes("foo/a.txt")).toBe(true);
    expect(keys.includes("foo/b.txt")).toBe(true);
    expect(keys.includes("bar/c.txt")).toBe(false);
  });

  it("can list objects with delimiter (directory-style listing)", async () => {
    const doName = "delimiter-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    // Create a directory-like structure
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "root.txt", Body: "root" })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "dir1/file1.txt",
        Body: "f1",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "dir1/file2.txt",
        Body: "f2",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "dir1/subdir/file3.txt",
        Body: "f3",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "dir2/file4.txt",
        Body: "f4",
      })
    );

    // List with delimiter at root - should get root.txt as Contents, dir1/ and dir2/ as CommonPrefixes
    const listResult = await s3Client.send(
      new ListObjectsV2Command({ Bucket: bucket, Delimiter: "/" })
    );

    // Should only have root.txt as a direct object
    const keys = listResult.Contents?.map((obj) => obj.Key) || [];
    expect(keys.includes("root.txt")).toBe(true);
    expect(keys.includes("dir1/file1.txt")).toBe(false); // Should be collapsed into CommonPrefix

    // Should have dir1/ and dir2/ as common prefixes
    const prefixes = listResult.CommonPrefixes?.map((p) => p.Prefix) || [];
    expect(prefixes.includes("dir1/")).toBe(true);
    expect(prefixes.includes("dir2/")).toBe(true);
  });

  it("can list objects with prefix and delimiter", async () => {
    const doName = "prefix-delimiter-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    // Create nested structure like node_modules
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "app/node_modules/lodash/index.js",
        Body: "a",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "app/node_modules/lodash/package.json",
        Body: "b",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "app/node_modules/express/index.js",
        Body: "c",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "app/node_modules/express/lib/router.js",
        Body: "d",
      })
    );

    // List with prefix "app/node_modules/" and delimiter "/" - should get package directories
    const listResult = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: "app/node_modules/",
        Delimiter: "/",
      })
    );

    // Should have no direct Contents (all files are in subdirectories)
    const keys = listResult.Contents?.map((obj) => obj.Key) || [];
    expect(keys.length).toBe(0);

    // Should have lodash/ and express/ as common prefixes
    const prefixes = listResult.CommonPrefixes?.map((p) => p.Prefix) || [];
    expect(prefixes.includes("app/node_modules/lodash/")).toBe(true);
    expect(prefixes.includes("app/node_modules/express/")).toBe(true);
    expect(prefixes.length).toBe(2);
  });

  it("can list objects with non-slash delimiter", async () => {
    const doName = "non-slash-delimiter-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    // Create keys using "-" as a logical delimiter
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "logs-2024-01-01.txt",
        Body: "a",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "logs-2024-01-02.txt",
        Body: "b",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "logs-2024-02-01.txt",
        Body: "c",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "logs-2025-01-01.txt",
        Body: "d",
      })
    );
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "other.txt", Body: "e" })
    );

    // List with prefix "logs-" and delimiter "-" - should group by year
    const listResult = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: "logs-",
        Delimiter: "-",
      })
    );

    // Should have no direct Contents (all have delimiter after prefix)
    const keys = listResult.Contents?.map((obj) => obj.Key) || [];
    expect(keys.length).toBe(0);

    // Should have "logs-2024-" and "logs-2025-" as common prefixes
    const prefixes = listResult.CommonPrefixes?.map((p) => p.Prefix) || [];
    expect(prefixes.includes("logs-2024-")).toBe(true);
    expect(prefixes.includes("logs-2025-")).toBe(true);
    expect(prefixes.length).toBe(2);

    // "other.txt" should not appear (doesn't match prefix)
  });

  it("can list objects with prefix containing special characters", async () => {
    const doName = "prefix-special-chars-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    // Test with characters that would break LIKE patterns: %, _, and other special chars
    const specialPrefix =
      "2412b134a4ad3ccbb04eabcb221eb96bca21c62b:package.json/";

    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: `${specialPrefix}file1.txt`,
        Body: "a",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: `${specialPrefix}file2.txt`,
        Body: "b",
      })
    );
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "other/file.txt", Body: "c" })
    );

    const listResult = await s3Client.send(
      new ListObjectsV2Command({ Bucket: bucket, Prefix: specialPrefix })
    );

    const keys = listResult.Contents?.map((obj) => obj.Key) || [];
    expect(listResult.KeyCount).toBe(2);
    expect(keys.includes(`${specialPrefix}file1.txt`)).toBe(true);
    expect(keys.includes(`${specialPrefix}file2.txt`)).toBe(true);
    expect(keys.includes("other/file.txt")).toBe(false);
  });

  it("can list objects with prefix containing LIKE special characters", async () => {
    const doName = "prefix-like-chars-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    // Test with LIKE pattern special characters: % and _
    const specialPrefix = "test_prefix%weird/";

    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: `${specialPrefix}file1.txt`,
        Body: "a",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: `${specialPrefix}file2.txt`,
        Body: "b",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "test_other/file.txt",
        Body: "c",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "testXprefixYweird/file.txt",
        Body: "d",
      })
    );

    const listResult = await s3Client.send(
      new ListObjectsV2Command({ Bucket: bucket, Prefix: specialPrefix })
    );

    const keys = listResult.Contents?.map((obj) => obj.Key) || [];
    expect(listResult.KeyCount).toBe(2);
    expect(keys.includes(`${specialPrefix}file1.txt`)).toBe(true);
    expect(keys.includes(`${specialPrefix}file2.txt`)).toBe(true);
    // These should NOT match - they would match if we were using LIKE incorrectly
    expect(keys.includes("test_other/file.txt")).toBe(false);
    expect(keys.includes("testXprefixYweird/file.txt")).toBe(false);
  });

  it("can handle large files with chunking", async () => {
    const doName = "chunk-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;
    const key = "large-file.bin";

    const size = 2 * 1024 * 1024;
    const largeData = new Uint8Array(size);
    for (let i = 0; i < size; i++) {
      largeData[i] = i % 256;
    }

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: key, Body: largeData })
    );

    const getResult = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: key })
    );

    expect(getResult.ContentLength).toBe(size);
    const retrieved = await getResult.Body?.transformToByteArray();
    expect(retrieved?.length).toBe(size);
    expect(retrieved?.[0]).toBe(0);
    expect(retrieved?.[size - 1]).toBe((size - 1) % 256);
  });

  it("can create and complete multipart upload", async () => {
    const doName = "multipart-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;
    const key = "multipart-file.txt";

    const createResult = await s3Client.send(
      new CreateMultipartUploadCommand({ Bucket: bucket, Key: key })
    );
    const uploadId = createResult.UploadId!;
    expect(uploadId).toBeTruthy();

    const part1Data = "part 1 data";
    const part1Result = await s3Client.send(
      new UploadPartCommand({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
        PartNumber: 1,
        Body: part1Data,
      })
    );

    const part2Data = "part 2 data";
    const part2Result = await s3Client.send(
      new UploadPartCommand({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
        PartNumber: 2,
        Body: part2Data,
      })
    );

    await s3Client.send(
      new CompleteMultipartUploadCommand({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: {
          Parts: [
            { PartNumber: 1, ETag: part1Result.ETag },
            { PartNumber: 2, ETag: part2Result.ETag },
          ],
        },
      })
    );

    const getResult = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: key })
    );
    const content = await getResult.Body?.transformToString();
    expect(content).toBe(part1Data + part2Data);
  });

  it("can abort multipart upload", async () => {
    const doName = "abort-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;
    const key = "aborted-file.txt";

    const createResult = await s3Client.send(
      new CreateMultipartUploadCommand({ Bucket: bucket, Key: key })
    );
    const uploadId = createResult.UploadId!;

    await s3Client.send(
      new UploadPartCommand({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
        PartNumber: 1,
        Body: "test data",
      })
    );

    const abortResult = await s3Client.send(
      new AbortMultipartUploadCommand({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
      })
    );
    expect(abortResult.$metadata.httpStatusCode).toBe(204);

    await expect(
      s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }))
    ).rejects.toThrow();
  });

  it("returns 404 for non-existent object", async () => {
    const doName = "not-found-test";
    const s3Client = await createS3ClientForBucket(doName);

    await expect(
      s3Client.send(
        new GetObjectCommand({
          Bucket: doName,
          Key: "does-not-exist.txt",
        })
      )
    ).rejects.toThrow();
  });

  it("handles keys with special characters and URL encoding", async () => {
    const doName = "special-chars-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;
    const key = "path/to/file with spaces & special-chars!.txt";
    const content = "special content";

    // PUT object (SDK will URL-encode the key in the request)
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: key, Body: content })
    );

    // GET object (SDK will URL-encode the key in the request)
    const getResult = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: key })
    );
    const retrieved = await getResult.Body?.transformToString();
    expect(retrieved).toBe(content);

    // List objects to verify the key is stored decoded (not URL-encoded)
    const listResult = await s3Client.send(
      new ListObjectsV2Command({ Bucket: bucket })
    );
    const keys = listResult.Contents?.map((obj) => obj.Key) || [];
    expect(keys.length).toBe(1);
    expect(keys[0]).toBe(key); // Should be decoded, with actual spaces and special chars
    // Verify no URL encoding in the stored key
    expect(keys[0]?.includes("%20")).toBe(false); // Should NOT contain URL-encoded space
    expect(keys[0]?.includes("%26")).toBe(false); // Should NOT contain URL-encoded &
    expect(keys[0]?.includes("%21")).toBe(false); // Should NOT contain URL-encoded !
  });

  it("preserves trailing slashes in keys for directory markers", async () => {
    const doName = "trailing-slash-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "foo", Body: "file content" })
    );

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "foo/", Body: "" })
    );

    const listResult = await s3Client.send(
      new ListObjectsV2Command({ Bucket: bucket })
    );

    const keys = listResult.Contents?.map((obj) => obj.Key) || [];
    expect(keys).toHaveLength(2);
    expect(keys.includes("foo")).toBe(true);
    expect(keys.includes("foo/")).toBe(true);
  });

  it("can GET directory markers with trailing slash", async () => {
    const doName = "dir-marker-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "dir/", Body: "" })
    );

    const getResult = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: "dir/" })
    );

    expect(getResult.ContentLength).toBe(0);

    const headResult = await s3Client.send(
      new HeadObjectCommand({ Bucket: bucket, Key: "dir/" })
    );

    expect(headResult.ContentLength).toBe(0);
  });

  it("can DELETE directory markers with trailing slash", async () => {
    const doName = "delete-dir-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "mydir/", Body: "" })
    );

    const deleteResult = await s3Client.send(
      new DeleteObjectCommand({ Bucket: bucket, Key: "mydir/" })
    );
    expect(deleteResult.$metadata.httpStatusCode).toBe(204);

    await expect(
      s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: "mydir/" }))
    ).rejects.toThrow();
  });

  it("can DELETE deeply nested directory markers with trailing slash", async () => {
    const doName = "delete-nested-dir-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    // This mimics the real failing case: my-react-router-app/node_modules/iconv-lite/encodings/
    const deepKey = "my-react-router-app/node_modules/iconv-lite/encodings/";

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: deepKey, Body: "" })
    );

    // Verify it exists
    const headResult = await s3Client.send(
      new HeadObjectCommand({ Bucket: bucket, Key: deepKey })
    );
    expect(headResult.$metadata.httpStatusCode).toBe(200);

    // Delete it
    const deleteResult = await s3Client.send(
      new DeleteObjectCommand({ Bucket: bucket, Key: deepKey })
    );
    expect(deleteResult.$metadata.httpStatusCode).toBe(204);

    // Verify it's gone
    await expect(
      s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: deepKey }))
    ).rejects.toThrow();
  });

  it("returns 204 when deleting non-existent object (S3 standard behavior)", async () => {
    const doName = "delete-nonexistent-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    // S3 returns 204 even when deleting an object that doesn't exist
    const deleteResult = await s3Client.send(
      new DeleteObjectCommand({ Bucket: bucket, Key: "does-not-exist.txt" })
    );
    expect(deleteResult.$metadata.httpStatusCode).toBe(204);
  });

  it("treats foo and foo/ as distinct keys", async () => {
    const doName = "distinct-keys-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;
    const fileContent = "this is a file";
    const dirContent = "";

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "item", Body: fileContent })
    );
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "item/", Body: dirContent })
    );

    const fileResult = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: "item" })
    );
    const fileBody = await fileResult.Body?.transformToString();
    expect(fileBody).toBe(fileContent);

    const dirResult = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: "item/" })
    );
    const dirBody = await dirResult.Body?.transformToString();
    expect(dirBody).toBe(dirContent);

    await s3Client.send(
      new DeleteObjectCommand({ Bucket: bucket, Key: "item" })
    );

    const listResult = await s3Client.send(
      new ListObjectsV2Command({ Bucket: bucket })
    );
    const keys = listResult.Contents?.map((obj) => obj.Key) || [];
    expect(keys).toHaveLength(1);
    expect(keys[0]).toBe("item/");
  });

  it("lists directory structure with nested paths and trailing slashes", async () => {
    const doName = "nested-dir-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "a/", Body: "" })
    );
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: "a/b/", Body: "" })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "a/b/file.txt",
        Body: "data",
      })
    );
    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: "a/file2.txt",
        Body: "data2",
      })
    );

    const listResult = await s3Client.send(
      new ListObjectsV2Command({ Bucket: bucket })
    );

    const keys = listResult.Contents?.map((obj) => obj.Key) || [];
    expect(keys).toHaveLength(4);
    expect(keys.includes("a/")).toBe(true);
    expect(keys.includes("a/b/")).toBe(true);
    expect(keys.includes("a/b/file.txt")).toBe(true);
    expect(keys.includes("a/file2.txt")).toBe(true);
  });

  it("can list multipart uploads", async () => {
    const doName = "list-multipart-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    // Create a few multipart uploads
    const upload1 = await s3Client.send(
      new CreateMultipartUploadCommand({ Bucket: bucket, Key: "file1.txt" })
    );
    const upload2 = await s3Client.send(
      new CreateMultipartUploadCommand({ Bucket: bucket, Key: "file2.txt" })
    );
    const upload3 = await s3Client.send(
      new CreateMultipartUploadCommand({ Bucket: bucket, Key: "dir/file3.txt" })
    );

    // List all multipart uploads
    const listResult = await s3Client.send(
      new ListMultipartUploadsCommand({ Bucket: bucket })
    );

    expect(listResult.Uploads).toHaveLength(3);
    const keys = listResult.Uploads?.map((u) => u.Key) || [];
    expect(keys.includes("file1.txt")).toBe(true);
    expect(keys.includes("file2.txt")).toBe(true);
    expect(keys.includes("dir/file3.txt")).toBe(true);

    // Verify upload IDs are present
    const uploadIds = listResult.Uploads?.map((u) => u.UploadId) || [];
    expect(uploadIds.includes(upload1.UploadId)).toBe(true);
    expect(uploadIds.includes(upload2.UploadId)).toBe(true);
    expect(uploadIds.includes(upload3.UploadId)).toBe(true);

    // Clean up
    await s3Client.send(
      new AbortMultipartUploadCommand({
        Bucket: bucket,
        Key: "file1.txt",
        UploadId: upload1.UploadId,
      })
    );
    await s3Client.send(
      new AbortMultipartUploadCommand({
        Bucket: bucket,
        Key: "file2.txt",
        UploadId: upload2.UploadId,
      })
    );
    await s3Client.send(
      new AbortMultipartUploadCommand({
        Bucket: bucket,
        Key: "dir/file3.txt",
        UploadId: upload3.UploadId,
      })
    );
  });

  it("can list multipart uploads with prefix", async () => {
    const doName = "list-multipart-prefix-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    // Create multipart uploads with different prefixes
    const upload1 = await s3Client.send(
      new CreateMultipartUploadCommand({ Bucket: bucket, Key: "foo/a.txt" })
    );
    const upload2 = await s3Client.send(
      new CreateMultipartUploadCommand({ Bucket: bucket, Key: "foo/b.txt" })
    );
    const upload3 = await s3Client.send(
      new CreateMultipartUploadCommand({ Bucket: bucket, Key: "bar/c.txt" })
    );

    // List with prefix
    const listResult = await s3Client.send(
      new ListMultipartUploadsCommand({ Bucket: bucket, Prefix: "foo/" })
    );

    expect(listResult.Uploads).toHaveLength(2);
    const keys = listResult.Uploads?.map((u) => u.Key) || [];
    expect(keys.includes("foo/a.txt")).toBe(true);
    expect(keys.includes("foo/b.txt")).toBe(true);
    expect(keys.includes("bar/c.txt")).toBe(false);

    // Clean up
    await s3Client.send(
      new AbortMultipartUploadCommand({
        Bucket: bucket,
        Key: "foo/a.txt",
        UploadId: upload1.UploadId,
      })
    );
    await s3Client.send(
      new AbortMultipartUploadCommand({
        Bucket: bucket,
        Key: "foo/b.txt",
        UploadId: upload2.UploadId,
      })
    );
    await s3Client.send(
      new AbortMultipartUploadCommand({
        Bucket: bucket,
        Key: "bar/c.txt",
        UploadId: upload3.UploadId,
      })
    );
  });

  it("returns empty list when no multipart uploads exist", async () => {
    const doName = "empty-multipart-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    const listResult = await s3Client.send(
      new ListMultipartUploadsCommand({ Bucket: bucket })
    );

    expect(listResult.Uploads).toBeUndefined();
    expect(listResult.IsTruncated).toBe(false);
  });

  it("can copy an object", async () => {
    const doName = "copy-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;
    const sourceKey = "source-file.txt";
    const destKey = "dest-file.txt";
    const content = "Hello, this is the content to copy!";

    // Create source object
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: sourceKey, Body: content })
    );

    // Copy object
    const copyResult = await s3Client.send(
      new CopyObjectCommand({
        Bucket: bucket,
        Key: destKey,
        CopySource: `${bucket}/${sourceKey}`,
      })
    );

    expect(copyResult.$metadata.httpStatusCode).toBe(200);
    expect(copyResult.CopyObjectResult?.ETag).toBeTruthy();
    expect(copyResult.CopyObjectResult?.LastModified).toBeTruthy();

    // Verify destination object exists with correct content
    const getResult = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: destKey })
    );
    const destContent = await getResult.Body?.transformToString();
    expect(destContent).toBe(content);

    // Verify source object still exists
    const sourceResult = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: sourceKey })
    );
    const sourceContent = await sourceResult.Body?.transformToString();
    expect(sourceContent).toBe(content);
  });

  it("can copy a large chunked object", async () => {
    const doName = "copy-large-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;
    const sourceKey = "large-source.bin";
    const destKey = "large-dest.bin";

    // Create a file larger than CHUNK_SIZE (1MB)
    const size = 2 * 1024 * 1024; // 2MB
    const largeData = new Uint8Array(size);
    for (let i = 0; i < size; i++) {
      largeData[i] = i % 256;
    }

    // Create source object
    await s3Client.send(
      new PutObjectCommand({ Bucket: bucket, Key: sourceKey, Body: largeData })
    );

    // Copy object
    const copyResult = await s3Client.send(
      new CopyObjectCommand({
        Bucket: bucket,
        Key: destKey,
        CopySource: `${bucket}/${sourceKey}`,
      })
    );

    expect(copyResult.$metadata.httpStatusCode).toBe(200);

    // Verify destination object has correct size and content
    const getResult = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: destKey })
    );
    expect(getResult.ContentLength).toBe(size);

    const destData = await getResult.Body?.transformToByteArray();
    expect(destData?.length).toBe(size);
    expect(destData?.[0]).toBe(0);
    expect(destData?.[size - 1]).toBe((size - 1) % 256);
  });

  it("returns error when copying non-existent source", async () => {
    const doName = "copy-error-test";
    const s3Client = await createS3ClientForBucket(doName);
    const bucket = doName;

    await expect(
      s3Client.send(
        new CopyObjectCommand({
          Bucket: bucket,
          Key: "dest.txt",
          CopySource: `${bucket}/non-existent.txt`,
        })
      )
    ).rejects.toThrow();
  });
});
