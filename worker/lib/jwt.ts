import { SignJWT, jwtVerify } from "jose";

export interface S3TokenPayload {
  sub: string; // Terminal name
  bucket: string; // s3-{doId}
  exp: number;
  iat: number;
}

export async function signToken(
  payload: { sub: string; bucket: string; expiresIn: number },
  secret: string
): Promise<string> {
  const encoder = new TextEncoder();
  const secretKey = encoder.encode(secret);

  const jwt = await new SignJWT({
    sub: payload.sub,
    bucket: payload.bucket,
  })
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    .setExpirationTime(Math.floor(Date.now() / 1000) + payload.expiresIn)
    .sign(secretKey);

  return jwt;
}

export async function verifyToken(
  token: string,
  secrets: string[]
): Promise<S3TokenPayload> {
  const encoder = new TextEncoder();

  // Try each secret (for rotation support)
  for (const secret of secrets) {
    try {
      const secretKey = encoder.encode(secret);
      const { payload } = await jwtVerify(token, secretKey);

      // Validate required fields
      if (!payload.sub || !payload.bucket) {
        throw new Error("Missing required fields");
      }

      return {
        sub: payload.sub as string,
        bucket: payload.bucket as string,
        exp: payload.exp as number,
        iat: payload.iat as number,
      };
    } catch (err) {
      // Try next secret
      continue;
    }
  }

  throw new Error("Invalid token");
}
