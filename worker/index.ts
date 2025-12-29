import { createRequestHandler } from "react-router";
import { Container, getContainer, getRandom } from "@cloudflare/containers";
import { S3 } from "./s3";
import { signToken } from "./lib/jwt";
export { S3 };

declare module "react-router" {
  export interface AppLoadContext {
    cloudflare: {
      env: Env;
      ctx: ExecutionContext;
    };
  }
}

const requestHandler = createRequestHandler(
  () => import("virtual:react-router/server-build"),
  import.meta.env.MODE
);

const CONTAINER_ENV_HEADER = "X-Container-Env";

function setContainerEnv(
  request: Request,
  envVars: Record<string, string>
): Request {
  const newRequest = new Request(request.url, request);
  for (const [key, value] of request.headers.entries()) {
    newRequest.headers.set(key, value);
  }
  newRequest.headers.set(CONTAINER_ENV_HEADER, JSON.stringify(envVars));
  return newRequest;
}

function getContainerEnv(request: Request): Record<string, string> {
  const header = request.headers.get(CONTAINER_ENV_HEADER);
  if (!header) return {};
  try {
    return JSON.parse(header);
  } catch {
    return {};
  }
}

export class Terminal extends Container<Env> {
  // Port the container listens on (default: 8283)
  defaultPort = 8283;
  // Time before container sleeps due to inactivity (default: 30s)
  sleepAfter = "2m";
  // Environment variables passed to the container
  envVars = {
    S3_AUTH_TOKEN: "",
  };

  // Override fetch to extract env vars from header and set them
  override async fetch(request: Request): Promise<Response> {
    const envVars = getContainerEnv(request);

    if (Object.keys(envVars).length > 0) {
      this.envVars = { ...this.envVars, ...envVars };

      // Remove the env header before passing to container
      const cleanRequest = new Request(request.url, request);
      cleanRequest.headers.delete(CONTAINER_ENV_HEADER);

      return super.fetch(cleanRequest);
    }

    return super.fetch(request);
  }
}

class Worker {
  constructor(
    private env: Env,
    private ctx: ExecutionContext
  ) {}

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // Check path-based routes first
    if (url.pathname.startsWith("/s3-")) {
      return this.handleS3Request(request);
    }
    if (url.pathname.startsWith("/ws")) {
      return this.handleWebSocketRequest(request);
    }

    // Fallback to React Router
    return this.handleReactRouterRequest(request);
  }

  private async handleWebSocketRequest(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const terminalName = url.searchParams.get("name") || "default";

    try {
      // Get the Terminal DO to generate JWT token with bucket name
      const terminalDO = this.env.TERMINAL.getByName(terminalName);
      const doId = this.env.TERMINAL.idFromName(terminalName).toString();
      const bucket = `s3-${doId}`;

      // Use a shared secret from environment (or hardcoded for demo)
      const secret = this.env.S3_JWT_SECRET || "demo-secret-change-in-production";

      // Generate JWT with bucket name in payload
      const token = await signToken(
        {
          sub: terminalName,
          bucket: bucket,
          expiresIn: 3600 * 24 * 7, // 7 days
        },
        secret
      );

      const requestWithEnv = this.createContainerRequest(
        request,
        terminalName,
        token
      );
      return terminalDO.fetch(requestWithEnv);
    } catch (error) {
      return new Response(
        error instanceof Error ? error.message : "Internal server error",
        { status: 500 }
      );
    }
  }

  private createContainerRequest(
    request: Request,
    terminalName: string,
    token: string
  ): Request {
    // For some reason, in dev, the url host doesn't contain the port.
    const hostHeader = request.headers.get("host") || "localhost";
    const protocol = request.headers.get("x-forwarded-proto") || "http";
    const origin = `${protocol}://${hostHeader}`;
    return setContainerEnv(request, {
      S3_AUTH_TOKEN: token,
      HOST: hostHeader,
    });
  }

  private async handleS3Request(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const pathMatch = url.pathname.match(/^\/([^\/]+)/);
    if (!pathMatch) {
      return new Response("Invalid S3 path", { status: 400 });
    }
    const bucket = pathMatch[1].slice(4);
    const t0 = performance.now();
    const resp = await this.env.S3.getByName(bucket).fetch(request);
    const t1 = performance.now();
    console.log(`S3 request took ${t1 - t0}ms`);
    return resp;
  }

  private handleReactRouterRequest(request: Request): Promise<Response> {
    return requestHandler(request, {
      cloudflare: { env: this.env, ctx: this.ctx },
    });
  }
}

export default {
  fetch: (request: Request, env: Env, ctx: ExecutionContext) =>
    new Worker(env, ctx).fetch(request),
};
