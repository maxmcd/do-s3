import { createRequestHandler } from "react-router";
import { Container } from "@cloudflare/containers";
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

async function shaString(str: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(str);
  const hashBuffer = await crypto.subtle.digest("SHA-256", data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("")
    .slice(0, 10);
}

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
    if (url.pathname === "/s3-logs-ws") {
      return this.handleS3LogsWebSocket(request);
    }
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
      const bucket = `s3-${await shaString(doId)}`;

      // Use a shared secret from environment (or hardcoded for demo)
      const secret = this.env.S3_JWT_SECRET;

      // Generate JWT with bucket name in payload
      const token = await signToken(
        {
          sub: terminalName,
          bucket: bucket,
          expiresIn: 3600 * 24 * 7, // 7 days
        },
        secret
      );

      const requestWithEnv = this.createContainerRequest(request, token);
      return terminalDO.fetch(requestWithEnv);
    } catch (error) {
      return new Response(
        error instanceof Error ? error.message : "Internal server error",
        { status: 500 }
      );
    }
  }

  private createContainerRequest(request: Request, token: string): Request {
    // For some reason, in dev, the url host doesn't contain the port.
    const hostHeader = request.headers.get("host") || "localhost";
    return setContainerEnv(request, {
      S3_AUTH_TOKEN: token,
      HOST: hostHeader,
    });
  }

  private async handleS3LogsWebSocket(request: Request): Promise<Response> {
    // Get the terminal name from query params
    const url = new URL(request.url);
    const terminalName = url.searchParams.get("name") || "default";
    // Get the S3 DO for this terminal
    const doId = this.env.TERMINAL.idFromName(terminalName).toString();

    // Forward the WebSocket upgrade request to the S3 DO
    return this.env.S3.getByName(await shaString(doId)).fetch(request);
  }

  private async handleS3Request(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const pathMatch = url.pathname.match(/^\/([^\/]+)/);
    if (!pathMatch) {
      return new Response("Invalid S3 path", { status: 400 });
    }
    const bucket = pathMatch[1].slice(3);
    const resp = await this.env.S3.getByName(bucket).fetch(request);
    return resp;
  }

  private handleReactRouterRequest(request: Request): Promise<Response> {
    return requestHandler(request, {
      cloudflare: { env: this.env, ctx: this.ctx },
    });
  }
}

export default {
  fetch: async (req: Request, env: Env, ctx: ExecutionContext) => {
    const startTime = Date.now();
    const url = new URL(req.url);
    const resp = await new Worker(env, ctx).fetch(req);
    const duration = Date.now() - startTime;
    console.log(
      `[${req.method}] ${url.pathname}${url.search} - ${resp.status} (${duration}ms)`
    );
    return resp;
  },
};
