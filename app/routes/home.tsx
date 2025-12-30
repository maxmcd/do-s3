import { useEffect, useRef, useState } from "react";
import { Window } from "../components/Window";
import { S3Activity } from "../components/S3Activity";

export function meta() {
  return [
    { title: "do-s3" },
    {
      name: "description",
      content:
        "A Cloudflare Container that uses a Durable Object for persistent filesystem storage.",
    },
  ];
}

export async function loader({ context }: any) {
  const { env } = context.cloudflare;
  return {};
}

export default function Home() {
  const containerRef = useRef<HTMLDivElement>(null);
  const terminalRef = useRef<any>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const fitAddonRef = useRef<any>(null);
  const [status, setStatus] = useState<
    "connecting" | "connected" | "disconnected"
  >("connecting");
  const [statusText, setStatusText] = useState("Connecting...");
  const [reconnectMessage, setReconnectMessage] = useState("");

  // Initialize terminal
  useEffect(() => {
    if (!containerRef.current) return;

    let mounted = true;

    async function initTerminal() {
      if (!containerRef.current) return;

      const { init, Terminal, FitAddon } = await import("ghostty-web");
      if (!mounted) return;

      await init();

      const term = new Terminal({
        cols: 80,
        rows: 24,
        fontFamily: "JetBrains Mono, Menlo, Monaco, monospace",
        fontSize: 14,
        cursorBlink: true,
        cursorStyle: "block",
        theme: {
          background: "#ffffff",
          foreground: "#1f2937",
          cursor: "#e879f9",
        },
      });

      const fitAddon = new FitAddon();
      term.loadAddon(fitAddon);

      await term.open(containerRef.current);
      fitAddon.fit();
      fitAddon.observeResize();

      terminalRef.current = term;
      fitAddonRef.current = fitAddon;

      function connect() {
        setStatus("connecting");
        setStatusText("Connecting...");
        let connected = false;

        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl = `${protocol}//${window.location.host}/ws?cols=${term.cols}&rows=${term.rows}`;
        const ws = new WebSocket(wsUrl);

        ws.onopen = () => {
          connected = true;
          setStatus("connected");
          setStatusText("Connected");
          setReconnectMessage("");
        };

        ws.onmessage = (event) => {
          term.write(event.data);
        };

        ws.onclose = () => {
          setStatus("disconnected");
          setStatusText("Disconnected");
          setReconnectMessage("Reconnecting in 2s...");
          setTimeout(connect, 2000);
        };

        ws.onerror = () => {
          setStatus("disconnected");
          setStatusText("Error");
        };

        wsRef.current = ws;
      }

      connect();

      term.onData((data: string) => {
        if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
          wsRef.current.send(data);
        }
      });

      term.onResize(({ cols, rows }: { cols: number; rows: number }) => {
        if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
          wsRef.current.send(JSON.stringify({ type: "resize", cols, rows }));
        }
      });

      let resizeTimeout: ReturnType<typeof setTimeout>;
      function debouncedFit() {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(() => fitAddon.fit(), 50);
      }

      window.addEventListener("resize", debouncedFit);
      const resizeObserver = new ResizeObserver(debouncedFit);
      if (containerRef.current) {
        resizeObserver.observe(containerRef.current);
      }

      return () => {
        mounted = false;
        window.removeEventListener("resize", debouncedFit);
        resizeObserver.disconnect();
        if (wsRef.current) {
          wsRef.current.close();
        }
        if (terminalRef.current) {
          terminalRef.current.dispose();
        }
      };
    }

    initTerminal();

    return () => {
      mounted = false;
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, []);

  return (
    <div className="min-h-screen w-full flex flex-col bg-gradient-to-br from-pink-200 via-purple-200 to-indigo-300">
      <style>{`
        body {
          background: linear-gradient(135deg, #fbcfe8 0%, #e9d5ff 50%, #c7d2fe 100%);
          margin: 0;
        }
      `}</style>

      {/* Content Container */}
      <div className="pt-6 flex-1 flex flex-col items-center px-4 md:px-10 pb-10 gap-6">
        {/* Documentation Section */}
        <div className="w-full max-w-4xl bg-white/80 backdrop-blur-sm rounded-xl shadow-lg p-6">
          <h2 className="text-xl font-bold text-purple-900 mb-3 font-mono">
            Cloudflare Container Durable Object FUSE Mount
          </h2>
          <p className="text-gray-700 leading-relaxed">
            This is a cloudflare container that uses a Durable Object for
            persistent filesystem storage. The
            <code className="bg-purple-100 px-2 py-0.5 rounded text-sm">
              /data
            </code>{" "}
            dir is a FUSE mount that connects to a DO over{" "}
            <a
              className="text-purple-600 underline"
              href="https://github.com/maxmcd/do-s3/blob/main/worker/s3.ts"
            >
              an S3 api
            </a>
            .
          </p>
          <p className="text-gray-700 leading-relaxed mt-2">
            Run{" "}
            <code className="bg-purple-100 px-2 py-0.5 rounded text-sm">
              rm -rf node_modules && bun install
            </code>{" "}
            to generate lots of request logs.
          </p>
        </div>

        {/* Terminal Window */}
        <div className="w-full max-w-6xl h-[600px] flex flex-col">
          <Window
            title={
              <>
                Terminal
                {reconnectMessage && (
                  <span className="text-pink-600 text-xs ml-4">
                    {reconnectMessage}
                  </span>
                )}
              </>
            }
            rightContent={
              <div className="flex items-center gap-1.5 text-[11px] text-purple-700">
                <div
                  className={`w-2 h-2 rounded-full ${
                    status === "connected"
                      ? "bg-green-400"
                      : status === "disconnected"
                        ? "bg-pink-400"
                        : "bg-purple-400"
                  }`}
                ></div>
                <span>{statusText}</span>
              </div>
            }
          >
            <div
              ref={containerRef}
              className="terminal-container flex-1 min-h-0 bg-white relative overflow-hidden"
              style={{ caretColor: "transparent" }}
            ></div>
          </Window>
        </div>

        {/* S3 Activity Stream */}
        <S3Activity />
      </div>
    </div>
  );
}
