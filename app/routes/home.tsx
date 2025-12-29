import { useEffect, useRef, useState } from "react";
import { Window } from "../components/Window";
import { S3Activity } from "../components/S3Activity";

export function meta() {
  return [
    { title: "Terminal Demo" },
    { name: "description", content: "A simple terminal emulator demo" },
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
            This is a demo terminal running in a Cloudflare Worker container
            with S3-backed persistent storage. Files are mounted at{" "}
            <code className="bg-purple-100 px-2 py-0.5 rounded text-sm">
              /data
            </code>{" "}
            and automatically synced to Durable Object storage via tigrisfs. Try
            creating files, running commands, and they'll persist across
            sessions!
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
