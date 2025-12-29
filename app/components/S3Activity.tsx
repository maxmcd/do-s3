import { useEffect, useRef, useState } from "react";

interface S3Request {
  method: string;
  path: string;
  status: number;
  duration: number;
  timestamp: string;
}

export function S3Activity() {
  const [requests, setRequests] = useState<S3Request[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function connect() {
      const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
      const ws = new WebSocket(
        `${protocol}//${window.location.host}/s3-logs-ws?name=default`
      );

      ws.onopen = () => {
        setIsConnected(true);
      };

      ws.onmessage = (event) => {
        try {
          const request: S3Request = JSON.parse(event.data);
          setRequests((prev) => {
            // Keep only the last 50 requests
            const newRequests = [request, ...prev].slice(0, 50);
            return newRequests;
          });
        } catch (err) {
          console.error("Failed to parse S3 request:", err);
        }
      };

      ws.onclose = () => {
        setIsConnected(false);
        // Reconnect after 2 seconds
        setTimeout(connect, 2000);
      };

      ws.onerror = () => {
        setIsConnected(false);
      };

      wsRef.current = ws;
    }

    connect();

    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, []);

  const getStatusColor = (status: number) => {
    if (status >= 200 && status < 300) return "text-green-600";
    if (status >= 300 && status < 400) return "text-blue-600";
    if (status >= 400 && status < 500) return "text-orange-600";
    return "text-red-600";
  };

  const getMethodColor = (method: string) => {
    switch (method) {
      case "GET":
        return "text-blue-600 bg-blue-50";
      case "PUT":
        return "text-green-600 bg-green-50";
      case "POST":
        return "text-purple-600 bg-purple-50";
      case "DELETE":
        return "text-red-600 bg-red-50";
      case "HEAD":
        return "text-gray-600 bg-gray-50";
      default:
        return "text-gray-600 bg-gray-50";
    }
  };

  const formatSize = (bytes: number) => {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + " " + sizes[i];
  };

  const formatTime = (timestamp: string) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString("en-US", {
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      fractionalSecondDigits: 3,
    });
  };

  return (
    <div className="w-full max-w-6xl">
      <div className="bg-white rounded-xl shadow-2xl overflow-hidden flex flex-col h-[400px]">
        {/* Content */}
        <div
          ref={scrollRef}
          className="flex-1 overflow-auto bg-gray-50 font-mono text-xs"
        >
          {!isConnected && (
            <div className="flex items-center justify-center text-gray-500">
              Connecting...
            </div>
          )}
          {requests.length === 0 ? (
            <div className="flex items-center justify-center h-full text-gray-500">
              Waiting for S3 requests...
            </div>
          ) : (
            <table className="w-full">
              <thead className="sticky top-0 bg-gray-100 border-b border-gray-300">
                <tr className="text-left">
                  <th className="px-3 py-2 font-semibold text-gray-700">
                    Time
                  </th>
                  <th className="px-3 py-2 font-semibold text-gray-700">
                    Method
                  </th>
                  <th className="px-3 py-2 font-semibold text-gray-700">
                    Path
                  </th>
                  <th className="px-3 py-2 font-semibold text-gray-700">
                    Status
                  </th>
                  <th className="px-3 py-2 font-semibold text-gray-700">
                    Duration
                  </th>
                </tr>
              </thead>
              <tbody>
                {requests.map((req, idx) => (
                  <tr
                    key={`${req.timestamp}-${idx}`}
                    className="border-b border-gray-200 hover:bg-gray-100"
                  >
                    <td className="px-3 py-2 text-gray-600 whitespace-nowrap">
                      {formatTime(req.timestamp)}
                    </td>
                    <td className="px-3 py-2">
                      <span
                        className={`px-2 py-0.5 rounded font-semibold ${getMethodColor(req.method)}`}
                      >
                        {req.method}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-gray-700 truncate max-w-md">
                      {req.path}
                    </td>
                    <td
                      className={`px-3 py-2 font-semibold ${getStatusColor(req.status)}`}
                    >
                      {req.status}
                    </td>
                    <td className="px-3 py-2 text-gray-600">
                      {req.duration}ms
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}
