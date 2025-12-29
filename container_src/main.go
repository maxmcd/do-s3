package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
)

const (
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	dataDir    = "/data"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// Allow all origins for development
		return true
	},
}

type ptySession struct {
	cmd    *exec.Cmd
	ptmx   *os.File
	ws     *websocket.Conn
	mu     sync.Mutex
	closed bool
}

type resizeMessage struct {
	Type string `json:"type"`
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

// waitForMount polls until the directory is a FUSE mount (not a regular directory)
func waitForMount(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	const FUSE_SUPER_MAGIC = 0x65735546 // FUSE filesystem magic number

	for range ticker.C {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(path, &stat); err == nil {
			// Check if it's a FUSE filesystem
			if stat.Type == FUSE_SUPER_MAGIC {
				log.Printf("Mount at %s is ready (FUSE detected)", path)
				return nil
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for FUSE mount at %s", path)
		}
	}
	return fmt.Errorf("ticker closed unexpectedly")
}

func getShell() string {
	if runtime.GOOS == "windows" {
		if comspec := os.Getenv("COMSPEC"); comspec != "" {
			return comspec
		}
		return "cmd.exe"
	}
	if shell := os.Getenv("SHELL"); shell != "" {
		return shell
	}
	return "/bin/bash"
}

func (s *ptySession) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true

	if s.ptmx != nil {
		s.ptmx.Close()
	}
	if s.cmd != nil && s.cmd.Process != nil {
		s.cmd.Process.Kill()
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Parse query params
	cols := 80
	rows := 24

	if colsStr := r.URL.Query().Get("cols"); colsStr != "" {
		if c, err := strconv.Atoi(colsStr); err == nil {
			cols = c
		}
	}
	if rowsStr := r.URL.Query().Get("rows"); rowsStr != "" {
		if rowsValue, err := strconv.Atoi(rowsStr); err == nil {
			rows = rowsValue
		}
	}

	// Upgrade to WebSocket
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}
	defer ws.Close()

	// Set up pong handler
	ws.SetReadDeadline(time.Now().Add(pongWait))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Create shell command
	shell := getShell()
	cmd := exec.Command(shell)
	cmd.Dir = dataDir
	cmd.Env = append(os.Environ(),
		"TERM=xterm-256color",
		"COLORTERM=truecolor",
	)

	// Start PTY
	ptmx, err := pty.Start(cmd)
	if err != nil {
		log.Printf("Failed to start PTY: %v", err)
		return
	}

	session := &ptySession{
		cmd:  cmd,
		ptmx: ptmx,
		ws:   ws,
	}
	defer session.close()

	// Set initial size
	if err := pty.Setsize(ptmx, &pty.Winsize{
		Rows: uint16(rows),
		Cols: uint16(cols),
	}); err != nil {
		log.Printf("Failed to set PTY size: %v", err)
	}

	// Start ping ticker to keep connection alive
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			session.mu.Lock()
			if session.closed {
				session.mu.Unlock()
				return
			}
			if err := ws.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
				log.Printf("Ping error: %v", err)
				session.mu.Unlock()
				return
			}
			session.mu.Unlock()
		}
	}()

	// PTY -> WebSocket (read from PTY, send to browser)
	go func() {
		buf := make([]byte, 8192)
		for {
			n, err := ptmx.Read(buf)
			if err != nil {
				if err != io.EOF {
					log.Printf("PTY read error: %v", err)
				}
				return
			}

			session.mu.Lock()
			if !session.closed {
				if err := ws.WriteMessage(websocket.TextMessage, buf[:n]); err != nil {
					log.Printf("WebSocket write error: %v", err)
					session.mu.Unlock()
					return
				}
			}
			session.mu.Unlock()
		}
	}()

	// WebSocket -> PTY (read from browser, write to PTY)
	for {
		msgType, data, err := ws.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket read error: %v", err)
			}
			break
		}

		if msgType == websocket.TextMessage {
			msg := string(data)

			// Check if it's a resize message
			if len(msg) > 0 && msg[0] == '{' {
				var resize resizeMessage
				if err := json.Unmarshal(data, &resize); err == nil && resize.Type == "resize" {
					if err := pty.Setsize(ptmx, &pty.Winsize{
						Rows: resize.Rows,
						Cols: resize.Cols,
					}); err != nil {
						log.Printf("Failed to resize PTY: %v", err)
					}
					continue
				}
			}

			// Regular input - write to PTY
			if _, err := ptmx.Write(data); err != nil {
				log.Printf("PTY write error: %v", err)
				break
			}
		}
	}

	// Wait for command to finish
	cmd.Wait()
}

func main() {
	loc := os.Getenv("CLOUDFLARE_LOCATION")

	// Don't mount fuse in local docker
	if loc != "" && loc != "loc01" {
		// Get Durable Object ID to use as S3 bucket name for isolation
		doID := os.Getenv("CLOUDFLARE_DURABLE_OBJECT_ID")
		if doID == "" {
			log.Fatalf("CLOUDFLARE_DURABLE_OBJECT_ID not set")
		}
		log.Printf("Using Durable Object ID as S3 bucket: %s", doID)

		// Get S3 auth token
		s3Token := os.Getenv("S3_AUTH_TOKEN")
		if s3Token == "" {
			log.Fatalf("S3_AUTH_TOKEN not set")
		}

		// Create mount point directory
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			log.Fatalf("Failed to create directory: %v", err)
		}

		bucket := fmt.Sprintf("s3-%s", doID)

		go func() {
			// Use Durable Object ID as the S3 bucket name for per-computer isolation
			cmd := exec.Command("/usr/local/bin/tigrisfs",
				"--endpoint", fmt.Sprintf("https://%s/", os.Getenv("HOST")),
				"--debug_s3",
				"--debug",
				"-f",
				bucket,
				dataDir)
			// Pass JWT token as AWS access key ID
			// tigrisfs will include this in the Authorization header's Credential field
			// Format: "AWS4-HMAC-SHA256 Credential=<jwt>/20231201/auto/s3/aws4_request, ..."
			// Our S3 DO extracts the JWT from the Credential field
			cmd.Env = append(os.Environ(),
				"AWS_ACCESS_KEY_ID="+s3Token,
				"AWS_SECRET_ACCESS_KEY=not-used", // Required by tigrisfs but ignored by S3 DO
			)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			if err := cmd.Run(); err != nil {
				log.Fatalf("tigrisfs failed: %v", err)
			}
			log.Fatalf("tigrisfs exited unexpectedly")
		}()

		// Wait for FUSE mount to be ready before proceeding
		log.Printf("Waiting for FUSE mount at %s...", dataDir)
		if err := waitForMount(dataDir, 10*time.Second); err != nil {
			log.Fatalf("Failed to wait for mount: %v", err)
		}
	}

	// Listen for SIGINT and SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	router := http.NewServeMux()

	// WebSocket endpoint for PTY
	router.HandleFunc("/ws", handleWebSocket)

	// Simple health check endpoint
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		instanceId := os.Getenv("CLOUDFLARE_DURABLE_OBJECT_ID")
		fmt.Fprintf(w, "Terminal server ready. Instance ID: %s", instanceId)
	})

	server := &http.Server{
		Addr:    ":8283",
		Handler: router,
	}

	go func() {
		log.Printf("Server listening on %s\n", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// Wait to receive a signal
	sig := <-stop

	log.Printf("Received signal (%s), shutting down server...", sig)

	// Give the server 5 seconds to shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}

	log.Println("Server shutdown successfully")
}
