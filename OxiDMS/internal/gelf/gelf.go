package gelf

import (
	"encoding/json"
	"net"
	"os"
	"strings"
	"time"
)

// Writer sends GELF messages over UDP and implements io.Writer
// so it can be used with log.SetOutput via io.MultiWriter.
type Writer struct {
	conn     net.Conn
	hostname string
}

// New creates a GELF UDP writer connected to addr (e.g. "172.17.0.1:12201").
func New(addr string) (*Writer, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "oxidms-server"
	}

	return &Writer{conn: conn, hostname: hostname}, nil
}

// Write implements io.Writer. Each call sends one GELF message.
// The standard log package writes lines like "2026/02/19 18:43:52 message\n".
// We strip the date prefix and trailing newline for a clean short_message.
func (w *Writer) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")

	// Strip Go log date/time prefix (format: "2006/01/02 15:04:05 ")
	// The prefix is exactly 20 characters when present.
	short := msg
	if len(msg) > 20 && msg[4] == '/' && msg[7] == '/' && msg[10] == ' ' && msg[13] == ':' {
		short = msg[20:]
	}

	level := 6 // Informational
	if strings.Contains(short, "PANIC:") || strings.Contains(short, "Fatal") {
		level = 3 // Error
	} else if strings.HasPrefix(short, "Warning:") {
		level = 4 // Warning
	}

	gelf := map[string]interface{}{
		"version":       "1.1",
		"host":          w.hostname,
		"short_message": short,
		"timestamp":     float64(time.Now().UnixNano()) / 1e9,
		"level":         level,
		"_service":      "oxidms",
	}

	payload, err := json.Marshal(gelf)
	if err != nil {
		return len(p), nil // don't fail the log call
	}

	// Fire-and-forget
	w.conn.Write(payload)
	return len(p), nil
}
