package main

// Minimal RESP (Redis-protocol) client for OxiMem — zero dependencies, just
// enough for the hybrid matcher: commands in, decoded replies out.

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type Resp struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func DialResp() (*Resp, error) {
	port := os.Getenv("OXIMEM_PORT")
	if port == "" {
		port = "6479"
	}
	conn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
	}
	return &Resp{conn: conn, r: bufio.NewReader(conn), w: bufio.NewWriter(conn)}, nil
}

func (c *Resp) Close() { c.conn.Close() }

// Do sends one command and reads one reply.
// Reply types: string (simple/bulk), int64, nil (null), []any (array), error.
func (c *Resp) Do(args ...string) (any, error) {
	fmt.Fprintf(c.w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(c.w, "$%d\r\n%s\r\n", len(a), a)
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	return c.read()
}

func (c *Resp) read() (any, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return nil, fmt.Errorf("empty reply")
	}
	body := line[1:]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return nil, fmt.Errorf("%s", body)
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, _ := strconv.Atoi(body)
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := ioReadFull(c.r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, _ := strconv.Atoi(body)
		if n < 0 {
			return nil, nil
		}
		out := make([]any, 0, n)
		for i := 0; i < n; i++ {
			v, err := c.read()
			if err != nil {
				// element-level -ERR inside arrays: keep as string
				out = append(out, fmt.Sprintf("ERR:%v", err))
				continue
			}
			out = append(out, v)
		}
		return out, nil
	}
	return nil, fmt.Errorf("bad reply: %q", line)
}

func ioReadFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}
