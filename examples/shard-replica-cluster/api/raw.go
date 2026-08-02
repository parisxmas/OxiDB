package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"time"
)

// rawCommand opens a fresh TCP connection to an OxiDB node, sends a single
// length-prefixed JSON command, and returns the decoded response.
//
// Used for commands the typed Go client doesn't expose (e.g. raft_metrics).
func rawCommand(target HostPort, payload map[string]any, timeout time.Duration) (map[string]any, error) {
	conn, err := net.DialTimeout("tcp", target.String(), timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(body)))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return nil, err
	}
	if _, err := conn.Write(body); err != nil {
		return nil, err
	}

	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(lenBuf[:])
	if n > 16*1024*1024 {
		return nil, fmt.Errorf("response too large: %d", n)
	}
	respBytes := make([]byte, n)
	if _, err := io.ReadFull(conn, respBytes); err != nil {
		return nil, err
	}

	var resp map[string]any
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, fmt.Errorf("decode response: %w (body=%q)", err, string(respBytes))
	}
	if ok, _ := resp["ok"].(bool); !ok {
		errMsg, _ := resp["error"].(string)
		return resp, fmt.Errorf("server: %s", errMsg)
	}
	return resp, nil
}

// crc32IEEE computes the IEEE polynomial CRC32 of a byte slice. Mirrors the
// Rust `crc32fast::hash` that oxipool uses for shard routing.
func crc32IEEE(b []byte) uint32 {
	return crc32.ChecksumIEEE(b)
}
