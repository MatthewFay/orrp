package main

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type DBClient struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *msgpack.Decoder
}

func NewClient(address string) (*DBClient, error) {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to db: %v", err)
	}

	return &DBClient{
		conn: conn,
		// We use a msgpack decoder directly on the stream to handle
		// message boundaries automatically
		decoder: msgpack.NewDecoder(conn),
	}, nil
}

func (c *DBClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// SendCommand sends a raw string command ending with newline
func (c *DBClient) SendCommand(cmd string) error {
	// Ensure newline termination for telnet compatibility
	if cmd[len(cmd)-1] != '\n' {
		cmd += "\n"
	}
	_, err := c.conn.Write([]byte(cmd))
	return err
}

// ReadResponse decodes the next MsgPack object from the stream
// and returns it as a generic interface (map or slice)
func (c *DBClient) ReadResponse() (any, error) {
	var result any
	err := c.decoder.Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// PrettyPrint converts the generic interface to indented JSON for display
func PrettyPrint(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("error marshalling json: %v", err)
	}
	return string(b)
}
