package client

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type DBClient struct {
	conn    net.Conn
	decoder *msgpack.Decoder
	Address string
}

func New(address string) (*DBClient, error) {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	return &DBClient{
		conn:    conn,
		decoder: msgpack.NewDecoder(conn),
		Address: address,
	}, nil
}

func (c *DBClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *DBClient) SendCommand(cmd string) error {
	if cmd == "" {
		return nil
	}
	if cmd[len(cmd)-1] != '\n' {
		cmd += "\n"
	}
	
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := c.conn.Write([]byte(cmd))
	return err
}

func (c *DBClient) ReadResponse() (any, error) {
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var result any
	err := c.decoder.Decode(&result)
	return result, err
}

func PrettyPrint(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}
