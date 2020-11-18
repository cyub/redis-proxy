package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

type Conn struct {
	config Config
	stats  commandStats
	conn   *net.TCPConn
	r      io.Reader
	w      io.Writer
}

type commandStats struct {
	count   int64
	success int64
	fail    int64
}

func NewConn(config Config) *Conn {
	return &Conn{
		config: config,
	}
}

func (c *Conn) Connect() error {
	var dialer = &net.Dialer{
		Timeout: c.config.DialerTimeout,
	}
	conn, err := dialer.Dial("tcp", c.config.ClusterAddrs[0])
	if err != nil {
		return err
	}

	c.conn = conn.(*net.TCPConn)
	c.conn.SetNoDelay(true)
	c.r = bufio.NewReader(c.conn)
	c.w = bufio.NewWriter(c.conn)
	if err = c.WriteCommand(PING()); err != nil {
		return err
	}
	var protResp *Resp
	protResp, err = c.ReadResponse()
	if err != nil {
		return err
	}
	// 检查ping命令响应是否ok
	if protResp.Type != RespTypeSimpleString || !bytes.Equal(protResp.State, []byte("PONG")) {
		return errors.New("backend redis error: ping failure")
	}
	return nil
}

func (c *Conn) ReadResponse() (*Resp, error) {
	br := c.r.(*bufio.Reader)
	buf, err := br.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	bufLen := len(buf)
	if bufLen < 4 {
		return nil, errors.New("invalid response")
	}
	return ParseResp(br, buf[0:bufLen-2])
}

func (c *Conn) Write(p []byte) (int, error) {
	c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	return c.w.Write(p)
}

func (c *Conn) Read(p []byte) (int, error) {
	c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	return c.r.Read(p)
}

func (c *Conn) WriteCommand(cmd *Command) error {
	_, err := cmd.WriteTo(c)
	if err != nil {
		return err
	}
	err = c.Flush()
	if err != nil {
		return err
	}
	return nil
}

type flusher interface {
	Flush() error
}

func (c *Conn) Flush() error {
	if f, ok := c.w.(flusher); ok {
		return f.Flush()
	}
	return nil
}

func printBytesByHex(bytesToPrint []byte) {
	group := make([]string, 0, len(bytesToPrint))
	for i := 0; i < len(bytesToPrint); i = i + 1 {
		group = append(group, fmt.Sprintf("%X", bytesToPrint[i:i+1]))
	}
	fmt.Printf("%s\n", strings.Join(group, " "))
}
