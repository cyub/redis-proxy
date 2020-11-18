package proxy

import (
	"bufio"
	"errors"
	"io"
	"net"
)

type Client struct {
	ID   int64
	ctx  context
	conn net.Conn
	r    io.Reader
	w    io.Writer
}

func NewClient(id int64, ctx context, conn net.Conn) *Client {
	return &Client{
		ID:   id,
		ctx:  ctx,
		conn: conn,
		r:    bufio.NewReader(conn),
		w:    bufio.NewWriter(conn),
	}
}

func (client *Client) IOLoop() {
	br := client.r.(*bufio.Reader)
	proxy := client.ctx.proxy
	var err error
	var buf []byte
	for {
		buf, err = br.ReadBytes('\n')
		if err != nil {
			goto exit
		}

		bufLen := len(buf)
		if bufLen < 4 {
			err = errors.New("invalid response")
			goto exit
		}
		cmd, err := UnpackCommand(br, buf[0:bufLen-2])
		if err != nil {
			goto exit
		}

		doneChan := make(chan *transaction)
		proxy.transactionChan <- &transaction{cmd: cmd, doneChan: doneChan}
		trans := <-doneChan
		if trans.Error != nil {
			goto exit
		}
		bytesToWrite, err := trans.resp.ToRESP()
		if err != nil {
			goto exit
		}
		client.conn.Write(bytesToWrite)
	}

exit:
	proxy.CloseClient(client.ID)
}

func (client *Client) Close() error {
	return client.conn.Close()
}
