package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	byteCRLF = []byte("\r\n")
	byteLF   = []byte("\n")
)

type Command struct {
	Name   string
	Params [][]byte
}

func (cmd *Command) ToRESP() []byte {
	var buf bytes.Buffer
	length := 1 + len(cmd.Params)
	buf.WriteByte('*')
	buf.Write(NumberToBytes(length))
	buf.Write(byteCRLF)
	params := make([][]byte, length-1, length)
	copy(params, cmd.Params)
	params = append([][]byte{[]byte(cmd.Name)}, params...)
	for _, param := range params {
		buf.WriteByte('$')
		buf.Write(NumberToBytes(len(param)))
		buf.Write(byteCRLF)
		buf.Write(param)
		buf.Write(byteCRLF)
	}
	return buf.Bytes()
}

func (cmd *Command) WriteTo(w io.Writer) (int, error) {
	return w.Write(cmd.ToRESP())
}

func (cmd *Command) String() string {
	return fmt.Sprintf("%s %v", cmd.Name, cmd.Params)
}

// 2a 31 0d 0a 24 34 0d 0a 50 49 4e 47 0d 0a  *1\r\n$4\r\nPING\r\n
func PING() *Command {
	return &Command{Name: "PING", Params: nil}
}

func GET(key string) *Command {
	return &Command{Name: "GET", Params: [][]byte{[]byte(key)}}
}

func SET(key string, val string) *Command {
	return &Command{Name: "SET", Params: [][]byte{[]byte(key), []byte(val)}}
}

func GETSET(key string, val string) *Command {
	return &Command{Name: "GETSET", Params: [][]byte{[]byte(key), []byte(val)}}
}

func INCR(key string) *Command {
	return INCRBY(key, 1)
}

func INCRBY(key string, increment int) *Command {
	return &Command{Name: "INCRBY", Params: [][]byte{NumberToBytes(increment)}}
}

func DECR(key string) *Command {
	return DECRBY(key, 1)
}

func DECRBY(key string, decrement int) *Command {
	return &Command{Name: "DECRBY", Params: [][]byte{NumberToBytes(decrement)}}
}

func MGET(key string, optionKey ...string) *Command {
	params := make([][]byte, 0, len(optionKey))
	for _, opKey := range optionKey {
		params = append(params, []byte(opKey))
	}
	return &Command{Name: "MGET", Params: params}
}

func DEL(key string) *Command {
	return &Command{Name: "DEL", Params: [][]byte{[]byte(key)}}
}

func EXISTS(key string) *Command {
	return &Command{Name: "EXISTS", Params: [][]byte{[]byte(key)}}
}

func EXPIRE(key string, second int) *Command {
	return &Command{Name: "EXPIRE", Params: [][]byte{NumberToBytes(second)}}
}

func LRANGE(key string, start int, stop int) *Command {
	return &Command{Name: "LRANGE", Params: [][]byte{[]byte(key), NumberToBytes(start), NumberToBytes(stop)}}
}

func INFO(key string, section ...string) *Command {
	var params [][]byte
	if len(section) > 0 {
		params = [][]byte{[]byte(section[0])}
	}
	return &Command{Name: "INFO", Params: params}
}

func LPUSH(key string, value string, more ...string) *Command {
	return listPush("LPUSH", key, value, more...)
}

func RPUSH(key string, value string, more ...string) *Command {
	return listPush("RPUSH", key, value, more...)
}

func listPush(name string, key string, value string, more ...string) *Command {
	params := [][]byte{
		[]byte(value),
	}
	for _, val := range more {
		params = append(params, []byte(val))
	}

	return &Command{Name: name, Params: params}
}

func LPOP(key string) *Command {
	return &Command{Name: "LPOP", Params: [][]byte{[]byte(key)}}
}

func RPOP(key string) *Command {
	return &Command{Name: "RPOP", Params: [][]byte{[]byte(key)}}
}

func BLPOP(key string, timeout time.Duration) *Command {
	return listBlockPop("BLPOP", key, timeout)
}

func BRPOP(key string, timeout time.Duration) *Command {
	return listBlockPop("BRPOP", key, timeout)
}

func listBlockPop(name string, key string, timeout time.Duration) *Command {
	if timeout == 0 {
		return &Command{Name: name, Params: [][]byte{NumberToBytes(0)}}
	}
	second := int(timeout.Seconds())
	if second < 1 {
		return BLPOP(key, 0)
	}

	return &Command{Name: name, Params: [][]byte{NumberToBytes(second)}}
}

func RawComand(name string, params ...[]byte) *Command {
	return &Command{Name: name, Params: params}
}

func UnpackCommand(br *bufio.Reader, req []byte) (*Command, error) {
	if bytes.Equal(req, []byte("PING")) { // PING命令存在未按照规范的情况。即直接是PING\r\n
		return PING(), nil
	}
	argLen := BytesToNumber(req[1:])
	if argLen < 1 {
		return nil, errors.New("command can't be empty")
	}
	args := make([][]byte, 0)
	var arg []byte
	var err error
	for i := 0; i < argLen; i++ {
		arg, err = parseCommandArg(br)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return RawComand(string(args[0]), args[1:]...), nil
}

func parseCommandArg(br *bufio.Reader) ([]byte, error) {
	buf, err := br.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if buf[0] != '$' {
		return nil, errors.New("it should be $ as identify")
	}

	bufLen := len(buf)
	if bufLen < 4 {
		return nil, errors.New("invalid args length")
	}

	arg := make([]byte, BytesToNumber(buf[1:bufLen-2])+2)
	if _, err = io.ReadFull(br, arg); err != nil {
		return nil, err
	}
	return arg[:len(arg)-2], nil
}
