package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	RespTypeSimpleString = iota + 1 // Simple String Response
	RespTypeError                   // Errors Response
	RespTypeInteger                 // Integers Response
	RespTypeBulkString              // Bulk Strings Response
	RespTypeArray                   // Arrays Response
)

var identify = map[byte]int{
	'+': RespTypeSimpleString,
	'-': RespTypeError,
	':': RespTypeInteger,
	'$': RespTypeBulkString,
	'*': RespTypeArray,
}

type Resp struct {
	Type    int
	State   []byte
	Error   RESPerror
	Integer []byte
	Bulk    []byte
	Array   []*Resp
}

func (resp *Resp) String() string {
	switch resp.Type {
	case RespTypeSimpleString:
		return fmt.Sprintf("type: %d, state: %s", resp.Type, resp.State)
	case RespTypeError:
		return fmt.Sprintf("type: %d, error: %s", resp.Type, resp.Error)
	case RespTypeInteger:
		return fmt.Sprintf("type: %d, integer: %d", resp.Type, BytesToNumber(resp.Integer))
	case RespTypeBulkString:
		if resp.Bulk == nil {
			return fmt.Sprintf("type: %d, bulkstring: nil", resp.Type)
		}

		return fmt.Sprintf("type: %d, bulkstring: %s", resp.Type, resp.Bulk)

	case RespTypeArray:
		if resp.Array == nil {
			return fmt.Sprintf("type: %d, array: nil", resp.Type)
		} else if len(resp.Array) == 0 {
			return fmt.Sprintf("type: %d, array: []", resp.Type)
		}
		var readable []string
		for _, subResp := range resp.Array {
			readable = append(readable, "{"+subResp.String()+"}")
		}
		return fmt.Sprintf("type: %d, array[%s]", resp.Type, strings.Join(readable, ","))
	default:
		return fmt.Sprintf("invalid resp: %#v", resp)
	}
}

func (resp *Resp) ToRESP() ([]byte, error) {
	identifyChar := make(map[int]byte, 0)
	for identChar, respType := range identify {
		identifyChar[respType] = identChar
	}
	var byteBuf bytes.Buffer
	byteBuf.WriteByte(identifyChar[resp.Type])
	switch resp.Type {
	case RespTypeSimpleString:
		byteBuf.Write(resp.State)
	case RespTypeError:
		byteBuf.Write(resp.Error.errType)
		byteBuf.Write([]byte(" "))
		byteBuf.Write(resp.Error.errMessage)
	case RespTypeInteger:
		byteBuf.Write(resp.Integer)
	case RespTypeBulkString:
		if resp.Bulk == nil {
			byteBuf.Write(NumberToBytes(-1))
		} else {
			byteBuf.Write(NumberToBytes(len(resp.Bulk)))
			byteBuf.Write(byteCRLF)
			byteBuf.Write(resp.Bulk)
		}
	case RespTypeArray:
		if resp.Array == nil {
			byteBuf.Write(NumberToBytes(-1))
		} else if len(resp.Array) == 0 {
			byteBuf.Write(NumberToBytes(0))
		} else {
			byteBuf.Write(NumberToBytes(len(resp.Array)))
			byteBuf.Write(byteCRLF)
			for index, subResp := range resp.Array {
				subRESP, err := subResp.ToRESP()
				if err != nil {
					return nil, err
				}
				if index == len(resp.Array)-1 { // 最后一次迭代会多出CTLF
					byteBuf.Write(subRESP[0 : len(subRESP)-2])
				} else {
					byteBuf.Write(subRESP)
				}
			}
		}
	}
	byteBuf.Write(byteCRLF)
	return byteBuf.Bytes(), nil
}

type RESPerror struct {
	errType    []byte
	errMessage []byte
}

func (re RESPerror) Error() string {
	return fmt.Sprintf("%s %s", re.errType, re.errMessage)
}

func NewResperror(errType []byte, errMessage []byte) RESPerror {
	return RESPerror{
		errType:    errType,
		errMessage: errMessage,
	}
}

func ParseResp(br *bufio.Reader, resp []byte) (*Resp, error) {
	switch identify[resp[0]] {
	case RespTypeSimpleString:
		return toSimpleString(resp[1:])
	case RespTypeError:
		return toError(resp[1:])
	case RespTypeInteger:
		return toInteger(resp[1:])
	case RespTypeBulkString:
		return toBulkString(br, resp[1:])
	case RespTypeArray:
		return toArray(br, resp[1:])
	default:
		return nil, fmt.Errorf("invalid response type: %d", identify[resp[0]])
	}
}

func toSimpleString(data []byte) (*Resp, error) {
	resp := &Resp{
		Type:  RespTypeSimpleString,
		State: data[:],
	}

	return resp, nil
}

func toError(data []byte) (*Resp, error) {
	fields := bytes.Split(data, []byte(" "))
	if len(fields) < 2 {
		return nil, errors.New("invalid error respose")
	}

	resp := &Resp{
		Type:  RespTypeError,
		Error: NewResperror(fields[0], bytes.Join(fields[1:], []byte(" "))),
	}
	return resp, nil
}

func toInteger(data []byte) (*Resp, error) {
	resp := &Resp{
		Type:    RespTypeInteger,
		Integer: data[:],
	}
	return resp, nil
}

func toBulkString(br *bufio.Reader, data []byte) (*Resp, error) {
	bytesNum := BytesToNumber(data)
	resp := &Resp{
		Type: RespTypeBulkString,
	}
	if bytesNum == -1 { // Null bulk string
		return resp, nil
	}
	if bytesNum <= 0 {
		return nil, errors.New("invalid bulk string length")
	}
	buf := make([]byte, bytesNum+2)
	_, err := io.ReadFull(br, buf)
	if err != nil {
		return nil, err
	}
	resp.Bulk = buf[:len(buf)-2]
	return resp, nil
}

func toArray(br *bufio.Reader, data []byte) (*Resp, error) {
	bytesNum := BytesToNumber(data)
	resp := &Resp{
		Type:  RespTypeArray,
		Array: nil, //  Null Array
	}

	if bytesNum == 0 {
		resp.Array = make([]*Resp, 0) // Empty Array
	} else if bytesNum < -1 {
		return nil, errors.New("invalid array respose number")
	}

	var err error
	for {
		if bytesNum <= 0 {
			break
		}
		bytesNum--
		buf, err := br.ReadBytes('\n')
		if err != nil {
			break
		}
		bufLen := len(buf)
		if bufLen < 4 {
			err = errors.New("invalid response")
			break
		}
		subResp, err := ParseResp(br, buf[0:len(buf)-2])
		if err != nil {
			break
		}
		resp.Array = append(resp.Array, subResp)
	}
	return resp, err
}
