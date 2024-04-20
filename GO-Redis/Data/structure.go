package Data

import (
	"fmt"
	"strconv"
	"strings"
)

var (
	CRLF = "\r\n"
	NIL  = []byte("$-1\r\n")
)

type RedisData interface {
	ToBytes() []byte  // return resp transfer format data
	ByteData() []byte // return byte data
	String() string   // return resp string
}

type StringData struct {
	data string
}

type BulkData struct {
	data []byte
}

type IntData struct {
	data int64
}

type ErrorData struct {
	data string
}

type ArrayData struct {
	data []RedisData
}

type PlainData struct {
	data string
}

// MakeBulkData make bulk data
func MakeBulkData(data []byte) *BulkData {
	return &BulkData{
		data: data,
	}
}

// convert bulk data to different types

func (bulkData *BulkData) ToBytes() []byte {
	if bulkData.data == nil {
		return NIL
	}
	// return data string
	return []byte("$" + strconv.Itoa(len(bulkData.data)) + CRLF + string(bulkData.data) + CRLF)
}

func (bulkData *BulkData) Data() []byte {
	return bulkData.data
}

func (bulkData *BulkData) ByteData() []byte {
	return bulkData.data
}

func (bulkData *BulkData) String() string {
	return string(bulkData.data)
}

func MakeStringData(data string) *StringData {
	return &StringData{
		data: data,
	}
}

func (stringData *StringData) ToBytes() []byte {
	return []byte("+" + stringData.data + CRLF)
}

func (stringData *StringData) Data() string {
	return stringData.data
}

func (stringData *StringData) ByteData() []byte {
	return []byte(stringData.data)
}

func (stringData *StringData) String() string {
	return stringData.data
}

func MakeIntData(data int64) *IntData {
	return &IntData{
		data: data,
	}
}

func (intData *IntData) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(intData.data, 10) + CRLF)
}

func (intData *IntData) Data() int64 {
	return intData.data
}

func (intData *IntData) ByteData() []byte {
	return []byte(strconv.FormatInt(intData.data, 10))
}

func (intData *IntData) String() string {
	return strconv.FormatInt(intData.data, 10)
}

func MakeErrorData(data ...string) *ErrorData {
	errMsg := ""
	for _, s := range data {
		errMsg += s
	}
	return &ErrorData{
		data: errMsg,
	}
}

func MakeWrongType() *ErrorData {
	return &ErrorData{data: fmt.Sprintf("WRONGTYPE Operation against a key holding the wrong kind of value")}
}

func MakeWrongNumberArgs(name string) *ErrorData {
	return &ErrorData{data: fmt.Sprintf("ERR wrong number of arguments for '%s' command", name)}
}

func (errorData *ErrorData) ToBytes() []byte {
	return []byte("-" + errorData.data + CRLF)
}

func (errorData *ErrorData) ByteData() []byte {
	return []byte(errorData.data)
}

func (errorData *ErrorData) String() string {
	return errorData.data
}

func MakeArrayData(data []RedisData) *ArrayData {
	return &ArrayData{
		data: data,
	}
}

func MakeEmptyArrayData() *ArrayData {
	return &ArrayData{
		data: []RedisData{},
	}
}

func (arrayData *ArrayData) ToBytes() []byte {
	if arrayData.data == nil {
		return []byte("*-1\r\n")
	}

	res := []byte("*" + strconv.Itoa(len(arrayData.data)) + CRLF)
	for _, v := range arrayData.data {
		res = append(res, v.ToBytes()...)
	}
	return res
}
func (arrayData *ArrayData) Data() []RedisData {
	return arrayData.data
}

func (arrayData *ArrayData) ToCommand() [][]byte {
	res := make([][]byte, 0)
	for _, v := range arrayData.data {
		res = append(res, v.ByteData())
	}
	return res
}

func (arrayData *ArrayData) ToStringCommand() []string {
	res := make([]string, 0, len(arrayData.data))
	for _, v := range arrayData.data {
		res = append(res, string(v.ByteData()))
	}
	return res
}

func (arrayData *ArrayData) String() string {
	return strings.Join(arrayData.ToStringCommand(), " ")
}

// ByteData is discarded. Use ToCommand() instead.
func (arrayData *ArrayData) ByteData() []byte {
	res := make([]byte, 0)
	for _, v := range arrayData.data {
		res = append(res, v.ByteData()...)
	}
	return res
}

func MakePlainData(data string) *PlainData {
	return &PlainData{
		data: data,
	}
}
func (plainData *PlainData) ToBytes() []byte {
	return []byte(plainData.data + CRLF)
}
func (plainData *PlainData) Data() string {
	return plainData.data
}

func (plainData *PlainData) String() string {
	return plainData.data
}

func (plainData *PlainData) ByteData() []byte {
	return []byte(plainData.data)
}
