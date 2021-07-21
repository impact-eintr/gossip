package gossip

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ugorji/go/codec"
)

func decodeCompoundMessage(buf []byte) (trunc int, parts [][]byte, err error) {
	if len(buf) < 1 {
		err = fmt.Errorf("聚合消息长度过小: %d", len(buf))
		return
	}

	numParts := uint8(buf[0])
	buf = buf[1:]

	if len(buf) < int(numParts*2) {
		err = fmt.Errorf("因为长度无法截断")
		return
	}

	lengths := make([]uint16, numParts) // 2^16 == 65536
	for i := 0; i < int(numParts); i++ {
		lengths[i] = binary.BigEndian.Uint16(buf[i*2 : i*2+2])
	}

	buf = buf[numParts*2:] // buf里现在全是消息

	for idx, msgLen := range lengths {
		if len(buf) < int(msgLen) {
			trunc = int(numParts) - idx
			return
		}
		slice := buf[:msgLen]
		buf = buf[msgLen:]
		parts = append(parts, slice)
	}
	return

}

func decode(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

func encode(msgType int, in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(uint8(msgType))
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

func makeCompoundMessage(msgs []*bytes.Buffer) *bytes.Buffer {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(uint8(compoundMsg))
	buf.WriteByte(uint8(len(msgs)))

	// 写入每条消息的长度
	for _, m := range msgs {
		binary.Write(buf, binary.BigEndian, uint16(m.Len()))
	}

	// 写入每条消息的内容
	for _, m := range msgs {
		buf.Write(m.Bytes())
	}
	return buf

}
