package gossip

import (
	"bytes"
	"fmt"
	"log"
	"net"

	"github.com/ugorji/go/codec"
)

const (
	pingMsg = iota
	indirectPingMsg
	ackRespMsg
	suspectMsg
	aliveMsg
	deadMsg
	pushPullMsg
	compoundMsg // 混合
)

type pushPullHeader struct {
	Nodes int
}

const (
	udpBufSize             = 65536
	udpSendBuf             = 1400
	compoundHeaderOverhead = 2 // compound混合
	compoundOverhead       = 2
)

type pushNodeState struct {
	Name        string
	Addr        []byte
	Incarnation uint32 // 典型?
	State       int
}

// AliveNode
type alive struct {
	Incarnation uint32
	Node        string
	Addr        []byte
}

// SuspectNode
type suspect struct {
	Incarnation uint32
	Node        string
}

// deadNode
type dead struct {
	Incarnation uint32
	Node        string
}

type ping struct {
	SeqNo uint32
}

type indirectPingReq struct {
	SeqNo  uint32
	Target []byte
}

type ackResp struct {
	SeqNo uint32
}

// tcp服务提供 同步节点状态 的功能
func (m *Memberlist) tcpListen() {
	for {
		conn, err := m.tcpListener.AcceptTCP()
		if err != nil {
			if m.shutdown {
				break
			}
			log.Printf("[ERROR] 接收失败 TCP connection: %s", err)
			continue
		}
		go m.handleConn(conn)
	}
}

func (m *Memberlist) handleConn(conn *net.TCPConn) {
	defer conn.Close()

	// 读取Remote的状态
	remoteNodes, err := readRemoteState(conn)
	if err != nil {
		log.Printf("[ERROR] 无法获取远端状态: %s", err)
	}
	// 发送本地节点的状态
	if err := m.sendLocalState(conn); err != nil {
		log.Printf("[ERROR] 推送本地状态失败", err)
	}
	// 将收到的Remote状态进行更新
	m.mergeState(remoteNodes)
}

func readRemoteState(conn net.Conn) ([]pushNodeState, error) {
	// 读取数据
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		return nil, err
	}

	msgType := uint8(buf[0])

	if msgType != pushPullMsg {
		err := fmt.Errorf("非法消息类型: %s", msgType)
		return nil, err
	}

	var header pushPullHeader
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(conn, &hd) // 这里会将 conn 读取完 并解码
	if err := dec.Decode(&header); err != nil {
		return nil, err
	}

	remoteNodes := make([]pushNodeState, header.Nodes)

	for i := 0; i < header.Nodes; i++ {
		if err := dec.Decode(&remoteNodes[i]); err != nil {
			return remoteNodes, err
		}
	}
	return remoteNodes, nil
}

func (m *Memberlist) sendLocalState(conn net.Conn) error {
	// 收集本地存储的节点状态信息
	m.nodeLock.RLock()
	localNodes := make([]pushNodeState, len(m.nodes))
	for idx, n := range m.nodes {
		localNodes[idx].Name = n.Name
		localNodes[idx].Addr = n.Addr
		localNodes[idx].Incarnation = n.Incarnation
		localNodes[idx].State = n.State
	}
	m.nodeLock.RUnlock()

	// 添加头部信息
	header := pushPullHeader{Nodes: len(localNodes)}
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(conn, &hd) // 这里会将我们构造好的数据编码并全部发送

	// 开始推送状态
	conn.Write([]byte{pushPullMsg})
	// 编码并发送
	if err := enc.Encode(&header); err != nil {
		return err
	}
	for i := 0; i < header.Nodes; i++ {
		if err := enc.Encode(&localNodes[i]); err != nil {
			return err
		}
	}
	return nil

}

func (m *Memberlist) udpListen() {
	mainBuf := make([]byte, udpBufSize)
	var n int
	var addr net.Addr
	var err error

	for {
		buf := mainBuf[0:udpBufSize]
		// 不断从udpListen中读取数据
		n, addr, err = m.udpListener.ReadFrom(buf)
		if err != nil {
			if m.shutdown {
				break
			}
			log.Printf("[ERROR] reading UDP packet: %s", err)
			continue
		}
		if n < 1 {
			log.Printf("[ERRO] UDP packet too short (%d bytes). From %s",
				len(buf), addr)
		}

		m.handleCommand(buf[:n], addr)
	}
}

func (m *Memberlist) handleCommand(buf []byte, from net.Addr) {
	msgType := uint8(buf[0])
	buf = buf[1:]

	switch msgType {
	case compoundMsg:
		m.handleCompound(buf, from)
	case pingMsg:
		m.handlePing(buf, from)
	case indirectPingMsg:
		m.handleIndirectPing(buf, from)
	case ackRespMsg:
		m.handleAck(buf, from)
	case suspectMsg:
		m.handleSuspect(buf, from)
	case aliveMsg:
		m.handleAlive(buf, from)
	case deadMsg:
		m.handleDead(buf, from)
	default:
		log.Printf("[ERROR] UDP msg type (%s) not supported. From: %s", msgType, from)
	}
}

// 多个消息聚合在一起，进行分割，然后再重新调用handleCommand
func (m *Memberlist) handleCompound(buf []byte, from net.Addr) {
	trunc, parts, err := decodeCompoundMessage(buf)
	if err != nil {
		log.Printf("[ERROR] 聚合消息解码失败: %s", err)
		return
	}

	if trunc > 0 {
		log.Printf("[WARNING] %d 个聚合请求被截断: %d", trunc)
	}

	for _, part := range parts {
		m.handleCommand(part, from)
	}
}

func (m *Memberlist) handlePing(buf []byte, from net.Addr) {
	var p ping
	if err := decode(buf, &p); err != nil {
		log.Printf("[ERROR] ping request 解码失败： %s", err)
		return
	}
	ack := ackResp{p.SeqNo}
	if err := m.encodeAndSendMsg(from, ackRespMsg, &ack); err != nil {
		log.Printf("[ERROR] 发送ack失败 %s", err)
	}
}

func (m *Memberlist) handleIndirectPing(buf []byte, from net.Addr) {
}

func (m *Memberlist) handleAck(buf []byte, from net.Addr) {
}

func (m *Memberlist) handleSuspect(buf []byte, from net.Addr) {
	var sus suspect
	if err := decode(buf, &sus); err != nil {
		log.Printf("[ERROR] suspect message 解码失败: %s", err)
		return
	}
	m.suspectNode(&sus)
}

func (m *Memberlist) handleAlive(buf []byte, from net.Addr) {
	var live alive
	if err := decode(buf, &live); err != nil {
		log.Printf("[ERROR] alive message 解码失败: %s", err)
		return
	}
	m.aliveNode(&live)
}

func (m *Memberlist) handleDead(buf []byte, from net.Addr) {
	var d dead
	if err := decode(buf, &d); err != nil {
		log.Printf("[ERROR] decode dead message 解码失败: %s", err)
		return
	}
	m.deadNode(&d)
}

func (m *Memberlist) encodeAndSendMsg(to net.Addr, msgType int, msg interface{}) error {
	out, err := encode(msgType, msg)
	if err != nil {
		return err
	}

	if err := m.sendMsg(to, out); err != nil {
		return err
	}
	return nil

}

func (m *Memberlist) sendMsg(to net.Addr, msg *bytes.Buffer) error {
	// 检查是否可以另外搬运一些Message
	byteAvail := udpSendBuf - msg.Len() - compoundHeaderOverhead
	extra := m.getBroadcasts(compoundOverhead, byteAvail)

	// 没有额外的消息
	if len(extra) == 0 {
		return m.rawSendMsg(to, msg)
	}

	msgs := make([]*bytes.Buffer, 0, 1+len(extra))
	msgs = append(msgs, msg)
	msgs = append(msgs, extra...)

	compound := makeCompoundMessage(msgs)
	return m.rawSendMsg(to, compound)
}

func (m *Memberlist) rawSendMsg(to net.Addr, msg *bytes.Buffer) error {
	_, err := m.udpListener.WriteTo(msg.Bytes(), to)
	return err
}
