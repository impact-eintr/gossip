package gossip

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

type Config struct {
	Name             string // Node name
	BindAddr         string // Bindding Address
	UDPPort          int
	TCPPort          int
	TCPTimeout       time.Duration
	IndirectChecks   int           // 要使用的间接检查次数
	RetransmitMult   int           // 重传(Retransmits) = RetransmitMult * log(N+1)
	SuspicionMult    int           // 怀疑时间(Suspicion time) = SuspcicionMult * log(N+1) * Interval
	PushPullInterval time.Duration // Push/Pull update 间隔
	RTT              time.Duration // 99% precentile of round-trip-time
	ProbeInterval    time.Duration // 故障探测间隔长度

	GossipNodes    int           // 每个 GossipInterval 要八卦的节点数
	GossipInterval time.Duration // 非搭载消息的 Gossip 间隔（仅当 GossipNodes > 0 时）

	JoinCh  chan<- *Node // 用于做新增node的时候，作为外部注册通知处理
	LeaveCh chan<- *Node // 用于做对去除一个node的时候，做为外部注册通知处理
}

type Memberlist struct {
	config   *Config
	shutdown bool // 本地服务关闭的标志位
	leave    bool // 本节点退出的标志位

	// udp与tcp的链接管理 对应的net.go 传输与协议管理
	udpListener *net.UDPConn
	tcpListener *net.TCPListener

	// 本地seq num
	sequenceNum uint32
	// 本地inc num
	incuenceNum uint32

	// node管理以及state管理对应的state.go
	nodeLock sync.RWMutex
	nodes    []*NodeState
	nodeMap  map[string]*NodeState // Maps Addt.String() -> NodeState

	tickerLock sync.Mutex
	tickers    []*time.Ticker
	stopTick   chan struct{}
	probeIndex int // probe 探测

	ackLock     sync.Mutex
	ackHandlers map[uint32]*ackHandler

	// broadcat管理 对应的broadcast.go
	broadcastLock sync.Mutex
	bcQueue       broadcasts
}

func DefaultConfig() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		Name:             hostname,
		BindAddr:         "0.0.0.0",
		UDPPort:          6431,
		TCPPort:          6431,
		TCPTimeout:       10 * time.Second,
		IndirectChecks:   3,                // 要使用的间接检查次数
		RetransmitMult:   4,                // 重传(Retransmits) = RetransmitMult * log(N+1)
		SuspicionMult:    6,                // 怀疑时间(Suspicion time) = SuspcicionMult * log(N+1) * Interval
		PushPullInterval: 30 * time.Second, // Push/Pull update 间隔
		RTT:              20 * time.Second, // 99% precentile of round-trip-time
		ProbeInterval:    1 * time.Second,  // 故障探测间隔长度

		GossipNodes:    8,                      // 每个 GossipInterval 要八卦的节点数
		GossipInterval: 200 * time.Millisecond, // 非搭载消息的 Gossip 间隔（仅当 GossipNodes > 0 时）

		JoinCh:  nil, // 用于做新增node的时候，作为外部注册通知处理
		LeaveCh: nil, // 用于做对去除一个node的时候，做为外部注册通知处理
	}
}

// 工厂方法
func newMemberlist(conf *Config) (*Memberlist, error) {
	tcpAddr := fmt.Sprintf("%s:%d", conf.BindAddr, conf.TCPPort)
	tcpListener, err := net.Listen("tcp", tcpAddr)
	if err != nil {
		return nil, fmt.Errorf("Failed to start TCP listener. Err: %s", err)
	}

	udpAddr := fmt.Sprintf("%s:%d", conf.BindAddr, conf.UDPPort)
	udpListener, err := net.ListenPacket("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("Failed to start UDP listener. Err: %s", err)
	}

	m := &Memberlist{
		udpListener: udpListener.(*net.UDPConn),
		tcpListener: tcpListener.(*net.TCPListener),
		nodeMap:     make(map[string]*NodeState),
		stopTick:    make(chan struct{}, 32),
		ackHandlers: make(map[uint32]*ackHandler),
	}

	go m.tcpListen()
	go m.udpListen()
	return m, nil

}

func Create(conf *Config) (*Memberlist, error) {
	m, err := newMemberlist(conf)
	if err != nil {
		return nil, err
	}
	if err := m.setAlive(); err != nil {
		m.Shutdown()
		return nil, err
	}

	m.schedule()
	return m, nil

}
