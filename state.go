package gossip

import (
	"net"
	"time"
)

const (
	stateAlive   = iota
	StateSuspect // 可疑的
	StateDead
)

type Node struct {
	Name string
	Addr net.IP
}

type NodeState struct {
	Node
	Incarnation uint32
	State       int
	StateChange time.Time
}

type ackHandler struct {
	handler func()
	timeer  *time.Timer
}

// 更新节点状态
func (m *Memberlist) mergeState(remote []pushNodeState) {
	for _, r := range remote {
		m.nodeLock.RLock()
		local, ok := m.nodeMap[r.Name]
		m.nodeLock.RUnlock()

		// 如果与本地保存结果一致就跳过
		if ok && local.State == r.State {
			continue
		}

		switch r.State {
		case stateAlive:
			a := alive{
				Incarnation: r.Incarnation,
				Node:        r.Name,
				Addr:        r.Addr,
			}
			m.aliveNode(&a)
		case StateSuspect:
			s := suspect{
				Incarnation: r.Incarnation,
				Node:        r.Name,
			}
			m.suspectNode(&s)

		case StateDead:
			d := dead{
				Incarnation: r.Incarnation,
				Node:        r.Name,
			}
			m.deadNode(&d)
		}
	}
}
