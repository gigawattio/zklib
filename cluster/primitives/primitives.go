package primitives

import (
	"fmt"

	"github.com/satori/go.uuid"
)

const (
	Leader   = "leader"
	Follower = "follower"
)

type Node struct {
	Uuid     uuid.UUID
	Hostname string
	Data     string
}

func NewNode(hostname string) *Node {
	node := &Node{
		Uuid:     uuid.Must(uuid.NewV4()),
		Hostname: hostname,
	}
	return node
}

func (node Node) String() string {
	s := fmt.Sprintf("Node{Uuid: %v, Hostname: %v, Data: %v}", node.Uuid.String(), node.Hostname, node.Data)
	return s
}

type Update struct {
	Leader Node
	Mode   string
}
