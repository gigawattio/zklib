package candidate

type Node struct {
	Uuid string
	Host string
	Port int
	Data interface{}
}

func NewNode(uid string, host string, port int, data interface{}) *Node {
	node := &Node{
		Uuid: uid,
		Host: host,
		Port: port,
		Data: data,
	}
	return node
}
