package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

type messageType string

const (
	MAX_MESSAGE messageType = "MAX"
	GET_MESSAGE messageType = "GET"
	NODES       int         = 1000
	MAX         int         = 1000
)

type Message struct {
	msgType messageType
	from    int
	//to          int
	payload int
}

type WorkerState interface {
}

type MaxState struct {
	value           int
	max             int
	shouldSendTable map[int]bool
}

func MaxWorkerProc(n *Node, initialState MaxState, send, recv []chan Message, result chan Message) {
	state := initialState
	for {
		for i := 0; i < len(recv); i++ {
			select {
			case m := <-recv[i]:
				// handle messages by type here
				log.Printf("Node %d received %v", n.Id, m)
				if m.payload > state.max {
					state.max = m.payload
					log.Printf("Node %d updated max to %v", n.Id, state.max)
					msg := Message{
						msgType: MAX_MESSAGE,
						from:    n.Id,
						payload: state.max,
					}
					result <- msg
					for id := range state.shouldSendTable {
						state.shouldSendTable[id] = true
					}
				}

			default:
				if state.shouldSendTable[i] {
					msg := Message{
						msgType: MAX_MESSAGE,
						from:    n.Id,
						payload: state.max,
					}
					log.Printf("Sending message %v to the %dth neighbor of Node %d", msg, i, n.Id)
					send[i] <- msg
					state.shouldSendTable[i] = false
				}
			}
		}
	}
}

type Node struct {
	Id int
}

type Edge struct {
	From     int
	To       int
	Weight   int
	Directed bool
}

type World struct {
	Nodes []*Node
	Edges []*Edge
}

func NewEdge(from, to, weight int, directed bool) *Edge {
	return &Edge{
		From:     from,
		To:       to,
		Weight:   weight,
		Directed: directed,
	}
}

func BlankWorld() *World {
	return &World{
		Edges: []*Edge{},
		Nodes: []*Node{},
	}
}

// returns a list of node IDs which neighbor the input node ID
func (w *World) Neighbors(id int) []int {
	neighbors := []int{}
	for _, e := range w.Edges {
		if e.From == id {
			neighbors = append(neighbors, e.To)
		} else if e.To == id {
			neighbors = append(neighbors, e.From)
		}
	}
	return neighbors
}

// generates a graph in neighbor list format
func generateWorld(nodes, d int, weighted bool) *World {
	rand.Seed(time.Now().UnixNano())
	neighborP := float32(d) / float32(nodes-1)
	fmt.Printf("Generating graph with neighbor probability %.03f\n", neighborP)
	world := BlankWorld()
	for i := 0; i < nodes; i++ {
		world.Nodes = append(world.Nodes, &Node{Id: i})
		for j := 0; j < nodes; j++ {
			if i != j && rand.Float32() <= neighborP {
				weight := 1
				if weighted {
					weight = rand.Intn(100)
				}
				e := NewEdge(i, j, weight, false)
				if !world.HasEdge(i, j) {
					world.Edges = append(world.Edges, e)
				}
			}

		}
	}
	return world
}

func (w *World) HasEdge(i, j int) bool {
	for _, e := range w.Edges {
		if e.From == i && e.To == j {
			return true
		} else if e.From == j && e.To == i {
			return true
		}
	}
	return false

}

func (w *World) String() string {
	str := ""
	for _, node := range w.Nodes {
		line := fmt.Sprintf("%d: [", node.Id)
		neighbors := w.Neighbors(node.Id)
		for _, n := range neighbors {
			line += fmt.Sprintf("%d, ", n)
		}
		line += "]\n"
		str += fmt.Sprintf("%s", line)
	}
	return str
}

func initializeChannels(w *World) (sendTable, receiveTable map[int][]chan Message) {
	// map of node ids to their receive channels
	var receiveChannel = make(map[int][]chan Message)

	// map of node ids to their send channels
	var sendChannel = make(map[int][]chan Message)
	for _, e := range w.Edges {
		FromTo := make(chan Message, NODES) // From sends, To receives
		ToFrom := make(chan Message, NODES) // To sends, From receives
		receiveChannel[e.To] = append(receiveChannel[e.To], FromTo)
		receiveChannel[e.From] = append(receiveChannel[e.From], ToFrom)
		sendChannel[e.To] = append(sendChannel[e.To], ToFrom)
		sendChannel[e.From] = append(sendChannel[e.From], FromTo)
	}
	return sendChannel, receiveChannel
}

func generateInitialMaxState(w *World, n *Node, max int) MaxState {
	val := rand.Intn(max)
	shouldSendTable := map[int]bool{}
	for sendIndex := range w.Neighbors(n.Id) {
		shouldSendTable[sendIndex] = true
	}
	return MaxState{
		value:           val,
		max:             val,
		shouldSendTable: shouldSendTable,
	}
}

func main() {
	w := generateWorld(NODES, 3, false)
	sendTable, receiveTable := initializeChannels(w)
	fmt.Printf("%v", w)
	resultChans := map[int]chan Message{}
	// Run MaxState worker
	for _, n := range w.Nodes {
		state := generateInitialMaxState(w, n, MAX)

		// For monitoring purposes only
		result := make(chan Message, NODES)
		log.Printf("Starting worker proc for Node %d, initial state %v, sendTable size %d, receiveTable size %d", n.Id, state, len(sendTable[n.Id]), len(receiveTable[n.Id]))
		resultChans[n.Id] = result
		go MaxWorkerProc(n, state, sendTable[n.Id], receiveTable[n.Id], result)

	}

	// Checking for termination/convergence
	curMaxes := map[int]int{}
	for {
		time.Sleep(5 * time.Second)
		for id, channel := range resultChans {
			select {
			case maxMsg := <-channel:
				curMaxes[id] = maxMsg.payload

			default:
				complete := true
				for _, max := range curMaxes {
					complete = complete && max == curMaxes[0]
				}
				if complete {
					fmt.Printf("Max: %d\n", curMaxes[0])
					return
				}

			}
		}
		fmt.Printf("%v\n", curMaxes)
	}

}
