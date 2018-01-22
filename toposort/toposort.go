package toposort

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

// Reworked from https://github.com/philopon/go-toposort

// Graph represents a directed acyclic graph.
type Graph struct {
	// Outgoing connections for every node.
	outputs map[string]map[string]int
	// How many parents each node has.
	inputs map[string]int
}

// NewGraph initializes a new Graph.
func NewGraph() *Graph {
	return &Graph{
		inputs:  map[string]int{},
		outputs: map[string]map[string]int{},
	}
}

// Copy clones the graph and returns the independent copy.
func (g *Graph) Copy() *Graph {
	clone := NewGraph()
	for k, v := range g.inputs {
		clone.inputs[k] = v
	}
	for k1, v1 := range g.outputs {
		m := map[string]int{}
		clone.outputs[k1] = m
		for k2, v2 := range v1 {
			m[k2] = v2
		}
	}
	return clone
}

// AddNode inserts a new node into the graph.
func (g *Graph) AddNode(name string) bool {
	if _, exists := g.outputs[name]; exists {
		return false
	}
	g.outputs[name] = make(map[string]int)
	g.inputs[name] = 0
	return true
}

// AddNodes inserts multiple nodes into the graph at once.
func (g *Graph) AddNodes(names ...string) bool {
	for _, name := range names {
		if ok := g.AddNode(name); !ok {
			return false
		}
	}
	return true
}

// AddEdge inserts the link from "from" node to "to" node.
func (g *Graph) AddEdge(from, to string) int {
	m, ok := g.outputs[from]
	if !ok {
		return 0
	}

	m[to] = len(m) + 1
	ni := g.inputs[to] + 1
	g.inputs[to] = ni

	return ni
}

// ReindexNode updates the internal representation of the node after edge removals.
func (g *Graph) ReindexNode(node string) {
	children, ok := g.outputs[node]
	if !ok {
		return
	}
	keys := []string{}
	for key := range children {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		children[key] = i + 1
	}
}

func (g *Graph) unsafeRemoveEdge(from, to string) {
	delete(g.outputs[from], to)
	g.inputs[to]--
}

// RemoveEdge deletes the link from "from" node to "to" node.
// Call ReindexNode(from) after you finish modifying the edges.
func (g *Graph) RemoveEdge(from, to string) bool {
	if _, ok := g.outputs[from]; !ok {
		return false
	}
	g.unsafeRemoveEdge(from, to)
	return true
}

// Toposort sorts the nodes in the graph in topological order.
func (g *Graph) Toposort() ([]string, bool) {
	L := make([]string, 0, len(g.outputs))
	S := make([]string, 0, len(g.outputs))

	for n := range g.outputs {
		if g.inputs[n] == 0 {
			S = append(S, n)
		}
	}
	sort.Strings(S)

	for len(S) > 0 {
		var n string
		n, S = S[0], S[1:]
		L = append(L, n)

		ms := make([]string, len(g.outputs[n]))
		for m, i := range g.outputs[n] {
			ms[i-1] = m
		}

		for _, m := range ms {
			g.unsafeRemoveEdge(n, m)

			if g.inputs[m] == 0 {
				S = append(S, m)
			}
		}
	}

	N := 0
	for _, v := range g.inputs {
		N += v
	}

	if N > 0 {
		return L, false
	}

	return L, true
}

// BreadthSort sorts the nodes in the graph in BFS order.
func (g *Graph) BreadthSort() []string {
	L := make([]string, 0, len(g.outputs))
	S := make([]string, 0, len(g.outputs))

	for n := range g.outputs {
		if g.inputs[n] == 0 {
			S = append(S, n)
		}
	}

	visited := map[string]bool{}
	for len(S) > 0 {
		node := S[0]
		S = S[1:]
		if _, exists := visited[node]; !exists {
			L = append(L, node)
			visited[node] = true
			for child := range g.outputs[node] {
				S = append(S, child)
			}
		}
	}

	return L
}

// FindCycle returns the cycle in the graph which contains "seed" node.
func (g *Graph) FindCycle(seed string) []string {
	type edge struct {
		node   string
		parent string
	}
	S := make([]edge, 0, len(g.outputs))
	S = append(S, edge{seed, ""})
	visited := map[string]string{}
	for len(S) > 0 {
		e := S[0]
		S = S[1:]
		if parent, exists := visited[e.node]; !exists || parent == "" {
			visited[e.node] = e.parent
			for child := range g.outputs[e.node] {
				S = append(S, edge{child, e.node})
			}
		}
		if e.node == seed && e.parent != "" {
			result := []string{}
			node := e.parent
			for node != seed {
				result = append(result, node)
				node = visited[node]
			}
			result = append(result, seed)
			// reverse
			for left, right := 0, len(result)-1; left < right; left, right = left+1, right-1 {
				result[left], result[right] = result[right], result[left]
			}
			return result
		}
	}
	return []string{}
}

// FindParents returns the other ends of incoming edges.
func (g *Graph) FindParents(to string) []string {
	result := []string{}
	for node, children := range g.outputs {
		if _, exists := children[to]; exists {
			result = append(result, node)
		}
	}
	return result
}

// FindChildren returns the other ends of outgoing edges.
func (g *Graph) FindChildren(from string) []string {
	result := []string{}
	for child := range g.outputs[from] {
		result = append(result, child)
	}
	sort.Strings(result)
	return result
}

// Serialize outputs the graph in Graphviz format.
func (g *Graph) Serialize(sorted []string) string {
	node2index := map[string]int{}
	for index, node := range sorted {
		node2index[node] = index
	}
	var buffer bytes.Buffer
	buffer.WriteString("digraph Hercules {\n")
	nodesFrom := []string{}
	for nodeFrom := range g.outputs {
		nodesFrom = append(nodesFrom, nodeFrom)
	}
	sort.Strings(nodesFrom)
	for _, nodeFrom := range nodesFrom {
		links := []string{}
		for nodeTo := range g.outputs[nodeFrom] {
			links = append(links, nodeTo)
		}
		sort.Strings(links)
		for _, nodeTo := range links {
			buffer.WriteString(fmt.Sprintf("  \"%d %s\" -> \"%d %s\"\n",
				node2index[nodeFrom], nodeFrom, node2index[nodeTo], nodeTo))
		}
	}
	buffer.WriteString("}")
	return buffer.String()
}

// DebugDump converts the graph to a string. As the name suggests, useful for debugging.
func (g *Graph) DebugDump() string {
	S := make([]string, 0, len(g.outputs))
	for n := range g.outputs {
		if g.inputs[n] == 0 {
			S = append(S, n)
		}
	}
	sort.Strings(S)
	var buffer bytes.Buffer
	buffer.WriteString(strings.Join(S, " ") + "\n")
	keys := []string{}
	vals := map[string][]string{}
	for key, val1 := range g.outputs {
		val2 := make([]string, len(val1))
		for name, idx := range val1 {
			val2[idx-1] = name
		}
		keys = append(keys, key)
		vals[key] = val2
	}
	sort.Strings(keys)
	for _, key := range keys {
		buffer.WriteString(fmt.Sprintf("%s %d = ", key, g.inputs[key]))
		outs := vals[key]
		buffer.WriteString(strings.Join(outs, " ") + "\n")
	}
	return buffer.String()
}
