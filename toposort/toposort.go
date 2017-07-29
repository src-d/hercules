package toposort

// Copied from https://github.com/philopon/go-toposort

type Graph struct {
	nodes   []string
	outputs map[string]map[string]int
	inputs  map[string]int
}

func NewGraph() *Graph {
	return &Graph{
		nodes:   []string{},
		inputs:  map[string]int{},
		outputs: map[string]map[string]int{},
	}
}

func (g *Graph) AddNode(name string) bool {
	g.nodes = append(g.nodes, name)

	if _, ok := g.outputs[name]; ok {
		return false
	}
	g.outputs[name] = make(map[string]int)
	g.inputs[name] = 0
	return true
}

func (g *Graph) AddNodes(names ...string) bool {
	for _, name := range names {
		if ok := g.AddNode(name); !ok {
			return false
		}
	}
	return true
}

func (g *Graph) AddEdge(from, to string) bool {
	m, ok := g.outputs[from]
	if !ok {
		return false
	}

	m[to] = len(m) + 1
	g.inputs[to]++

	return true
}

func (g *Graph) unsafeRemoveEdge(from, to string) {
	delete(g.outputs[from], to)
	g.inputs[to]--
}

func (g *Graph) RemoveEdge(from, to string) bool {
	if _, ok := g.outputs[from]; !ok {
		return false
	}
	g.unsafeRemoveEdge(from, to)
	return true
}

func (g *Graph) Toposort() ([]string, bool) {
	L := make([]string, 0, len(g.nodes))
	S := make([]string, 0, len(g.nodes))

	for _, n := range g.nodes {
		if g.inputs[n] == 0 {
			S = append(S, n)
		}
	}

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
