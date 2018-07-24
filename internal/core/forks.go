package core

import (
	"log"
	"reflect"
	"sort"

	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/hercules.v4/internal/toposort"
)

// OneShotMergeProcessor provides the convenience method to consume merges only once.
type OneShotMergeProcessor struct {
	merges map[plumbing.Hash]bool
}

// Initialize resets OneShotMergeProcessor.
func (proc *OneShotMergeProcessor) Initialize() {
	proc.merges = map[plumbing.Hash]bool{}
}

// ShouldConsumeCommit returns true on regular commits. It also returns true upon
// the first occurrence of a particular merge commit.
func (proc *OneShotMergeProcessor) ShouldConsumeCommit(deps map[string]interface{}) bool {
	commit := deps[DependencyCommit].(*object.Commit)
	if commit.NumParents() <= 1 {
		return true
	}
	if !proc.merges[commit.Hash] {
		proc.merges[commit.Hash] = true
		return true
	}
	return false
}

// NoopMerger provides an empty Merge() method suitable for PipelineItem.
type NoopMerger struct {
}

// Merge does nothing.
func (merger *NoopMerger) Merge(branches []PipelineItem) {
	// no-op
}

// ForkSamePipelineItem clones items by referencing the same origin.
func ForkSamePipelineItem(origin PipelineItem, n int) []PipelineItem {
	clones := make([]PipelineItem, n)
	for i := 0; i < n; i++ {
		clones[i] = origin
	}
	return clones
}

// ForkCopyPipelineItem clones items by copying them by value from the origin.
func ForkCopyPipelineItem(origin PipelineItem, n int) []PipelineItem {
	originValue := reflect.Indirect(reflect.ValueOf(origin))
	originType := originValue.Type()
	clones := make([]PipelineItem, n)
	for i := 0; i < n; i++ {
		cloneValue := reflect.New(originType).Elem()
		cloneValue.Set(originValue)
		clones[i] = cloneValue.Addr().Interface().(PipelineItem)
	}
	return clones
}

const (
	// runActionCommit corresponds to a regular commit
	runActionCommit = 0
	// runActionFork splits a branch into several parts
	runActionFork = iota
	// runActionMerge merges several branches together
	runActionMerge = iota
	// runActionDelete removes the branch as it is no longer needed
	runActionDelete = iota
)

type runAction struct {
	Action int
	Commit *object.Commit
	Items []int
}

func cloneItems(origin []PipelineItem, n int) [][]PipelineItem {
	clones := make([][]PipelineItem, n)
	for j := 0; j < n; j++ {
		clones[j] = make([]PipelineItem, len(origin))
	}
	for i, item := range origin {
		itemClones := item.Fork(n)
		for j := 0; j < n; j++ {
			clones[j][i] = itemClones[j]
		}
	}
	return clones
}

func mergeItems(branches [][]PipelineItem) {
	buffer := make([]PipelineItem, len(branches) - 1)
	for i, item := range branches[0] {
		for j := 0; j < len(branches)-1; j++ {
			buffer[j] = branches[j+1][i]
		}
		item.Merge(buffer)
	}
}

// getMasterBranch returns the branch with the smallest index.
func getMasterBranch(branches map[int][]PipelineItem) []PipelineItem {
	minKey := 1 << 31
	var minVal []PipelineItem
	for key, val := range branches {
		if key < minKey {
			minKey = key
			minVal = val
		}
	}
	return minVal
}

// prepareRunPlan schedules the actions for Pipeline.Run().
func prepareRunPlan(commits []*object.Commit) []runAction {
	hashes, dag := buildDag(commits)
	leaveRootComponent(hashes, dag)
	numParents := bindNumParents(hashes, dag)
	mergedDag, mergedSeq := mergeDag(numParents, hashes, dag)
	orderNodes := bindOrderNodes(mergedDag)
	collapseFastForwards(orderNodes, hashes, mergedDag, dag, mergedSeq)

	/*fmt.Printf("digraph Hercules {\n")
	for i, c := range order {
		commit := hashes[c]
		fmt.Printf("  \"%s\"[label=\"[%d] %s\"]\n", commit.Hash.String(), i, commit.Hash.String()[:6])
		for _, child := range mergedDag[commit.Hash] {
			fmt.Printf("  \"%s\" -> \"%s\"\n", commit.Hash.String(), child.Hash.String())
		}
	}
	fmt.Printf("}\n")*/

	plan := generatePlan(orderNodes, numParents, hashes, mergedDag, dag, mergedSeq)
	plan = optimizePlan(plan)
	return plan
}

// buildDag generates the raw commit DAG and the commit hash map.
func buildDag(commits []*object.Commit) (
	map[string]*object.Commit, map[plumbing.Hash][]*object.Commit) {

	hashes := map[string]*object.Commit{}
	for _, commit := range commits {
		hashes[commit.Hash.String()] = commit
	}
	dag := map[plumbing.Hash][]*object.Commit{}
	for _, commit := range commits {
		if _, exists := dag[commit.Hash]; !exists {
			dag[commit.Hash] = make([]*object.Commit, 0, 1)
		}
		for _, parent := range commit.ParentHashes {
			if _, exists := hashes[parent.String()]; !exists {
				continue
			}
			children := dag[parent]
			if children == nil {
				children = make([]*object.Commit, 0, 1)
			}
			dag[parent] = append(children, commit)
		}
	}
	return hashes, dag
}

// bindNumParents returns curried "numParents" function.
func bindNumParents(
	hashes map[string]*object.Commit,
	dag map[plumbing.Hash][]*object.Commit) func(c *object.Commit) int {
	return func(c *object.Commit) int {
		r := 0
		for _, parent := range c.ParentHashes {
			if p, exists := hashes[parent.String()]; exists {
				for _, pc := range dag[p.Hash] {
					if pc.Hash == c.Hash {
						r++
						break
					}
				}
			}
		}
		return r
	}
}

// leaveRootComponent runs connected components analysis and throws away everything
// but the part which grows from the root.
func leaveRootComponent(
	hashes map[string]*object.Commit,
	dag map[plumbing.Hash][]*object.Commit) {

	visited := map[plumbing.Hash]bool{}
	var sets [][]plumbing.Hash
	for key := range dag {
		if visited[key] {
			continue
		}
		var set []plumbing.Hash
		for queue := []plumbing.Hash{key}; len(queue) > 0; {
			head := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			if visited[head] {
				continue
			}
			set = append(set, head)
			visited[head] = true
			for _, c := range dag[head] {
				if !visited[c.Hash] {
					queue = append(queue, c.Hash)
				}
			}
			if commit, exists := hashes[head.String()]; exists {
				for _, p := range commit.ParentHashes {
					if !visited[p] {
						if _, exists := hashes[p.String()]; exists {
							queue = append(queue, p)
						}
					}
				}
			}
		}
		sets = append(sets, set)
	}
	if len(sets) > 1 {
		maxlen := 0
		maxind := -1
		for i, set := range sets {
			if len(set) > maxlen {
				maxlen = len(set)
				maxind = i
			}
		}
		for i, set := range sets {
			if i == maxind {
				continue
			}
			for _, h := range set {
				log.Printf("warning: dropped %s from the analysis - disjoint", h.String())
				delete(dag, h)
				delete(hashes, h.String())
			}
		}
	}
}

// bindOrderNodes returns curried "orderNodes" function.
func bindOrderNodes(mergedDag map[plumbing.Hash][]*object.Commit) func(reverse bool) []string {
	return func(reverse bool) []string {
		graph := toposort.NewGraph()
		keys := make([]plumbing.Hash, 0, len(mergedDag))
		for key := range mergedDag {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
		for _, key := range keys {
			graph.AddNode(key.String())
		}
		for _, key := range keys {
			children := mergedDag[key]
			sort.Slice(children, func(i, j int) bool {
				return children[i].Hash.String() < children[j].Hash.String()
			})
			for _, c := range children {
				graph.AddEdge(key.String(), c.Hash.String())
			}
		}
		order, ok := graph.Toposort()
		if !ok {
			// should never happen
			panic("Could not topologically sort the DAG of commits")
		}
		if reverse {
			// one day this must appear in the standard library...
			for i, j := 0, len(order)-1; i < len(order)/2; i, j = i+1, j-1 {
				order[i], order[j] = order[j], order[i]
			}
		}
		return order
	}
}

// mergeDag turns sequences of consecutive commits into single nodes.
func mergeDag(
	numParents func(c *object.Commit) int,
	hashes map[string]*object.Commit,
	dag map[plumbing.Hash][]*object.Commit) (
	mergedDag, mergedSeq map[plumbing.Hash][]*object.Commit) {

	parentOf := func(c *object.Commit) plumbing.Hash {
		var parent plumbing.Hash
		for _, p := range c.ParentHashes {
			if _, exists := hashes[p.String()]; exists {
				if parent != plumbing.ZeroHash {
					// more than one parent
					return plumbing.ZeroHash
				}
				parent = p
			}
		}
		return parent
	}
	mergedDag = map[plumbing.Hash][]*object.Commit{}
	mergedSeq = map[plumbing.Hash][]*object.Commit{}
	visited := map[plumbing.Hash]bool{}
	for ch := range dag {
		c := hashes[ch.String()]
		if visited[c.Hash] {
			continue
		}
		for true {
			parent := parentOf(c)
			if parent == plumbing.ZeroHash || len(dag[parent]) != 1 {
				break
			}
			c = hashes[parent.String()]
		}
		head := c
		var seq []*object.Commit
		children := dag[c.Hash]
		for true {
			visited[c.Hash] = true
			seq = append(seq, c)
			if len(children) != 1 {
				break
			}
			c = children[0]
			children = dag[c.Hash]
			if numParents(c) != 1 {
				break
			}
		}
		mergedSeq[head.Hash] = seq
		mergedDag[head.Hash] = dag[seq[len(seq)-1].Hash]
	}
	return
}

// collapseFastForwards removes the fast forward merges.
func collapseFastForwards(
	orderNodes func(reverse bool) []string,
	hashes map[string]*object.Commit,
	mergedDag, dag, mergedSeq map[plumbing.Hash][]*object.Commit)  {

	for _, strkey := range orderNodes(true) {
		key := hashes[strkey].Hash
		vals, exists := mergedDag[key]
		if !exists {
			continue
		}
		if len(vals) == 2 {
			grand1 := mergedDag[vals[0].Hash]
			grand2 := mergedDag[vals[1].Hash]
			if len(grand2) == 1 && vals[0].Hash == grand2[0].Hash {
				mergedDag[key] = mergedDag[vals[0].Hash]
				dag[key] = vals[1:]
				delete(mergedDag, vals[0].Hash)
				delete(mergedDag, vals[1].Hash)
				mergedSeq[key] = append(mergedSeq[key], mergedSeq[vals[1].Hash]...)
				mergedSeq[key] = append(mergedSeq[key], mergedSeq[vals[0].Hash]...)
				delete(mergedSeq, vals[0].Hash)
				delete(mergedSeq, vals[1].Hash)
			}
			// symmetric
			if len(grand1) == 1 && vals[1].Hash == grand1[0].Hash {
				mergedDag[key] = mergedDag[vals[1].Hash]
				dag[key] = vals[:1]
				delete(mergedDag, vals[0].Hash)
				delete(mergedDag, vals[1].Hash)
				mergedSeq[key] = append(mergedSeq[key], mergedSeq[vals[0].Hash]...)
				mergedSeq[key] = append(mergedSeq[key], mergedSeq[vals[1].Hash]...)
				delete(mergedSeq, vals[0].Hash)
				delete(mergedSeq, vals[1].Hash)
			}
		}
	}
}

// generatePlan creates the list of actions from the commit DAG.
func generatePlan(
	orderNodes func(reverse bool) []string,
	numParents func(c *object.Commit) int,
	hashes map[string]*object.Commit,
	mergedDag, dag, mergedSeq map[plumbing.Hash][]*object.Commit) []runAction {

	var plan []runAction
	branches := map[plumbing.Hash]int{}
	counter := 1
	for seqIndex, name := range orderNodes(false) {
		commit := hashes[name]
		if seqIndex == 0 {
			branches[commit.Hash] = 0
		}
		var branch int
		{
			var exists bool
			branch, exists = branches[commit.Hash]
			if !exists {
				branch = -1
			}
		}
		branchExists := func() bool { return branch >= 0 }
		appendCommit := func(c *object.Commit, branch int) {
			plan = append(plan, runAction{
				Action: runActionCommit,
				Commit: c,
				Items: []int{branch},
			})
		}
		appendMergeIfNeeded := func() {
			if numParents(commit) < 2 {
				return
			}
			// merge after the merge commit (the first in the sequence)
			var items []int
			minBranch := 1 << 31
			for _, parent := range commit.ParentHashes {
				if _, exists := hashes[parent.String()]; exists {
					parentBranch := branches[parent]
					if len(dag[parent]) == 1 && minBranch > parentBranch {
						minBranch = parentBranch
					}
					items = append(items, parentBranch)
					if parentBranch != branch {
						appendCommit(commit, parentBranch)
					}
				}
			}
			if minBranch < 1 << 31 {
				branch = minBranch
				branches[commit.Hash] = minBranch
			} else if !branchExists() {
				panic("!branchExists()")
			}
			plan = append(plan, runAction{
				Action: runActionMerge,
				Commit: nil,
				Items: items,
			})
		}
		if subseq, exists := mergedSeq[commit.Hash]; exists {
			for subseqIndex, offspring := range subseq {
				if branchExists() {
					appendCommit(offspring, branch)
				}
				if subseqIndex == 0 {
					appendMergeIfNeeded()
				}
			}
			branches[subseq[len(subseq)-1].Hash] = branch
		}
		if len(mergedDag[commit.Hash]) > 1 {
			branches[mergedDag[commit.Hash][0].Hash] = branch
			children := []int{branch}
			for i, child := range mergedDag[commit.Hash] {
				if i > 0 {
					branches[child.Hash] = counter
					children = append(children, counter)
					counter++
				}
			}
			plan = append(plan, runAction{
				Action: runActionFork,
				Commit: nil,
				Items: children,
			})
		}
	}
	return plan
}

// optimizePlan removes "dead" nodes and inserts `runActionDelete` disposal steps.
//
// |   *
// *  /
// |\/
// |/
// *
//
func optimizePlan(plan []runAction) []runAction {
	// lives maps branch index to the number of commits in that branch
	lives := map[int]int{}
	// lastMentioned maps branch index to the index inside `plan` when that branch was last used
	lastMentioned := map[int]int{}
	for i, p := range plan {
		firstItem := p.Items[0]
		switch p.Action {
		case runActionCommit:
			lives[firstItem]++
			lastMentioned[firstItem] = i
		case runActionFork:
			lastMentioned[firstItem] = i
		case runActionMerge:
			for _, item := range p.Items {
				lastMentioned[item] = i
			}
		}
	}
	branchesToDelete := map[int]bool{}
	for key, life := range lives {
		if life == 1 {
			branchesToDelete[key] = true
			delete(lastMentioned, key)
		}
	}
	var optimizedPlan []runAction
	lastMentionedArr := make([][2]int, 0, len(lastMentioned) + 1)
	for key, val := range lastMentioned {
		if val != len(plan) - 1 {
			lastMentionedArr = append(lastMentionedArr, [2]int{val, key})
		}
	}
	if len(lastMentionedArr) == 0 && len(branchesToDelete) == 0 {
		// early return - we have nothing to optimize
		return plan
	}
	sort.Slice(lastMentionedArr, func(i, j int) bool {
		return lastMentionedArr[i][0] < lastMentionedArr[j][0]
	})
	lastMentionedArr = append(lastMentionedArr, [2]int{len(plan)-1, -1})
	prevpi := -1
	for _, pair := range lastMentionedArr {
		for pi := prevpi + 1; pi <= pair[0]; pi++ {
			p := plan[pi]
			switch p.Action {
			case runActionCommit:
				if !branchesToDelete[p.Items[0]] {
					optimizedPlan = append(optimizedPlan, p)
				}
			case runActionFork:
				var newBranches []int
				for _, b := range p.Items {
					if !branchesToDelete[b] {
						newBranches = append(newBranches, b)
					}
				}
				if len(newBranches) > 1 {
					optimizedPlan = append(optimizedPlan, runAction{
						Action: runActionFork,
						Commit: p.Commit,
						Items:  newBranches,
					})
				}
			case runActionMerge:
				var newBranches []int
				for _, b := range p.Items {
					if !branchesToDelete[b] {
						newBranches = append(newBranches, b)
					}
				}
				if len(newBranches) > 1 {
					optimizedPlan = append(optimizedPlan, runAction{
						Action: runActionMerge,
						Commit: p.Commit,
						Items:  newBranches,
					})
				}
			}
		}
		if pair[1] >= 0 {
			prevpi = pair[0]
			optimizedPlan = append(optimizedPlan, runAction{
				Action: runActionDelete,
				Commit: nil,
				Items:  []int{pair[1]},
			})
		}
	}
	// single commit can be detected as redundant
	if len(optimizedPlan) > 0 {
		return optimizedPlan
	}
	return plan
	// TODO(vmarkovtsev): there can be also duplicate redundant merges, e.g.
	/*
	0 4e34f03d829fbacb71cde0e010de87ea945dc69a [3]
	0 4e34f03d829fbacb71cde0e010de87ea945dc69a [12]
	2                                          [3 12]
	0 06716c2b39422938b77ddafa4d5c39bb9e4476da [3]
	0 06716c2b39422938b77ddafa4d5c39bb9e4476da [12]
	2                                          [3 12]
	0 1219c7bf9e0e1a93459a052ab8b351bfc379dc19 [12]
	*/
}
