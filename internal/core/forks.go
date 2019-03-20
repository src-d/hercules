package core

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/toposort"
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
	// runActionEmerge starts a root branch
	runActionEmerge = iota
	// runActionDelete removes the branch as it is no longer needed
	runActionDelete = iota
	// runActionHibernate preserves the items in the branch
	runActionHibernate = iota
	// runActionBoot does the opposite to runActionHibernate - recovers the original memory
	runActionBoot = iota

	// rootBranchIndex is the minimum branch index in the plan
	rootBranchIndex = 1
)

// planPrintFunc is used to print the execution plan in prepareRunPlan().
var planPrintFunc = func(args ...interface{}) {
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, args...)
}

type runAction struct {
	Action int
	Commit *object.Commit
	Items  []int
}

func (ra runAction) String() string {
	switch ra.Action {
	case runActionCommit:
		return ra.Commit.Hash.String()[:7]
	case runActionFork:
		return fmt.Sprintf("fork^%d", len(ra.Items))
	case runActionMerge:
		return fmt.Sprintf("merge^%d", len(ra.Items))
	case runActionEmerge:
		return "emerge"
	case runActionDelete:
		return "delete"
	case runActionHibernate:
		return "hibernate"
	case runActionBoot:
		return "boot"
	}
	return ""
}

type orderer = func(reverse, direction bool) []string

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
	buffer := make([]PipelineItem, len(branches)-1)
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
func prepareRunPlan(commits []*object.Commit, hibernationDistance int,
	printResult bool) []runAction {
	hashes, dag := buildDag(commits)
	leaveRootComponent(hashes, dag)
	mergedDag, mergedSeq := mergeDag(hashes, dag)
	orderNodes := bindOrderNodes(mergedDag)
	collapseFastForwards(orderNodes, hashes, mergedDag, dag, mergedSeq)
	/*fmt.Printf("digraph Hercules {\n")
	for i, c := range orderNodes(false, false) {
		commit := hashes[c]
		fmt.Printf("  \"%s\"[label=\"[%d] %s\"]\n", commit.Hash.String(), i, commit.Hash.String()[:6])
		for _, child := range mergedDag[commit.Hash] {
			fmt.Printf("  \"%s\" -> \"%s\"\n", commit.Hash.String(), child.Hash.String())
		}
	}
	fmt.Printf("}\n")*/
	plan := generatePlan(orderNodes, hashes, mergedDag, dag, mergedSeq)
	plan = collectGarbage(plan)
	if hibernationDistance > 0 {
		plan = insertHibernateBoot(plan, hibernationDistance)
	}
	if printResult {
		for _, p := range plan {
			printAction(p)
		}
	}
	return plan
}

// printAction prints the specified action to stderr.
func printAction(p runAction) {
	firstItem := p.Items[0]
	switch p.Action {
	case runActionCommit:
		planPrintFunc("C", firstItem, p.Commit.Hash.String())
	case runActionFork:
		planPrintFunc("F", p.Items)
	case runActionMerge:
		planPrintFunc("M", p.Items)
	case runActionEmerge:
		planPrintFunc("E", p.Items)
	case runActionDelete:
		planPrintFunc("D", p.Items)
	case runActionHibernate:
		planPrintFunc("H", firstItem)
	case runActionBoot:
		planPrintFunc("B", firstItem)
	}
}

// getCommitParents returns the list of *unique* commit parents.
// Yes, it *is* possible to have several identical parents, and Hercules used to crash because of that.
func getCommitParents(commit *object.Commit) []plumbing.Hash {
	result := make([]plumbing.Hash, 0, len(commit.ParentHashes))
	var parents map[plumbing.Hash]bool
	if len(commit.ParentHashes) > 1 {
		parents = map[plumbing.Hash]bool{}
	}
	for _, parent := range commit.ParentHashes {
		if _, exists := parents[parent]; !exists {
			if parents != nil {
				parents[parent] = true
			}
			result = append(result, parent)
		}
	}
	return result
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

		for _, parent := range getCommitParents(commit) {
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
				for _, p := range getCommitParents(commit) {
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
func bindOrderNodes(mergedDag map[plumbing.Hash][]*object.Commit) orderer {
	return func(reverse, direction bool) []string {
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
				if !direction {
					graph.AddEdge(key.String(), c.Hash.String())
				} else {
					graph.AddEdge(c.Hash.String(), key.String())
				}
			}
		}
		order, ok := graph.Toposort()
		if !ok {
			// should never happen
			panic("Could not topologically sort the DAG of commits")
		}
		if reverse != direction {
			// one day this must appear in the standard library...
			for i, j := 0, len(order)-1; i < len(order)/2; i, j = i+1, j-1 {
				order[i], order[j] = order[j], order[i]
			}
		}
		return order
	}
}

// inverts `dag`
func buildParents(dag map[plumbing.Hash][]*object.Commit) map[plumbing.Hash]map[plumbing.Hash]bool {
	parents := map[plumbing.Hash]map[plumbing.Hash]bool{}
	for key, vals := range dag {
		for _, val := range vals {
			myps := parents[val.Hash]
			if myps == nil {
				myps = map[plumbing.Hash]bool{}
				parents[val.Hash] = myps
			}
			myps[key] = true
		}
	}
	return parents
}

// mergeDag turns sequences of consecutive commits into single nodes.
func mergeDag(
	hashes map[string]*object.Commit,
	dag map[plumbing.Hash][]*object.Commit) (
	mergedDag, mergedSeq map[plumbing.Hash][]*object.Commit) {

	parents := buildParents(dag)
	mergedDag = map[plumbing.Hash][]*object.Commit{}
	mergedSeq = map[plumbing.Hash][]*object.Commit{}
	visited := map[plumbing.Hash]bool{}
	for head := range dag {
		if visited[head] {
			continue
		}
		c := head
		for true {
			nextParents := parents[c]
			var next plumbing.Hash
			for p := range nextParents {
				next = p
				break
			}
			if len(nextParents) != 1 || len(dag[next]) != 1 {
				break
			}
			c = next
		}
		head = c
		var seq []*object.Commit
		for true {
			visited[c] = true
			seq = append(seq, hashes[c.String()])
			if len(dag[c]) != 1 {
				break
			}
			c = dag[c][0].Hash
			if len(parents[c]) != 1 {
				break
			}
		}
		mergedSeq[head] = seq
		mergedDag[head] = dag[seq[len(seq)-1].Hash]
	}
	return
}

// collapseFastForwards removes the fast forward merges.
func collapseFastForwards(
	orderNodes orderer, hashes map[string]*object.Commit,
	mergedDag, dag, mergedSeq map[plumbing.Hash][]*object.Commit) {

	parents := buildParents(mergedDag)
	processed := map[plumbing.Hash]bool{}
	for _, strkey := range orderNodes(false, true) {
		key := hashes[strkey].Hash
		processed[key] = true
	repeat:
		vals, exists := mergedDag[key]
		if !exists {
			continue
		}
		if len(vals) < 2 {
			continue
		}
		toRemove := map[plumbing.Hash]bool{}
		sort.Slice(vals, func(i, j int) bool { return vals[i].Hash.String() < vals[j].Hash.String() })
		for _, child := range vals {
			var queue []plumbing.Hash
			visited := map[plumbing.Hash]bool{child.Hash: true}
			childParents := parents[child.Hash]
			childNumOtherParents := 0
			for parent := range childParents {
				if parent != key {
					visited[parent] = true
					childNumOtherParents++
					queue = append(queue, parent)
				}
			}
			var immediateParent plumbing.Hash
			if childNumOtherParents == 1 {
				immediateParent = queue[0]
			}
			for len(queue) > 0 {
				head := queue[len(queue)-1]
				queue = queue[:len(queue)-1]
				if processed[head] {
					if head == key {
						toRemove[child.Hash] = true
						if childNumOtherParents == 1 && len(mergedDag[immediateParent]) == 1 {
							mergedSeq[immediateParent] = append(
								mergedSeq[immediateParent], mergedSeq[child.Hash]...)
							delete(mergedSeq, child.Hash)
							mergedDag[immediateParent] = mergedDag[child.Hash]
							delete(mergedDag, child.Hash)
							parents[child.Hash] = parents[immediateParent]
							for _, vals := range parents {
								for v := range vals {
									if v == child.Hash {
										delete(vals, v)
										vals[immediateParent] = true
										break
									}
								}
							}
						}
						break
					}
				} else {
					for parent := range parents[head] {
						if !visited[parent] {
							visited[head] = true
							queue = append(queue, parent)
						}
					}
				}
			}
		}
		if len(toRemove) == 0 {
			continue
		}

		// update dag
		var newVals []*object.Commit
		node := mergedSeq[key][len(mergedSeq[key])-1].Hash
		for _, child := range dag[node] {
			if !toRemove[child.Hash] {
				newVals = append(newVals, child)
			}
		}
		dag[node] = newVals

		// update mergedDag
		newVals = []*object.Commit{}
		for _, child := range vals {
			if !toRemove[child.Hash] {
				newVals = append(newVals, child)
			}
		}
		merged := false
		if len(newVals) == 1 {
			onlyChild := newVals[0].Hash
			if len(parents[onlyChild]) == 1 {
				merged = true
				mergedSeq[key] = append(mergedSeq[key], mergedSeq[onlyChild]...)
				delete(mergedSeq, onlyChild)
				mergedDag[key] = mergedDag[onlyChild]
				delete(mergedDag, onlyChild)
				parents[onlyChild] = parents[key]
				for _, vals := range parents {
					for v := range vals {
						if v == onlyChild {
							delete(vals, v)
							vals[key] = true
							break
						}
					}
				}
			}
		}

		// update parents
		for rm := range toRemove {
			delete(parents[rm], key)
		}

		if !merged {
			mergedDag[key] = newVals
		} else {
			goto repeat
		}
	}
}

// generatePlan creates the list of actions from the commit DAG.
func generatePlan(
	orderNodes orderer, hashes map[string]*object.Commit,
	mergedDag, dag, mergedSeq map[plumbing.Hash][]*object.Commit) []runAction {

	parents := buildParents(dag)
	var plan []runAction
	branches := map[plumbing.Hash]int{}
	branchers := map[plumbing.Hash]map[plumbing.Hash]int{}
	counter := rootBranchIndex
	for _, name := range orderNodes(false, true) {
		commit := hashes[name]
		if len(parents[commit.Hash]) == 0 {
			branches[commit.Hash] = counter
			plan = append(plan, runAction{
				Action: runActionEmerge,
				Commit: commit,
				Items:  []int{counter},
			})
			counter++
		}
		var branch int
		{
			var exists bool
			branch, exists = branches[commit.Hash]
			if !exists {
				branch = -1
			}
		}
		branchExists := func() bool { return branch >= rootBranchIndex }
		appendCommit := func(c *object.Commit, branch int) {
			if branch == 0 {
				log.Panicf("setting a zero branch for %s", c.Hash.String())
			}
			plan = append(plan, runAction{
				Action: runActionCommit,
				Commit: c,
				Items:  []int{branch},
			})
		}
		appendMergeIfNeeded := func() bool {
			if len(parents[commit.Hash]) < 2 {
				return false
			}
			// merge after the merge commit (the first in the sequence)
			var items []int
			minBranch := 1 << 31
			for parent := range parents[commit.Hash] {
				parentBranch := -1
				if parents, exists := branchers[commit.Hash]; exists {
					if inheritedBranch, exists := parents[parent]; exists {
						parentBranch = inheritedBranch
					}
				}
				if parentBranch == -1 {
					parentBranch = branches[parent]
					if parentBranch < rootBranchIndex {
						log.Panicf("parent %s > %s does not have a branch assigned",
							parent.String(), commit.Hash.String())
					}
				}
				if len(dag[parent]) == 1 && minBranch > parentBranch {
					minBranch = parentBranch
				}
				items = append(items, parentBranch)
				if parentBranch != branch {
					appendCommit(commit, parentBranch)
				}
			}
			// there should be no duplicates in items
			if minBranch < 1<<31 {
				branch = minBranch
				branches[commit.Hash] = minBranch
			} else if !branchExists() {
				log.Panicf("failed to assign the branch to merge %s", commit.Hash.String())
			}
			plan = append(plan, runAction{
				Action: runActionMerge,
				Commit: nil,
				Items:  items,
			})
			return true
		}
		var head plumbing.Hash
		if subseq, exists := mergedSeq[commit.Hash]; exists {
			for subseqIndex, offspring := range subseq {
				if branchExists() {
					appendCommit(offspring, branch)
				}
				if subseqIndex == 0 {
					if !appendMergeIfNeeded() && !branchExists() {
						log.Panicf("head of the sequence does not have an assigned branch: %s",
							commit.Hash.String())
					}
				}
			}
			head = subseq[len(subseq)-1].Hash
			branches[head] = branch
		} else {
			head = commit.Hash
		}
		if len(mergedDag[commit.Hash]) > 1 {
			children := []int{branch}
			for i, child := range mergedDag[commit.Hash] {
				if i == 0 {
					branches[child.Hash] = branch
					continue
				}
				if _, exists := branches[child.Hash]; !exists {
					branches[child.Hash] = counter
				}
				parents := branchers[child.Hash]
				if parents == nil {
					parents = map[plumbing.Hash]int{}
					branchers[child.Hash] = parents
				}
				parents[head] = counter
				children = append(children, counter)
				counter++
			}
			plan = append(plan, runAction{
				Action: runActionFork,
				Commit: hashes[head.String()],
				Items:  children,
			})
		}
	}
	return plan
}

// collectGarbage inserts `runActionDelete` disposal steps.
func collectGarbage(plan []runAction) []runAction {
	// lastMentioned maps branch index to the index inside `plan` when that branch was last used
	lastMentioned := map[int]int{}
	for i, p := range plan {
		firstItem := p.Items[0]
		switch p.Action {
		case runActionCommit:
			lastMentioned[firstItem] = i
			if firstItem < rootBranchIndex {
				log.Panicf("commit %s does not have an assigned branch",
					p.Commit.Hash.String())
			}
		case runActionFork:
			lastMentioned[firstItem] = i
		case runActionMerge:
			for _, item := range p.Items {
				lastMentioned[item] = i
			}
		case runActionEmerge:
			lastMentioned[firstItem] = i
		}
	}
	var garbageCollectedPlan []runAction
	lastMentionedArr := make([][2]int, 0, len(lastMentioned)+1)
	for key, val := range lastMentioned {
		if val != len(plan)-1 {
			lastMentionedArr = append(lastMentionedArr, [2]int{val, key})
		}
	}
	if len(lastMentionedArr) == 0 {
		// early return - we have nothing to collect
		return plan
	}
	sort.Slice(lastMentionedArr, func(i, j int) bool {
		return lastMentionedArr[i][0] < lastMentionedArr[j][0]
	})
	lastMentionedArr = append(lastMentionedArr, [2]int{len(plan) - 1, -1})
	prevpi := -1
	for _, pair := range lastMentionedArr {
		for pi := prevpi + 1; pi <= pair[0]; pi++ {
			garbageCollectedPlan = append(garbageCollectedPlan, plan[pi])
		}
		if pair[1] >= 0 {
			prevpi = pair[0]
			garbageCollectedPlan = append(garbageCollectedPlan, runAction{
				Action: runActionDelete,
				Commit: nil,
				Items:  []int{pair[1]},
			})
		}
	}
	return garbageCollectedPlan
}

type hbAction struct {
	Branch    int
	Hibernate bool
}

func insertHibernateBoot(plan []runAction, hibernationDistance int) []runAction {
	addons := map[int][]hbAction{}
	lastUsed := map[int]int{}
	addonsCount := 0
	for x, action := range plan {
		if action.Action == runActionDelete {
			continue
		}
		for _, item := range action.Items {
			if i, exists := lastUsed[item]; exists && (x-i-1) > hibernationDistance {
				if addons[x] == nil {
					addons[x] = make([]hbAction, 0, 1)
				}
				addons[x] = append(addons[x], hbAction{item, false})
				if addons[i] == nil {
					addons[i] = make([]hbAction, 0, 1)
				}
				addons[i] = append(addons[i], hbAction{item, true})
				addonsCount += 2
			}
			lastUsed[item] = x
		}
	}
	newPlan := make([]runAction, 0, len(plan)+addonsCount)
	for x, action := range plan {
		xaddons := addons[x]
		var boots []int
		var hibernates []int
		if len(xaddons) > 0 {
			boots = make([]int, 0, len(xaddons))
			hibernates = make([]int, 0, len(xaddons))
			for _, addon := range xaddons {
				if !addon.Hibernate {
					boots = append(boots, addon.Branch)
				} else {
					hibernates = append(hibernates, addon.Branch)
				}
			}
		}
		if len(boots) > 0 {
			newPlan = append(newPlan, runAction{
				Action: runActionBoot,
				Commit: action.Commit,
				Items:  boots,
			})
		}
		newPlan = append(newPlan, action)
		if len(hibernates) > 0 {
			newPlan = append(newPlan, runAction{
				Action: runActionHibernate,
				Commit: action.Commit,
				Items:  hibernates,
			})
		}
	}
	return newPlan
}
