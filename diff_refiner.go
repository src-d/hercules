package hercules

import (
	"unicode/utf8"

	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/src-d/go-git.v4"
)

type FileDiffRefiner struct {
}

func (ref *FileDiffRefiner) Name() string {
	return "FileDiffRefiner"
}

func (ref *FileDiffRefiner) Provides() []string {
	arr := [...]string{DependencyFileDiff}
	return arr[:]
}

func (ref *FileDiffRefiner) Requires() []string {
	arr := [...]string{DependencyFileDiff, DependencyUastChanges}
	return arr[:]
}

func (ref *FileDiffRefiner) Features() []string {
	arr := [...]string{FeatureUast}
	return arr[:]
}

func (ref *FileDiffRefiner) ListConfigurationOptions() []ConfigurationOption {
	return []ConfigurationOption{}
}

func (ref *FileDiffRefiner) Configure(facts map[string]interface{}) {}

func (ref *FileDiffRefiner) Initialize(repository *git.Repository) {
}

func (ref *FileDiffRefiner) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	changesList := deps[DependencyUastChanges].([]UASTChange)
	changes := map[string]UASTChange{}
	for _, change := range changesList {
		if change.Before != nil && change.After != nil {
			changes[change.Change.To.Name] = change
		}
	}
	diffs := deps[DependencyFileDiff].(map[string]FileDiffData)
	result := map[string]FileDiffData{}
	for fileName, oldDiff := range diffs {
		suspicious := map[int][2]int{}
		line := 0
		for i, diff := range oldDiff.Diffs {
			if i == len(oldDiff.Diffs)-1 {
				break
			}
			if diff.Type == diffmatchpatch.DiffInsert &&
				oldDiff.Diffs[i+1].Type == diffmatchpatch.DiffEqual {
				matched := 0
				runesAdded := []rune(diff.Text)
				runesEqual := []rune(oldDiff.Diffs[i+1].Text)
				for ; matched < len(runesAdded) && matched < len(runesEqual) &&
					runesAdded[matched] == runesEqual[matched]; matched++ {
				}
				if matched > 0 {
					suspicious[i] = [2]int{line, matched}
				}
			}
			if diff.Type != diffmatchpatch.DiffDelete {
				line += utf8.RuneCountInString(diff.Text)
			}
		}
		if len(suspicious) == 0 {
			result[fileName] = oldDiff
			continue
		}
		uastChange := changes[fileName]
		line2node := make([][]*uast.Node, oldDiff.NewLinesOfCode)
		visitEachNode(uastChange.After, func(node *uast.Node) {
			if node.StartPosition != nil && node.EndPosition != nil {
				for l := node.StartPosition.Line; l <= node.EndPosition.Line; l++ {
					nodes := line2node[l-1]  // line starts with 1
					if nodes == nil {
						nodes = []*uast.Node{}
					}
					line2node[l-1] = append(nodes, node)
				}
			}
		})
		newDiff := FileDiffData{
			OldLinesOfCode: oldDiff.OldLinesOfCode,
			NewLinesOfCode: oldDiff.NewLinesOfCode,
			Diffs:          []diffmatchpatch.Diff{},
		}
		skipNext := false
		for i, diff := range oldDiff.Diffs {
			if skipNext {
				skipNext = false
				continue
			}
			info, exists := suspicious[i]
			if !exists {
				newDiff.Diffs = append(newDiff.Diffs, diff)
				continue
			}
			line := info[0]
			matched := info[1]
			size := utf8.RuneCountInString(diff.Text)
			n1 := countNodesInInterval(line2node, line, line+size)
			n2 := countNodesInInterval(line2node, line+matched, line+size+matched)
			if n1 <= n2 {
				newDiff.Diffs = append(newDiff.Diffs, diff)
				continue
			}
			skipNext = true
			runes := []rune(diff.Text)
			newDiff.Diffs = append(newDiff.Diffs, diffmatchpatch.Diff{
				Type: diffmatchpatch.DiffEqual, Text: string(runes[:matched]),
			})
			newDiff.Diffs = append(newDiff.Diffs, diffmatchpatch.Diff{
				Type: diffmatchpatch.DiffInsert, Text: string(runes[matched:]) + string(runes[:matched]),
			})
			runes = []rune(oldDiff.Diffs[i+1].Text)
			if len(runes) > matched {
				newDiff.Diffs = append(newDiff.Diffs, diffmatchpatch.Diff{
					Type: diffmatchpatch.DiffEqual, Text: string(runes[matched:]),
				})
			}
		}
		result[fileName] = newDiff
	}
	return map[string]interface{}{DependencyFileDiff: result}, nil
}

// Depth first tree traversal.
func visitEachNode(root *uast.Node, payload func(*uast.Node)) {
	queue := []*uast.Node{}
	queue = append(queue, root)
	for len(queue) > 0 {
		node := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		payload(node)
		for _, child := range node.Children {
			queue = append(queue, child)
		}
	}
}

func countNodesInInterval(occupiedMap [][]*uast.Node, start, end int) int {
	nodes := map[*uast.Node]bool{}
	for i := start; i < end; i++ {
		for _, node := range occupiedMap[i] {
			nodes[node] = true
		}
	}
	return len(nodes)
}

func init() {
	Registry.Register(&FileDiffRefiner{})
}
