package hercules

import (
	"fmt"
	"io"
	"os"
	"sort"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/bblfsh/client-go.v2/tools"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v3/pb"
)

// ShotnessAnalysis contains the intermediate state which is mutated by Consume(). It should implement
// LeafPipelineItem.
type ShotnessAnalysis struct {
	XpathStruct string
	XpathName   string

	nodes map[string]*nodeShotness
	files map[string]map[string]*nodeShotness
}

const (
	ConfigShotnessXpathStruct = "Shotness.XpathStruct"
	ConfigShotnessXpathName   = "Shotness.XpathName"

	DefaultShotnessXpathStruct = "//*[@roleFunction and @roleDeclaration]"
	DefaultShotnessXpathName   = "/*[@roleFunction and @roleIdentifier and @roleName] | /*/*[@roleFunction and @roleIdentifier and @roleName]"
)

type nodeShotness struct {
	Count   int
	Summary NodeSummary
	Couples map[string]int
}

type NodeSummary struct {
	InternalRole string
	Roles        []uast.Role
	Name         string
	File         string
}

// ShotnessResult is returned by Finalize() and represents the analysis result.
type ShotnessResult struct {
	Nodes    []NodeSummary
	Counters []map[int]int
}

func (node NodeSummary) String() string {
	return node.InternalRole + "_" + node.Name + "_" + node.File
}

func (shotness *ShotnessAnalysis) Name() string {
	return "Shotness"
}

func (shotness *ShotnessAnalysis) Provides() []string {
	return []string{}
}

func (ref *ShotnessAnalysis) Features() []string {
	arr := [...]string{FeatureUast}
	return arr[:]
}

func (shotness *ShotnessAnalysis) Requires() []string {
	arr := [...]string{DependencyFileDiff, DependencyUastChanges}
	return arr[:]
}

// ListConfigurationOptions tells the engine which parameters can be changed through the command
// line.
func (shotness *ShotnessAnalysis) ListConfigurationOptions() []ConfigurationOption {
	opts := [...]ConfigurationOption{{
		Name:        ConfigShotnessXpathStruct,
		Description: "UAST XPath query to use for filtering the nodes.",
		Flag:        "shotness-xpath-struct",
		Type:        StringConfigurationOption,
		Default:     DefaultShotnessXpathStruct}, {
		Name:        ConfigShotnessXpathName,
		Description: "UAST XPath query to determine the names of the filtered nodes.",
		Flag:        "shotness-xpath-name",
		Type:        StringConfigurationOption,
		Default:     DefaultShotnessXpathName},
	}
	return opts[:]
}

// Flag returns the command line switch which activates the analysis.
func (shotness *ShotnessAnalysis) Flag() string {
	return "shotness"
}

// Configure applies the parameters specified in the command line.
func (shotness *ShotnessAnalysis) Configure(facts map[string]interface{}) {
	if val, exists := facts[ConfigShotnessXpathStruct]; exists {
		shotness.XpathStruct = val.(string)
	} else {
		shotness.XpathStruct = DefaultShotnessXpathStruct
	}
	if val, exists := facts[ConfigShotnessXpathName]; exists {
		shotness.XpathName = val.(string)
	} else {
		shotness.XpathName = DefaultShotnessXpathName
	}
}

// Initialize resets the internal temporary data structures and prepares the object for Consume().
func (shotness *ShotnessAnalysis) Initialize(repository *git.Repository) {
	shotness.nodes = map[string]*nodeShotness{}
	shotness.files = map[string]map[string]*nodeShotness{}
}

// Consume is called for every commit in the sequence.
func (shotness *ShotnessAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps["commit"].(*object.Commit)
	changesList := deps[DependencyUastChanges].([]UASTChange)
	diffs := deps[DependencyFileDiff].(map[string]FileDiffData)
	allNodes := map[string]bool{}

	addNode := func(name string, node *uast.Node, fileName string) {
		nodeSummary := NodeSummary{
			InternalRole: node.InternalType,
			Roles:        node.Roles,
			Name:         name,
			File:         fileName,
		}
		key := nodeSummary.String()
		exists := allNodes[key]
		allNodes[key] = true
		var count int
		if ns := shotness.nodes[key]; ns != nil {
			count = ns.Count
		}
		if count == 0 {
			shotness.nodes[key] = &nodeShotness{
				Summary: nodeSummary, Count: 1, Couples: map[string]int{}}
			fmap := shotness.files[nodeSummary.File]
			if fmap == nil {
				fmap = map[string]*nodeShotness{}
			}
			fmap[key] = shotness.nodes[key]
			shotness.files[nodeSummary.File] = fmap
		} else if !exists { // in case there are removals and additions in the same node
			shotness.nodes[key].Count = count + 1
		}
	}

	for _, change := range changesList {
		if change.After == nil {
			for key, summary := range shotness.files[change.Change.From.Name] {
				for subkey := range summary.Couples {
					delete(shotness.nodes[subkey].Couples, key)
				}
			}
			for key := range shotness.files[change.Change.From.Name] {
				delete(shotness.nodes, key)
			}
			delete(shotness.files, change.Change.From.Name)
			continue
		}
		toName := change.Change.To.Name
		if change.Before == nil {
			nodes, err := shotness.extractNodes(change.After)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Shotness: commit %s file %s failed to filter UAST: %s\n",
					commit.Hash.String(), toName, err.Error())
				continue
			}
			for name, node := range nodes {
				addNode(name, node, toName)
			}
			continue
		}
		// Before -> After
		if change.Change.From.Name != toName {
			// renamed
			oldFile := shotness.files[change.Change.From.Name]
			newFile := map[string]*nodeShotness{}
			shotness.files[toName] = newFile
			for oldKey, ns := range oldFile {
				ns.Summary.File = toName
				newKey := ns.Summary.String()
				newFile[newKey] = ns
				shotness.nodes[newKey] = ns
				for coupleKey, count := range ns.Couples {
					coupleCouples := shotness.nodes[coupleKey].Couples
					delete(coupleCouples, oldKey)
					coupleCouples[newKey] = count
				}
			}
			// deferred cleanup is needed
			for key := range oldFile {
				delete(shotness.nodes, key)
			}
			delete(shotness.files, change.Change.From.Name)
		}
		// pass through old UAST
		// pass through new UAST
		nodesBefore, err := shotness.extractNodes(change.Before)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Shotness: commit ^%s file %s failed to filter UAST: %s\n",
				commit.Hash.String(), change.Change.From.Name, err.Error())
			continue
		}
		reversedNodesBefore := reverseNodeMap(nodesBefore)
		nodesAfter, err := shotness.extractNodes(change.After)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Shotness: commit %s file %s failed to filter UAST: %s\n",
				commit.Hash.String(), toName, err.Error())
			continue
		}
		reversedNodesAfter := reverseNodeMap(nodesAfter)
		genLine2Node := func(nodes map[string]*uast.Node, linesNum int) [][]*uast.Node {
			res := make([][]*uast.Node, linesNum)
			for _, node := range nodes {
				if node.StartPosition == nil {
					continue
				}
				startLine := node.StartPosition.Line
				endLine := node.StartPosition.Line
				if node.EndPosition != nil && node.EndPosition.Line > node.StartPosition.Line {
					endLine = node.EndPosition.Line
				} else {
					// we need to determine node.EndPosition.Line
					VisitEachNode(node, func(child *uast.Node) {
						if child.StartPosition != nil {
							candidate := child.StartPosition.Line
							if child.EndPosition != nil {
								candidate = child.EndPosition.Line
							}
							if candidate > endLine {
								endLine = candidate
							}
						}
					})
				}
				for l := startLine; l <= endLine; l++ {
					lineNodes := res[l-1]
					if lineNodes == nil {
						lineNodes = []*uast.Node{}
					}
					lineNodes = append(lineNodes, node)
					res[l-1] = lineNodes
				}
			}
			return res
		}
		diff := diffs[toName]
		line2nodeBefore := genLine2Node(nodesBefore, diff.OldLinesOfCode)
		line2nodeAfter := genLine2Node(nodesAfter, diff.NewLinesOfCode)
		// Scan through all the edits. Given the line numbers, get the list of active nodes
		// and add them.
		var lineNumBefore, lineNumAfter int
		for _, edit := range diff.Diffs {
			size := utf8.RuneCountInString(edit.Text)
			switch edit.Type {
			case diffmatchpatch.DiffDelete:
				for l := lineNumBefore; l < lineNumBefore+size; l++ {
					nodes := line2nodeBefore[l]
					for _, node := range nodes {
						// toName because we handled a possible rename before
						addNode(reversedNodesBefore[node], node, toName)
					}
				}
				lineNumBefore += size
			case diffmatchpatch.DiffInsert:
				for l := lineNumAfter; l < lineNumAfter+size; l++ {
					nodes := line2nodeAfter[l]
					for _, node := range nodes {
						addNode(reversedNodesAfter[node], node, toName)
					}
				}
				lineNumAfter += size
			case diffmatchpatch.DiffEqual:
				lineNumBefore += size
				lineNumAfter += size
			}
		}
	}
	for keyi := range allNodes {
		for keyj := range allNodes {
			if keyi == keyj {
				continue
			}
			shotness.nodes[keyi].Couples[keyj]++
		}
	}
	return nil, nil
}

// Finalize produces the result of the analysis. No more Consume() calls are expected afterwards.
func (shotness *ShotnessAnalysis) Finalize() interface{} {
	result := ShotnessResult{
		Nodes:    make([]NodeSummary, len(shotness.nodes)),
		Counters: make([]map[int]int, len(shotness.nodes)),
	}
	keys := make([]string, len(shotness.nodes))
	i := 0
	for key := range shotness.nodes {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	reverseKeys := map[string]int{}
	for i, key := range keys {
		reverseKeys[key] = i
	}
	for i, key := range keys {
		node := shotness.nodes[key]
		result.Nodes[i] = node.Summary
		counter := map[int]int{}
		result.Counters[i] = counter
		counter[i] = node.Count
		for ck, val := range node.Couples {
			counter[reverseKeys[ck]] = val
		}
	}
	return result
}

// Serialize converts the result from Finalize() to either Protocol Buffers or YAML.
func (shotness *ShotnessAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	shotnessResult := result.(ShotnessResult)
	if binary {
		return shotness.serializeBinary(&shotnessResult, writer)
	}
	shotness.serializeText(&shotnessResult, writer)
	return nil
}

func (shotness *ShotnessAnalysis) serializeText(result *ShotnessResult, writer io.Writer) {
	for i, summary := range result.Nodes {
		fmt.Fprintf(writer, "  - name: %s\n    file: %s\n    internal_role: %s\n    roles: [",
			summary.Name, summary.File, summary.InternalRole)
		for j, r := range summary.Roles {
			if j < len(summary.Roles)-1 {
				fmt.Fprintf(writer, "%d,", r)
			} else {
				fmt.Fprintf(writer, "%d]\n    counters: {", r)
			}
		}
		keys := make([]int, len(result.Counters[i]))
		j := 0
		for key := range result.Counters[i] {
			keys[j] = key
			j++
		}
		sort.Ints(keys)
		j = 0
		for _, key := range keys {
			val := result.Counters[i][key]
			if j < len(result.Counters[i])-1 {
				fmt.Fprintf(writer, "\"%d\":%d,", key, val)
			} else {
				fmt.Fprintf(writer, "\"%d\":%d}\n", key, val)
			}
			j++
		}
	}
}

func (shotness *ShotnessAnalysis) serializeBinary(result *ShotnessResult, writer io.Writer) error {
	message := pb.ShotnessAnalysisResults{
		Records: make([]*pb.ShotnessRecord, len(result.Nodes)),
	}
	for i, summary := range result.Nodes {
		record := &pb.ShotnessRecord{
			Name:         summary.Name,
			File:         summary.File,
			InternalRole: summary.InternalRole,
			Roles:        make([]int32, len(summary.Roles)),
			Counters:     map[int32]int32{},
		}
		for j, r := range summary.Roles {
			record.Roles[j] = int32(r)
		}
		for key, val := range result.Counters[i] {
			record.Counters[int32(key)] = int32(val)
		}
		message.Records[i] = record
	}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	writer.Write(serialized)
	return nil
}

func (shotness *ShotnessAnalysis) extractNodes(root *uast.Node) (map[string]*uast.Node, error) {
	structs, err := tools.Filter(root, shotness.XpathStruct)
	if err != nil {
		return nil, err
	}
	// some structs may be inside other structs; we pick the outermost
	// otherwise due to UAST quirks there may be false positives
	internal := map[*uast.Node]bool{}
	for _, mainNode := range structs {
		subs, err := tools.Filter(mainNode, shotness.XpathStruct)
		if err != nil {
			return nil, err
		}
		for _, sub := range subs {
			if sub != mainNode {
				internal[sub] = true
			}
		}
	}
	res := map[string]*uast.Node{}
	for _, node := range structs {
		if internal[node] {
			continue
		}
		nodeNames, err := tools.Filter(node, shotness.XpathName)
		if err != nil {
			return nil, err
		}
		if len(nodeNames) == 0 {
			continue
		}
		res[nodeNames[0].Token] = node
	}
	return res, nil
}

func reverseNodeMap(nodes map[string]*uast.Node) map[*uast.Node]string {
	res := map[*uast.Node]string{}
	for key, node := range nodes {
		res[node] = key
	}
	return res
}

func init() {
	Registry.Register(&ShotnessAnalysis{})
}
