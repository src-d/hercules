package leaves

import (
	"fmt"
	"io"
	"log"
	"sort"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/bblfsh/client-go.v2/tools"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v5/internal/core"
	"gopkg.in/src-d/hercules.v5/internal/pb"
	items "gopkg.in/src-d/hercules.v5/internal/plumbing"
	uast_items "gopkg.in/src-d/hercules.v5/internal/plumbing/uast"
)

// ShotnessAnalysis contains the intermediate state which is mutated by Consume(). It should implement
// LeafPipelineItem.
type ShotnessAnalysis struct {
	core.NoopMerger
	core.OneShotMergeProcessor
	XpathStruct string
	XpathName   string

	nodes map[string]*nodeShotness
	files map[string]map[string]*nodeShotness
}

const (
	// ConfigShotnessXpathStruct is the name of the configuration option (ShotnessAnalysis.Configure())
	// which sets the UAST XPath to choose the analysed nodes.
	ConfigShotnessXpathStruct = "Shotness.XpathStruct"
	// ConfigShotnessXpathName is the name of the configuration option (ShotnessAnalysis.Configure())
	// which sets the UAST XPath to find the name of the nodes chosen by ConfigShotnessXpathStruct.
	// These XPath-s can be different for some languages.
	ConfigShotnessXpathName = "Shotness.XpathName"

	// DefaultShotnessXpathStruct is the default UAST XPath to choose the analysed nodes.
	// It extracts functions.
	DefaultShotnessXpathStruct = "//*[@roleFunction and @roleDeclaration]"
	// DefaultShotnessXpathName is the default UAST XPath to choose the names of the analysed nodes.
	// It looks at the current tree level and at the immediate children.
	DefaultShotnessXpathName = "/*[@roleFunction and @roleIdentifier and @roleName] | /*/*[@roleFunction and @roleIdentifier and @roleName]"
)

type nodeShotness struct {
	Count   int
	Summary NodeSummary
	Couples map[string]int
}

// NodeSummary carries the node attributes which annotate the "shotness" analysis' counters.
// These attributes are supposed to uniquely identify each node.
type NodeSummary struct {
	InternalRole string
	Roles        []uast.Role
	Name         string
	File         string
}

// ShotnessResult is returned by ShotnessAnalysis.Finalize() and represents the analysis result.
type ShotnessResult struct {
	Nodes    []NodeSummary
	Counters []map[int]int
}

func (node NodeSummary) String() string {
	return node.InternalRole + "_" + node.Name + "_" + node.File
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (shotness *ShotnessAnalysis) Name() string {
	return "Shotness"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (shotness *ShotnessAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (shotness *ShotnessAnalysis) Requires() []string {
	arr := [...]string{items.DependencyFileDiff, uast_items.DependencyUastChanges}
	return arr[:]
}

// Features which must be enabled for this PipelineItem to be automatically inserted into the DAG.
func (shotness *ShotnessAnalysis) Features() []string {
	arr := [...]string{uast_items.FeatureUast}
	return arr[:]
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (shotness *ShotnessAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	opts := [...]core.ConfigurationOption{{
		Name:        ConfigShotnessXpathStruct,
		Description: "UAST XPath query to use for filtering the nodes.",
		Flag:        "shotness-xpath-struct",
		Type:        core.StringConfigurationOption,
		Default:     DefaultShotnessXpathStruct}, {
		Name:        ConfigShotnessXpathName,
		Description: "UAST XPath query to determine the names of the filtered nodes.",
		Flag:        "shotness-xpath-name",
		Type:        core.StringConfigurationOption,
		Default:     DefaultShotnessXpathName},
	}
	return opts[:]
}

// Flag returns the command line switch which activates the analysis.
func (shotness *ShotnessAnalysis) Flag() string {
	return "shotness"
}

// Description returns the text which explains what the analysis is doing.
func (shotness *ShotnessAnalysis) Description() string {
	return "Structural hotness - the granular alternative to --couples. " +
		"Given an XPath over UASTs - selecting functions by default - we build the square " +
		"co-occurrence matrix. The value in each cell equals to the number of times the pair " +
		"of selected UAST units appeared in the same commit."
}

// Configure sets the properties previously published by ListConfigurationOptions().
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

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (shotness *ShotnessAnalysis) Initialize(repository *git.Repository) {
	shotness.nodes = map[string]*nodeShotness{}
	shotness.files = map[string]map[string]*nodeShotness{}
	shotness.OneShotMergeProcessor.Initialize()
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (shotness *ShotnessAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if !shotness.ShouldConsumeCommit(deps) {
		return nil, nil
	}
	commit := deps[core.DependencyCommit].(*object.Commit)
	changesList := deps[uast_items.DependencyUastChanges].([]uast_items.Change)
	diffs := deps[items.DependencyFileDiff].(map[string]items.FileDiffData)
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
				log.Printf("Shotness: commit %s file %s failed to filter UAST: %s\n",
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
			log.Printf("Shotness: commit ^%s file %s failed to filter UAST: %s\n",
				commit.Hash.String(), change.Change.From.Name, err.Error())
			continue
		}
		reversedNodesBefore := reverseNodeMap(nodesBefore)
		nodesAfter, err := shotness.extractNodes(change.After)
		if err != nil {
			log.Printf("Shotness: commit %s file %s failed to filter UAST: %s\n",
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
					uast_items.VisitEachNode(node, func(child *uast.Node) {
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

// Fork clones this PipelineItem.
func (shotness *ShotnessAnalysis) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(shotness, n)
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
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

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
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
	_, err = writer.Write(serialized)
	return err
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
		if internal[mainNode] {
			continue
		}
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
	core.Registry.Register(&ShotnessAnalysis{})
}
