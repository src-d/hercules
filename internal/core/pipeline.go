package core

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v4/internal/pb"
	"gopkg.in/src-d/hercules.v4/internal/toposort"
)

// ConfigurationOptionType represents the possible types of a ConfigurationOption's value.
type ConfigurationOptionType int

const (
	// BoolConfigurationOption reflects the boolean value type.
	BoolConfigurationOption ConfigurationOptionType = iota
	// IntConfigurationOption reflects the integer value type.
	IntConfigurationOption
	// StringConfigurationOption reflects the string value type.
	StringConfigurationOption
	// FloatConfigurationOption reflects a floating point value type.
	FloatConfigurationOption
	// StringsConfigurationOption reflects the array of strings value type.
	StringsConfigurationOption
)

// String() returns an empty string for the boolean type, "int" for integers and "string" for
// strings. It is used in the command line interface to show the argument's type.
func (opt ConfigurationOptionType) String() string {
	switch opt {
	case BoolConfigurationOption:
		return ""
	case IntConfigurationOption:
		return "int"
	case StringConfigurationOption:
		return "string"
	case FloatConfigurationOption:
		return "float"
	case StringsConfigurationOption:
		return "string"
	}
	panic(fmt.Sprintf("Invalid ConfigurationOptionType value %d", opt))
}

// ConfigurationOption allows for the unified, retrospective way to setup PipelineItem-s.
type ConfigurationOption struct {
	// Name identifies the configuration option in facts.
	Name string
	// Description represents the help text about the configuration option.
	Description string
	// Flag corresponds to the CLI token with "--" prepended.
	Flag string
	// Type specifies the kind of the configuration option's value.
	Type ConfigurationOptionType
	// Default is the initial value of the configuration option.
	Default interface{}
}

// FormatDefault converts the default value of ConfigurationOption to string.
// Used in the command line interface to show the argument's default value.
func (opt ConfigurationOption) FormatDefault() string {
	if opt.Type == StringsConfigurationOption {
		return fmt.Sprintf("\"%s\"", strings.Join(opt.Default.([]string), ","))
	}
	if opt.Type != StringConfigurationOption {
		return fmt.Sprint(opt.Default)
	}
	return fmt.Sprintf("\"%s\"", opt.Default)
}

// PipelineItem is the interface for all the units in the Git commits analysis pipeline.
type PipelineItem interface {
	// Name returns the name of the analysis.
	Name() string
	// Provides returns the list of keys of reusable calculated entities.
	// Other items may depend on them.
	Provides() []string
	// Requires returns the list of keys of needed entities which must be supplied in Consume().
	Requires() []string
	// ListConfigurationOptions returns the list of available options which can be consumed by Configure().
	ListConfigurationOptions() []ConfigurationOption
	// Configure performs the initial setup of the object by applying parameters from facts.
	// It allows to create PipelineItems in a universal way.
	Configure(facts map[string]interface{})
	// Initialize prepares and resets the item. Consume() requires Initialize()
	// to be called at least once beforehand.
	Initialize(*git.Repository)
	// Consume processes the next commit.
	// deps contains the required entities which match Depends(). Besides, it always includes
	// "commit" and "index".
	// Returns the calculated entities which match Provides().
	Consume(deps map[string]interface{}) (map[string]interface{}, error)
}

// FeaturedPipelineItem enables switching the automatic insertion of pipeline items on or off.
type FeaturedPipelineItem interface {
	PipelineItem
	// Features returns the list of names which enable this item to be automatically inserted
	// in Pipeline.DeployItem().
	Features() []string
}

// LeafPipelineItem corresponds to the top level pipeline items which produce the end results.
type LeafPipelineItem interface {
	PipelineItem
	// Flag returns the cmdline name of the item.
	Flag() string
	// Finalize returns the result of the analysis.
	Finalize() interface{}
	// Serialize encodes the object returned by Finalize() to YAML or Protocol Buffers.
	Serialize(result interface{}, binary bool, writer io.Writer) error
}

// MergeablePipelineItem specifies the methods to combine several analysis results together.
type MergeablePipelineItem interface {
	LeafPipelineItem
	// Deserialize loads the result from Protocol Buffers blob.
	Deserialize(pbmessage []byte) (interface{}, error)
	// MergeResults joins two results together. Common-s are specified as the global state.
	MergeResults(r1, r2 interface{}, c1, c2 *CommonAnalysisResult) interface{}
}

// CommonAnalysisResult holds the information which is always extracted at Pipeline.Run().
type CommonAnalysisResult struct {
	// Time of the first commit in the analysed sequence.
	BeginTime int64
	// Time of the last commit in the analysed sequence.
	EndTime int64
	// The number of commits in the analysed sequence.
	CommitsNumber int
	// The duration of Pipeline.Run().
	RunTime time.Duration
}

// BeginTimeAsTime converts the UNIX timestamp of the beginning to Go time.
func (car *CommonAnalysisResult) BeginTimeAsTime() time.Time {
	return time.Unix(car.BeginTime, 0)
}

// EndTimeAsTime converts the UNIX timestamp of the ending to Go time.
func (car *CommonAnalysisResult) EndTimeAsTime() time.Time {
	return time.Unix(car.EndTime, 0)
}

// Merge combines the CommonAnalysisResult with an other one.
// We choose the earlier BeginTime, the later EndTime, sum the number of commits and the
// elapsed run times.
func (car *CommonAnalysisResult) Merge(other *CommonAnalysisResult) {
	if car.EndTime == 0 || other.BeginTime == 0 {
		panic("Merging with an uninitialized CommonAnalysisResult")
	}
	if other.BeginTime < car.BeginTime {
		car.BeginTime = other.BeginTime
	}
	if other.EndTime > car.EndTime {
		car.EndTime = other.EndTime
	}
	car.CommitsNumber += other.CommitsNumber
	car.RunTime += other.RunTime
}

// FillMetadata copies the data to a Protobuf message.
func (car *CommonAnalysisResult) FillMetadata(meta *pb.Metadata) *pb.Metadata {
	meta.BeginUnixTime = car.BeginTime
	meta.EndUnixTime = car.EndTime
	meta.Commits = int32(car.CommitsNumber)
	meta.RunTime = car.RunTime.Nanoseconds() / 1e6
	return meta
}

// Metadata is defined in internal/pb/pb.pb.go - header of the binary file.
type Metadata = pb.Metadata

// MetadataToCommonAnalysisResult copies the data from a Protobuf message.
func MetadataToCommonAnalysisResult(meta *Metadata) *CommonAnalysisResult {
	return &CommonAnalysisResult{
		BeginTime:     meta.BeginUnixTime,
		EndTime:       meta.EndUnixTime,
		CommitsNumber: int(meta.Commits),
		RunTime:       time.Duration(meta.RunTime * 1e6),
	}
}

// Pipeline is the core Hercules entity which carries several PipelineItems and executes them.
// See the extended example of how a Pipeline works in doc.go
type Pipeline struct {
	// OnProgress is the callback which is invoked in Analyse() to output it's
	// progress. The first argument is the number of processed commits and the
	// second is the total number of commits.
	OnProgress func(int, int)

	// Repository points to the analysed Git repository struct from go-git.
	repository *git.Repository

	// Items are the registered building blocks in the pipeline. The order defines the
	// execution sequence.
	items []PipelineItem

	// The collection of parameters to create items.
	facts map[string]interface{}

	// Feature flags which enable the corresponding items.
	features map[string]bool
}

const (
	// ConfigPipelineDumpPath is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which enables saving the items DAG to the specified file.
	ConfigPipelineDumpPath = "Pipeline.DumpPath"
	// ConfigPipelineDryRun is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which disables Configure() and Initialize() invocation on each PipelineItem during the
	// Pipeline initialization.
	// Subsequent Run() calls are going to fail. Useful with ConfigPipelineDumpPath=true.
	ConfigPipelineDryRun = "Pipeline.DryRun"
	// ConfigPipelineCommits is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which allows to specify the custom commit sequence. By default, Pipeline.Commits() is used.
	ConfigPipelineCommits = "commits"
)

// NewPipeline initializes a new instance of Pipeline struct.
func NewPipeline(repository *git.Repository) *Pipeline {
	return &Pipeline{
		repository: repository,
		items:      []PipelineItem{},
		facts:      map[string]interface{}{},
		features:   map[string]bool{},
	}
}

// GetFact returns the value of the fact with the specified name.
func (pipeline *Pipeline) GetFact(name string) interface{} {
	return pipeline.facts[name]
}

// SetFact sets the value of the fact with the specified name.
func (pipeline *Pipeline) SetFact(name string, value interface{}) {
	pipeline.facts[name] = value
}

// GetFeature returns the state of the feature with the specified name (enabled/disabled) and
// whether it exists. See also: FeaturedPipelineItem.
func (pipeline *Pipeline) GetFeature(name string) (bool, bool) {
	val, exists := pipeline.features[name]
	return val, exists
}

// SetFeature sets the value of the feature with the specified name.
// See also: FeaturedPipelineItem.
func (pipeline *Pipeline) SetFeature(name string) {
	pipeline.features[name] = true
}

// SetFeaturesFromFlags enables the features which were specified through the command line flags
// which belong to the given PipelineItemRegistry instance.
// See also: AddItem().
func (pipeline *Pipeline) SetFeaturesFromFlags(registry ...*PipelineItemRegistry) {
	var ffr *PipelineItemRegistry
	if len(registry) == 0 {
		ffr = Registry
	} else if len(registry) == 1 {
		ffr = registry[0]
	} else {
		panic("Zero or one registry is allowed to be passed.")
	}
	for _, feature := range ffr.featureFlags.Flags {
		pipeline.SetFeature(feature)
	}
}

// DeployItem inserts a PipelineItem into the pipeline. It also recursively creates all of it's
// dependencies (PipelineItem.Requires()). Returns the same item as specified in the arguments.
func (pipeline *Pipeline) DeployItem(item PipelineItem) PipelineItem {
	fpi, ok := item.(FeaturedPipelineItem)
	if ok {
		for _, f := range fpi.Features() {
			pipeline.SetFeature(f)
		}
	}
	queue := []PipelineItem{}
	queue = append(queue, item)
	added := map[string]PipelineItem{}
	for _, item := range pipeline.items {
		added[item.Name()] = item
	}
	added[item.Name()] = item
	pipeline.AddItem(item)
	for len(queue) > 0 {
		head := queue[0]
		queue = queue[1:]
		for _, dep := range head.Requires() {
			for _, sibling := range Registry.Summon(dep) {
				if _, exists := added[sibling.Name()]; !exists {
					disabled := false
					// If this item supports features, check them against the activated in pipeline.features
					if fpi, matches := sibling.(FeaturedPipelineItem); matches {
						for _, feature := range fpi.Features() {
							if !pipeline.features[feature] {
								disabled = true
								break
							}
						}
					}
					if disabled {
						continue
					}
					added[sibling.Name()] = sibling
					queue = append(queue, sibling)
					pipeline.AddItem(sibling)
				}
			}
		}
	}
	return item
}

// AddItem inserts a PipelineItem into the pipeline. It does not check any dependencies.
// See also: DeployItem().
func (pipeline *Pipeline) AddItem(item PipelineItem) PipelineItem {
	pipeline.items = append(pipeline.items, item)
	return item
}

// RemoveItem deletes a PipelineItem from the pipeline. It leaves all the rest of the items intact.
func (pipeline *Pipeline) RemoveItem(item PipelineItem) {
	for i, reg := range pipeline.items {
		if reg == item {
			pipeline.items = append(pipeline.items[:i], pipeline.items[i+1:]...)
			return
		}
	}
}

// Len returns the number of items in the pipeline.
func (pipeline *Pipeline) Len() int {
	return len(pipeline.items)
}

// Commits returns the critical path in the repository's history. It starts
// from HEAD and traces commits backwards till the root. When it encounters
// a merge (more than one parent), it always chooses the first parent.
func (pipeline *Pipeline) Commits() []*object.Commit {
	result := []*object.Commit{}
	repository := pipeline.repository
	head, err := repository.Head()
	if err != nil {
		panic(err)
	}
	commit, err := repository.CommitObject(head.Hash())
	if err != nil {
		panic(err)
	}
	// the first parent matches the head
	for ; err != io.EOF; commit, err = commit.Parents().Next() {
		if err != nil {
			panic(err)
		}
		result = append(result, commit)
	}
	// reverse the order
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

type sortablePipelineItems []PipelineItem

func (items sortablePipelineItems) Len() int {
	return len(items)
}

func (items sortablePipelineItems) Less(i, j int) bool {
	return items[i].Name() < items[j].Name()
}

func (items sortablePipelineItems) Swap(i, j int) {
	items[i], items[j] = items[j], items[i]
}

func (pipeline *Pipeline) resolve(dumpPath string) {
	graph := toposort.NewGraph()
	sort.Sort(sortablePipelineItems(pipeline.items))
	name2item := map[string]PipelineItem{}
	ambiguousMap := map[string][]string{}
	nameUsages := map[string]int{}
	for _, item := range pipeline.items {
		nameUsages[item.Name()]++
	}
	counters := map[string]int{}
	for _, item := range pipeline.items {
		name := item.Name()
		if nameUsages[name] > 1 {
			index := counters[item.Name()] + 1
			counters[item.Name()] = index
			name = fmt.Sprintf("%s_%d", item.Name(), index)
		}
		graph.AddNode(name)
		name2item[name] = item
		for _, key := range item.Provides() {
			key = "[" + key + "]"
			graph.AddNode(key)
			if graph.AddEdge(name, key) > 1 {
				if ambiguousMap[key] != nil {
					fmt.Fprintln(os.Stderr, "Pipeline:")
					for _, item2 := range pipeline.items {
						if item2 == item {
							fmt.Fprint(os.Stderr, "> ")
						}
						fmt.Fprint(os.Stderr, item2.Name(), " [")
						for i, key2 := range item2.Provides() {
							fmt.Fprint(os.Stderr, key2)
							if i < len(item.Provides())-1 {
								fmt.Fprint(os.Stderr, ", ")
							}
						}
						fmt.Fprintln(os.Stderr, "]")
					}
					panic("Failed to resolve pipeline dependencies: ambiguous graph.")
				}
				ambiguousMap[key] = graph.FindParents(key)
			}
		}
	}
	counters = map[string]int{}
	for _, item := range pipeline.items {
		name := item.Name()
		if nameUsages[name] > 1 {
			index := counters[item.Name()] + 1
			counters[item.Name()] = index
			name = fmt.Sprintf("%s_%d", item.Name(), index)
		}
		for _, key := range item.Requires() {
			key = "[" + key + "]"
			if graph.AddEdge(key, name) == 0 {
				panic(fmt.Sprintf("Unsatisfied dependency: %s -> %s", key, item.Name()))
			}
		}
	}
	// Try to break the cycles in some known scenarios.
	if len(ambiguousMap) > 0 {
		ambiguous := []string{}
		for key := range ambiguousMap {
			ambiguous = append(ambiguous, key)
		}
		sort.Strings(ambiguous)
		bfsorder := graph.BreadthSort()
		bfsindex := map[string]int{}
		for i, s := range bfsorder {
			bfsindex[s] = i
		}
		for len(ambiguous) > 0 {
			key := ambiguous[0]
			ambiguous = ambiguous[1:]
			pair := ambiguousMap[key]
			inheritor := pair[1]
			if bfsindex[pair[1]] < bfsindex[pair[0]] {
				inheritor = pair[0]
			}
			removed := graph.RemoveEdge(key, inheritor)
			cycle := map[string]bool{}
			for _, node := range graph.FindCycle(key) {
				cycle[node] = true
			}
			if len(cycle) == 0 {
				cycle[inheritor] = true
			}
			if removed {
				graph.AddEdge(key, inheritor)
			}
			graph.RemoveEdge(inheritor, key)
			graph.ReindexNode(inheritor)
			// for all nodes key links to except those in cycle, put the link from inheritor
			for _, node := range graph.FindChildren(key) {
				if _, exists := cycle[node]; !exists {
					graph.AddEdge(inheritor, node)
					graph.RemoveEdge(key, node)
				}
			}
			graph.ReindexNode(key)
		}
	}
	var graphCopy *toposort.Graph
	if dumpPath != "" {
		graphCopy = graph.Copy()
	}
	strplan, ok := graph.Toposort()
	if !ok {
		panic("Failed to resolve pipeline dependencies: unable to topologically sort the items.")
	}
	pipeline.items = make([]PipelineItem, 0, len(pipeline.items))
	for _, key := range strplan {
		if item, ok := name2item[key]; ok {
			pipeline.items = append(pipeline.items, item)
		}
	}
	if dumpPath != "" {
		// If there is a floating difference, uncomment this:
		// fmt.Fprint(os.Stderr, graphCopy.DebugDump())
		ioutil.WriteFile(dumpPath, []byte(graphCopy.Serialize(strplan)), 0666)
		absPath, _ := filepath.Abs(dumpPath)
		log.Printf("Wrote the DAG to %s\n", absPath)
	}
}

// Initialize prepares the pipeline for the execution (Run()). This function
// resolves the execution DAG, Configure()-s and Initialize()-s the items in it in the
// topological dependency order. `facts` are passed inside Configure(). They are mutable.
func (pipeline *Pipeline) Initialize(facts map[string]interface{}) {
	if facts == nil {
		facts = map[string]interface{}{}
	}
	if _, exists := facts[ConfigPipelineCommits]; !exists {
		facts[ConfigPipelineCommits] = pipeline.Commits()
	}
	dumpPath, _ := facts[ConfigPipelineDumpPath].(string)
	pipeline.resolve(dumpPath)
	if dryRun, _ := facts[ConfigPipelineDryRun].(bool); dryRun {
		return
	}
	for _, item := range pipeline.items {
		item.Configure(facts)
	}
	for _, item := range pipeline.items {
		item.Initialize(pipeline.repository)
	}
}

// Run method executes the pipeline.
//
// `commits` is a slice with the git commits to analyse. Multiple branches are supported.
//
// Returns the mapping from each LeafPipelineItem to the corresponding analysis result.
// There is always a "nil" record with CommonAnalysisResult.
func (pipeline *Pipeline) Run(commits []*object.Commit) (map[LeafPipelineItem]interface{}, error) {
	startRunTime := time.Now()
	onProgress := pipeline.OnProgress
	if onProgress == nil {
		onProgress = func(int, int) {}
	}

	for index, commit := range commits {
		onProgress(index, len(commits))
		state := map[string]interface{}{"commit": commit, "index": index}
		for _, item := range pipeline.items {
			update, err := item.Consume(state)
			if err != nil {
				log.Printf("%s failed on commit #%d %s\n",
					item.Name(), index, commit.Hash.String())
				return nil, err
			}
			for _, key := range item.Provides() {
				val, ok := update[key]
				if !ok {
					panic(fmt.Sprintf("%s: Consume() did not return %s", item.Name(), key))
				}
				state[key] = val
			}
		}
	}
	onProgress(len(commits), len(commits))
	result := map[LeafPipelineItem]interface{}{}
	for _, item := range pipeline.items {
		if casted, ok := item.(LeafPipelineItem); ok {
			result[casted] = casted.Finalize()
		}
	}
	result[nil] = &CommonAnalysisResult{
		BeginTime:     commits[0].Author.When.Unix(),
		EndTime:       commits[len(commits)-1].Author.When.Unix(),
		CommitsNumber: len(commits),
		RunTime:       time.Since(startRunTime),
	}
	return result, nil
}

// LoadCommitsFromFile reads the file by the specified FS path and generates the sequence of commits
// by interpreting each line as a Git commit hash.
func LoadCommitsFromFile(path string, repository *git.Repository) ([]*object.Commit, error) {
	var file io.ReadCloser
	if path != "-" {
		var err error
		file, err = os.Open(path)
		if err != nil {
			return nil, err
		}
		defer file.Close()
	} else {
		file = os.Stdin
	}
	scanner := bufio.NewScanner(file)
	commits := []*object.Commit{}
	for scanner.Scan() {
		hash := plumbing.NewHash(scanner.Text())
		if len(hash) != 20 {
			return nil, errors.New("invalid commit hash " + scanner.Text())
		}
		commit, err := repository.CommitObject(hash)
		if err != nil {
			return nil, err
		}
		commits = append(commits, commit)
	}
	return commits, nil
}

const (
	runActionCommit = 0
	runActionFork = iota
	runActionMerge = iota
	runActionDelete = iota
)

type runAction struct {
	Action int
	Commit *object.Commit
	Items []int
}

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