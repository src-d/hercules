package hercules

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v3/pb"
	"gopkg.in/src-d/hercules.v3/toposort"
)

// ConfigurationOptionType represents the possible types of a ConfigurationOption's value.
type ConfigurationOptionType int

const (
	// Boolean value type.
	BoolConfigurationOption ConfigurationOptionType = iota
	// Integer value type.
	IntConfigurationOption
	// String value type.
	StringConfigurationOption
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

// FormatDefault() converts the default value of ConfigurationOption to string.
// Used in the command line interface to show the argument's default value.
func (opt ConfigurationOption) FormatDefault() string {
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

// BeginTimeAsTime() converts the UNIX timestamp of the beginning to Go time.
func (car *CommonAnalysisResult) BeginTimeAsTime() time.Time {
	return time.Unix(car.BeginTime, 0)
}

// EndTimeAsTime() converts the UNIX timestamp of the ending to Go time.
func (car *CommonAnalysisResult) EndTimeAsTime() time.Time {
	return time.Unix(car.EndTime, 0)
}

// Merge() combines the CommonAnalysisResult with an other one.
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

// FillMetadata() copies the data to a Protobuf message.
func (car *CommonAnalysisResult) FillMetadata(meta *pb.Metadata) *pb.Metadata {
	meta.BeginUnixTime = car.BeginTime
	meta.EndUnixTime = car.EndTime
	meta.Commits = int32(car.CommitsNumber)
	meta.RunTime = car.RunTime.Nanoseconds() / 1e6
	return meta
}

// MetadataToCommonAnalysisResult() copies the data from a Protobuf message.
func MetadataToCommonAnalysisResult(meta *pb.Metadata) *CommonAnalysisResult {
	return &CommonAnalysisResult{
		BeginTime:     meta.BeginUnixTime,
		EndTime:       meta.EndUnixTime,
		CommitsNumber: int(meta.Commits),
		RunTime:       time.Duration(meta.RunTime * 1e6),
	}
}

// The core Hercules entity which carries several PipelineItems and executes them.
// See the extended example of how a Pipeline works in doc.go.
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
	// Makes Pipeline to save the DAG to the specified file.
	ConfigPipelineDumpPath = "Pipeline.DumpPath"
	// Disables Configure() and Initialize() invokation on each PipelineItem during the initialization.
	// Subsequent Run() calls are going to fail. Useful with ConfigPipelineDumpPath=true.
	ConfigPipelineDryRun = "Pipeline.DryRun"
	// Allows to specify the custom commit chain. By default, Pipeline.Commits() is used.
	FactPipelineCommits = "commits"
)

func NewPipeline(repository *git.Repository) *Pipeline {
	return &Pipeline{
		repository: repository,
		items:      []PipelineItem{},
		facts:      map[string]interface{}{},
		features:   map[string]bool{},
	}
}

func (pipeline *Pipeline) GetFact(name string) interface{} {
	return pipeline.facts[name]
}

func (pipeline *Pipeline) SetFact(name string, value interface{}) {
	pipeline.facts[name] = value
}

func (pipeline *Pipeline) GetFeature(name string) (bool, bool) {
	val, exists := pipeline.features[name]
	return val, exists
}

func (pipeline *Pipeline) SetFeature(name string) {
	pipeline.features[name] = true
}

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

func (pipeline *Pipeline) AddItem(item PipelineItem) PipelineItem {
	pipeline.items = append(pipeline.items, item)
	return item
}

func (pipeline *Pipeline) RemoveItem(item PipelineItem) {
	for i, reg := range pipeline.items {
		if reg == item {
			pipeline.items = append(pipeline.items[:i], pipeline.items[i+1:]...)
			return
		}
	}
}

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
		fmt.Fprintf(os.Stderr, "Wrote the DAG to %s\n", absPath)
	}
}

func (pipeline *Pipeline) Initialize(facts map[string]interface{}) {
	if facts == nil {
		facts = map[string]interface{}{}
	}
	if _, exists := facts[FactPipelineCommits]; !exists {
		facts[FactPipelineCommits] = pipeline.Commits()
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
// commits is a slice with the sequential commit history. It shall start from
// the root (ascending order).
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
				fmt.Fprintf(os.Stderr, "%s failed on commit #%d %s\n",
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
