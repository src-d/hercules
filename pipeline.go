package hercules

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
	"unsafe"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v3/toposort"
	"gopkg.in/src-d/hercules.v3/pb"
)

type ConfigurationOptionType int

const (
	// Boolean value type.
	BoolConfigurationOption ConfigurationOptionType = iota
	// Integer value type.
	IntConfigurationOption
	// String value type.
	StringConfigurationOption
)

const (
	ConfigPipelineDumpPath = "Pipeline.DumpPath"
	ConfigPipelineDryRun   = "Pipeline.DryRun"
)

// ConfigurationOption allows for the unified, retrospective way to setup PipelineItem-s.
type ConfigurationOption struct {
	// Name identifies the configuration option in facts.
	Name string
	// Description represents the help text about the configuration option.
	Description string
	// Flag corresponds to the CLI token with "-" prepended.
	Flag string
	// Type specifies the kind of the configuration option's value.
	Type ConfigurationOptionType
	// Default is the initial value of the configuration option.
	Default interface{}
}

// PipelineItem is the interface for all the units of the Git commit analysis pipeline.
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
	MergeResults(r1, r2 interface{}, c1, c2 *CommonAnalysisResult) (interface{}, CommonAnalysisResult)
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

func (car *CommonAnalysisResult) FillMetadata(meta *pb.Metadata) {
	meta.BeginUnixTime = car.BeginTime
	meta.EndUnixTime = car.EndTime
	meta.Commits = int32(car.CommitsNumber)
	meta.RunTime = car.RunTime.Nanoseconds() / 1e6
}

func MetadataToCommonAnalysisResult(meta *pb.Metadata) *CommonAnalysisResult {
  return &CommonAnalysisResult{
	  BeginTime:     meta.BeginUnixTime,
	  EndTime:       meta.EndUnixTime,
	  CommitsNumber: int(meta.Commits),
	  RunTime:       time.Duration(meta.RunTime * 1e6),
  }
}

// PipelineItemRegistry contains all the known PipelineItem-s.
type PipelineItemRegistry struct {
	provided   map[string][]reflect.Type
	registered map[string]reflect.Type
	flags      map[string]reflect.Type
}

// Register adds another PipelineItem to the registry.
func (registry *PipelineItemRegistry) Register(example PipelineItem) {
	t := reflect.TypeOf(example)
	registry.registered[example.Name()] = t
	if fpi, ok := example.(LeafPipelineItem); ok {
		registry.flags[fpi.Flag()] = t
	}
	for _, dep := range example.Provides() {
		ts := registry.provided[dep]
		if ts == nil {
			ts = []reflect.Type{}
		}
		ts = append(ts, t)
		registry.provided[dep] = ts
	}
}

func (registry *PipelineItemRegistry) Summon(providesOrName string) []PipelineItem {
	if registry.provided == nil {
		return []PipelineItem{}
	}
	ts := registry.provided[providesOrName]
	items := []PipelineItem{}
	for _, t := range ts {
		items = append(items, reflect.New(t.Elem()).Interface().(PipelineItem))
	}
	if t, exists := registry.registered[providesOrName]; exists {
		items = append(items, reflect.New(t.Elem()).Interface().(PipelineItem))
	}
	return items
}

type arrayFeatureFlags struct {
	// Flags containts the features activated through the command line.
	Flags []string
	// Choices contains all registered features.
	Choices map[string]bool
}

func (acf *arrayFeatureFlags) String() string {
	return strings.Join([]string(acf.Flags), ", ")
}

func (acf *arrayFeatureFlags) Set(value string) error {
	if _, exists := acf.Choices[value]; !exists {
		return errors.New(fmt.Sprintf("Feature \"%s\" is not registered.", value))
	}
	acf.Flags = append(acf.Flags, value)
	return nil
}

var featureFlags = arrayFeatureFlags{Flags: []string{}, Choices: map[string]bool{}}

// AddFlags inserts the cmdline options from PipelineItem.ListConfigurationOptions(),
// FeaturedPipelineItem().Features() and LeafPipelineItem.Flag() into the global "flag" parser
// built into the Go runtime.
// Returns the "facts" which can be fed into PipelineItem.Configure() and the dictionary of
// runnable analysis (LeafPipelineItem) choices. E.g. if "BurndownAnalysis" was activated
// through "-burndown" cmdline argument, this mapping would contain ["BurndownAnalysis"] = *true.
func (registry *PipelineItemRegistry) AddFlags() (map[string]interface{}, map[string]*bool) {
	flags := map[string]interface{}{}
	deployed := map[string]*bool{}
	for name, it := range registry.registered {
		formatHelp := func(desc string) string {
			return fmt.Sprintf("%s [%s]", desc, name)
		}
		itemIface := reflect.New(it.Elem()).Interface()
		for _, opt := range itemIface.(PipelineItem).ListConfigurationOptions() {
			var iface interface{}
			switch opt.Type {
			case BoolConfigurationOption:
				iface = interface{}(true)
				ptr := (**bool)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
				*ptr = flag.Bool(opt.Flag, opt.Default.(bool), formatHelp(opt.Description))
			case IntConfigurationOption:
				iface = interface{}(0)
				ptr := (**int)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
				*ptr = flag.Int(opt.Flag, opt.Default.(int), formatHelp(opt.Description))
			case StringConfigurationOption:
				iface = interface{}("")
				ptr := (**string)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
				*ptr = flag.String(opt.Flag, opt.Default.(string), formatHelp(opt.Description))
			}
			flags[opt.Name] = iface
		}
		if fpi, ok := itemIface.(FeaturedPipelineItem); ok {
			for _, f := range fpi.Features() {
				featureFlags.Choices[f] = true
			}
		}
		if fpi, ok := itemIface.(LeafPipelineItem); ok {
			deployed[fpi.Name()] = flag.Bool(
				fpi.Flag(), false, fmt.Sprintf("Runs %s analysis.", fpi.Name()))
		}
	}
	{
		// Pipeline flags
		iface := interface{}("")
		ptr1 := (**string)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
		*ptr1 = flag.String("dump-dag", "", "Write the pipeline DAG to a Graphviz file.")
		flags[ConfigPipelineDumpPath] = iface
		iface = interface{}(true)
		ptr2 := (**bool)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
		*ptr2 = flag.Bool("dry-run", false, "Do not run any analyses - only resolve the DAG. "+
			"Useful for -dump-dag.")
		flags[ConfigPipelineDryRun] = iface
	}
	features := []string{}
	for f := range featureFlags.Choices {
		features = append(features, f)
	}
	flag.Var(&featureFlags, "feature",
		fmt.Sprintf("Enables specific analysis features, can be specified "+
			"multiple times. Available features: [%s].", strings.Join(features, ", ")))
	return flags, deployed
}

// Registry contains all known pipeline item types.
var Registry = &PipelineItemRegistry{
	provided:   map[string][]reflect.Type{},
	registered: map[string]reflect.Type{},
	flags:      map[string]reflect.Type{},
}

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

const FactPipelineCommits = "commits"

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

func (pipeline *Pipeline) SetFeaturesFromFlags() {
	for _, feature := range featureFlags.Flags {
		pipeline.SetFeature(feature)
	}
}

func (pipeline *Pipeline) DeployItem(item PipelineItem) PipelineItem {
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
