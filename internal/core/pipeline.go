package core

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	"gopkg.in/src-d/hercules.v10/internal/toposort"
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
	// PathConfigurationOption reflects the file system path value type.
	PathConfigurationOption
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
	case PathConfigurationOption:
		return "path"
	}
	log.Panicf("Invalid ConfigurationOptionType value %d", opt)
	return ""
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
	Configure(facts map[string]interface{}) error
	// Initialize prepares and resets the item. Consume() requires Initialize()
	// to be called at least once beforehand.
	Initialize(*git.Repository) error
	// Consume processes the next commit.
	// deps contains the required entities which match Depends(). Besides, it always includes
	// DependencyCommit and DependencyIndex.
	// Returns the calculated entities which match Provides().
	Consume(deps map[string]interface{}) (map[string]interface{}, error)
	// Fork clones the item the requested number of times. The data links between the clones
	// are up to the implementation. Needed to handle Git branches. See also Merge().
	// Returns a slice with `n` fresh clones. In other words, it does not include the original item.
	Fork(n int) []PipelineItem
	// Merge combines several branches together. Each is supposed to have been created with Fork().
	// The result is stored in the called item, thus this function returns nothing.
	// Merge() must update all the branches, not only self. When several branches merge, some of
	// them may continue to live, hence this requirement.
	Merge(branches []PipelineItem)
}

// FeaturedPipelineItem enables switching the automatic insertion of pipeline items on or off.
type FeaturedPipelineItem interface {
	PipelineItem
	// Features returns the list of names which enable this item to be automatically inserted
	// in Pipeline.DeployItem().
	Features() []string
}

// DisposablePipelineItem enables resources cleanup after finishing running the pipeline.
type DisposablePipelineItem interface {
	PipelineItem
	// Dispose frees any previously allocated unmanaged resources. No Consume() calls are possible
	// afterwards. The item needs to be Initialize()-d again.
	// This method is invoked once for each item in the pipeline, **in a single forked instance**.
	// Thus it is the responsibility of the item's programmer to deal with forks and merges, if
	// necessary.
	Dispose()
}

// LeafPipelineItem corresponds to the top level pipeline items which produce the end results.
type LeafPipelineItem interface {
	PipelineItem
	// Flag returns the cmdline switch to run the analysis. Should be dash-lower-case
	// without the leading dashes.
	Flag() string
	// Description returns the text which explains what the analysis is doing.
	// Should start with a capital letter and end with a dot.
	Description() string
	// Finalize returns the result of the analysis.
	Finalize() interface{}
	// Serialize encodes the object returned by Finalize() to YAML or Protocol Buffers.
	Serialize(result interface{}, binary bool, writer io.Writer) error
}

// ResultMergeablePipelineItem specifies the methods to combine several analysis results together.
type ResultMergeablePipelineItem interface {
	LeafPipelineItem
	// Deserialize loads the result from Protocol Buffers blob.
	Deserialize(pbmessage []byte) (interface{}, error)
	// MergeResults joins two results together. Common-s are specified as the global state.
	MergeResults(r1, r2 interface{}, c1, c2 *CommonAnalysisResult) interface{}
}

// HibernateablePipelineItem is the interface to allow pipeline items to be frozen (compacted, unloaded)
// while they are not needed in the hosting branch.
type HibernateablePipelineItem interface {
	PipelineItem
	// Hibernate signals that the item is temporarily not needed and it's memory can be optimized.
	Hibernate() error
	// Boot signals that the item is needed again and must be de-hibernate-d.
	Boot() error
}

// CommonAnalysisResult holds the information which is always extracted at Pipeline.Run().
type CommonAnalysisResult struct {
	// BeginTime is the time of the first commit in the analysed sequence.
	BeginTime int64
	// EndTime is the time of the last commit in the analysed sequence.
	EndTime int64
	// CommitsNumber is the number of commits in the analysed sequence.
	CommitsNumber int
	// RunTime is the duration of Pipeline.Run().
	RunTime time.Duration
	// RunTimePerItem is the time elapsed by each PipelineItem.
	RunTimePerItem map[string]float64
}

// Copy produces a deep clone of the object.
func (car CommonAnalysisResult) Copy() CommonAnalysisResult {
	result := car
	result.RunTimePerItem = map[string]float64{}
	for key, val := range car.RunTimePerItem {
		result.RunTimePerItem[key] = val
	}
	return result
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
	for key, val := range other.RunTimePerItem {
		car.RunTimePerItem[key] += val
	}
}

// FillMetadata copies the data to a Protobuf message.
func (car *CommonAnalysisResult) FillMetadata(meta *pb.Metadata) *pb.Metadata {
	meta.BeginUnixTime = car.BeginTime
	meta.EndUnixTime = car.EndTime
	meta.Commits = int32(car.CommitsNumber)
	meta.RunTime = car.RunTime.Nanoseconds() / 1e6
	meta.RunTimePerItem = car.RunTimePerItem
	return meta
}

// Metadata is defined in internal/pb/pb.pb.go - header of the binary file.
type Metadata = pb.Metadata

// MetadataToCommonAnalysisResult copies the data from a Protobuf message.
func MetadataToCommonAnalysisResult(meta *Metadata) *CommonAnalysisResult {
	return &CommonAnalysisResult{
		BeginTime:      meta.BeginUnixTime,
		EndTime:        meta.EndUnixTime,
		CommitsNumber:  int(meta.Commits),
		RunTime:        time.Duration(meta.RunTime * 1e6),
		RunTimePerItem: meta.RunTimePerItem,
	}
}

// Pipeline is the core Hercules entity which carries several PipelineItems and executes them.
// See the extended example of how a Pipeline works in doc.go
type Pipeline struct {
	// OnProgress is the callback which is invoked in Analyse() to output it's
	// progress. The first argument is the number of complete steps, the
	// second is the total number of steps and the third is some description of the current action.
	OnProgress func(int, int, string)

	// HibernationDistance is the minimum number of actions between two sequential usages of
	// a branch to activate the hibernation optimization (cpu-memory trade-off). 0 disables.
	HibernationDistance int

	// DryRun indicates whether the items are not executed.
	DryRun bool

	// DumpPlan indicates whether to print the execution plan to stderr.
	DumpPlan bool

	// PrintActions indicates whether to print the taken actions during the execution.
	PrintActions bool

	// Repository points to the analysed Git repository struct from go-git.
	repository *git.Repository

	// Items are the registered building blocks in the pipeline. The order defines the
	// execution sequence.
	items []PipelineItem

	// The collection of parameters to create items.
	facts map[string]interface{}

	// Feature flags which enable the corresponding items.
	features map[string]bool

	// The logger for printing output.
	l Logger
}

const (
	// ConfigPipelineDAGPath is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which enables saving the items DAG to the specified file.
	ConfigPipelineDAGPath = "Pipeline.DAGPath"
	// ConfigPipelineDryRun is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which disables Configure() and Initialize() invocation on each PipelineItem during the
	// Pipeline initialization.
	// Subsequent Run() calls are going to fail. Useful with ConfigPipelineDAGPath=true.
	ConfigPipelineDryRun = "Pipeline.DryRun"
	// ConfigPipelineCommits is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which allows to specify the custom commit sequence. By default, Pipeline.Commits() is used.
	ConfigPipelineCommits = "Pipeline.Commits"
	// ConfigPipelineDumpPlan is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which outputs the execution plan to stderr.
	ConfigPipelineDumpPlan = "Pipeline.DumpPlan"
	// ConfigPipelineHibernationDistance is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which is the minimum number of actions between two sequential usages of
	// a branch to activate the hibernation optimization (cpu-memory trade-off). 0 disables.
	ConfigPipelineHibernationDistance = "Pipeline.HibernationDistance"
	// ConfigPipelinePrintActions is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which enables printing the taken actions of the execution plan to stderr.
	ConfigPipelinePrintActions = "Pipeline.PrintActions"
	// DependencyCommit is the name of one of the three items in `deps` supplied to PipelineItem.Consume()
	// which always exists. It corresponds to the currently analyzed commit.
	DependencyCommit = "commit"
	// DependencyIndex is the name of one of the three items in `deps` supplied to PipelineItem.Consume()
	// which always exists. It corresponds to the currently analyzed commit's index.
	DependencyIndex = "index"
	// DependencyIsMerge is the name of one of the three items in `deps` supplied to PipelineItem.Consume()
	// which always exists. It indicates whether the analyzed commit is a merge commit.
	// Checking the number of parents is not correct - we remove the back edges during the DAG simplification.
	DependencyIsMerge = "is_merge"
	// MessageFinalize is the status text reported before calling LeafPipelineItem.Finalize()-s.
	MessageFinalize = "finalize"
)

// NewPipeline initializes a new instance of Pipeline struct.
func NewPipeline(repository *git.Repository) *Pipeline {
	return &Pipeline{
		repository: repository,
		items:      []PipelineItem{},
		facts:      map[string]interface{}{},
		features:   map[string]bool{},
		l:          NewLogger(),
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
	var queue []PipelineItem
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

// Commits returns the list of commits from the history similar to `git log` over the HEAD.
// `firstParent` specifies whether to leave only the first parent after each merge
// (`git log --first-parent`) - effectively decreasing the accuracy but increasing performance.
func (pipeline *Pipeline) Commits(firstParent bool) ([]*object.Commit, error) {
	var result []*object.Commit
	repository := pipeline.repository
	heads, err := pipeline.HeadCommit()
	if err != nil {
		return nil, err
	}
	head := heads[0]
	if firstParent {
		// the first parent matches the head
		for commit := head; err != io.EOF; commit, err = commit.Parents().Next() {
			if err != nil {
				panic(err)
			}
			result = append(result, commit)
		}
		// reverse the order
		for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
			result[i], result[j] = result[j], result[i]
		}
		return result, nil
	}
	cit, err := repository.Log(&git.LogOptions{From: head.Hash})
	if err != nil {
		return nil, errors.Wrap(err, "unable to collect the commit history")
	}
	defer cit.Close()
	err = cit.ForEach(func(commit *object.Commit) error {
		result = append(result, commit)
		return nil
	})
	return result, err
}

// HeadCommit returns the latest commit in the repository (HEAD).
func (pipeline *Pipeline) HeadCommit() ([]*object.Commit, error) {
	repository := pipeline.repository
	head, err := repository.Head()
	if err == plumbing.ErrReferenceNotFound {
		refs, errr := repository.References()
		if errr != nil {
			return nil, errors.Wrap(errr, "unable to list the references")
		}
		var refnames []string
		refByName := map[string]*plumbing.Reference{}
		err = refs.ForEach(func(ref *plumbing.Reference) error {
			refname := ref.Name().String()
			refnames = append(refnames, refname)
			refByName[refname] = ref
			if strings.HasPrefix(refname, "refs/heads/HEAD/") {
				head = ref
				return storer.ErrStop
			}
			return nil
		})
		if head == nil {
			sort.Strings(refnames)
			headName := refnames[len(refnames)-1]
			pipeline.l.Warnf("could not determine the HEAD, falling back to %s", headName)
			head = refByName[headName]
		}
	}
	if head == nil {
		return nil, errors.Wrap(err, "unable to find the head reference")
	}
	commit, err := repository.CommitObject(head.Hash())
	if err != nil {
		return nil, err
	}
	return []*object.Commit{commit}, nil
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

func (pipeline *Pipeline) resolve(dumpPath string) error {
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
					pipeline.l.Critical("Failed to resolve pipeline dependencies: ambiguous graph.")
					return errors.New("ambiguous graph")
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
				pipeline.l.Criticalf("Unsatisfied dependency: %s -> %s", key, item.Name())
				return errors.New("unsatisfied dependency")
			}
		}
	}
	// Try to break the cycles in some known scenarios.
	if len(ambiguousMap) > 0 {
		var ambiguous []string
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
		pipeline.l.Critical("Failed to resolve pipeline dependencies: unable to topologically sort the items.")
		return errors.New("topological sort failure")
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
		pipeline.l.Infof("Wrote the DAG to %s\n", absPath)
	}
	return nil
}

// Initialize prepares the pipeline for the execution (Run()). This function
// resolves the execution DAG, Configure()-s and Initialize()-s the items in it in the
// topological dependency order. `facts` are passed inside Configure(). They are mutable.
func (pipeline *Pipeline) Initialize(facts map[string]interface{}) error {
	cleanReturn := false
	defer func() {
		if !cleanReturn {
			remotes, _ := pipeline.repository.Remotes()
			if len(remotes) > 0 {
				pipeline.l.Errorf("Failed to initialize the pipeline on %s", remotes[0].Config().URLs)
			}
		}
	}()
	if facts == nil {
		facts = map[string]interface{}{}
	}

	// set logger from facts, otherwise set the pipeline's logger as the logger
	// to be used by all analysis tasks by setting the fact
	if l, exists := facts[ConfigLogger].(Logger); exists {
		pipeline.l = l
	} else {
		facts[ConfigLogger] = pipeline.l
	}

	if _, exists := facts[ConfigPipelineCommits]; !exists {
		var err error
		facts[ConfigPipelineCommits], err = pipeline.Commits(false)
		if err != nil {
			pipeline.l.Errorf("failed to list the commits: %v", err)
			return err
		}
	}
	pipeline.PrintActions, _ = facts[ConfigPipelinePrintActions].(bool)
	if val, exists := facts[ConfigPipelineHibernationDistance].(int); exists {
		if val < 0 {
			err := fmt.Errorf("--hibernation-distance cannot be negative (got %d)", val)
			pipeline.l.Error(err)
			return err
		}
		pipeline.HibernationDistance = val
	}
	dumpPath, _ := facts[ConfigPipelineDAGPath].(string)
	err := pipeline.resolve(dumpPath)
	if err != nil {
		return err
	}
	if dumpPlan, exists := facts[ConfigPipelineDumpPlan].(bool); exists {
		pipeline.DumpPlan = dumpPlan
	}
	if dryRun, exists := facts[ConfigPipelineDryRun].(bool); exists {
		pipeline.DryRun = dryRun
		if dryRun {
			cleanReturn = true
			return nil
		}
	}
	for _, item := range pipeline.items {
		err := item.Configure(facts)
		if err != nil {
			cleanReturn = true
			return errors.Wrapf(err, "%s failed to configure", item.Name())
		}
	}
	for _, item := range pipeline.items {
		err := item.Initialize(pipeline.repository)
		if err != nil {
			cleanReturn = true
			return errors.Wrapf(err, "%s failed to initialize", item.Name())
		}
	}
	if pipeline.HibernationDistance > 0 {
		// if we want hibernation, then we want to minimize RSS
		debug.SetGCPercent(20) // the default is 100
	}
	cleanReturn = true
	return nil
}

// Run method executes the pipeline.
//
// `commits` is a slice with the git commits to analyse. Multiple branches are supported.
//
// Returns the mapping from each LeafPipelineItem to the corresponding analysis result.
// There is always a "nil" record with CommonAnalysisResult.
func (pipeline *Pipeline) Run(commits []*object.Commit) (map[LeafPipelineItem]interface{}, error) {
	startRunTime := time.Now()
	cleanReturn := false
	defer func() {
		if !cleanReturn {
			remotes, _ := pipeline.repository.Remotes()
			if len(remotes) > 0 {
				pipeline.l.Errorf("Failed to run the pipeline on %s", remotes[0].Config().URLs)
			}
		}
	}()
	onProgress := pipeline.OnProgress
	if onProgress == nil {
		onProgress = func(int, int, string) {}
	}
	plan := prepareRunPlan(commits, pipeline.HibernationDistance, pipeline.DumpPlan)
	progressSteps := len(plan) + 2
	branches := map[int][]PipelineItem{}
	// we will need rootClone if there is more than one root branch
	var rootClone []PipelineItem
	if !pipeline.DryRun {
		rootClone = cloneItems(pipeline.items, 1)[0]
	}
	var newestTime int64
	runTimePerItem := map[string]float64{}

	isMerge := func(index int, commit plumbing.Hash) bool {
		match := false
		// look for the same hash backward
		for i := index - 1; i > 0; i-- {
			switch plan[i].Action {
			case runActionHibernate, runActionBoot:
				continue
			case runActionCommit:
				match = plan[i].Commit.Hash == commit
				fallthrough
			default:
				i = 0
			}
		}
		if match {
			return true
		}
		// look for the same hash forward
		for i := index + 1; i < len(plan); i++ {
			switch plan[i].Action {
			case runActionHibernate, runActionBoot:
				continue
			case runActionCommit:
				match = plan[i].Commit.Hash == commit
				fallthrough
			default:
				i = len(plan)
			}
		}
		return match
	}

	commitIndex := 0
	for index, step := range plan {
		onProgress(index+1, progressSteps, step.String())
		if pipeline.DryRun {
			continue
		}
		if pipeline.PrintActions {
			printAction(step)
		}
		if index > 0 && index%100 == 0 && pipeline.HibernationDistance > 0 {
			debug.FreeOSMemory()
		}
		firstItem := step.Items[0]
		switch step.Action {
		case runActionCommit:
			state := map[string]interface{}{
				DependencyCommit:  step.Commit,
				DependencyIndex:   commitIndex,
				DependencyIsMerge: isMerge(index, step.Commit.Hash),
			}
			for _, item := range branches[firstItem] {
				startTime := time.Now()
				update, err := item.Consume(state)
				runTimePerItem[item.Name()] += time.Now().Sub(startTime).Seconds()
				if err != nil {
					pipeline.l.Errorf("%s failed on commit #%d (%d) %s: %v\n",
						item.Name(), commitIndex+1, index+1, step.Commit.Hash.String(), err)
					return nil, err
				}
				for _, key := range item.Provides() {
					val, ok := update[key]
					if !ok {
						err := fmt.Errorf("%s: Consume() did not return %s", item.Name(), key)
						pipeline.l.Critical(err)
						return nil, err
					}
					state[key] = val
				}
			}
			commitTime := step.Commit.Committer.When.Unix()
			if commitTime > newestTime {
				newestTime = commitTime
			}
			commitIndex++
		case runActionFork:
			startTime := time.Now()
			for i, clone := range cloneItems(branches[firstItem], len(step.Items)-1) {
				branches[step.Items[i+1]] = clone
			}
			runTimePerItem["*.Fork"] += time.Now().Sub(startTime).Seconds()
		case runActionMerge:
			startTime := time.Now()
			merged := make([][]PipelineItem, len(step.Items))
			for i, b := range step.Items {
				merged[i] = branches[b]
			}
			mergeItems(merged)
			runTimePerItem["*.Merge"] += time.Now().Sub(startTime).Seconds()
		case runActionEmerge:
			if firstItem == rootBranchIndex {
				branches[firstItem] = pipeline.items
			} else {
				branches[firstItem] = cloneItems(rootClone, 1)[0]
			}
		case runActionDelete:
			delete(branches, firstItem)
		case runActionHibernate:
			for _, item := range step.Items {
				for _, item := range branches[item] {
					if hi, ok := item.(HibernateablePipelineItem); ok {
						startTime := time.Now()
						err := hi.Hibernate()
						if err != nil {
							pipeline.l.Errorf("Failed to hibernate %s: %v\n", item.Name(), err)
							return nil, err
						}
						runTimePerItem[item.Name()+".Hibernation"] += time.Now().Sub(startTime).Seconds()
					}
				}
			}
		case runActionBoot:
			for _, item := range step.Items {
				for _, item := range branches[item] {
					if hi, ok := item.(HibernateablePipelineItem); ok {
						startTime := time.Now()
						err := hi.Boot()
						if err != nil {
							pipeline.l.Errorf("Failed to boot %s: %v\n", item.Name(), err)
							return nil, err
						}
						runTimePerItem[item.Name()+".Hibernation"] += time.Now().Sub(startTime).Seconds()
					}
				}
			}
		}
	}
	onProgress(len(plan)+1, progressSteps, MessageFinalize)
	result := map[LeafPipelineItem]interface{}{}
	if !pipeline.DryRun {
		for index, item := range getMasterBranch(branches) {
			if casted, ok := item.(DisposablePipelineItem); ok {
				casted.Dispose()
			}
			if casted, ok := item.(LeafPipelineItem); ok {
				result[pipeline.items[index].(LeafPipelineItem)] = casted.Finalize()
			}
		}
	}
	onProgress(progressSteps, progressSteps, "")
	result[nil] = &CommonAnalysisResult{
		BeginTime:      plan[0].Commit.Committer.When.Unix(),
		EndTime:        newestTime,
		CommitsNumber:  len(commits),
		RunTime:        time.Since(startRunTime),
		RunTimePerItem: runTimePerItem,
	}
	cleanReturn = true
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
	var commits []*object.Commit
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

// GetSensibleRemote extracts a remote URL of the repository to identify it.
func GetSensibleRemote(repository *git.Repository) string {
	if r, err := repository.Remotes(); err == nil && len(r) > 0 {
		return r[0].Config().URLs[0]
	}
	return "<no remote>"
}
