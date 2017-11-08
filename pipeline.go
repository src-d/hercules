package hercules

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v2/toposort"
)

type PipelineItem interface {
	// Name returns the name of the analysis.
	Name() string
	// Provides returns the list of keys of reusable calculated entities.
	// Other items may depend on them.
	Provides() []string
	// Requires returns the list of keys of needed entities which must be supplied in Consume().
	Requires() []string
	// Construct performs the initial creation of the object by taking parameters from facts.
	// It allows to create PipelineItems in a universal way.
	Construct(facts map[string]interface{})
	// Initialize prepares and resets the item. Consume() requires Initialize()
	// to be called at least once beforehand.
	Initialize(*git.Repository)
	// Consume processes the next commit.
	// deps contains the required entities which match Depends(). Besides, it always includes
	// "commit" and "index".
	// Returns the calculated entities which match Provides().
	Consume(deps map[string]interface{}) (map[string]interface{}, error)
	// Finalize returns the result of the analysis.
	Finalize() interface{}
}

type FeaturedPipelineItem interface {
	PipelineItem
	// Features returns the list of names which enable this item to be automatically inserted
	// in Pipeline.DeployItem().
	Features() []string
}

type PipelineItemRegistry struct {
	provided map[string][]reflect.Type
}

func (registry *PipelineItemRegistry) Register(example PipelineItem) {
	if registry.provided == nil {
		registry.provided = map[string][]reflect.Type{}
	}
	t := reflect.TypeOf(example)
	for _, dep := range example.Provides() {
		ts := registry.provided[dep]
		if ts == nil {
			ts = []reflect.Type{}
		}
		ts = append(ts, t)
		registry.provided[dep] = ts
	}
}

func (registry *PipelineItemRegistry) Summon(provides string) []PipelineItem {
	if registry.provided == nil {
		return []PipelineItem{}
	}
	ts := registry.provided[provides]
	items := []PipelineItem{}
	for _, t := range ts {
		items = append(items, reflect.New(t.Elem()).Interface().(PipelineItem))
	}
	return items
}

var Registry = &PipelineItemRegistry{}

type wrappedPipelineItem struct {
	Item PipelineItem
	Children []wrappedPipelineItem
}

type Pipeline struct {
	// OnProgress is the callback which is invoked in Analyse() to output it's
	// progress. The first argument is the number of processed commits and the
	// second is the total number of commits.
	OnProgress func(int, int)

	// repository points to the analysed Git repository struct from go-git.
	repository *git.Repository

	// items are the registered building blocks in the pipeline. The order defines the
	// execution sequence.
	items []PipelineItem

	// the collection of parameters to create items.
	facts map[string]interface{}

	// Feature flags which enable the corresponding items.
	features map[string]bool
}

func NewPipeline(repository *git.Repository) *Pipeline {
	return &Pipeline{
		repository: repository,
		items: []PipelineItem{},
		facts: map[string]interface{}{},
		features: map[string]bool{},
	}
}

func (pipeline *Pipeline) GetFact(name string) interface{} {
	return pipeline.facts[name]
}

func (pipeline *Pipeline) SetFact(name string, value interface{}) {
	pipeline.facts[name] = value
}

func (pipeline *Pipeline) GetFeature(name string) bool {
	return pipeline.features[name]
}

func (pipeline *Pipeline) SetFeature(name string) {
	pipeline.features[name] = true
}

func (pipeline *Pipeline) DeployItem(item PipelineItem) PipelineItem {
	queue := []PipelineItem{}
	queue = append(queue, item)
	added := map[string]PipelineItem{}
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
				  if fpi, matches := interface{}(sibling).(FeaturedPipelineItem); matches {
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

func (pipeline *Pipeline) resolve(dumpPath string) {
	graph := toposort.NewGraph()
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
					panic("Failed to resolve pipeline dependencies.")
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
		panic("Failed to resolve pipeline dependencies.")
	}
	pipeline.items = make([]PipelineItem, 0, len(pipeline.items))
	for _, key := range strplan {
		if item, ok := name2item[key]; ok {
			pipeline.items = append(pipeline.items, item)
		}
	}
	if dumpPath != "" {
		ioutil.WriteFile(dumpPath, []byte(graphCopy.Serialize(strplan)), 0666)
	}
}

func (pipeline *Pipeline) Initialize(facts map[string]interface{}) {
	pipeline.resolve(facts["Pipeline.DumpPath"].(string))
	if facts["Pipeline.DryRun"].(bool) {
		return
	}
	for _, item := range pipeline.items {
		item.Construct(facts)
	}
	for _, item := range pipeline.items {
		item.Initialize(pipeline.repository)
	}
}

// Run executes the pipeline.
//
// commits is a slice with the sequential commit history. It shall start from
// the root (ascending order).
func (pipeline *Pipeline) Run(commits []*object.Commit) (map[PipelineItem]interface{}, error) {
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
	result := map[PipelineItem]interface{}{}
	for _, item := range pipeline.items {
		result[item] = item.Finalize()
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
