package hercules

import (
	"errors"
	"fmt"
	"io"
	"os"

	"bufio"
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

type Pipeline struct {
	// OnProgress is the callback which is invoked in Analyse() to output it's
	// progress. The first argument is the number of processed commits and the
	// second is the total number of commits.
	OnProgress func(int, int)

	// repository points to the analysed Git repository struct from go-git.
	repository *git.Repository

	// items are the registered analysers in the pipeline.
	items []PipelineItem

	// plan is the resolved execution sequence.
	plan []PipelineItem
}

func NewPipeline(repository *git.Repository) *Pipeline {
	return &Pipeline{repository: repository, items: []PipelineItem{}, plan: []PipelineItem{}}
}

func (pipeline *Pipeline) AddItem(item PipelineItem) {
	for _, reg := range pipeline.items {
		if reg == item {
			return
		}
	}
	pipeline.items = append(pipeline.items, item)
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

func (pipeline *Pipeline) Initialize() {
	graph := toposort.NewGraph()
	name2item := map[string]PipelineItem{}
	for index, item := range pipeline.items {
		name := fmt.Sprintf("%s_%d", item.Name(), index)
		graph.AddNode(name)
		name2item[name] = item
		for _, key := range item.Provides() {
			key = "[" + key + "]"
			graph.AddNode(key)
			graph.AddEdge(name, key)
		}
	}
	for index, item := range pipeline.items {
		name := fmt.Sprintf("%s_%d", item.Name(), index)
		for _, key := range item.Requires() {
			key = "[" + key + "]"
			if !graph.AddEdge(key, name) {
				panic(fmt.Sprintf("Unsatisfied dependency: %s -> %s", key, item.Name()))
			}
		}
	}
	strplan, ok := graph.Toposort()
	if !ok {
		panic("Failed to resolve pipeline dependencies.")
	}
	for _, key := range strplan {
		item, ok := name2item[key]
		if ok {
			pipeline.plan = append(pipeline.plan, item)
		}
	}
	if len(pipeline.plan) != len(pipeline.items) {
		panic("Internal pipeline dependency resolution error.")
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
		for _, item := range pipeline.plan {
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
