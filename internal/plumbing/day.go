package plumbing

import (
	"time"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v4/internal/core"
)

// DaysSinceStart provides the relative date information for every commit.
// It is a PipelineItem.
type DaysSinceStart struct {
	day0        time.Time
	previousDay int
	commits     map[int][]plumbing.Hash
}

const (
	// DependencyDay is the name of the dependency which DaysSinceStart provides - the number
	// of days since the first commit in the analysed sequence.
	DependencyDay = "day"

	// FactCommitsByDay contains the mapping between day indices and the corresponding commits.
	FactCommitsByDay = "DaysSinceStart.Commits"
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (days *DaysSinceStart) Name() string {
	return "DaysSinceStart"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (days *DaysSinceStart) Provides() []string {
	arr := [...]string{DependencyDay}
	return arr[:]
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (days *DaysSinceStart) Requires() []string {
	return []string{}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (days *DaysSinceStart) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (days *DaysSinceStart) Configure(facts map[string]interface{}) {
	if days.commits == nil {
		days.commits = map[int][]plumbing.Hash{}
	}
	facts[FactCommitsByDay] = days.commits
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (days *DaysSinceStart) Initialize(repository *git.Repository) {
	days.day0 = time.Time{}
	days.previousDay = 0
	if len(days.commits) > 0 {
		keys := make([]int, len(days.commits))
		for key := range days.commits {
			keys = append(keys, key)
		}
		for _, key := range keys {
			delete(days.commits, key)
		}
	}
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (days *DaysSinceStart) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps[core.DependencyCommit].(*object.Commit)
	index := deps[core.DependencyIndex].(int)
	if index == 0 {
		// first iteration - initialize the file objects from the tree
		days.day0 = commit.Author.When
		// our precision is 1 day
		days.day0 = days.day0.Truncate(24 * time.Hour)
	}
	day := int(commit.Author.When.Sub(days.day0).Hours() / 24)
	if day < days.previousDay {
		// rebase works miracles, but we need the monotonous time
		day = days.previousDay
	}
	days.previousDay = day
	dayCommits := days.commits[day]
	if dayCommits == nil {
		dayCommits = []plumbing.Hash{}
	}
	exists := false
	if commit.NumParents() > 0 {
		for i := range dayCommits {
			if dayCommits[len(dayCommits)-i-1] == commit.Hash {
				exists = true
			}
		}
	}
	if !exists {
		days.commits[day] = append(dayCommits, commit.Hash)
	}
	return map[string]interface{}{DependencyDay: day}, nil
}

func (days *DaysSinceStart) Fork(n int) []core.PipelineItem {
	clones := make([]core.PipelineItem, n)
	for i := 0; i < n; i++ {
		clones[i] = &DaysSinceStart{
			previousDay: days.previousDay,
			day0: days.day0,
			commits: days.commits,
		}
	}
	return clones
}

func (days *DaysSinceStart) Merge(branches []core.PipelineItem) {
	// no-op
}

func init() {
	core.Registry.Register(&DaysSinceStart{})
}
