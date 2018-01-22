package hercules

import (
	"time"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

// DaysSinceStart provides the relative date information for every commit.
// It is a PipelineItem.
type DaysSinceStart struct {
	day0        time.Time
	previousDay int
}

const (
	// DependencyDay is the name of the dependency which DaysSinceStart provides - the number
	// of days since the first commit in the analysed sequence.
	DependencyDay = "day"
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (days *DaysSinceStart) Name() string {
	return "DaysSinceStart"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by hercules.Registry to build the global map of providers.
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
func (days *DaysSinceStart) ListConfigurationOptions() []ConfigurationOption {
	return []ConfigurationOption{}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (days *DaysSinceStart) Configure(facts map[string]interface{}) {}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (days *DaysSinceStart) Initialize(repository *git.Repository) {
	days.day0 = time.Time{}
	days.previousDay = 0
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, "commit" is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (days *DaysSinceStart) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps["commit"].(*object.Commit)
	index := deps["index"].(int)
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
	return map[string]interface{}{DependencyDay: day}, nil
}

func init() {
	Registry.Register(&DaysSinceStart{})
}
