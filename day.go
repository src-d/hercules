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

func (days *DaysSinceStart) Name() string {
	return "DaysSinceStart"
}

func (days *DaysSinceStart) Provides() []string {
	arr := [...]string{DependencyDay}
	return arr[:]
}

func (days *DaysSinceStart) Requires() []string {
	return []string{}
}

func (days *DaysSinceStart) ListConfigurationOptions() []ConfigurationOption {
	return []ConfigurationOption{}
}

func (days *DaysSinceStart) Configure(facts map[string]interface{}) {}

func (days *DaysSinceStart) Initialize(repository *git.Repository) {
	days.day0 = time.Time{}
	days.previousDay = 0
}

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
