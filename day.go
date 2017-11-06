package hercules

import (
	"time"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type DaysSinceStart struct {
	day0        time.Time
	previousDay int
}

func (days *DaysSinceStart) Name() string {
	return "DaysSinceStart"
}

func (days *DaysSinceStart) Provides() []string {
	arr := [...]string{"day"}
	return arr[:]
}

func (days *DaysSinceStart) Requires() []string {
	return []string{}
}

func (days *DaysSinceStart) Construct(facts map[string]interface{}) {}

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
	}
	day := int(commit.Author.When.Sub(days.day0).Hours() / 24)
	if day < days.previousDay {
		// rebase works miracles, but we need the monotonous time
		day = days.previousDay
	}
	days.previousDay = day
	return map[string]interface{}{"day": day}, nil
}

func (days *DaysSinceStart) Finalize() interface{} {
	return nil
}

func init() {
  Registry.Register(&DaysSinceStart{})
}
