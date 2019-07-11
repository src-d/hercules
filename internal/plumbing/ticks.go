package plumbing

import (
	"time"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
)

// TicksSinceStart provides relative tick information for every commit.
// It is a PipelineItem.
type TicksSinceStart struct {
	core.NoopMerger
	TickSize time.Duration

	remote       string
	tick0        *time.Time
	previousTick int
	commits      map[int][]plumbing.Hash

	l core.Logger
}

const (
	// DependencyTick is the name of the dependency which DaysSinceStart provides - the number
	// of ticks since the first commit in the analysed sequence.
	DependencyTick = "tick"

	// FactCommitsByTick contains the mapping between day indices and the corresponding commits.
	FactCommitsByTick = "TicksSinceStart.Commits"

	// FactTickSize contains the time.Duration of each tick.
	FactTickSize = "TicksSinceStart.TickSize"

	// ConfigTicksSinceStartTickSize sets the size of each 'tick' in hours.
	ConfigTicksSinceStartTickSize = "TicksSinceStart.TickSize"

	// DefaultTicksSinceStartTickSize is the default number of hours in each 'tick' (24*hour = 1day).
	DefaultTicksSinceStartTickSize = 24
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (ticks *TicksSinceStart) Name() string {
	return "TicksSinceStart"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (ticks *TicksSinceStart) Provides() []string {
	return []string{DependencyTick}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (ticks *TicksSinceStart) Requires() []string {
	return []string{}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (ticks *TicksSinceStart) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{{
		Name:        ConfigTicksSinceStartTickSize,
		Description: "How long each 'tick' represents in hours.",
		Flag:        "tick-size",
		Type:        core.IntConfigurationOption,
		Default:     DefaultTicksSinceStartTickSize},
	}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (ticks *TicksSinceStart) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		ticks.l = l
	}
	if val, exists := facts[ConfigTicksSinceStartTickSize].(int); exists {
		ticks.TickSize = time.Duration(val) * time.Hour
	} else {
		ticks.TickSize = DefaultTicksSinceStartTickSize * time.Hour
	}
	if ticks.commits == nil {
		ticks.commits = map[int][]plumbing.Hash{}
	}
	facts[FactCommitsByTick] = ticks.commits
	facts[FactTickSize] = ticks.TickSize
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (ticks *TicksSinceStart) Initialize(repository *git.Repository) error {
	ticks.l = core.NewLogger()
	if ticks.TickSize == 0 {
		ticks.TickSize = DefaultTicksSinceStartTickSize * time.Hour
	}
	ticks.tick0 = &time.Time{}
	ticks.previousTick = 0
	if len(ticks.commits) > 0 {
		keys := make([]int, len(ticks.commits))
		for key := range ticks.commits {
			keys = append(keys, key)
		}
		for _, key := range keys {
			delete(ticks.commits, key)
		}
	}
	ticks.remote = core.GetSensibleRemote(repository)
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (ticks *TicksSinceStart) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps[core.DependencyCommit].(*object.Commit)
	index := deps[core.DependencyIndex].(int)
	if index == 0 {
		// first iteration - initialize the file objects from the tree
		// our precision is 1 day
		tick0 := commit.Committer.When
		if tick0.Unix() < 631152000 { // 01.01.1990, that was 30 years ago
			ticks.l.Warnf("suspicious committer timestamp in %s > %s: %d",
				ticks.remote, commit.Hash.String(), tick0.Unix())
		}
		*ticks.tick0 = FloorTime(tick0, ticks.TickSize)
	}

	tick := int(commit.Committer.When.Sub(*ticks.tick0) / ticks.TickSize)
	if tick < ticks.previousTick {
		// rebase works miracles, but we need the monotonous time
		tick = ticks.previousTick
	}

	ticks.previousTick = tick
	tickCommits := ticks.commits[tick]
	if tickCommits == nil {
		tickCommits = []plumbing.Hash{}
	}

	exists := false
	if commit.NumParents() > 0 {
		for i := range tickCommits {
			if tickCommits[len(tickCommits)-i-1] == commit.Hash {
				exists = true
				break
			}
		}
	}
	if !exists {
		ticks.commits[tick] = append(tickCommits, commit.Hash)
	}

	return map[string]interface{}{DependencyTick: tick}, nil
}

// Fork clones this PipelineItem.
func (ticks *TicksSinceStart) Fork(n int) []core.PipelineItem {
	return core.ForkCopyPipelineItem(ticks, n)
}

// FloorTime is the missing implementation of time.Time.Floor() - round to the nearest less than or equal.
func FloorTime(t time.Time, d time.Duration) time.Time {
	// We have check if the regular rounding resulted in Floor() + d.
	result := t.Round(d)
	if result.After(t) {
		result = result.Add(-d)
	}
	return result
}

func init() {
	core.Registry.Register(&TicksSinceStart{})
}
