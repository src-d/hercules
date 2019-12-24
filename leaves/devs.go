package leaves

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	items "gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v10/internal/yaml"
)

// DevsAnalysis calculates the number of commits through time per developer.
// It also records the numbers of added, deleted and changed lines through time per developer.
// Those numbers are additionally measured per language.
type DevsAnalysis struct {
	core.NoopMerger
	core.OneShotMergeProcessor
	// ConsiderEmptyCommits indicates whether empty commits (e.g., merges) should be taken
	// into account.
	ConsiderEmptyCommits bool

	// ticks maps ticks to developers to stats
	ticks map[int]map[int]*DevTick
	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
	// TickSize references TicksSinceStart.TickSize
	tickSize time.Duration

	l core.Logger
}

// DevsResult is returned by DevsAnalysis.Finalize() and carries the daily statistics
// per developer.
type DevsResult struct {
	// Ticks is <tick index> -> <developer index> -> daily stats
	Ticks map[int]map[int]*DevTick

	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
	// TickSize references TicksSinceStart.TickSize
	tickSize time.Duration
}

// DevTick is the statistics for a development tick and a particular developer.
type DevTick struct {
	// Commits is the number of commits made by a particular developer in a particular tick.
	Commits int
	items.LineStats
	// LanguagesDetection carries fine-grained line stats per programming language.
	Languages map[string]items.LineStats
}

const (
	// ConfigDevsConsiderEmptyCommits is the name of the option to set DevsAnalysis.ConsiderEmptyCommits.
	ConfigDevsConsiderEmptyCommits = "Devs.ConsiderEmptyCommits"
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (devs *DevsAnalysis) Name() string {
	return "Devs"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (devs *DevsAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (devs *DevsAnalysis) Requires() []string {
	return []string{
		identity.DependencyAuthor, items.DependencyTreeChanges, items.DependencyTick,
		items.DependencyLanguages, items.DependencyLineStats}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (devs *DevsAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigDevsConsiderEmptyCommits,
		Description: "Take into account empty commits such as trivial merges.",
		Flag:        "empty-commits",
		Type:        core.BoolConfigurationOption,
		Default:     false}}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (devs *DevsAnalysis) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		devs.l = l
	}
	if val, exists := facts[ConfigDevsConsiderEmptyCommits].(bool); exists {
		devs.ConsiderEmptyCommits = val
	}
	if val, exists := facts[identity.FactIdentityDetectorReversedPeopleDict].([]string); exists {
		devs.reversedPeopleDict = val
	}
	if val, exists := facts[items.FactTickSize].(time.Duration); exists {
		devs.tickSize = val
	}
	return nil
}

// Flag for the command line switch which enables this analysis.
func (devs *DevsAnalysis) Flag() string {
	return "devs"
}

// Description returns the text which explains what the analysis is doing.
func (devs *DevsAnalysis) Description() string {
	return "Calculates the number of commits, added, removed and changed lines per developer through time."
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (devs *DevsAnalysis) Initialize(repository *git.Repository) error {
	if devs.tickSize == 0 {
		return errors.New("tick size must be specified")
	}
	devs.l = core.NewLogger()
	devs.ticks = map[int]map[int]*DevTick{}
	devs.OneShotMergeProcessor.Initialize()
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (devs *DevsAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if !devs.ShouldConsumeCommit(deps) {
		return nil, nil
	}
	author := deps[identity.DependencyAuthor].(int)
	treeDiff := deps[items.DependencyTreeChanges].(object.Changes)
	if len(treeDiff) == 0 && !devs.ConsiderEmptyCommits {
		return nil, nil
	}
	tick := deps[items.DependencyTick].(int)
	devstick, exists := devs.ticks[tick]
	if !exists {
		devstick = map[int]*DevTick{}
		devs.ticks[tick] = devstick
	}
	dd, exists := devstick[author]
	if !exists {
		dd = &DevTick{Languages: map[string]items.LineStats{}}
		devstick[author] = dd
	}
	dd.Commits++
	if deps[core.DependencyIsMerge].(bool) {
		// we ignore merge commit diffs
		// TODO(vmarkovtsev): handle them
		return nil, nil
	}
	langs := deps[items.DependencyLanguages].(map[plumbing.Hash]string)
	lineStats := deps[items.DependencyLineStats].(map[object.ChangeEntry]items.LineStats)
	for changeEntry, stats := range lineStats {
		dd.Added += stats.Added
		dd.Removed += stats.Removed
		dd.Changed += stats.Changed
		lang := langs[changeEntry.TreeEntry.Hash]
		langStats := dd.Languages[lang]
		dd.Languages[lang] = items.LineStats{
			Added:   langStats.Added + stats.Added,
			Removed: langStats.Removed + stats.Removed,
			Changed: langStats.Changed + stats.Changed,
		}
	}
	return nil, nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (devs *DevsAnalysis) Finalize() interface{} {
	return DevsResult{
		Ticks:              devs.ticks,
		reversedPeopleDict: devs.reversedPeopleDict,
		tickSize:           devs.tickSize,
	}
}

// Fork clones this pipeline item.
func (devs *DevsAnalysis) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(devs, n)
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (devs *DevsAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	devsResult := result.(DevsResult)
	if binary {
		return devs.serializeBinary(&devsResult, writer)
	}
	devs.serializeText(&devsResult, writer)
	return nil
}

// Deserialize converts the specified protobuf bytes to DevsResult.
func (devs *DevsAnalysis) Deserialize(pbmessage []byte) (interface{}, error) {
	message := pb.DevsAnalysisResults{}
	err := proto.Unmarshal(pbmessage, &message)
	if err != nil {
		return nil, err
	}
	ticks := map[int]map[int]*DevTick{}
	for tick, dd := range message.Ticks {
		rdd := map[int]*DevTick{}
		ticks[int(tick)] = rdd
		for dev, stats := range dd.Devs {
			if dev == -1 {
				dev = identity.AuthorMissing
			}
			languages := map[string]items.LineStats{}
			rdd[int(dev)] = &DevTick{
				Commits: int(stats.Commits),
				LineStats: items.LineStats{
					Added:   int(stats.Stats.Added),
					Removed: int(stats.Stats.Removed),
					Changed: int(stats.Stats.Changed),
				},
				Languages: languages,
			}
			for lang, ls := range stats.Languages {
				languages[lang] = items.LineStats{
					Added:   int(ls.Added),
					Removed: int(ls.Removed),
					Changed: int(ls.Changed),
				}
			}
		}
	}
	result := DevsResult{
		Ticks:              ticks,
		reversedPeopleDict: message.DevIndex,
		tickSize:           time.Duration(message.TickSize),
	}
	return result, nil
}

// MergeResults combines two DevsAnalysis-es together.
func (devs *DevsAnalysis) MergeResults(r1, r2 interface{}, c1, c2 *core.CommonAnalysisResult) interface{} {
	cr1 := r1.(DevsResult)
	cr2 := r2.(DevsResult)
	if cr1.tickSize != cr2.tickSize {
		return fmt.Errorf("mismatching tick sizes (r1: %d, r2: %d) received",
			cr1.tickSize, cr2.tickSize)
	}
	t01 := items.FloorTime(c1.BeginTimeAsTime(), cr1.tickSize)
	t02 := items.FloorTime(c2.BeginTimeAsTime(), cr2.tickSize)
	t0 := t01
	if t02.Before(t0) {
		t0 = t02
	}
	offset1 := int(t01.Sub(t0) / cr1.tickSize)
	offset2 := int(t02.Sub(t0) / cr2.tickSize)

	merged := DevsResult{tickSize: cr1.tickSize}
	var mergedIndex map[string]identity.MergedIndex
	mergedIndex, merged.reversedPeopleDict = identity.MergeReversedDictsIdentities(
		cr1.reversedPeopleDict, cr2.reversedPeopleDict)
	newticks := map[int]map[int]*DevTick{}
	merged.Ticks = newticks
	for tick, dd := range cr1.Ticks {
		tick += offset1
		newdd, exists := newticks[tick]
		if !exists {
			newdd = map[int]*DevTick{}
			newticks[tick] = newdd
		}
		for dev, stats := range dd {
			newdev := dev
			if newdev != identity.AuthorMissing {
				newdev = mergedIndex[cr1.reversedPeopleDict[dev]].Final
			}
			newstats, exists := newdd[newdev]
			if !exists {
				newstats = &DevTick{Languages: map[string]items.LineStats{}}
				newdd[newdev] = newstats
			}
			newstats.Commits += stats.Commits
			newstats.Added += stats.Added
			newstats.Removed += stats.Removed
			newstats.Changed += stats.Changed
			for lang, ls := range stats.Languages {
				prev := newstats.Languages[lang]
				newstats.Languages[lang] = items.LineStats{
					Added:   prev.Added + ls.Added,
					Removed: prev.Removed + ls.Removed,
					Changed: prev.Changed + ls.Changed,
				}
			}
		}
	}
	for tick, dd := range cr2.Ticks {
		tick += offset2
		newdd, exists := newticks[tick]
		if !exists {
			newdd = map[int]*DevTick{}
			newticks[tick] = newdd
		}
		for dev, stats := range dd {
			newdev := dev
			if newdev != identity.AuthorMissing {
				newdev = mergedIndex[cr2.reversedPeopleDict[dev]].Final
			}
			newstats, exists := newdd[newdev]
			if !exists {
				newstats = &DevTick{Languages: map[string]items.LineStats{}}
				newdd[newdev] = newstats
			}
			newstats.Commits += stats.Commits
			newstats.Added += stats.Added
			newstats.Removed += stats.Removed
			newstats.Changed += stats.Changed
			for lang, ls := range stats.Languages {
				prev := newstats.Languages[lang]
				newstats.Languages[lang] = items.LineStats{
					Added:   prev.Added + ls.Added,
					Removed: prev.Removed + ls.Removed,
					Changed: prev.Changed + ls.Changed,
				}
			}
		}
	}
	return merged
}

func (devs *DevsAnalysis) serializeText(result *DevsResult, writer io.Writer) {
	fmt.Fprintln(writer, "  ticks:")
	ticks := make([]int, len(result.Ticks))
	{
		i := 0
		for tick := range result.Ticks {
			ticks[i] = tick
			i++
		}
	}
	sort.Ints(ticks)
	for _, tick := range ticks {
		fmt.Fprintf(writer, "    %d:\n", tick)
		rtick := result.Ticks[tick]
		devseq := make([]int, len(rtick))
		{
			i := 0
			for dev := range rtick {
				devseq[i] = dev
				i++
			}
		}
		sort.Ints(devseq)
		for _, dev := range devseq {
			stats := rtick[dev]
			if dev == identity.AuthorMissing {
				dev = -1
			}
			var langs []string
			for lang, ls := range stats.Languages {
				if lang == "" {
					lang = "none"
				}
				langs = append(langs,
					fmt.Sprintf("%s: [%d, %d, %d]", lang, ls.Added, ls.Removed, ls.Changed))
			}
			sort.Strings(langs)
			fmt.Fprintf(writer, "      %d: [%d, %d, %d, %d, {%s}]\n",
				dev, stats.Commits, stats.Added, stats.Removed, stats.Changed,
				strings.Join(langs, ", "))
		}
	}
	fmt.Fprintln(writer, "  people:")
	for _, person := range result.reversedPeopleDict {
		fmt.Fprintf(writer, "  - %s\n", yaml.SafeString(person))
	}
	fmt.Fprintln(writer, "  tick_size:", int(result.tickSize.Seconds()))
}

func (devs *DevsAnalysis) serializeBinary(result *DevsResult, writer io.Writer) error {
	message := pb.DevsAnalysisResults{}
	message.DevIndex = result.reversedPeopleDict
	message.TickSize = int64(result.tickSize)
	message.Ticks = map[int32]*pb.TickDevs{}
	for tick, devs := range result.Ticks {
		dd := &pb.TickDevs{}
		message.Ticks[int32(tick)] = dd
		dd.Devs = map[int32]*pb.DevTick{}
		for dev, stats := range devs {
			if dev == identity.AuthorMissing {
				dev = -1
			}
			languages := map[string]*pb.LineStats{}
			dd.Devs[int32(dev)] = &pb.DevTick{
				Commits: int32(stats.Commits),
				Stats: &pb.LineStats{
					Added:   int32(stats.Added),
					Changed: int32(stats.Changed),
					Removed: int32(stats.Removed),
				},
				Languages: languages,
			}
			for lang, ls := range stats.Languages {
				languages[lang] = &pb.LineStats{
					Added:   int32(ls.Added),
					Changed: int32(ls.Changed),
					Removed: int32(ls.Removed),
				}
			}
		}
	}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	_, err = writer.Write(serialized)
	return err
}

// GetTickSize returns the tick size used to generate this devs analysis result.
func (dr DevsResult) GetTickSize() time.Duration {
	return dr.tickSize
}

// GetIdentities returns the list of developer identities used to generate this devs analysis result.
// The format is |-joined keys, see internals/plumbing/identity for details.
func (dr DevsResult) GetIdentities() []string {
	return dr.reversedPeopleDict
}

func init() {
	core.Registry.Register(&DevsAnalysis{})
}
