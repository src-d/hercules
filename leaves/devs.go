package leaves

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/gogo/protobuf/proto"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v9/internal/core"
	"gopkg.in/src-d/hercules.v9/internal/pb"
	items "gopkg.in/src-d/hercules.v9/internal/plumbing"
	"gopkg.in/src-d/hercules.v9/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v9/internal/yaml"
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

	// days maps days to developers to stats
	days map[int]map[int]*DevDay
	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
}

// DevsResult is returned by DevsAnalysis.Finalize() and carries the daily statistics
// per developer.
type DevsResult struct {
	// Days is <day index> -> <developer index> -> daily stats
	Days map[int]map[int]*DevDay

	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
}

// DevDay is the statistics for a development day and a particular developer.
type DevDay struct {
	// Commits is the number of commits made by a particular developer in a particular day.
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
	arr := [...]string{
		identity.DependencyAuthor, items.DependencyTreeChanges, items.DependencyTick,
		items.DependencyLanguages, items.DependencyLineStats}
	return arr[:]
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
	if val, exists := facts[ConfigDevsConsiderEmptyCommits].(bool); exists {
		devs.ConsiderEmptyCommits = val
	}
	if val, exists := facts[identity.FactIdentityDetectorReversedPeopleDict].([]string); exists {
		devs.reversedPeopleDict = val
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
	devs.days = map[int]map[int]*DevDay{}
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
	day := deps[items.DependencyTick].(int)
	devsDay, exists := devs.days[day]
	if !exists {
		devsDay = map[int]*DevDay{}
		devs.days[day] = devsDay
	}
	dd, exists := devsDay[author]
	if !exists {
		dd = &DevDay{Languages: map[string]items.LineStats{}}
		devsDay[author] = dd
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
		Days:               devs.days,
		reversedPeopleDict: devs.reversedPeopleDict,
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
	days := map[int]map[int]*DevDay{}
	for day, dd := range message.Days {
		rdd := map[int]*DevDay{}
		days[int(day)] = rdd
		for dev, stats := range dd.Devs {
			if dev == -1 {
				dev = identity.AuthorMissing
			}
			languages := map[string]items.LineStats{}
			rdd[int(dev)] = &DevDay{
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
		Days:               days,
		reversedPeopleDict: message.DevIndex,
	}
	return result, nil
}

// MergeResults combines two DevsAnalysis-es together.
func (devs *DevsAnalysis) MergeResults(r1, r2 interface{}, c1, c2 *core.CommonAnalysisResult) interface{} {
	cr1 := r1.(DevsResult)
	cr2 := r2.(DevsResult)
	merged := DevsResult{}
	type devIndexPair struct {
		Index1 int
		Index2 int
	}
	devIndex := map[string]devIndexPair{}
	for dev, devName := range cr1.reversedPeopleDict {
		devIndex[devName] = devIndexPair{Index1: dev + 1, Index2: devIndex[devName].Index2}
	}
	for dev, devName := range cr2.reversedPeopleDict {
		devIndex[devName] = devIndexPair{Index1: devIndex[devName].Index1, Index2: dev + 1}
	}
	jointDevSeq := make([]string, len(devIndex))
	{
		i := 0
		for dev := range devIndex {
			jointDevSeq[i] = dev
			i++
		}
	}
	sort.Strings(jointDevSeq)
	merged.reversedPeopleDict = jointDevSeq
	invDevIndex1 := map[int]int{}
	invDevIndex2 := map[int]int{}
	for i, dev := range jointDevSeq {
		pair := devIndex[dev]
		if pair.Index1 > 0 {
			invDevIndex1[pair.Index1-1] = i
		}
		if pair.Index2 > 0 {
			invDevIndex2[pair.Index2-1] = i
		}
	}
	newDays := map[int]map[int]*DevDay{}
	merged.Days = newDays
	for day, dd := range cr1.Days {
		newdd, exists := newDays[day]
		if !exists {
			newdd = map[int]*DevDay{}
			newDays[day] = newdd
		}
		for dev, stats := range dd {
			newdev := dev
			if newdev != identity.AuthorMissing {
				newdev = invDevIndex1[dev]
			}
			newstats, exists := newdd[newdev]
			if !exists {
				newstats = &DevDay{Languages: map[string]items.LineStats{}}
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
	for day, dd := range cr2.Days {
		newdd, exists := newDays[day]
		if !exists {
			newdd = map[int]*DevDay{}
			newDays[day] = newdd
		}
		for dev, stats := range dd {
			newdev := dev
			if newdev != identity.AuthorMissing {
				newdev = invDevIndex2[dev]
			}
			newstats, exists := newdd[newdev]
			if !exists {
				newstats = &DevDay{Languages: map[string]items.LineStats{}}
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
	fmt.Fprintln(writer, "  days:")
	days := make([]int, len(result.Days))
	{
		i := 0
		for day := range result.Days {
			days[i] = day
			i++
		}
	}
	sort.Ints(days)
	for _, day := range days {
		fmt.Fprintf(writer, "    %d:\n", day)
		rday := result.Days[day]
		devseq := make([]int, len(rday))
		{
			i := 0
			for dev := range rday {
				devseq[i] = dev
				i++
			}
		}
		sort.Ints(devseq)
		for _, dev := range devseq {
			stats := rday[dev]
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
}

func (devs *DevsAnalysis) serializeBinary(result *DevsResult, writer io.Writer) error {
	message := pb.DevsAnalysisResults{}
	message.DevIndex = result.reversedPeopleDict
	message.Days = map[int32]*pb.DayDevs{}
	for day, devs := range result.Days {
		dd := &pb.DayDevs{}
		message.Days[int32(day)] = dd
		dd.Devs = map[int32]*pb.DevDay{}
		for dev, stats := range devs {
			if dev == identity.AuthorMissing {
				dev = -1
			}
			languages := map[string]*pb.LineStats{}
			dd.Devs[int32(dev)] = &pb.DevDay{
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

func init() {
	core.Registry.Register(&DevsAnalysis{})
}
