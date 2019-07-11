package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v10"
)

// ChurnAnalysis contains the intermediate state which is mutated by Consume(). It should implement
// hercules.LeafPipelineItem.
type ChurnAnalysis struct {
	// No special merge logic is required
	hercules.NoopMerger
	// Process each merge only once
	hercules.OneShotMergeProcessor
	TrackPeople bool

	global []editInfo
	people map[int][]editInfo

	// references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string

	l hercules.Logger
}

type editInfo struct {
	Tick    int
	Added   int
	Removed int
}

// ChurnAnalysisResult is returned by Finalize() and represents the analysis result.
type ChurnAnalysisResult struct {
	Global Edits
	People map[string]Edits
}

type Edits struct {
	Ticks     []int
	Additions []int
	Removals  []int
}

const (
	ConfigChurnTrackPeople = "Churn.TrackPeople"
)

// Analysis' name in the graph is usually the same as the type's name, however, does not have to.
func (churn *ChurnAnalysis) Name() string {
	return "ChurnAnalysis"
}

// LeafPipelineItem-s normally do not act as intermediate nodes and thus we return an empty slice.
func (churn *ChurnAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of dependencies which must be supplied in Consume().
// file_diff - line diff for each commit change
// changes - list of changed files for each commit
// blob_cache - set of blobs affected by each commit
// tick - number of ticks since start for each commit
// author - author of the commit
func (churn *ChurnAnalysis) Requires() []string {
	return []string{
		hercules.DependencyFileDiff,
		hercules.DependencyTreeChanges,
		hercules.DependencyBlobCache,
		hercules.DependencyTick,
		hercules.DependencyAuthor}
}

// ListConfigurationOptions tells the engine which parameters can be changed through the command
// line.
func (churn *ChurnAnalysis) ListConfigurationOptions() []hercules.ConfigurationOption {
	opts := [...]hercules.ConfigurationOption{{
		Name:        ConfigChurnTrackPeople,
		Description: "Record detailed statistics per each developer.",
		Flag:        "churn-people",
		Type:        hercules.BoolConfigurationOption,
		Default:     false},
	}
	return opts[:]
}

// Flag returns the command line switch which activates the analysis.
func (churn *ChurnAnalysis) Flag() string {
	return "churn"
}

// Description returns the text which explains what the analysis is doing.
func (churn *ChurnAnalysis) Description() string {
	return "Collects the daily numbers of inserted and removed lines."
}

// Configure applies the parameters specified in the command line. Map keys correspond to "Name".
func (churn *ChurnAnalysis) Configure(facts map[string]interface{}) error {
	if l, exists := facts[hercules.ConfigLogger].(hercules.Logger); exists {
		churn.l = l
	}
	if val, exists := facts[ConfigChurnTrackPeople].(bool); exists {
		churn.TrackPeople = val
	}
	if churn.TrackPeople {
		churn.reversedPeopleDict = facts[hercules.FactIdentityDetectorReversedPeopleDict].([]string)
	}
	return nil
}

// Initialize resets the internal temporary data structures and prepares the object for Consume().
func (churn *ChurnAnalysis) Initialize(repository *git.Repository) error {
	churn.l = hercules.NewLogger()
	churn.global = []editInfo{}
	churn.people = map[int][]editInfo{}
	churn.OneShotMergeProcessor.Initialize()
	return nil
}

func (churn *ChurnAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if !churn.ShouldConsumeCommit(deps) {
		return nil, nil
	}
	fileDiffs := deps[hercules.DependencyFileDiff].(map[string]hercules.FileDiffData)
	treeDiffs := deps[hercules.DependencyTreeChanges].(object.Changes)
	cache := deps[hercules.DependencyBlobCache].(map[plumbing.Hash]*hercules.CachedBlob)
	tick := deps[hercules.DependencyTick].(int)
	author := deps[hercules.DependencyAuthor].(int)
	for _, change := range treeDiffs {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		added := 0
		removed := 0
		switch action {
		case merkletrie.Insert:
			added, _ = cache[change.To.TreeEntry.Hash].CountLines()
		case merkletrie.Delete:
			removed, _ = cache[change.From.TreeEntry.Hash].CountLines()
		case merkletrie.Modify:
			diffs := fileDiffs[change.To.Name]
			for _, edit := range diffs.Diffs {
				length := utf8.RuneCountInString(edit.Text)
				switch edit.Type {
				case diffmatchpatch.DiffEqual:
					continue
				case diffmatchpatch.DiffInsert:
					added += length
				case diffmatchpatch.DiffDelete:
					removed += length
				}
			}

		}
		if err != nil {
			return nil, err
		}
		ei := editInfo{Tick: tick, Added: added, Removed: removed}
		churn.global = append(churn.global, ei)
		if churn.TrackPeople {
			seq, exists := churn.people[author]
			if !exists {
				seq = []editInfo{}
			}
			seq = append(seq, ei)
			churn.people[author] = seq
		}
	}
	return nil, nil
}

// Fork clones the same item several times on branches.
func (churn *ChurnAnalysis) Fork(n int) []hercules.PipelineItem {
	return hercules.ForkSamePipelineItem(churn, n)
}

func (churn *ChurnAnalysis) Finalize() interface{} {
	result := ChurnAnalysisResult{
		Global: editInfosToEdits(churn.global),
		People: map[string]Edits{},
	}
	if churn.TrackPeople {
		for key, val := range churn.people {
			result.People[churn.reversedPeopleDict[key]] = editInfosToEdits(val)
		}
	}
	return result
}

func (churn *ChurnAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	burndownResult := result.(ChurnAnalysisResult)
	if binary {
		return churn.serializeBinary(&burndownResult, writer)
	}
	churn.serializeText(&burndownResult, writer)
	return nil
}

func (churn *ChurnAnalysis) serializeText(result *ChurnAnalysisResult, writer io.Writer) {
	fmt.Fprintln(writer, "  global:")
	printEdits(result.Global, writer, 4)
	for key, val := range result.People {
		fmt.Fprintf(writer, "  %s:\n", hercules.SafeYamlString(key))
		printEdits(val, writer, 4)
	}
}

func (churn *ChurnAnalysis) serializeBinary(result *ChurnAnalysisResult, writer io.Writer) error {
	message := ChurnAnalysisResultMessage{
		Global: editsToEditsMessage(result.Global),
		People: map[string]*EditsMessage{},
	}
	for key, val := range result.People {
		message.People[key] = editsToEditsMessage(val)
	}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	writer.Write(serialized)
	return nil
}

func editInfosToEdits(eis []editInfo) Edits {
	aux := map[int]*editInfo{}
	for _, ei := range eis {
		ptr := aux[ei.Tick]
		if ptr == nil {
			ptr = &editInfo{Tick: ei.Tick}
		}
		ptr.Added += ei.Added
		ptr.Removed += ei.Removed
		aux[ei.Tick] = ptr
	}
	seq := []int{}
	for key := range aux {
		seq = append(seq, key)
	}
	sort.Ints(seq)
	edits := Edits{
		Ticks:     make([]int, len(seq)),
		Additions: make([]int, len(seq)),
		Removals:  make([]int, len(seq)),
	}
	for i, tick := range seq {
		edits.Ticks[i] = tick
		edits.Additions[i] = aux[tick].Added
		edits.Removals[i] = aux[tick].Removed
	}
	return edits
}

func printEdits(edits Edits, writer io.Writer, indent int) {
	strIndent := strings.Repeat(" ", indent)
	printArray := func(arr []int, name string) {
		fmt.Fprintf(writer, "%s%s: [", strIndent, name)
		for i, v := range arr {
			if i < len(arr)-1 {
				fmt.Fprintf(writer, "%d, ", v)
			} else {
				fmt.Fprintf(writer, "%d]\n", v)
			}
		}
	}
	printArray(edits.Ticks, "ticks")
	printArray(edits.Additions, "additions")
	printArray(edits.Removals, "removals")
}

func editsToEditsMessage(edits Edits) *EditsMessage {
	message := &EditsMessage{
		Ticks:     make([]uint32, len(edits.Ticks)),
		Additions: make([]uint32, len(edits.Additions)),
		Removals:  make([]uint32, len(edits.Removals)),
	}
	copyInts := func(arr []int, where []uint32) {
		for i, v := range arr {
			where[i] = uint32(v)
		}
	}
	copyInts(edits.Ticks, message.Ticks)
	copyInts(edits.Additions, message.Additions)
	copyInts(edits.Removals, message.Removals)
	return message
}

func init() {
	hercules.Registry.Register(&ChurnAnalysis{})
}
