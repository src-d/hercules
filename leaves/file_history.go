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
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	items "gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
)

// FileHistoryAnalysis contains the intermediate state which is mutated by Consume(). It should implement
// LeafPipelineItem.
type FileHistoryAnalysis struct {
	core.NoopMerger
	core.OneShotMergeProcessor
	files      map[string]*FileHistory
	lastCommit *object.Commit

	l core.Logger
}

// FileHistoryResult is returned by Finalize() and represents the analysis result.
type FileHistoryResult struct {
	Files map[string]FileHistory
}

// FileHistory is the gathered stats about a particular file.
type FileHistory struct {
	// Hashes is the list of commit hashes which changed this file.
	Hashes []plumbing.Hash
	// People is the mapping from developers to the number of lines they altered.
	People map[int]items.LineStats
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (history *FileHistoryAnalysis) Name() string {
	return "FileHistoryAnalysis"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (history *FileHistoryAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (history *FileHistoryAnalysis) Requires() []string {
	return []string{items.DependencyTreeChanges, items.DependencyLineStats, identity.DependencyAuthor}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (history *FileHistoryAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{}
}

// Flag for the command line switch which enables this analysis.
func (history *FileHistoryAnalysis) Flag() string {
	return "file-history"
}

// Description returns the text which explains what the analysis is doing.
func (history *FileHistoryAnalysis) Description() string {
	return "Each file path is mapped to the list of commits which touch that file and the mapping " +
		"from involved developers to the corresponding line statistics: how many lines were added, " +
		"removed and changed throughout the whole history."
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (history *FileHistoryAnalysis) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		history.l = l
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (history *FileHistoryAnalysis) Initialize(repository *git.Repository) error {
	history.l = core.NewLogger()
	history.files = map[string]*FileHistory{}
	history.OneShotMergeProcessor.Initialize()
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (history *FileHistoryAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if deps[core.DependencyIsMerge].(bool) {
		// we ignore merge commits
		// TODO(vmarkovtsev): handle them better
		return nil, nil
	}
	history.lastCommit = deps[core.DependencyCommit].(*object.Commit)
	commit := history.lastCommit.Hash
	changes := deps[items.DependencyTreeChanges].(object.Changes)
	for _, change := range changes {
		action, _ := change.Action()
		var fh *FileHistory
		if action != merkletrie.Delete {
			fh = history.files[change.To.Name]
		} else {
			fh = history.files[change.From.Name]
		}
		if fh == nil {
			fh = &FileHistory{}
			history.files[change.To.Name] = fh
		}
		switch action {
		case merkletrie.Insert:
			fh.Hashes = []plumbing.Hash{commit}
		case merkletrie.Delete:
			fh.Hashes = append(fh.Hashes, commit)
		case merkletrie.Modify:
			hashes := history.files[change.From.Name].Hashes
			if change.From.Name != change.To.Name {
				delete(history.files, change.From.Name)
			}
			hashes = append(hashes, commit)
			fh.Hashes = hashes
		}
	}
	lineStats := deps[items.DependencyLineStats].(map[object.ChangeEntry]items.LineStats)
	author := deps[identity.DependencyAuthor].(int)
	for changeEntry, stats := range lineStats {
		file := history.files[changeEntry.Name]
		if file == nil {
			file = &FileHistory{}
			history.files[changeEntry.Name] = file
		}
		people := file.People
		if people == nil {
			people = map[int]items.LineStats{}
			file.People = people
		}
		oldStats := people[author]
		people[author] = items.LineStats{
			Added:   oldStats.Added + stats.Added,
			Removed: oldStats.Removed + stats.Removed,
			Changed: oldStats.Changed + stats.Changed,
		}
	}
	return nil, nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (history *FileHistoryAnalysis) Finalize() interface{} {
	files := map[string]FileHistory{}
	fileIter, err := history.lastCommit.Files()
	if err != nil {
		history.l.Errorf("Failed to iterate files of %s", history.lastCommit.Hash.String())
		return err
	}
	err = fileIter.ForEach(func(file *object.File) error {
		if fh := history.files[file.Name]; fh != nil {
			files[file.Name] = *fh
		}
		return nil
	})
	if err != nil {
		history.l.Errorf("Failed to iterate files of %s", history.lastCommit.Hash.String())
		return err
	}
	return FileHistoryResult{Files: files}
}

// Fork clones this PipelineItem.
func (history *FileHistoryAnalysis) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(history, n)
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (history *FileHistoryAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	historyResult := result.(FileHistoryResult)
	if binary {
		return history.serializeBinary(&historyResult, writer)
	}
	history.serializeText(&historyResult, writer)
	return nil
}

func (history *FileHistoryAnalysis) serializeText(result *FileHistoryResult, writer io.Writer) {
	keys := make([]string, len(result.Files))
	i := 0
	for key := range result.Files {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(writer, "  - %s:\n", key)
		file := result.Files[key]
		hashes := file.Hashes
		strhashes := make([]string, len(hashes))
		for i, hash := range hashes {
			strhashes[i] = "\"" + hash.String() + "\""
		}
		sort.Strings(strhashes)
		fmt.Fprintf(writer, "    commits: [%s]\n", strings.Join(strhashes, ","))
		strpeople := make([]string, 0, len(file.People))
		for key, val := range file.People {
			strpeople = append(strpeople, fmt.Sprintf("%d:[%d,%d,%d]", key, val.Added, val.Removed, val.Changed))
		}
		sort.Strings(strpeople)
		fmt.Fprintf(writer, "    people: {%s}\n", strings.Join(strpeople, ","))
	}
}

func (history *FileHistoryAnalysis) serializeBinary(result *FileHistoryResult, writer io.Writer) error {
	message := pb.FileHistoryResultMessage{
		Files: map[string]*pb.FileHistory{},
	}
	for key, vals := range result.Files {
		fh := &pb.FileHistory{
			Commits:            make([]string, len(vals.Hashes)),
			ChangesByDeveloper: map[int32]*pb.LineStats{},
		}
		for i, hash := range vals.Hashes {
			fh.Commits[i] = hash.String()
		}
		for key, val := range vals.People {
			fh.ChangesByDeveloper[int32(key)] = &pb.LineStats{
				Added:   int32(val.Added),
				Removed: int32(val.Removed),
				Changed: int32(val.Changed),
			}
		}
		message.Files[key] = fh
	}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	_, err = writer.Write(serialized)
	return err
}

func init() {
	core.Registry.Register(&FileHistoryAnalysis{})
}
