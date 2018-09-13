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
	"gopkg.in/src-d/hercules.v4/internal/core"
	"gopkg.in/src-d/hercules.v4/internal/pb"
	items "gopkg.in/src-d/hercules.v4/internal/plumbing"
)

// FileHistory contains the intermediate state which is mutated by Consume(). It should implement
// LeafPipelineItem.
type FileHistory struct {
	core.NoopMerger
	core.OneShotMergeProcessor
	files map[string][]plumbing.Hash
}

// FileHistoryResult is returned by Finalize() and represents the analysis result.
type FileHistoryResult struct {
	Files map[string][]plumbing.Hash
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (history *FileHistory) Name() string {
	return "FileHistory"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (history *FileHistory) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (history *FileHistory) Requires() []string {
	arr := [...]string{items.DependencyTreeChanges}
	return arr[:]
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (history *FileHistory) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{}
}

// Flag for the command line switch which enables this analysis.
func (history *FileHistory) Flag() string {
	return "file-history"
}

// Description returns the text which explains what the analysis is doing.
func (history *FileHistory) Description() string {
	return "Each file path is mapped to the list of commits which involve that file."
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (history *FileHistory) Configure(facts map[string]interface{}) {
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (history *FileHistory) Initialize(repository *git.Repository) {
	history.files = map[string][]plumbing.Hash{}
	history.OneShotMergeProcessor.Initialize()
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (history *FileHistory) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if !history.ShouldConsumeCommit(deps) {
		return nil, nil
	}
	commit := deps[core.DependencyCommit].(*object.Commit).Hash
	changes := deps[items.DependencyTreeChanges].(object.Changes)
	for _, change := range changes {
		action, _ := change.Action()
		switch action {
		case merkletrie.Insert:
			hashes := make([]plumbing.Hash, 1)
			hashes[0] = commit
			history.files[change.To.Name] = hashes
		case merkletrie.Delete:
			delete(history.files, change.From.Name)
		case merkletrie.Modify:
			hashes := history.files[change.From.Name]
			if change.From.Name != change.To.Name {
				delete(history.files, change.From.Name)
			}
			hashes = append(hashes, commit)
			history.files[change.To.Name] = hashes
		}
	}
	return nil, nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (history *FileHistory) Finalize() interface{} {
	return FileHistoryResult{Files: history.files}
}

// Fork clones this PipelineItem.
func (history *FileHistory) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(history, n)
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (history *FileHistory) Serialize(result interface{}, binary bool, writer io.Writer) error {
	historyResult := result.(FileHistoryResult)
	if binary {
		return history.serializeBinary(&historyResult, writer)
	}
	history.serializeText(&historyResult, writer)
	return nil
}

func (history *FileHistory) serializeText(result *FileHistoryResult, writer io.Writer) {
	keys := make([]string, len(result.Files))
	i := 0
	for key := range result.Files {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		hashes := result.Files[key]
		strhashes := make([]string, len(hashes))
		for i, hash := range hashes {
			strhashes[i] = "\"" + hash.String() + "\""
		}
		fmt.Fprintf(writer, "  - %s: [%s]\n", key, strings.Join(strhashes, ","))
	}
}

func (history *FileHistory) serializeBinary(result *FileHistoryResult, writer io.Writer) error {
	message := pb.FileHistoryResultMessage{
		Files: map[string]*pb.FileHistory{},
	}
	for key, vals := range result.Files {
		hashes := &pb.FileHistory{
			Commits: make([]string, len(vals)),
		}
		for i, hash := range vals {
			hashes.Commits[i] = hash.String()
		}
		message.Files[key] = hashes
	}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	writer.Write(serialized)
	return nil
}

func init() {
	core.Registry.Register(&FileHistory{})
}
