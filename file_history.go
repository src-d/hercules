package hercules

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
	"gopkg.in/src-d/hercules.v3/pb"
)

// FileHistory contains the intermediate state which is mutated by Consume(). It should implement
// LeafPipelineItem.
type FileHistory struct {
	files map[string][]plumbing.Hash
}

// FileHistoryResult is returned by Finalize() and represents the analysis result.
type FileHistoryResult struct {
	Files map[string][]plumbing.Hash
}

func (history *FileHistory) Name() string {
	return "FileHistory"
}

func (history *FileHistory) Provides() []string {
	return []string{}
}

func (history *FileHistory) Requires() []string {
	arr := [...]string{DependencyTreeChanges}
	return arr[:]
}

func (history *FileHistory) ListConfigurationOptions() []ConfigurationOption {
	return []ConfigurationOption{}
}

func (history *FileHistory) Flag() string {
	return "file-history"
}

func (history *FileHistory) Configure(facts map[string]interface{}) {
}

// Initialize resets the internal temporary data structures and prepares the object for Consume().
func (history *FileHistory) Initialize(repository *git.Repository) {
	history.files = map[string][]plumbing.Hash{}
}

// Consume is called for every commit in the sequence.
func (history *FileHistory) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps["commit"].(*object.Commit).Hash
	changes := deps[DependencyTreeChanges].(object.Changes)
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

func (history *FileHistory) Finalize() interface{} {
	return FileHistoryResult{Files: history.files}
}

// Serialize converts the result from Finalize() to either Protocol Buffers or YAML.
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
	Registry.Register(&FileHistory{})
}
