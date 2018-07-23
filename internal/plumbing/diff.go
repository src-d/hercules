package plumbing

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"unicode/utf8"

	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v4/internal/core"
)

// FileDiff calculates the difference of files which were modified.
// It is a PipelineItem.
type FileDiff struct {
	CleanupDisabled bool
}

const (
	// ConfigFileDiffDisableCleanup is the name of the configuration option (FileDiff.Configure())
	// to suppress diffmatchpatch.DiffCleanupSemanticLossless() which is supposed to improve
	// the human interpretability of diffs.
	ConfigFileDiffDisableCleanup = "FileDiff.NoCleanup"

	// DependencyFileDiff is the name of the dependency provided by FileDiff.
	DependencyFileDiff = "file_diff"
)

// FileDiffData is the type of the dependency provided by FileDiff.
type FileDiffData struct {
	OldLinesOfCode int
	NewLinesOfCode int
	Diffs          []diffmatchpatch.Diff
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (diff *FileDiff) Name() string {
	return "FileDiff"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (diff *FileDiff) Provides() []string {
	arr := [...]string{DependencyFileDiff}
	return arr[:]
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (diff *FileDiff) Requires() []string {
	arr := [...]string{DependencyTreeChanges, DependencyBlobCache}
	return arr[:]
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (diff *FileDiff) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigFileDiffDisableCleanup,
		Description: "Do not apply additional heuristics to improve diffs.",
		Flag:        "no-diff-cleanup",
		Type:        core.BoolConfigurationOption,
		Default:     false},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (diff *FileDiff) Configure(facts map[string]interface{}) {
	if val, exists := facts[ConfigFileDiffDisableCleanup].(bool); exists {
		diff.CleanupDisabled = val
	}
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (diff *FileDiff) Initialize(repository *git.Repository) {}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (diff *FileDiff) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	result := map[string]FileDiffData{}
	cache := deps[DependencyBlobCache].(map[plumbing.Hash]*object.Blob)
	treeDiff := deps[DependencyTreeChanges].(object.Changes)
	for _, change := range treeDiff {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		switch action {
		case merkletrie.Modify:
			blobFrom := cache[change.From.TreeEntry.Hash]
			blobTo := cache[change.To.TreeEntry.Hash]
			// we are not validating UTF-8 here because for example
			// git/git 4f7770c87ce3c302e1639a7737a6d2531fe4b160 fetch-pack.c is invalid UTF-8
			strFrom, err := BlobToString(blobFrom)
			if err != nil {
				return nil, err
			}
			strTo, err := BlobToString(blobTo)
			if err != nil {
				return nil, err
			}
			dmp := diffmatchpatch.New()
			src, dst, _ := dmp.DiffLinesToRunes(strFrom, strTo)
			diffs := dmp.DiffMainRunes(src, dst, false)
			if !diff.CleanupDisabled {
				diffs = dmp.DiffCleanupMerge(dmp.DiffCleanupSemanticLossless(diffs))
			}
			result[change.To.Name] = FileDiffData{
				OldLinesOfCode: len(src),
				NewLinesOfCode: len(dst),
				Diffs:          diffs,
			}
		default:
			continue
		}
	}
	return map[string]interface{}{DependencyFileDiff: result}, nil
}

func (diff *FileDiff) Fork(n int) []core.PipelineItem {
	diffs := make([]core.PipelineItem, n)
	for i := 0; i < n; i++ {
		diffs[i] = diff
	}
	return diffs
}

func (diff *FileDiff) Merge(branches []core.PipelineItem) {
	// no-op
}

// CountLines returns the number of lines in a *object.Blob.
func CountLines(file *object.Blob) (int, error) {
	if file == nil {
		return -1, errors.New("blob is nil: probably not cached")
	}
	reader, err := file.Reader()
	if err != nil {
		return -1, err
	}
	defer checkClose(reader)
	var scanner *bufio.Scanner
	buffer := make([]byte, bufio.MaxScanTokenSize)
	counter := 0
	for scanner == nil || scanner.Err() == bufio.ErrTooLong {
		if scanner != nil && !utf8.Valid(scanner.Bytes()) {
			return -1, errors.New("binary")
		}
		scanner = bufio.NewScanner(reader)
		scanner.Buffer(buffer, 0)
		for scanner.Scan() {
			if !utf8.Valid(scanner.Bytes()) {
				return -1, errors.New("binary")
			}
			counter++
		}
	}
	return counter, nil
}

// BlobToString reads *object.Blob and returns its contents as a string.
func BlobToString(file *object.Blob) (string, error) {
	if file == nil {
		return "", errors.New("blob is nil: probably not cached")
	}
	reader, err := file.Reader()
	if err != nil {
		return "", err
	}
	defer checkClose(reader)
	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	return buf.String(), nil
}

func checkClose(c io.Closer) {
	if err := c.Close(); err != nil {
		panic(err)
	}
}

func init() {
	core.Registry.Register(&FileDiff{})
}
