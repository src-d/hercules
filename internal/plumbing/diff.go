package plumbing

import (
	"strings"
	"time"

	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v10/internal/core"
)

// FileDiff calculates the difference of files which were modified.
// It is a PipelineItem.
type FileDiff struct {
	core.NoopMerger
	CleanupDisabled  bool
	WhitespaceIgnore bool
	Timeout          time.Duration

	l core.Logger
}

const (
	// ConfigFileDiffDisableCleanup is the name of the configuration option (FileDiff.Configure())
	// to suppress diffmatchpatch.DiffCleanupSemanticLossless() which is supposed to improve
	// the human interpretability of diffs.
	ConfigFileDiffDisableCleanup = "FileDiff.NoCleanup"

	// DependencyFileDiff is the name of the dependency provided by FileDiff.
	DependencyFileDiff = "file_diff"

	// ConfigFileWhitespaceIgnore is the name of the configuration option (FileDiff.Configure())
	// to suppress whitespace changes which can pollute the core diff of the files
	ConfigFileWhitespaceIgnore = "FileDiff.WhitespaceIgnore"

	// ConfigFileDiffTimeout is the number of milliseconds a single diff calculation may elapse.
	// We need this timeout to avoid spending too much time comparing big or "bad" files.
	ConfigFileDiffTimeout = "FileDiff.Timeout"
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
	return []string{DependencyFileDiff}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (diff *FileDiff) Requires() []string {
	return []string{DependencyTreeChanges, DependencyBlobCache}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (diff *FileDiff) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{
		{
			Name:        ConfigFileDiffDisableCleanup,
			Description: "Do not apply additional heuristics to improve diffs.",
			Flag:        "no-diff-cleanup",
			Type:        core.BoolConfigurationOption,
			Default:     false},
		{
			Name:        ConfigFileWhitespaceIgnore,
			Description: "Ignore whitespace when computing diffs.",
			Flag:        "no-diff-whitespace",
			Type:        core.BoolConfigurationOption,
			Default:     false},
		{
			Name:        ConfigFileDiffTimeout,
			Description: "Maximum time in milliseconds a single diff calculation may elapse.",
			Flag:        "diff-timeout",
			Type:        core.IntConfigurationOption,
			Default:     1000},
	}

	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (diff *FileDiff) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		diff.l = l
	}
	if val, exists := facts[ConfigFileDiffDisableCleanup].(bool); exists {
		diff.CleanupDisabled = val
	}
	if val, exists := facts[ConfigFileWhitespaceIgnore].(bool); exists {
		diff.WhitespaceIgnore = val
	}
	if val, exists := facts[ConfigFileDiffTimeout].(int); exists {
		if val <= 0 {
			diff.l.Warnf("invalid timeout value: %d", val)
		}
		diff.Timeout = time.Duration(val) * time.Millisecond
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (diff *FileDiff) Initialize(repository *git.Repository) error {
	diff.l = core.NewLogger()
	return nil
}

func stripWhitespace(str string, ignoreWhitespace bool) string {
	if ignoreWhitespace {
		response := strings.Replace(str, " ", "", -1)
		return response
	}
	return str
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (diff *FileDiff) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	result := map[string]FileDiffData{}
	cache := deps[DependencyBlobCache].(map[plumbing.Hash]*CachedBlob)
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
			strFrom, strTo := string(blobFrom.Data), string(blobTo.Data)
			dmp := diffmatchpatch.New()
			dmp.DiffTimeout = diff.Timeout
			src, dst, _ := dmp.DiffLinesToRunes(stripWhitespace(strFrom, diff.WhitespaceIgnore), stripWhitespace(strTo, diff.WhitespaceIgnore))
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

// Fork clones this PipelineItem.
func (diff *FileDiff) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(diff, n)
}

func init() {
	core.Registry.Register(&FileDiff{})
}
