package plumbing

import (
	"path"

	"github.com/src-d/enry/v2"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
)

// LanguagesDetection run programming language detection over the changed files.
type LanguagesDetection struct {
	core.NoopMerger

	l core.Logger
}

const (
	// DependencyLanguages is the name of the dependency provided by LanguagesDetection.
	DependencyLanguages = "languages"
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (langs *LanguagesDetection) Name() string {
	return "LanguagesDetection"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (langs *LanguagesDetection) Provides() []string {
	return []string{DependencyLanguages}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (langs *LanguagesDetection) Requires() []string {
	return []string{DependencyTreeChanges, DependencyBlobCache}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (langs *LanguagesDetection) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (langs *LanguagesDetection) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		langs.l = l
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (langs *LanguagesDetection) Initialize(repository *git.Repository) error {
	langs.l = core.NewLogger()
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (langs *LanguagesDetection) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	changes := deps[DependencyTreeChanges].(object.Changes)
	cache := deps[DependencyBlobCache].(map[plumbing.Hash]*CachedBlob)
	result := map[plumbing.Hash]string{}
	for _, change := range changes {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		switch action {
		case merkletrie.Insert:
			result[change.To.TreeEntry.Hash] = langs.detectLanguage(
				change.To.Name, cache[change.To.TreeEntry.Hash])
		case merkletrie.Delete:
			result[change.From.TreeEntry.Hash] = langs.detectLanguage(
				change.From.Name, cache[change.From.TreeEntry.Hash])
		case merkletrie.Modify:
			result[change.To.TreeEntry.Hash] = langs.detectLanguage(
				change.To.Name, cache[change.To.TreeEntry.Hash])
			result[change.From.TreeEntry.Hash] = langs.detectLanguage(
				change.From.Name, cache[change.From.TreeEntry.Hash])
		}
	}
	return map[string]interface{}{DependencyLanguages: result}, nil
}

// Fork clones this PipelineItem.
func (langs *LanguagesDetection) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(langs, n)
}

// detectLanguage returns the programming language of a blob.
func (langs *LanguagesDetection) detectLanguage(name string, blob *CachedBlob) string {
	_, err := blob.CountLines()
	if err == ErrorBinary {
		return ""
	}
	lang := enry.GetLanguage(path.Base(name), blob.Data)
	return lang
}

func init() {
	core.Registry.Register(&LanguagesDetection{})
}
