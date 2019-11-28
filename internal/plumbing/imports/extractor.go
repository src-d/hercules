package imports

import (
	"runtime"
	"sync"

	"github.com/src-d/imports"
	_ "github.com/src-d/imports/languages/all" // register the supported languages
	"gopkg.in/src-d/go-git.v4"
	gitplumbing "gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/plumbing"
)

// Extractor reports the imports in the changed files.
type Extractor struct {
	core.NoopMerger
	// Goroutines is the number of goroutines to run for imports extraction.
	Goroutines int
	// MaxFileSize is the file size threshold. Files that exceed it are ignored.
	MaxFileSize int

	l core.Logger
}

const (
	// DependencyImports is the name of the dependency provided by Extractor.
	DependencyImports = "imports"
	// ConfigImportsGoroutines is the name of the configuration option for
	// Extractor.Configure() to set the number of parallel goroutines for imports extraction.
	ConfigImportsGoroutines = "Imports.Goroutines"
	// ConfigMaxFileSize is the name of the configuration option for
	// Extractor.Configure() to set the file size threshold after which they are ignored.
	ConfigMaxFileSize = "Imports.MaxFileSize"
	// DefaultMaxFileSize is the default value for Extractor.MaxFileSize.
	DefaultMaxFileSize = 1 << 20
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (ex *Extractor) Name() string {
	return "Imports"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (ex *Extractor) Provides() []string {
	return []string{DependencyImports}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (ex *Extractor) Requires() []string {
	return []string{plumbing.DependencyTreeChanges, plumbing.DependencyBlobCache}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (ex *Extractor) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{{
		Name:        ConfigImportsGoroutines,
		Description: "Specifies the number of goroutines to run in parallel for the imports extraction.",
		Flag:        "import-goroutines",
		Type:        core.IntConfigurationOption,
		Default:     runtime.NumCPU()}, {
		Name:        ConfigMaxFileSize,
		Description: "Specifies the file size threshold. Files that exceed it are ignored.",
		Flag:        "import-max-file-size",
		Type:        core.IntConfigurationOption,
		Default:     DefaultMaxFileSize},
	}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (ex *Extractor) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		ex.l = l
	}
	if gr, exists := facts[ConfigImportsGoroutines].(int); exists {
		if gr < 1 {
			if ex.l != nil {
				ex.l.Warnf("invalid number of goroutines for the imports extraction: %d. Set to %d.",
					gr, runtime.NumCPU())
			}
			gr = runtime.NumCPU()
		}
		ex.Goroutines = gr
	}
	if size, exists := facts[ConfigMaxFileSize].(int); exists {
		if size <= 0 {
			if ex.l != nil {
				ex.l.Warnf("invalid maximum file size: %d. Set to %d.", size, DefaultMaxFileSize)
			}
			size = DefaultMaxFileSize
		}
		ex.MaxFileSize = size
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (ex *Extractor) Initialize(repository *git.Repository) error {
	ex.l = core.NewLogger()
	if ex.Goroutines < 1 {
		ex.Goroutines = runtime.NumCPU()
	}
	if ex.MaxFileSize == 0 {
		ex.MaxFileSize = DefaultMaxFileSize
	}
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (ex *Extractor) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	changes := deps[plumbing.DependencyTreeChanges].(object.Changes)
	cache := deps[plumbing.DependencyBlobCache].(map[gitplumbing.Hash]*plumbing.CachedBlob)
	result := map[gitplumbing.Hash]imports.File{}
	jobs := make(chan *object.Change, ex.Goroutines)
	resultSync := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(ex.Goroutines)
	for i := 0; i < ex.Goroutines; i++ {
		go func() {
			for change := range jobs {
				blob := cache[change.To.TreeEntry.Hash]
				if blob.Size > int64(ex.MaxFileSize) {
					ex.l.Warnf("skipped %s %s: size is too big: %d > %d",
						change.To.TreeEntry.Name, change.To.TreeEntry.Hash.String(),
						blob.Size, ex.MaxFileSize)
					continue
				}
				file, err := imports.Extract(change.To.TreeEntry.Name, blob.Data)
				if err != nil {
					ex.l.Errorf("failed to extract imports from %s %s: %v",
						change.To.TreeEntry.Name, change.To.TreeEntry.Hash.String(), err)
				} else {
					resultSync.Lock()
					result[change.To.TreeEntry.Hash] = *file
					resultSync.Unlock()
				}
			}
			wg.Done()
		}()
	}
	for _, change := range changes {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		switch action {
		case merkletrie.Modify, merkletrie.Insert:
			jobs <- change
		case merkletrie.Delete:
			continue
		}
	}
	close(jobs)
	wg.Wait()
	return map[string]interface{}{DependencyImports: result}, nil
}

// Fork clones this PipelineItem.
func (ex *Extractor) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(ex, n)
}

func init() {
	core.Registry.Register(&Extractor{})
}
