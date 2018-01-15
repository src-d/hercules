package hercules

import (
	"fmt"
	"os"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
)

// BlobCache loads the blobs which correspond to the changed files in a commit.
// It is a PipelineItem.
// It must provide the old and the new objects; "blobCache" rotates and allows to not load
// the same blobs twice. Outdated objects are removed so "blobCache" never grows big.
type BlobCache struct {
	// Specifies how to handle the situation when we encounter a git submodule - an object without
	// the blob. If false, we look inside .gitmodules and if don't find, raise an error.
	// If true, we do not look inside .gitmodules and always succeed.
	IgnoreMissingSubmodules bool

	repository *git.Repository
	cache      map[plumbing.Hash]*object.Blob
}

const (
	// ConfigBlobCacheIgnoreMissingSubmodules is the name of the configuration option for
	// BlobCache.Configure() to not check if the referenced submodules exist.
	ConfigBlobCacheIgnoreMissingSubmodules = "BlobCache.IgnoreMissingSubmodules"
	// DependencyBlobCache identifies the dependency provided by BlobCache.
	DependencyBlobCache                    = "blob_cache"
)

func (blobCache *BlobCache) Name() string {
	return "BlobCache"
}

func (blobCache *BlobCache) Provides() []string {
	arr := [...]string{DependencyBlobCache}
	return arr[:]
}

func (blobCache *BlobCache) Requires() []string {
	arr := [...]string{DependencyTreeChanges}
	return arr[:]
}

func (blobCache *BlobCache) ListConfigurationOptions() []ConfigurationOption {
	options := [...]ConfigurationOption{{
		Name: ConfigBlobCacheIgnoreMissingSubmodules,
		Description: "Specifies whether to panic if some referenced submodules do not exist and thus" +
			" the corresponding Git objects cannot be loaded. Override this if you know that the " +
				"history is dirty and you want to get things done.",
		Flag:    "ignore-missing-submodules",
		Type:    BoolConfigurationOption,
		Default: false}}
	return options[:]
}

func (blobCache *BlobCache) Configure(facts map[string]interface{}) {
	if val, exists := facts[ConfigBlobCacheIgnoreMissingSubmodules].(bool); exists {
		blobCache.IgnoreMissingSubmodules = val
	}
}

func (blobCache *BlobCache) Initialize(repository *git.Repository) {
	blobCache.repository = repository
	blobCache.cache = map[plumbing.Hash]*object.Blob{}
}

func (blobCache *BlobCache) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps["commit"].(*object.Commit)
	changes := deps[DependencyTreeChanges].(object.Changes)
	cache := map[plumbing.Hash]*object.Blob{}
	newCache := map[plumbing.Hash]*object.Blob{}
	for _, change := range changes {
		action, err := change.Action()
		if err != nil {
			fmt.Fprintf(os.Stderr, "no action in %s\n", change.To.TreeEntry.Hash)
			return nil, err
		}
		var exists bool
		var blob *object.Blob
		switch action {
		case merkletrie.Insert:
			blob, err = blobCache.getBlob(&change.To, commit.File)
			if err != nil {
				fmt.Fprintf(os.Stderr, "file to %s %s\n", change.To.Name, change.To.TreeEntry.Hash)
			} else {
				cache[change.To.TreeEntry.Hash] = blob
				newCache[change.To.TreeEntry.Hash] = blob
			}
		case merkletrie.Delete:
			cache[change.From.TreeEntry.Hash], exists = blobCache.cache[change.From.TreeEntry.Hash]
			if !exists {
				cache[change.From.TreeEntry.Hash], err = blobCache.getBlob(&change.From, commit.File)
				if err != nil {
					if err.Error() != plumbing.ErrObjectNotFound.Error() {
						fmt.Fprintf(os.Stderr, "file from %s %s\n", change.From.Name,
							change.From.TreeEntry.Hash)
					} else {
						cache[change.From.TreeEntry.Hash], err = createDummyBlob(
							change.From.TreeEntry.Hash)
					}
				}
			}
		case merkletrie.Modify:
			blob, err = blobCache.getBlob(&change.To, commit.File)
			if err != nil {
				fmt.Fprintf(os.Stderr, "file to %s\n", change.To.Name)
			} else {
				cache[change.To.TreeEntry.Hash] = blob
				newCache[change.To.TreeEntry.Hash] = blob
			}
			cache[change.From.TreeEntry.Hash], exists = blobCache.cache[change.From.TreeEntry.Hash]
			if !exists {
				cache[change.From.TreeEntry.Hash], err = blobCache.getBlob(&change.From, commit.File)
				if err != nil {
					fmt.Fprintf(os.Stderr, "file from %s\n", change.From.Name)
				}
			}
		}
		if err != nil {
			return nil, err
		}
	}
	blobCache.cache = newCache
	return map[string]interface{}{DependencyBlobCache: cache}, nil
}

// FileGetter defines a function which loads the Git file by the specified path.
// The state can be arbitrary though here it always corresponds to the currently processed
// commit.
type FileGetter func(path string) (*object.File, error)

// Returns the blob which corresponds to the specified ChangeEntry.
func (blobCache *BlobCache) getBlob(entry *object.ChangeEntry, fileGetter FileGetter) (
	*object.Blob, error) {
	blob, err := blobCache.repository.BlobObject(entry.TreeEntry.Hash)
	if err != nil {
		if err.Error() != plumbing.ErrObjectNotFound.Error() {
			fmt.Fprintf(os.Stderr, "getBlob(%s)\n", entry.TreeEntry.Hash.String())
			return nil, err
		}
		if entry.TreeEntry.Mode != 0160000 {
			// this is not a submodule
			return nil, err
		} else if blobCache.IgnoreMissingSubmodules {
			return createDummyBlob(entry.TreeEntry.Hash)
		}
		file, errModules := fileGetter(".gitmodules")
		if errModules != nil {
			return nil, errModules
		}
		contents, errModules := file.Contents()
		if errModules != nil {
			return nil, errModules
		}
		modules := config.NewModules()
		errModules = modules.Unmarshal([]byte(contents))
		if errModules != nil {
			return nil, errModules
		}
		_, exists := modules.Submodules[entry.Name]
		if exists {
			// we found that this is a submodule
			return createDummyBlob(entry.TreeEntry.Hash)
		}
		return nil, err
	}
	return blob, nil
}

func init() {
	Registry.Register(&BlobCache{})
}
