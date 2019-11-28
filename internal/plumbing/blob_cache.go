package plumbing

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v10/internal"
	"gopkg.in/src-d/hercules.v10/internal/core"
)

// ErrorBinary is raised in CachedBlob.CountLines() if the file is binary.
var ErrorBinary = errors.New("binary")

// CachedBlob allows to explicitly cache the binary data associated with the Blob object.
type CachedBlob struct {
	object.Blob
	// Data is the read contents of the blob object.
	Data []byte
}

// Reader returns a reader allow the access to the content of the blob
func (b *CachedBlob) Reader() (io.ReadCloser, error) {
	return ioutil.NopCloser(bytes.NewReader(b.Data)), nil
}

// Cache reads the underlying blob object and sets CachedBlob.Data.
func (b *CachedBlob) Cache() error {
	reader, err := b.Blob.Reader()
	if err != nil {
		return err
	}
	defer reader.Close()
	buf := new(bytes.Buffer)
	buf.Grow(int(b.Size))
	size, err := buf.ReadFrom(reader)
	if err != nil {
		return err
	}
	if size != b.Size {
		return fmt.Errorf("incomplete read of %s: %d while the declared size is %d",
			b.Hash.String(), size, b.Size)
	}
	b.Data = buf.Bytes()
	return nil
}

// CountLines returns the number of lines in the blob or (0, ErrorBinary) if it is binary.
func (b *CachedBlob) CountLines() (int, error) {
	if len(b.Data) == 0 {
		return 0, nil
	}
	// 8000 was taken from go-git's utils/binary.IsBinary()
	sniffLen := 8000
	sniff := b.Data
	if len(sniff) > sniffLen {
		sniff = sniff[:sniffLen]
	}
	if bytes.IndexByte(sniff, 0) >= 0 {
		return 0, ErrorBinary
	}
	lines := bytes.Count(b.Data, []byte{'\n'})
	if b.Data[len(b.Data)-1] != '\n' {
		lines++
	}
	return lines, nil
}

// BlobCache loads the blobs which correspond to the changed files in a commit.
// It is a PipelineItem.
// It must provide the old and the new objects; "blobCache" rotates and allows to not load
// the same blobs twice. Outdated objects are removed so "blobCache" never grows big.
type BlobCache struct {
	core.NoopMerger
	// Specifies how to handle the situation when we encounter a git submodule - an object
	// without the blob. If true, we look inside .gitmodules and if we don't find it,
	// raise an error. If false, we do not look inside .gitmodules and always succeed.
	FailOnMissingSubmodules bool

	repository *git.Repository
	cache      map[plumbing.Hash]*CachedBlob

	l core.Logger
}

const (
	// ConfigBlobCacheFailOnMissingSubmodules is the name of the configuration option for
	// BlobCache.Configure() to check if the referenced submodules are registered in .gitignore.
	ConfigBlobCacheFailOnMissingSubmodules = "BlobCache.FailOnMissingSubmodules"
	// DependencyBlobCache identifies the dependency provided by BlobCache.
	DependencyBlobCache = "blob_cache"
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (blobCache *BlobCache) Name() string {
	return "BlobCache"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (blobCache *BlobCache) Provides() []string {
	return []string{DependencyBlobCache}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (blobCache *BlobCache) Requires() []string {
	return []string{DependencyTreeChanges}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (blobCache *BlobCache) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name: ConfigBlobCacheFailOnMissingSubmodules,
		Description: "Specifies whether to panic if any referenced submodule does " +
			"not exist in .gitmodules and thus the corresponding Git object cannot be loaded. " +
			"Override this if you want to ensure that your repository is integral.",
		Flag:    "fail-on-missing-submodules",
		Type:    core.BoolConfigurationOption,
		Default: false}}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (blobCache *BlobCache) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		blobCache.l = l
	} else {
		blobCache.l = core.NewLogger()
	}
	if val, exists := facts[ConfigBlobCacheFailOnMissingSubmodules].(bool); exists {
		blobCache.FailOnMissingSubmodules = val
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (blobCache *BlobCache) Initialize(repository *git.Repository) error {
	blobCache.l = core.NewLogger()
	blobCache.repository = repository
	blobCache.cache = map[plumbing.Hash]*CachedBlob{}
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents
// the analysed *object.Commit. This function returns the mapping with analysis
// results. The keys must be the same as in Provides(). If there was an error,
// nil is returned.
func (blobCache *BlobCache) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps[core.DependencyCommit].(*object.Commit)
	changes := deps[DependencyTreeChanges].(object.Changes)
	cache := map[plumbing.Hash]*CachedBlob{}
	newCache := map[plumbing.Hash]*CachedBlob{}
	for _, change := range changes {
		action, err := change.Action()
		if err != nil {
			blobCache.l.Errorf("no action in %s\n", change.To.TreeEntry.Hash)
			return nil, err
		}
		var exists bool
		var blob *object.Blob
		switch action {
		case merkletrie.Insert:
			cache[change.To.TreeEntry.Hash] = &CachedBlob{}
			newCache[change.To.TreeEntry.Hash] = &CachedBlob{}
			blob, err = blobCache.getBlob(&change.To, commit.File)
			if err != nil {
				blobCache.l.Errorf("file to %s %s: %v\n", change.To.Name, change.To.TreeEntry.Hash, err)
			} else {
				cb := &CachedBlob{Blob: *blob}
				err = cb.Cache()
				if err == nil {
					cache[change.To.TreeEntry.Hash] = cb
					newCache[change.To.TreeEntry.Hash] = cb
				} else {
					blobCache.l.Errorf("file to %s %s: %v\n", change.To.Name, change.To.TreeEntry.Hash, err)
				}
			}
		case merkletrie.Delete:
			cache[change.From.TreeEntry.Hash], exists =
				blobCache.cache[change.From.TreeEntry.Hash]
			if !exists {
				cache[change.From.TreeEntry.Hash] = &CachedBlob{}
				blob, err = blobCache.getBlob(&change.From, commit.File)
				if err != nil {
					if err.Error() != plumbing.ErrObjectNotFound.Error() {
						blobCache.l.Errorf("file from %s %s: %v\n", change.From.Name,
							change.From.TreeEntry.Hash, err)
					} else {
						blob, err = internal.CreateDummyBlob(change.From.TreeEntry.Hash)
						cache[change.From.TreeEntry.Hash] = &CachedBlob{Blob: *blob}
					}
				} else {
					cb := &CachedBlob{Blob: *blob}
					err = cb.Cache()
					if err == nil {
						cache[change.From.TreeEntry.Hash] = cb
					} else {
						blobCache.l.Errorf("file from %s %s: %v\n", change.From.Name,
							change.From.TreeEntry.Hash, err)
					}
				}
			}
		case merkletrie.Modify:
			blob, err = blobCache.getBlob(&change.To, commit.File)
			cache[change.To.TreeEntry.Hash] = &CachedBlob{}
			newCache[change.To.TreeEntry.Hash] = &CachedBlob{}
			if err != nil {
				blobCache.l.Errorf("file to %s: %v\n", change.To.Name, err)
			} else {
				cb := &CachedBlob{Blob: *blob}
				err = cb.Cache()
				if err == nil {
					cache[change.To.TreeEntry.Hash] = cb
					newCache[change.To.TreeEntry.Hash] = cb
				} else {
					blobCache.l.Errorf("file to %s: %v\n", change.To.Name, err)
				}
			}
			cache[change.From.TreeEntry.Hash], exists =
				blobCache.cache[change.From.TreeEntry.Hash]
			if !exists {
				cache[change.From.TreeEntry.Hash] = &CachedBlob{}
				blob, err = blobCache.getBlob(&change.From, commit.File)
				if err != nil {
					blobCache.l.Errorf("file from %s: %v\n", change.From.Name, err)
				} else {
					cb := &CachedBlob{Blob: *blob}
					err = cb.Cache()
					if err == nil {
						cache[change.From.TreeEntry.Hash] = cb
					} else {
						blobCache.l.Errorf("file from %s: %v\n", change.From.Name, err)
					}
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

// Fork clones this PipelineItem.
func (blobCache *BlobCache) Fork(n int) []core.PipelineItem {
	caches := make([]core.PipelineItem, n)
	for i := 0; i < n; i++ {
		cache := map[plumbing.Hash]*CachedBlob{}
		for k, v := range blobCache.cache {
			cache[k] = v
		}
		caches[i] = &BlobCache{
			FailOnMissingSubmodules: blobCache.FailOnMissingSubmodules,
			repository:              blobCache.repository,
			cache:                   cache,
		}
	}
	return caches
}

// FileGetter defines a function which loads the Git file by
// the specified path. The state can be arbitrary though here it always
// corresponds to the currently processed commit.
type FileGetter func(path string) (*object.File, error)

// Returns the blob which corresponds to the specified ChangeEntry.
func (blobCache *BlobCache) getBlob(entry *object.ChangeEntry, fileGetter FileGetter) (
	*object.Blob, error) {
	blob, err := blobCache.repository.BlobObject(entry.TreeEntry.Hash)

	if err != nil {
		if err.Error() != plumbing.ErrObjectNotFound.Error() {
			blobCache.l.Errorf("getBlob(%s)\n", entry.TreeEntry.Hash.String())
			return nil, err
		}
		if entry.TreeEntry.Mode != 0160000 {
			// this is not a submodule
			return nil, err
		} else if !blobCache.FailOnMissingSubmodules {
			return internal.CreateDummyBlob(entry.TreeEntry.Hash)
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
			return internal.CreateDummyBlob(entry.TreeEntry.Hash)
		}
		return nil, err
	}
	return blob, nil
}

func init() {
	core.Registry.Register(&BlobCache{})
}
