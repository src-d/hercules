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

type BlobCache struct {
	repository *git.Repository
}

func (cache *BlobCache) Name() string {
	return "BlobCache"
}

func (cache *BlobCache) Provides() []string {
	arr := [...]string{"blob_cache"}
	return arr[:]
}

func (cache *BlobCache) Requires() []string {
	arr := [...]string{"changes"}
	return arr[:]
}

func (cache *BlobCache) Initialize(repository *git.Repository) {
	cache.repository = repository
}

func (self *BlobCache) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps["commit"].(*object.Commit)
	changes := deps["changes"].(object.Changes)
	cache := make(map[plumbing.Hash]*object.Blob)
	for _, change := range changes {
		action, err := change.Action()
		if err != nil {
			fmt.Fprintf(os.Stderr, "no action in %s\n", change.To.TreeEntry.Hash)
			return nil, err
		}
		switch action {
		case merkletrie.Insert:
			cache[change.To.TreeEntry.Hash], err = self.getBlob(&change.To, commit)
			if err != nil {
				fmt.Fprintf(os.Stderr, "file to %s %s\n", change.To.Name, change.To.TreeEntry.Hash)
			}
		case merkletrie.Delete:
			cache[change.From.TreeEntry.Hash], err = self.getBlob(&change.From, commit)
			if err != nil {
				if err.Error() != plumbing.ErrObjectNotFound.Error() {
					fmt.Fprintf(os.Stderr, "file from %s %s\n", change.From.Name, change.From.TreeEntry.Hash)
				} else {
					cache[change.From.TreeEntry.Hash], err = createDummyBlob(
						&change.From.TreeEntry.Hash)
				}
			}
		case merkletrie.Modify:
			cache[change.To.TreeEntry.Hash], err = self.getBlob(&change.To, commit)
			if err != nil {
				fmt.Fprintf(os.Stderr, "file to %s\n", change.To.Name)
			}
			cache[change.From.TreeEntry.Hash], err = self.getBlob(&change.From, commit)
			if err != nil {
				fmt.Fprintf(os.Stderr, "file from %s\n", change.From.Name)
			}
		default:
			panic(fmt.Sprintf("unsupported action: %d", change.Action))
		}
		if err != nil {
			return nil, err
		}
	}
	return map[string]interface{}{"blob_cache": cache}, nil
}

func (cache *BlobCache) Finalize() interface{} {
	return nil
}

func (cache *BlobCache) getBlob(entry *object.ChangeEntry, commit *object.Commit) (
	*object.Blob, error) {
	blob, err := cache.repository.BlobObject(entry.TreeEntry.Hash)
	if err != nil {
		if err.Error() != plumbing.ErrObjectNotFound.Error() {
			fmt.Fprintf(os.Stderr, "getBlob(%s)\n", entry.TreeEntry.Hash.String())
			return nil, err
		}
		file, err_modules := commit.File(".gitmodules")
		if err_modules != nil {
			return nil, err
		}
		contents, err_modules := file.Contents()
		if err_modules != nil {
			return nil, err
		}
		modules := config.NewModules()
		err_modules = modules.Unmarshal([]byte(contents))
		if err_modules != nil {
			return nil, err
		}
		_, exists := modules.Submodules[entry.Name]
		if exists {
			// we found that this is a submodule
			return createDummyBlob(&entry.TreeEntry.Hash)
		}
		return nil, err
	}
	return blob, nil
}
