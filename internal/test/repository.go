package test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"

	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/memory"
)

// Repository is a boilerplate sample repository (Hercules itself).
var Repository *git.Repository

// FakeChangeForName creates an artificial Git Change from a file name and two arbitrary hashes.
func FakeChangeForName(name string, hashFrom string, hashTo string) *object.Change {
	return &object.Change{
		From: object.ChangeEntry{Name: name, TreeEntry: object.TreeEntry{
			Name: name, Hash: plumbing.NewHash(hashFrom),
		}},
		To: object.ChangeEntry{Name: name, TreeEntry: object.TreeEntry{
			Name: name, Hash: plumbing.NewHash(hashTo),
		}},
	}
}

func init() {
	cwd, err := os.Getwd()
	if err == nil {
		for true {
			files, err := ioutil.ReadDir(cwd)
			if err != nil {
				break
			}
			found := false
			for _, f := range files {
				if f.Name() == "README.md" {
					found = true
					break
				}
			}
			if found {
				break
			}
			oldCwd := cwd
			cwd = path.Dir(cwd)
			if oldCwd == cwd {
				break
			}
		}
		Repository, err = git.PlainOpen(cwd)
		if err == nil {
			iter, err := Repository.CommitObjects()
			if err == nil {
				commits := -1
				for ; err != io.EOF; _, err = iter.Next() {
					if err != nil {
						panic(err)
					}
					commits++
					if commits >= 100 {
						return
					}
				}
			}
		}
	}
	Repository, err = git.Clone(memory.NewStorage(), nil, &git.CloneOptions{
		URL: "https://github.com/src-d/hercules",
	})
	if err != nil {
		panic(err)
	}
}

// InMemRepositoryOptions declares config for NewInMemRepository
type InMemRepositoryOptions struct {
	CreateBranch string
}

// InMemRepositoryOutput provides output from options provided in InMemRepositoryOptions
type InMemRepositoryOutput struct {
	CreatedBranchHash plumbing.Hash
}

// NewInMemRepository initializes a new in-memory repository
func NewInMemRepository(opts *InMemRepositoryOptions) (*git.Repository, InMemRepositoryOutput) {
	var out InMemRepositoryOutput

	repo, err := git.Clone(memory.NewStorage(), memfs.New(), &git.CloneOptions{
		URL: "https://github.com/src-d/hercules",
	})
	if err != nil {
		panic(err)
	}

	if opts != nil && opts.CreateBranch != "" {
		t, err := repo.Worktree()
		if err != nil {
			panic(err)
		}
		if err := t.Checkout(&git.CheckoutOptions{
			Branch: plumbing.NewBranchReferenceName(opts.CreateBranch),
			Force:  true,
			Create: true,
		}); err != nil {
			panic(err)
		}
		out.CreatedBranchHash, err = t.Commit(
			fmt.Sprintf("test commit on %s", opts.CreateBranch),
			&git.CommitOptions{
				All:    true,
				Author: &object.Signature{Name: "bobheadxi", Email: "bobheadxi@email.com", When: time.Now()},
			},
		)
		if err != nil {
			panic(err)
		}

		// check out master again
		if err := t.Checkout(&git.CheckoutOptions{
			Branch: plumbing.NewBranchReferenceName("master"),
			Force:  true,
		}); err != nil {
			panic(err)
		}
	}

	return repo, out
}
