package test

import (
	"io"
	"os"

	git "gopkg.in/src-d/go-git.v4"
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
	Repository, _ = git.Clone(memory.NewStorage(), nil, &git.CloneOptions{
		URL: "https://github.com/src-d/hercules",
	})
}
