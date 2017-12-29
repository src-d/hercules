package hercules

import (
	"io"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type TreeDiff struct {
	previousTree *object.Tree
}

const (
	DependencyTreeChanges = "changes"
)

func (treediff *TreeDiff) Name() string {
	return "TreeDiff"
}

func (treediff *TreeDiff) Provides() []string {
	arr := [...]string{DependencyTreeChanges}
	return arr[:]
}

func (treediff *TreeDiff) Requires() []string {
	return []string{}
}

func (treediff *TreeDiff) ListConfigurationOptions() []ConfigurationOption {
	return []ConfigurationOption{}
}

func (treediff *TreeDiff) Configure(facts map[string]interface{}) {}

func (treediff *TreeDiff) Initialize(repository *git.Repository) {
	treediff.previousTree = nil
}

func (treediff *TreeDiff) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps["commit"].(*object.Commit)
	tree, err := commit.Tree()
	if err != nil {
		return nil, err
	}
	var diff object.Changes
	if treediff.previousTree != nil {
		diff, err = object.DiffTree(treediff.previousTree, tree)
		if err != nil {
			return nil, err
		}
	} else {
		diff = []*object.Change{}
		err = func() error {
			file_iter := tree.Files()
			defer file_iter.Close()
			for {
				file, err := file_iter.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				diff = append(diff, &object.Change{
					To: object.ChangeEntry{Name: file.Name, Tree: tree, TreeEntry: object.TreeEntry{
						Name: file.Name, Mode: file.Mode, Hash: file.Hash}}})
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}
	treediff.previousTree = tree
	return map[string]interface{}{DependencyTreeChanges: diff}, nil
}

func init() {
	Registry.Register(&TreeDiff{})
}
