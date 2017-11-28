package hercules

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
)

func fixtureTreeDiff() *TreeDiff {
	td := TreeDiff{}
	td.Configure(nil)
	td.Initialize(testRepository)
	return &td
}

func TestTreeDiffMeta(t *testing.T) {
	td := fixtureTreeDiff()
	assert.Equal(t, td.Name(), "TreeDiff")
	assert.Equal(t, len(td.Requires()), 0)
	assert.Equal(t, len(td.Provides()), 1)
	assert.Equal(t, td.Provides()[0], "changes")
	opts := td.ListConfigurationOptions()
	assert.Len(t, opts, 0)
}

func TestTreeDiffRegistration(t *testing.T) {
	tp, exists := Registry.registered[(&TreeDiff{}).Name()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "TreeDiff")
	tps, exists := Registry.provided[(&TreeDiff{}).Provides()[0]]
	assert.True(t, exists)
	assert.True(t, len(tps) >= 1)
	matched := false
	for _, tp := range tps {
		matched = matched || tp.Elem().Name() == "TreeDiff"
	}
	assert.True(t, matched)
}

func TestTreeDiffConsume(t *testing.T) {
	td := fixtureTreeDiff()
	commit, _ := testRepository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	deps := map[string]interface{}{}
	deps["commit"] = commit
	prevCommit, _ := testRepository.CommitObject(plumbing.NewHash(
		"fbe766ffdc3f87f6affddc051c6f8b419beea6a2"))
	td.previousTree, _ = prevCommit.Tree()
	res, err := td.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	changes := res["changes"].(object.Changes)
	assert.Equal(t, len(changes), 12)
	baseline := map[string]merkletrie.Action{
		"analyser.go":               merkletrie.Delete,
		"cmd/hercules/main.go":      merkletrie.Modify,
		"blob_cache.go":             merkletrie.Insert,
		"burndown.go":               merkletrie.Insert,
		"day.go":                    merkletrie.Insert,
		"dummies.go":                merkletrie.Insert,
		"identity.go":               merkletrie.Insert,
		"pipeline.go":               merkletrie.Insert,
		"renames.go":                merkletrie.Insert,
		"toposort/toposort.go":      merkletrie.Insert,
		"toposort/toposort_test.go": merkletrie.Insert,
		"tree_diff.go":              merkletrie.Insert,
	}
	for _, change := range changes {
		action, err := change.Action()
		assert.Nil(t, err)
		if change.From.Name != "" {
			assert.Contains(t, baseline, change.From.Name)
			assert.Equal(t, baseline[change.From.Name], action)
		} else {
			assert.Contains(t, baseline, change.To.Name)
			assert.Equal(t, baseline[change.To.Name], action)
		}
	}
}

func TestTreeDiffConsumeFirst(t *testing.T) {
	td := fixtureTreeDiff()
	commit, _ := testRepository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	deps := map[string]interface{}{}
	deps["commit"] = commit
	res, err := td.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	changes := res["changes"].(object.Changes)
	assert.Equal(t, len(changes), 21)
	for _, change := range changes {
		action, err := change.Action()
		assert.Nil(t, err)
		assert.Equal(t, action, merkletrie.Insert)
	}
}
