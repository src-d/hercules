package plumbing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v6/internal/core"
	"gopkg.in/src-d/hercules.v6/internal/test"
)

func fixtureTreeDiff() *TreeDiff {
	td := TreeDiff{}
	td.Configure(nil)
	td.Initialize(test.Repository)
	return &td
}

func TestTreeDiffMeta(t *testing.T) {
	td := fixtureTreeDiff()
	assert.Equal(t, td.Name(), "TreeDiff")
	assert.Equal(t, len(td.Requires()), 0)
	assert.Equal(t, len(td.Provides()), 1)
	assert.Equal(t, td.Provides()[0], DependencyTreeChanges)
	opts := td.ListConfigurationOptions()
	assert.Len(t, opts, 4)
}

func TestTreeDiffRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&TreeDiff{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "TreeDiff")
	summoned = core.Registry.Summon((&TreeDiff{}).Provides()[0])
	assert.True(t, len(summoned) >= 1)
	matched := false
	for _, tp := range summoned {
		matched = matched || tp.Name() == "TreeDiff"
	}
	assert.True(t, matched)
}

func TestTreeDiffConsume(t *testing.T) {
	td := fixtureTreeDiff()
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	prevCommit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"fbe766ffdc3f87f6affddc051c6f8b419beea6a2"))
	td.previousTree, _ = prevCommit.Tree()
	res, err := td.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	changes := res[DependencyTreeChanges].(object.Changes)
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
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	res, err := td.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	changes := res[DependencyTreeChanges].(object.Changes)
	assert.Equal(t, len(changes), 21)
	for _, change := range changes {
		action, err := change.Action()
		assert.Nil(t, err)
		assert.Equal(t, action, merkletrie.Insert)
	}
}

func TestTreeDiffBadCommit(t *testing.T) {
	td := fixtureTreeDiff()
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	commit.TreeHash = plumbing.NewHash("0000000000000000000000000000000000000000")
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	res, err := td.Consume(deps)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func TestTreeDiffConsumeSkip(t *testing.T) {
	// consume without skiping
	td := fixtureTreeDiff()
	assert.Contains(t, td.Languages, allLanguages)
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"aefdedf7cafa6ee110bae9a3910bf5088fdeb5a9"))
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	prevCommit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"1e076dc56989bc6aa1ef5f55901696e9e01423d4"))
	td.previousTree, _ = prevCommit.Tree()
	res, err := td.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	changes := res[DependencyTreeChanges].(object.Changes)
	assert.Equal(t, 37, len(changes))

	// consume with skipping
	td = fixtureTreeDiff()
	td.previousTree, _ = prevCommit.Tree()
	td.Configure(map[string]interface{}{
		ConfigTreeDiffEnableBlacklist:     true,
		ConfigTreeDiffBlacklistedPrefixes: []string{"vendor/"},
	})
	res, err = td.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	changes = res[DependencyTreeChanges].(object.Changes)
	assert.Equal(t, 31, len(changes))
}

func TestTreeDiffConsumeOnlyFilesThatMatchFilter(t *testing.T) {
	// consume without skiping
	td := fixtureTreeDiff()
	assert.Contains(t, td.Languages, allLanguages)
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"aefdedf7cafa6ee110bae9a3910bf5088fdeb5a9"))
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	prevCommit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"1e076dc56989bc6aa1ef5f55901696e9e01423d4"))
	td.previousTree, _ = prevCommit.Tree()
	res, err := td.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	changes := res[DependencyTreeChanges].(object.Changes)
	assert.Equal(t, 37, len(changes))

	// consume with skipping
	td = fixtureTreeDiff()
	td.previousTree, _ = prevCommit.Tree()
	td.Configure(map[string]interface{}{
		ConfigTreeDiffFilterRegexp: ".*go",
	})
	res, err = td.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	changes = res[DependencyTreeChanges].(object.Changes)
	assert.Equal(t, 27, len(changes))
}

func TestTreeDiffConsumeLanguageFilterFirst(t *testing.T) {
	td := fixtureTreeDiff()
	td.Configure(map[string]interface{}{ConfigTreeDiffLanguages: "Go"})
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"fbe766ffdc3f87f6affddc051c6f8b419beea6a2"))
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	res, err := td.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	changes := res[DependencyTreeChanges].(object.Changes)
	assert.Equal(t, len(changes), 6)
	assert.Equal(t, changes[0].To.Name, "analyser.go")
	assert.Equal(t, changes[1].To.Name, "cmd/hercules/main.go")
	assert.Equal(t, changes[2].To.Name, "doc.go")
	assert.Equal(t, changes[3].To.Name, "file.go")
	assert.Equal(t, changes[4].To.Name, "file_test.go")
	assert.Equal(t, changes[5].To.Name, "rbtree.go")
}

func TestTreeDiffConsumeLanguageFilter(t *testing.T) {
	td := fixtureTreeDiff()
	td.Configure(map[string]interface{}{ConfigTreeDiffLanguages: "Python"})
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"e89c1d10fb31e32668ad905eb59dc44d7a4a021e"))
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	res, err := td.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"fbe766ffdc3f87f6affddc051c6f8b419beea6a2"))
	deps[core.DependencyCommit] = commit
	res, err = td.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(res), 1)
	changes := res[DependencyTreeChanges].(object.Changes)
	assert.Equal(t, len(changes), 1)
	assert.Equal(t, changes[0].To.Name, "labours.py")
}

func TestTreeDiffFork(t *testing.T) {
	td1 := fixtureTreeDiff()
	td1.SkipDirs = append(td1.SkipDirs, "skip")
	clones := td1.Fork(1)
	assert.Len(t, clones, 1)
	td2 := clones[0].(*TreeDiff)
	assert.False(t, td1 == td2)
	assert.Equal(t, td1.SkipDirs, td2.SkipDirs)
	assert.Equal(t, td1.previousTree, td2.previousTree)
	td1.Merge([]core.PipelineItem{td2})
}
