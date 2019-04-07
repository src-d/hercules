package uast

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/stretchr/testify/assert"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes/nodesproto"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func fixtureFileDiffRefiner() *FileDiffRefiner {
	fd := &FileDiffRefiner{}
	fd.Initialize(test.Repository)
	return fd
}

func TestFileDiffRefinerMeta(t *testing.T) {
	fd := fixtureFileDiffRefiner()
	assert.Equal(t, fd.Name(), "FileDiffRefiner")
	assert.Equal(t, len(fd.Provides()), 1)
	assert.Equal(t, fd.Provides()[0], plumbing.DependencyFileDiff)
	assert.Equal(t, len(fd.Requires()), 2)
	assert.Equal(t, fd.Requires()[0], plumbing.DependencyFileDiff)
	assert.Equal(t, fd.Requires()[1], DependencyUastChanges)
	assert.Len(t, fd.ListConfigurationOptions(), 0)
	fd.Configure(nil)
	features := fd.Features()
	assert.Len(t, features, 1)
	assert.Equal(t, features[0], FeatureUast)
	logger := core.NewLogger()
	assert.NoError(t, fd.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, fd.l)
}

func TestFileDiffRefinerRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&FileDiffRefiner{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "FileDiffRefiner")
	summoned = core.Registry.Summon((&FileDiffRefiner{}).Provides()[0])
	assert.True(t, len(summoned) >= 1)
	matched := false
	for _, tp := range summoned {
		matched = matched || tp.Name() == "FileDiffRefiner"
	}
	assert.True(t, matched)
}

func loadUast(t *testing.T, name string) nodes.Node {
	filename := path.Join("..", "..", "test_data", name)
	reader, err := os.Open(filename)
	if err != nil {
		assert.Failf(t, "cannot load %s: %v", filename, err)
	}
	node, err := nodesproto.ReadTree(reader)
	if err != nil {
		assert.Failf(t, "cannot load %s: %v", filename, err)
	}
	return node
}

func TestFileDiffRefinerConsume(t *testing.T) {
	bytes1, err := ioutil.ReadFile(path.Join("..", "..", "test_data", "1.java"))
	assert.Nil(t, err)
	bytes2, err := ioutil.ReadFile(path.Join("..", "..", "test_data", "2.java"))
	assert.Nil(t, err)
	dmp := diffmatchpatch.New()
	dmp.DiffTimeout = time.Hour
	src, dst, _ := dmp.DiffLinesToRunes(string(bytes1), string(bytes2))
	state := map[string]interface{}{}
	fileDiffs := map[string]plumbing.FileDiffData{}
	const fileName = "test.java"
	fileDiffs[fileName] = plumbing.FileDiffData{
		OldLinesOfCode: len(src),
		NewLinesOfCode: len(dst),
		Diffs:          dmp.DiffMainRunes(src, dst, false),
	}
	state[plumbing.DependencyFileDiff] = fileDiffs
	uastChanges := make([]Change, 1)
	state[DependencyUastChanges] = uastChanges
	uastChanges[0] = Change{
		Change: &object.Change{
			From: object.ChangeEntry{Name: fileName},
			To:   object.ChangeEntry{Name: fileName}},
		Before: loadUast(t, "uast1.pb"), After: loadUast(t, "uast2.pb"),
	}
	fd := fixtureFileDiffRefiner()
	iresult, err := fd.Consume(state)
	assert.Nil(t, err)
	result := iresult[plumbing.DependencyFileDiff].(map[string]plumbing.FileDiffData)
	assert.Len(t, result, 1)

	oldDiff := fileDiffs[fileName]
	newDiff := result[fileName]
	assert.Equal(t, oldDiff.OldLinesOfCode, newDiff.OldLinesOfCode)
	assert.Equal(t, oldDiff.NewLinesOfCode, newDiff.NewLinesOfCode)
	assert.Equal(t, len(oldDiff.Diffs)+1, len(newDiff.Diffs))
	assert.Equal(t, dmp.DiffText2(oldDiff.Diffs), dmp.DiffText2(newDiff.Diffs))
	// Some hardcoded length checks
	assert.Equal(t, utf8.RuneCountInString(newDiff.Diffs[6].Text), 11)
	assert.Equal(t, utf8.RuneCountInString(newDiff.Diffs[7].Text), 41)
	assert.Equal(t, utf8.RuneCountInString(newDiff.Diffs[8].Text), 231)
}

func TestFileDiffRefinerConsumeNoUast(t *testing.T) {
	bytes1, err := ioutil.ReadFile(path.Join("..", "..", "test_data", "1.java"))
	assert.Nil(t, err)
	bytes2, err := ioutil.ReadFile(path.Join("..", "..", "test_data", "2.java"))
	assert.Nil(t, err)
	dmp := diffmatchpatch.New()
	dmp.DiffTimeout = time.Hour
	src, dst, _ := dmp.DiffLinesToRunes(string(bytes1), string(bytes2))
	state := map[string]interface{}{}
	fileDiffs := map[string]plumbing.FileDiffData{}
	const fileName = "test.java"
	fileDiffs[fileName] = plumbing.FileDiffData{
		OldLinesOfCode: len(src),
		NewLinesOfCode: len(dst),
		Diffs:          dmp.DiffMainRunes(src, dst, false),
	}
	state[plumbing.DependencyFileDiff] = fileDiffs
	uastChanges := make([]Change, 1)
	state[DependencyUastChanges] = uastChanges
	uastChanges[0] = Change{
		Change: &object.Change{
			From: object.ChangeEntry{Name: fileName},
			To:   object.ChangeEntry{Name: fileName}},
		Before: loadUast(t, "uast1.pb"), After: nil,
	}
	fd := fixtureFileDiffRefiner()
	iresult, err := fd.Consume(state)
	assert.Nil(t, err)
	result := iresult[plumbing.DependencyFileDiff].(map[string]plumbing.FileDiffData)
	assert.Len(t, result, 1)
	assert.Equal(t, fileDiffs[fileName], result[fileName])
	fileDiffs[fileName] = plumbing.FileDiffData{
		OldLinesOfCode: 100,
		NewLinesOfCode: 100,
		Diffs:          []diffmatchpatch.Diff{{}},
	}
	uastChanges[0] = Change{
		Change: &object.Change{
			From: object.ChangeEntry{Name: fileName},
			To:   object.ChangeEntry{Name: fileName}},
		Before: loadUast(t, "uast1.pb"), After: loadUast(t, "uast2.pb"),
	}
	iresult, err = fd.Consume(state)
	assert.Nil(t, err)
	result = iresult[plumbing.DependencyFileDiff].(map[string]plumbing.FileDiffData)
	assert.Len(t, result, 1)
	assert.Equal(t, fileDiffs[fileName], result[fileName])
}

func TestFileDiffRefinerFork(t *testing.T) {
	fd1 := fixtureFileDiffRefiner()
	clones := fd1.Fork(1)
	assert.Len(t, clones, 1)
	fd2 := clones[0].(*FileDiffRefiner)
	assert.True(t, fd1 == fd2)
	fd1.Merge([]core.PipelineItem{fd2})
}
