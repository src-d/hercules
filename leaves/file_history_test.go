package leaves

import (
	"bytes"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	items "gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v10/internal/test"
	"gopkg.in/src-d/hercules.v10/internal/test/fixtures"
)

func fixtureFileHistory() *FileHistoryAnalysis {
	fh := FileHistoryAnalysis{}
	fh.Initialize(test.Repository)
	return &fh
}

func TestFileHistoryMeta(t *testing.T) {
	fh := fixtureFileHistory()
	assert.Equal(t, fh.Name(), "FileHistoryAnalysis")
	assert.Equal(t, len(fh.Provides()), 0)
	assert.Equal(t, len(fh.Requires()), 3)
	assert.Equal(t, fh.Requires()[0], items.DependencyTreeChanges)
	assert.Equal(t, fh.Requires()[1], items.DependencyLineStats)
	assert.Equal(t, fh.Requires()[2], identity.DependencyAuthor)
	assert.Len(t, fh.ListConfigurationOptions(), 0)
	assert.Nil(t, fh.Configure(nil))
	logger := core.NewLogger()
	assert.NoError(t, fh.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, fh.l)
}

func TestFileHistoryRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&FileHistoryAnalysis{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "FileHistoryAnalysis")
	leaves := core.Registry.GetLeaves()
	matched := false
	for _, tp := range leaves {
		if tp.Flag() == (&FileHistoryAnalysis{}).Flag() {
			matched = true
			break
		}
	}
	assert.True(t, matched)
}

func TestFileHistoryConsume(t *testing.T) {
	fh, deps := bakeFileHistoryForSerialization(t)
	validate := func() {
		assert.Len(t, fh.files, 3)
		assert.Equal(t, fh.files["cmd/hercules/main.go"].People,
			map[int]items.LineStats{1: ls(0, 207, 0)})
		assert.Equal(t, fh.files[".travis.yml"].People, map[int]items.LineStats{1: ls(12, 0, 0)})
		assert.Equal(t, fh.files["analyser.go"].People, map[int]items.LineStats{1: ls(628, 9, 67)})
		assert.Len(t, fh.files["analyser.go"].Hashes, 2)
		assert.Equal(t, fh.files["analyser.go"].Hashes[0], plumbing.NewHash(
			"ffffffffffffffffffffffffffffffffffffffff"))
		assert.Equal(t, fh.files["analyser.go"].Hashes[1], plumbing.NewHash(
			"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
		assert.Len(t, fh.files[".travis.yml"].Hashes, 1)
		assert.Equal(t, fh.files[".travis.yml"].Hashes[0], plumbing.NewHash(
			"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
		assert.Len(t, fh.files["cmd/hercules/main.go"].Hashes, 2)
		assert.Equal(t, fh.files["cmd/hercules/main.go"].Hashes[0], plumbing.NewHash(
			"0000000000000000000000000000000000000000"))
		assert.Equal(t, fh.files["cmd/hercules/main.go"].Hashes[1], plumbing.NewHash(
			"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	}
	validate()
	res := fh.Finalize().(FileHistoryResult)
	assert.Equal(t, 2, len(res.Files))
	for key, val := range res.Files {
		assert.Equal(t, val, *fh.files[key])
	}
	deps[core.DependencyIsMerge] = true
	cres, err := fh.Consume(deps)
	assert.Nil(t, cres)
	assert.Nil(t, err)
	validate()
	fh.lastCommit = &object.Commit{}
	assert.Panics(t, func() { fh.Finalize() })
}

func TestFileHistoryFork(t *testing.T) {
	fh1 := fixtureFileHistory()
	clones := fh1.Fork(1)
	assert.Len(t, clones, 1)
	fh2 := clones[0].(*FileHistoryAnalysis)
	assert.True(t, fh1 == fh2)
	fh1.Merge([]core.PipelineItem{fh2})
}

func TestFileHistorySerializeText(t *testing.T) {
	fh, _ := bakeFileHistoryForSerialization(t)
	res := fh.Finalize().(FileHistoryResult)
	buffer := &bytes.Buffer{}
	assert.Nil(t, fh.Serialize(res, false, buffer))
	assert.Equal(t, buffer.String(), `  - .travis.yml:
    commits: ["2b1ed978194a94edeabbca6de7ff3b5771d4d665"]
    people: {1:[12,0,0]}
  - cmd/hercules/main.go:
    commits: ["0000000000000000000000000000000000000000","2b1ed978194a94edeabbca6de7ff3b5771d4d665"]
    people: {1:[0,207,0]}
`)
}

func TestFileHistorySerializeBinary(t *testing.T) {
	fh, _ := bakeFileHistoryForSerialization(t)
	res := fh.Finalize().(FileHistoryResult)
	buffer := &bytes.Buffer{}
	assert.Nil(t, fh.Serialize(res, true, buffer))
	msg := pb.FileHistoryResultMessage{}
	assert.Nil(t, proto.Unmarshal(buffer.Bytes(), &msg))
	assert.Len(t, msg.Files, 2)
	assert.Len(t, msg.Files[".travis.yml"].Commits, 1)
	assert.Equal(t, msg.Files[".travis.yml"].Commits[0], "2b1ed978194a94edeabbca6de7ff3b5771d4d665")
	assert.Len(t, msg.Files["cmd/hercules/main.go"].Commits, 2)
	assert.Equal(t, msg.Files["cmd/hercules/main.go"].Commits[0],
		"0000000000000000000000000000000000000000")
	assert.Equal(t, msg.Files["cmd/hercules/main.go"].Commits[1],
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665")
	assert.Equal(t, msg.Files[".travis.yml"].ChangesByDeveloper,
		map[int32]*pb.LineStats{1: {Added: 12, Removed: 0, Changed: 0}})
	assert.Equal(t, msg.Files["cmd/hercules/main.go"].ChangesByDeveloper,
		map[int32]*pb.LineStats{1: {Added: 0, Removed: 207, Changed: 0}})
}

func bakeFileHistoryForSerialization(t *testing.T) (*FileHistoryAnalysis, map[string]interface{}) {
	fh := fixtureFileHistory()
	deps := map[string]interface{}{}
	cache := map[plumbing.Hash]*items.CachedBlob{}
	AddHash(t, cache, "291286b4ac41952cbd1389fda66420ec03c1a9fe")
	AddHash(t, cache, "c29112dbd697ad9b401333b80c18a63951bc18d9")
	AddHash(t, cache, "baa64828831d174f40140e4b3cfa77d1e917a2c1")
	AddHash(t, cache, "dc248ba2b22048cc730c571a748e8ffcf7085ab9")
	deps[items.DependencyBlobCache] = cache
	changes := make(object.Changes, 3)
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"a1eb2ea76eb7f9bfbde9b243861474421000eb96"))
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
		"994eac1cd07235bb9815e547a75c84265dea00f5"))
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name: "analyser.go",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "analyser.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("dc248ba2b22048cc730c571a748e8ffcf7085ab9"),
		},
	}, To: object.ChangeEntry{
		Name: "analyser.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "analyser.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1"),
		},
	}}
	changes[1] = &object.Change{To: object.ChangeEntry{}, From: object.ChangeEntry{
		Name: "cmd/hercules/main.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9"),
		},
	},
	}
	changes[2] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: ".travis.yml",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: ".travis.yml",
			Mode: 0100644,
			Hash: plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe"),
		},
	},
	}
	deps[items.DependencyTreeChanges] = changes
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIsMerge] = false
	deps[identity.DependencyAuthor] = 1
	fd := fixtures.FileDiff()
	result, err := fd.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]

	lineStats, err := (&items.LinesStatsCalculator{}).Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyLineStats] = lineStats[items.DependencyLineStats]

	fh.files["cmd/hercules/main.go"] = &FileHistory{Hashes: []plumbing.Hash{plumbing.NewHash(
		"0000000000000000000000000000000000000000")}}
	fh.files["analyser.go"] = &FileHistory{Hashes: []plumbing.Hash{plumbing.NewHash(
		"ffffffffffffffffffffffffffffffffffffffff")}}
	cres, err := fh.Consume(deps)
	assert.Nil(t, cres)
	assert.Nil(t, err)
	return fh, deps
}
