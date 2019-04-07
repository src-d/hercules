package plumbing_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
	items "gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v10/internal/test"
	"gopkg.in/src-d/hercules.v10/internal/test/fixtures"
)

func TestLinesStatsMeta(t *testing.T) {
	ra := &items.LinesStatsCalculator{}
	assert.Equal(t, ra.Name(), "LinesStats")
	assert.Equal(t, len(ra.Provides()), 1)
	assert.Equal(t, ra.Provides()[0], items.DependencyLineStats)
	assert.Equal(t, len(ra.Requires()), 3)
	assert.Equal(t, ra.Requires()[0], items.DependencyTreeChanges)
	assert.Equal(t, ra.Requires()[1], items.DependencyBlobCache)
	assert.Equal(t, ra.Requires()[2], items.DependencyFileDiff)
	assert.Nil(t, ra.ListConfigurationOptions())
	assert.NoError(t, ra.Configure(map[string]interface{}{
		core.ConfigLogger: core.NewLogger(),
	}))
	for _, f := range ra.Fork(10) {
		assert.Equal(t, f, ra)
	}
}

func TestLinesStatsRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&items.LinesStatsCalculator{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "LinesStats")
	summoned = core.Registry.Summon((&items.LinesStatsCalculator{}).Provides()[0])
	assert.True(t, len(summoned) >= 1)
	matched := false
	for _, tp := range summoned {
		matched = matched || tp.Name() == "LinesStats"
	}
	assert.True(t, matched)
}

func TestLinesStatsConsume(t *testing.T) {
	deps := map[string]interface{}{}

	// stage 1
	deps[identity.DependencyAuthor] = 0
	cache := map[plumbing.Hash]*items.CachedBlob{}
	items.AddHash(t, cache, "291286b4ac41952cbd1389fda66420ec03c1a9fe")
	items.AddHash(t, cache, "c29112dbd697ad9b401333b80c18a63951bc18d9")
	items.AddHash(t, cache, "baa64828831d174f40140e4b3cfa77d1e917a2c1")
	items.AddHash(t, cache, "dc248ba2b22048cc730c571a748e8ffcf7085ab9")
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
		Name: "analyser2.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "analyser2.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1"),
		},
	}}
	changes[1] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: "cmd/hercules/main.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9"),
		},
	},
	}
	changes[2] = &object.Change{From: object.ChangeEntry{
		Name: ".travis.yml",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: ".travis.yml",
			Mode: 0100644,
			Hash: plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe"),
		},
	}, To: object.ChangeEntry{},
	}
	deps[items.DependencyTreeChanges] = changes
	fd := fixtures.FileDiff()
	result, err := fd.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))
	deps[core.DependencyIsMerge] = false
	lsc := &items.LinesStatsCalculator{}
	result, err = lsc.Consume(deps)
	assert.Nil(t, err)
	stats := result[items.DependencyLineStats].(map[object.ChangeEntry]items.LineStats)
	assert.Len(t, stats, 3)
	nameMap := map[string]items.LineStats{}
	for ch, val := range stats {
		nameMap[ch.Name] = val
	}
	assert.Equal(t, nameMap["analyser2.go"], items.LineStats{
		Added:   628,
		Removed: 9,
		Changed: 67,
	})
	assert.Equal(t, nameMap[".travis.yml"], items.LineStats{
		Added:   0,
		Removed: 12,
		Changed: 0,
	})
	assert.Equal(t, nameMap["cmd/hercules/main.go"], items.LineStats{
		Added:   207,
		Removed: 0,
		Changed: 0,
	})
}
