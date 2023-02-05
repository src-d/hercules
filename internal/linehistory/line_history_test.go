package linehistory

import (
	"github.com/cyraxred/hercules/internal/core"
	items "github.com/cyraxred/hercules/internal/plumbing"
	"github.com/cyraxred/hercules/internal/plumbing/identity"
	"github.com/cyraxred/hercules/internal/test"
	"github.com/cyraxred/hercules/internal/test/fixtures"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/stretchr/testify/assert"
	"testing"
)

func AddHash(t *testing.T, cache map[plumbing.Hash]*items.CachedBlob, hash string) {
	objHash := plumbing.NewHash(hash)
	blob, err := test.Repository.BlobObject(objHash)
	assert.Nil(t, err)
	cb := &items.CachedBlob{Blob: *blob}
	err = cb.Cache()
	assert.Nil(t, err)
	cache[objHash] = cb
}

func TestLinesMeta(t *testing.T) {
	bd := &LineHistoryAnalyser{}
	assert.Equal(t, bd.Name(), "LineHistory")
	assert.Len(t, bd.Provides(), 1)
	assert.Equal(t, bd.Provides()[0], DependencyLineHistory)
	required := [...]string{
		items.DependencyFileDiff, items.DependencyTreeChanges, items.DependencyBlobCache,
		items.DependencyTick, identity.DependencyAuthor}
	assert.Len(t, bd.Requires(), len(required))
	for _, name := range required {
		assert.Contains(t, bd.Requires(), name)
	}
	opts := bd.ListConfigurationOptions()
	matches := 0
	for _, opt := range opts {
		switch opt.Name {
		case ConfigLinesHibernationThreshold,
			ConfigLinesHibernationToDisk, ConfigLinesHibernationDirectory,
			ConfigLinesDebug:
			matches++
		}
	}
	assert.Len(t, opts, matches)
	logger := core.NewLogger()
	assert.NoError(t, bd.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, bd.l)
}

func TestLinesConfigure(t *testing.T) {
	bd := &LineHistoryAnalyser{}
	facts := map[string]interface{}{}
	facts[ConfigLinesDebug] = true
	facts[ConfigLinesHibernationThreshold] = 100
	facts[ConfigLinesHibernationToDisk] = true
	facts[ConfigLinesHibernationDirectory] = "xxx"
	assert.Nil(t, bd.Configure(facts))
	assert.Equal(t, bd.HibernationThreshold, 100)
	assert.True(t, bd.HibernationToDisk)
	assert.Equal(t, bd.HibernationDirectory, "xxx")
	assert.Equal(t, bd.Debug, true)

	assert.Nil(t, bd.Configure(map[string]interface{}{}))
	assert.Equal(t, bd.Debug, true)
}

func TestLinesRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&LineHistoryAnalyser{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "LineHistory")
}

func TestLinesInitialize(t *testing.T) {
	bd := &LineHistoryAnalyser{}
	bd.HibernationThreshold = 10
	assert.Nil(t, bd.Initialize(test.Repository))
	assert.Equal(t, bd.fileAllocator.HibernationThreshold, 10)
}

func TestLinesConsume(t *testing.T) {
	deps := map[string]interface{}{}

	// stage 1
	deps[identity.DependencyAuthor] = 0
	deps[items.DependencyTick] = 0
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
	fd := fixtures.FileDiff()
	result, err := fd.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))
	deps[core.DependencyIsMerge] = false

	expectedChanges := append([]core.LineHistoryChange(nil), core.LineHistoryChange{
		FileId: 1, Delta: 926,
	}, core.LineHistoryChange{
		FileId: 2, Delta: 207,
	}, core.LineHistoryChange{
		FileId: 3, Delta: 12,
	})
	totalLines := int64(0)
	for _, c := range expectedChanges {
		totalLines += int64(c.Delta)
	}

	bd := &LineHistoryAnalyser{}

	assert.Nil(t, bd.Initialize(test.Repository))
	result, err = bd.Consume(deps)
	assert.Nil(t, err)

	assert.Equal(t, core.TickNumber(0), bd.previousTick)

	assert.Len(t, result, 1)
	resultChanges := result[DependencyLineHistory].(core.LineHistoryChanges)
	{
		resolver := resultChanges.Resolver
		assert.Equal(t, "", resolver.NameOf(0))
		assert.Equal(t, "", resolver.NameOf(0))
		assert.Equal(t, "analyser.go", resolver.NameOf(1))
		assert.Equal(t, "cmd/hercules/main.go", resolver.NameOf(2))
		assert.Equal(t, ".travis.yml", resolver.NameOf(3))
		assert.Equal(t, "", resolver.NameOf(4))

		id, name, present := resolver.MergedWith(4)
		assert.Zero(t, id)
		assert.Empty(t, name)
		assert.False(t, present)

		id, name, present = resolver.MergedWith(3)
		assert.Equal(t, FileId(3), id)
		assert.Equal(t, ".travis.yml", name)
		assert.True(t, present)

		assert.Len(t, bd.files, len(expectedChanges))
	}
	assert.Equal(t, resultChanges.Changes, expectedChanges)

	// stage 2
	// 2b1ed978194a94edeabbca6de7ff3b5771d4d665
	deps[identity.DependencyAuthor] = 1
	deps[items.DependencyTick] = 30
	cache = map[plumbing.Hash]*items.CachedBlob{}
	AddHash(t, cache, "291286b4ac41952cbd1389fda66420ec03c1a9fe")
	AddHash(t, cache, "baa64828831d174f40140e4b3cfa77d1e917a2c1")
	AddHash(t, cache, "29c9fafd6a2fae8cd20298c3f60115bc31a4c0f2")
	AddHash(t, cache, "c29112dbd697ad9b401333b80c18a63951bc18d9")
	AddHash(t, cache, "f7d918ec500e2f925ecde79b51cc007bac27de72")
	deps[items.DependencyBlobCache] = cache
	treeFrom, _ = test.Repository.TreeObject(plumbing.NewHash(
		"96c6ece9b2f3c7c51b83516400d278dea5605100"))
	treeTo, _ = test.Repository.TreeObject(plumbing.NewHash(
		"251f2094d7b523d5bcc60e663b6cf38151bf8844"))
	changes = make(object.Changes, 3)
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name: "analyser.go",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "analyser.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1"),
		},
	}, To: object.ChangeEntry{
		Name: "burndown.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "burndown.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("29c9fafd6a2fae8cd20298c3f60115bc31a4c0f2"),
		},
	},
	}
	changes[1] = &object.Change{From: object.ChangeEntry{
		Name: "cmd/hercules/main.go",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9"),
		},
	}, To: object.ChangeEntry{
		Name: "cmd/hercules/main.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("f7d918ec500e2f925ecde79b51cc007bac27de72"),
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
	fd = fixtures.FileDiff()
	result, err = fd.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]
	result, err = bd.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, core.TickNumber(30), bd.previousTick)

	assert.Len(t, result, 1)
	resultChanges = result[DependencyLineHistory].(core.LineHistoryChanges)

	{
		assert.Len(t, bd.files, 2)
		assert.Len(t, bd.fileAbandonedNames, 1)

		resolver := resultChanges.Resolver
		assert.Equal(t, "", resolver.NameOf(0))
		assert.Equal(t, "burndown.go", resolver.NameOf(1))
		assert.Equal(t, "cmd/hercules/main.go", resolver.NameOf(2))
		assert.Equal(t, ".travis.yml", resolver.NameOf(3))
		assert.Equal(t, "", resolver.NameOf(4))

		id, name, present := resolver.MergedWith(3)
		assert.Zero(t, id)
		assert.Equal(t, ".travis.yml", name)
		assert.False(t, present)
		assert.Len(t, bd.fileNames, 2)
	}

	{
		lines := map[FileId]int{}
		deleted := map[FileId]int{}
		for _, c := range expectedChanges {
			lines[c.FileId] += c.Delta
		}

		for _, c := range resultChanges.Changes {
			if c.IsDelete() {
				assert.Equal(t, core.AuthorId(core.AuthorMissing), c.CurrAuthor)
				assert.Equal(t, core.AuthorId(core.AuthorMissing), c.PrevAuthor)
				deleted[c.FileId] += 1
			} else {
				assert.NotEqual(t, core.AuthorId(core.AuthorMissing), c.CurrAuthor)
				assert.NotEqual(t, core.AuthorId(core.AuthorMissing), c.PrevAuthor)
				lines[c.FileId] += c.Delta
			}
			assert.Equal(t, core.TickNumber(30), c.CurrTick)
		}

		assert.Equal(t, 543, bd.files["burndown.go"].Len())
		assert.Equal(t, 543, lines[1])

		assert.Equal(t, 290, bd.files["cmd/hercules/main.go"].Len())
		assert.Equal(t, 290, lines[2])

		assert.Equal(t, 0, lines[3])
		assert.Equal(t, map[FileId]int{3: 1}, deleted)
	}
}

func bakeBurndownForSerialization(t *testing.T, firstAuthor, secondAuthor int) *LineHistoryAnalyser {
	bd := &LineHistoryAnalyser{}
	assert.Nil(t, bd.Initialize(test.Repository))
	deps := map[string]interface{}{}
	// stage 1
	deps[identity.DependencyAuthor] = firstAuthor
	deps[items.DependencyTick] = 0
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
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))
	fd := fixtures.FileDiff()
	result, _ := fd.Consume(deps)
	deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]
	_, _ = bd.Consume(deps)

	// stage 2
	// 2b1ed978194a94edeabbca6de7ff3b5771d4d665
	deps[identity.DependencyAuthor] = secondAuthor
	deps[items.DependencyTick] = 30
	cache = map[plumbing.Hash]*items.CachedBlob{}
	AddHash(t, cache, "291286b4ac41952cbd1389fda66420ec03c1a9fe")
	AddHash(t, cache, "baa64828831d174f40140e4b3cfa77d1e917a2c1")
	AddHash(t, cache, "29c9fafd6a2fae8cd20298c3f60115bc31a4c0f2")
	AddHash(t, cache, "c29112dbd697ad9b401333b80c18a63951bc18d9")
	AddHash(t, cache, "f7d918ec500e2f925ecde79b51cc007bac27de72")
	deps[items.DependencyBlobCache] = cache
	changes = make(object.Changes, 3)
	treeFrom, _ = test.Repository.TreeObject(plumbing.NewHash(
		"96c6ece9b2f3c7c51b83516400d278dea5605100"))
	treeTo, _ = test.Repository.TreeObject(plumbing.NewHash(
		"251f2094d7b523d5bcc60e663b6cf38151bf8844"))
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name: "analyser.go",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "analyser.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1"),
		},
	}, To: object.ChangeEntry{
		Name: "burndown.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "burndown.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("29c9fafd6a2fae8cd20298c3f60115bc31a4c0f2"),
		},
	},
	}
	changes[1] = &object.Change{From: object.ChangeEntry{
		Name: "cmd/hercules/main.go",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9"),
		},
	}, To: object.ChangeEntry{
		Name: "cmd/hercules/main.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("f7d918ec500e2f925ecde79b51cc007bac27de72"),
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
	fd = fixtures.FileDiff()
	result, _ = fd.Consume(deps)
	deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]
	_, _ = bd.Consume(deps)
	return bd
}

func TestLinesHibernateBoot(t *testing.T) {
	bd := bakeBurndownForSerialization(t, 0, 1)
	assert.Equal(t, bd.fileAllocator.Size(), 157)
	assert.Equal(t, bd.fileAllocator.Used(), 155)
	assert.Nil(t, bd.Hibernate())
	assert.PanicsWithValue(t, "LineHistoryAnalyser.Consume() was called on a hibernated instance",
		func() { _, _ = bd.Consume(nil) })
	assert.Equal(t, bd.fileAllocator.Size(), 0)
	assert.Nil(t, bd.Boot())
	assert.Equal(t, bd.fileAllocator.Size(), 157)
	assert.Equal(t, bd.fileAllocator.Used(), 155)
}

func TestLinesHibernateBootSerialize(t *testing.T) {
	bd := bakeBurndownForSerialization(t, 0, 1)
	assert.Equal(t, bd.fileAllocator.Size(), 157)
	assert.Equal(t, bd.fileAllocator.Used(), 155)
	bd.HibernationToDisk = true
	assert.Nil(t, bd.Hibernate())
	assert.NotEmpty(t, bd.hibernatedFileName)
	assert.PanicsWithValue(t, "LineHistoryAnalyser.Consume() was called on a hibernated instance",
		func() { _, _ = bd.Consume(nil) })
	assert.Equal(t, bd.fileAllocator.Size(), 0)
	assert.Nil(t, bd.Boot())
	assert.Equal(t, bd.fileAllocator.Size(), 157)
	assert.Equal(t, bd.fileAllocator.Used(), 155)
	assert.Empty(t, bd.hibernatedFileName)
}
