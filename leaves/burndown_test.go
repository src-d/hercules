package leaves

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"gopkg.in/src-d/hercules.v10/internal/burndown"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/test/fixtures"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	items "gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func AddHash(t *testing.T, cache map[plumbing.Hash]*items.CachedBlob, hash string) {
	objhash := plumbing.NewHash(hash)
	blob, err := test.Repository.BlobObject(objhash)
	assert.Nil(t, err)
	cb := &items.CachedBlob{Blob: *blob}
	err = cb.Cache()
	assert.Nil(t, err)
	cache[objhash] = cb
}

func TestBurndownMeta(t *testing.T) {
	bd := BurndownAnalysis{}
	assert.Equal(t, bd.Name(), "Burndown")
	assert.Len(t, bd.Provides(), 0)
	required := [...]string{
		items.DependencyFileDiff, items.DependencyTreeChanges, items.DependencyBlobCache,
		items.DependencyTick, identity.DependencyAuthor}
	for _, name := range required {
		assert.Contains(t, bd.Requires(), name)
	}
	opts := bd.ListConfigurationOptions()
	matches := 0
	for _, opt := range opts {
		switch opt.Name {
		case ConfigBurndownGranularity, ConfigBurndownSampling, ConfigBurndownTrackFiles,
			ConfigBurndownTrackPeople, ConfigBurndownHibernationThreshold,
			ConfigBurndownHibernationToDisk, ConfigBurndownHibernationDirectory,
			ConfigBurndownDebug:
			matches++
		}
	}
	assert.Len(t, opts, matches)
	assert.Equal(t, bd.Flag(), "burndown")
	logger := core.NewLogger()
	assert.NoError(t, bd.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, bd.l)
}

func TestBurndownConfigure(t *testing.T) {
	bd := BurndownAnalysis{}
	facts := map[string]interface{}{}
	facts[ConfigBurndownGranularity] = 100
	facts[ConfigBurndownSampling] = 200
	facts[ConfigBurndownTrackFiles] = true
	facts[ConfigBurndownTrackPeople] = true
	facts[ConfigBurndownDebug] = true
	facts[ConfigBurndownHibernationThreshold] = 100
	facts[ConfigBurndownHibernationToDisk] = true
	facts[ConfigBurndownHibernationDirectory] = "xxx"
	facts[items.FactTickSize] = 24 * time.Hour
	facts[identity.FactIdentityDetectorPeopleCount] = 5
	facts[identity.FactIdentityDetectorReversedPeopleDict] = bd.Requires()
	assert.Nil(t, bd.Configure(facts))
	assert.Equal(t, bd.Granularity, 100)
	assert.Equal(t, bd.Sampling, 200)
	assert.Equal(t, bd.TrackFiles, true)
	assert.Equal(t, bd.PeopleNumber, 5)
	assert.Equal(t, bd.HibernationThreshold, 100)
	assert.True(t, bd.HibernationToDisk)
	assert.Equal(t, bd.HibernationDirectory, "xxx")
	assert.Equal(t, bd.Debug, true)
	assert.Equal(t, bd.TickSize, 24*time.Hour)
	assert.Equal(t, bd.reversedPeopleDict, bd.Requires())
	facts[ConfigBurndownTrackPeople] = false
	facts[identity.FactIdentityDetectorPeopleCount] = 50
	assert.Nil(t, bd.Configure(facts))
	assert.Equal(t, bd.PeopleNumber, 0)
	facts = map[string]interface{}{}
	assert.Nil(t, bd.Configure(facts))
	assert.Equal(t, bd.Granularity, 100)
	assert.Equal(t, bd.Sampling, 200)
	assert.Equal(t, bd.TrackFiles, true)
	assert.Equal(t, bd.PeopleNumber, 0)
	assert.Equal(t, bd.Debug, true)
	assert.Equal(t, bd.reversedPeopleDict, bd.Requires())
}

func TestBurndownRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&BurndownAnalysis{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "Burndown")
	leaves := core.Registry.GetLeaves()
	matched := false
	for _, tp := range leaves {
		if tp.Flag() == (&BurndownAnalysis{}).Flag() {
			matched = true
			break
		}
	}
	assert.True(t, matched)
}

func TestBurndownInitialize(t *testing.T) {
	bd := BurndownAnalysis{}
	bd.Sampling = -10
	bd.Granularity = DefaultBurndownGranularity
	bd.HibernationThreshold = 10
	assert.Nil(t, bd.Initialize(test.Repository))
	assert.Equal(t, bd.Sampling, DefaultBurndownGranularity)
	assert.Equal(t, bd.Granularity, DefaultBurndownGranularity)
	assert.Equal(t, bd.fileAllocator.HibernationThreshold, 10)
	bd.Sampling = 0
	bd.Granularity = DefaultBurndownGranularity - 1
	assert.Nil(t, bd.Initialize(test.Repository))
	assert.Equal(t, bd.Sampling, DefaultBurndownGranularity-1)
	assert.Equal(t, bd.Granularity, DefaultBurndownGranularity-1)
	bd.Sampling = DefaultBurndownGranularity - 1
	bd.Granularity = -10
	assert.Nil(t, bd.Initialize(test.Repository))
	assert.Equal(t, bd.Sampling, DefaultBurndownGranularity-1)
	assert.Equal(t, bd.Granularity, DefaultBurndownGranularity)
}

func TestBurndownConsumeFinalize(t *testing.T) {
	bd := BurndownAnalysis{
		Granularity:  30,
		Sampling:     30,
		PeopleNumber: 2,
		TrackFiles:   true,
	}
	assert.Nil(t, bd.Initialize(test.Repository))
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
	result, err = bd.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Equal(t, bd.previousTick, 0)
	assert.Len(t, bd.files, 3)
	assert.Equal(t, bd.files["cmd/hercules/main.go"].Len(), 207)
	assert.Equal(t, bd.files["analyser.go"].Len(), 926)
	assert.Equal(t, bd.files[".travis.yml"].Len(), 12)
	assert.Len(t, bd.peopleHistories, 2)
	assert.Equal(t, bd.peopleHistories[0][0][0], int64(12+207+926))
	assert.Len(t, bd.globalHistory, 1)
	assert.Equal(t, bd.globalHistory[0][0], int64(12+207+926))
	assert.Len(t, bd.fileHistories, 3)
	bd2 := BurndownAnalysis{
		Granularity: 30,
		Sampling:    0,
	}
	assert.Nil(t, bd2.Initialize(test.Repository))
	_, err = bd2.Consume(deps)
	assert.Nil(t, err)
	assert.Len(t, bd2.peopleHistories, 0)
	assert.Len(t, bd2.fileHistories, 0)

	// check merge hashes
	burndown3 := BurndownAnalysis{}
	assert.Nil(t, burndown3.Initialize(test.Repository))
	deps[identity.DependencyAuthor] = 1
	deps[core.DependencyIsMerge] = true
	_, err = burndown3.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, 1, burndown3.mergedAuthor)
	assert.True(t, burndown3.mergedFiles["cmd/hercules/main.go"])
	assert.True(t, burndown3.mergedFiles["analyser.go"], plumbing.ZeroHash)
	assert.True(t, burndown3.mergedFiles[".travis.yml"], plumbing.ZeroHash)

	// stage 2
	// 2b1ed978194a94edeabbca6de7ff3b5771d4d665
	deps[core.DependencyIsMerge] = false
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
	result, err = fd.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]
	result, err = bd.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Equal(t, bd.previousTick, 30)
	assert.Len(t, bd.files, 2)
	assert.Equal(t, bd.files["cmd/hercules/main.go"].Len(), 290)
	assert.Equal(t, bd.files["burndown.go"].Len(), 543)
	assert.Len(t, bd.peopleHistories, 2)
	assert.Len(t, bd.globalHistory, 2)
	assert.Equal(t, bd.globalHistory[0][0], int64(1145))
	assert.Equal(t, bd.globalHistory[30][0], int64(-681))
	assert.Equal(t, bd.globalHistory[30][30], int64(369))
	assert.Len(t, bd.fileHistories, 2)
	out := bd.Finalize().(BurndownResult)
	/*
			GlobalHistory   [][]int64
			FileHistories   map[string][][]int64
		    FileOwnership   map[string]map[int]int
			PeopleHistories [][][]int64
			PeopleMatrix    [][]int64
	*/
	assert.Len(t, out.GlobalHistory, 2)
	for i := 0; i < 2; i++ {
		assert.Len(t, out.GlobalHistory[i], 2)
	}
	assert.Len(t, out.GlobalHistory, 2)
	assert.Equal(t, out.GlobalHistory[0][0], int64(1145))
	assert.Equal(t, out.GlobalHistory[0][1], int64(0))
	assert.Equal(t, out.GlobalHistory[1][0], int64(464))
	assert.Equal(t, out.GlobalHistory[1][1], int64(369))
	assert.Len(t, out.FileHistories, 2)
	assert.Len(t, out.FileHistories["cmd/hercules/main.go"], 2)
	assert.Len(t, out.FileHistories["burndown.go"], 2)
	assert.Len(t, out.FileHistories["cmd/hercules/main.go"][0], 2)
	assert.Len(t, out.FileHistories["burndown.go"][0], 2)
	assert.Len(t, out.FileOwnership, 2)
	assert.Equal(t, out.FileOwnership["cmd/hercules/main.go"], map[int]int{0: 171, 1: 119})
	assert.Equal(t, out.FileOwnership["burndown.go"], map[int]int{0: 293, 1: 250})
	assert.Len(t, out.PeopleMatrix, 2)
	assert.Len(t, out.PeopleMatrix[0], 4)
	assert.Len(t, out.PeopleMatrix[1], 4)
	assert.Equal(t, out.PeopleMatrix[0][0], int64(1145))
	assert.Equal(t, out.PeopleMatrix[0][1], int64(0))
	assert.Equal(t, out.PeopleMatrix[0][2], int64(0))
	assert.Equal(t, out.PeopleMatrix[0][3], int64(-681))
	assert.Equal(t, out.PeopleMatrix[1][0], int64(369))
	assert.Equal(t, out.PeopleMatrix[1][1], int64(0))
	assert.Equal(t, out.PeopleMatrix[1][2], int64(0))
	assert.Equal(t, out.PeopleMatrix[1][3], int64(0))
	assert.Len(t, out.PeopleHistories, 2)
	for i := 0; i < 2; i++ {
		assert.Len(t, out.PeopleHistories[i], 2)
		assert.Len(t, out.PeopleHistories[i][0], 2)
		assert.Len(t, out.PeopleHistories[i][1], 2)
	}
}

func TestBurndownConsumeMergeAuthorMissing(t *testing.T) {
	deps := map[string]interface{}{}
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
	filediff, err := fd.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyFileDiff] = filediff[items.DependencyFileDiff]
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))

	// check that we survive merge + missing author
	bd := BurndownAnalysis{PeopleNumber: 1}
	assert.Nil(t, bd.Initialize(test.Repository))
	deps[identity.DependencyAuthor] = 0
	deps[core.DependencyIsMerge] = false
	_, err = bd.Consume(deps)
	assert.Nil(t, err)

	AddHash(t, cache, "4cdb0d969cf976f76634d1f348da3a175c9b4501")
	treeFrom, _ = test.Repository.TreeObject(plumbing.NewHash(
		"994eac1cd07235bb9815e547a75c84265dea00f5"))
	treeTo, _ = test.Repository.TreeObject(plumbing.NewHash(
		"89f33a2320f6cd0bd3d16351cfc10bea7e3dce1a"))
	changes = object.Changes{
		&object.Change{
			From: object.ChangeEntry{
				Name: ".travis.yml",
				Tree: treeFrom,
				TreeEntry: object.TreeEntry{
					Name: ".travis.yml",
					Mode: 0100644,
					Hash: plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe"),
				},
			}, To: object.ChangeEntry{
				Name: ".travis.yml",
				Tree: treeTo,
				TreeEntry: object.TreeEntry{
					Name: ".travis.yml",
					Mode: 0100644,
					Hash: plumbing.NewHash("4cdb0d969cf976f76634d1f348da3a175c9b4501"),
				},
			},
		},
	}
	deps[items.DependencyTreeChanges] = changes
	filediff, err = fd.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyFileDiff] = filediff[items.DependencyFileDiff]
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(plumbing.NewHash(
		"7ef5c47aa79a1b229e3227d9ffe2401dbcbeb22f"))
	deps[identity.DependencyAuthor] = identity.AuthorMissing
	deps[core.DependencyIsMerge] = true
	_, err = bd.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, identity.AuthorMissing, bd.mergedAuthor)
}

func bakeBurndownForSerialization(t *testing.T, firstAuthor, secondAuthor int) (
	BurndownResult, *BurndownAnalysis) {
	bd := BurndownAnalysis{
		Granularity:  30,
		Sampling:     30,
		PeopleNumber: 2,
		TrackFiles:   true,
		TickSize:     24 * time.Hour,
	}
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
	deps[core.DependencyIsMerge] = false
	fd := fixtures.FileDiff()
	result, _ := fd.Consume(deps)
	deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]
	bd.Consume(deps)

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
	people := [...]string{"one@srcd", "two@srcd"}
	bd.reversedPeopleDict = people[:]
	bd.Consume(deps)
	out := bd.Finalize().(BurndownResult)
	return out, &bd
}

func TestBurndownSerialize(t *testing.T) {
	out, _ := bakeBurndownForSerialization(t, 0, 1)
	bd := &BurndownAnalysis{}

	buffer := &bytes.Buffer{}
	assert.Nil(t, bd.Serialize(out, false, buffer))
	assert.Equal(t, buffer.String(), `  granularity: 30
  sampling: 30
  tick_size: 86400
  "project": |-
    1145    0
     464  369
  files:
    "burndown.go": |-
      926   0
      293 250
    "cmd/hercules/main.go": |-
      207   0
      171 119
  files_ownership:
    - 0: 293
      1: 250
    - 0: 171
      1: 119
  people_sequence:
    - "one@srcd"
    - "two@srcd"
  people:
    "one@srcd": |-
      1145    0
       464    0
    "two@srcd": |-
      0     0
        0 369
  people_interaction: |-
    1145    0    0 -681
     369    0    0    0
`)
	buffer = &bytes.Buffer{}
	assert.NoError(t, bd.Serialize(out, true, buffer))
	msg := pb.BurndownAnalysisResults{}
	assert.NoError(t, proto.Unmarshal(buffer.Bytes(), &msg))
	assert.Equal(t, msg.TickSize, int64(24*time.Hour))
	assert.Equal(t, msg.Granularity, int32(30))
	assert.Equal(t, msg.Sampling, int32(30))
	assert.Equal(t, msg.Project.Name, "project")
	assert.Equal(t, msg.Project.NumberOfRows, int32(2))
	assert.Equal(t, msg.Project.NumberOfColumns, int32(2))
	assert.Len(t, msg.Project.Rows, 2)
	assert.Len(t, msg.Project.Rows[0].Columns, 1)
	assert.Equal(t, msg.Project.Rows[0].Columns[0], uint32(1145))
	assert.Len(t, msg.Project.Rows[1].Columns, 2)
	assert.Equal(t, msg.Project.Rows[1].Columns[0], uint32(464))
	assert.Equal(t, msg.Project.Rows[1].Columns[1], uint32(369))
	assert.Len(t, msg.Files, 2)
	assert.Equal(t, msg.Files[0].Name, "burndown.go")
	assert.Equal(t, msg.Files[1].Name, "cmd/hercules/main.go")
	assert.Len(t, msg.Files[0].Rows, 2)
	assert.Len(t, msg.Files[0].Rows[0].Columns, 1)
	assert.Equal(t, msg.Files[0].Rows[0].Columns[0], uint32(926))
	assert.Len(t, msg.Files[0].Rows[1].Columns, 2)
	assert.Equal(t, msg.Files[0].Rows[1].Columns[0], uint32(293))
	assert.Equal(t, msg.Files[0].Rows[1].Columns[1], uint32(250))
	assert.Len(t, msg.FilesOwnership, 2)
	assert.Equal(t, msg.FilesOwnership[0].Value, map[int32]int32{0: 293, 1: 250})
	assert.Equal(t, msg.FilesOwnership[1].Value, map[int32]int32{0: 171, 1: 119})
	assert.Len(t, msg.People, 2)
	assert.Equal(t, msg.People[0].Name, "one@srcd")
	assert.Equal(t, msg.People[1].Name, "two@srcd")
	assert.Len(t, msg.People[0].Rows, 2)
	assert.Len(t, msg.People[0].Rows[0].Columns, 1)
	assert.Len(t, msg.People[0].Rows[1].Columns, 1)
	assert.Equal(t, msg.People[0].Rows[0].Columns[0], uint32(1145))
	assert.Equal(t, msg.People[0].Rows[1].Columns[0], uint32(464))
	assert.Len(t, msg.People[1].Rows, 2)
	assert.Len(t, msg.People[1].Rows[0].Columns, 0)
	assert.Len(t, msg.People[1].Rows[1].Columns, 2)
	assert.Equal(t, msg.People[1].Rows[1].Columns[0], uint32(0))
	assert.Equal(t, msg.People[1].Rows[1].Columns[1], uint32(369))
	assert.Equal(t, msg.PeopleInteraction.NumberOfRows, int32(2))
	assert.Equal(t, msg.PeopleInteraction.NumberOfColumns, int32(4))
	data := [...]int64{1145, -681, 369}
	assert.Equal(t, msg.PeopleInteraction.Data, data[:])
	indices := [...]int32{0, 3, 0}
	assert.Equal(t, msg.PeopleInteraction.Indices, indices[:])
	indptr := [...]int64{0, 2, 3}
	assert.Equal(t, msg.PeopleInteraction.Indptr, indptr[:])
}

func TestBurndownSerializeAuthorMissing(t *testing.T) {
	out, _ := bakeBurndownForSerialization(t, 0, identity.AuthorMissing)
	bd := &BurndownAnalysis{}

	buffer := &bytes.Buffer{}
	assert.Nil(t, bd.Serialize(out, false, buffer))
	assert.Equal(t, buffer.String(), `  granularity: 30
  sampling: 30
  tick_size: 86400
  "project": |-
    1145    0
     464  369
  files:
    "burndown.go": |-
      926   0
      293 250
    "cmd/hercules/main.go": |-
      207   0
      171 119
  files_ownership:
    - 0: 293
      -1: 250
    - 0: 171
      -1: 119
  people_sequence:
    - "one@srcd"
    - "two@srcd"
  people:
    "one@srcd": |-
      1145    0
       464    0
    "two@srcd": |-
      0 0
      0 0
  people_interaction: |-
    1145 -681    0    0
       0    0    0    0
`)
	buffer = &bytes.Buffer{}
	assert.NoError(t, bd.Serialize(out, true, buffer))
	msg := pb.BurndownAnalysisResults{}
	assert.NoError(t, proto.Unmarshal(buffer.Bytes(), &msg))
	assert.Equal(t, msg.Granularity, int32(30))
	assert.Equal(t, msg.Sampling, int32(30))
	assert.Equal(t, msg.Project.Name, "project")
	assert.Equal(t, msg.Project.NumberOfRows, int32(2))
	assert.Equal(t, msg.Project.NumberOfColumns, int32(2))
	assert.Len(t, msg.Project.Rows, 2)
	assert.Len(t, msg.Project.Rows[0].Columns, 1)
	assert.Equal(t, msg.Project.Rows[0].Columns[0], uint32(1145))
	assert.Len(t, msg.Project.Rows[1].Columns, 2)
	assert.Equal(t, msg.Project.Rows[1].Columns[0], uint32(464))
	assert.Equal(t, msg.Project.Rows[1].Columns[1], uint32(369))
	assert.Len(t, msg.Files, 2)
	assert.Equal(t, msg.Files[0].Name, "burndown.go")
	assert.Equal(t, msg.Files[1].Name, "cmd/hercules/main.go")
	assert.Len(t, msg.Files[0].Rows, 2)
	assert.Len(t, msg.Files[0].Rows[0].Columns, 1)
	assert.Equal(t, msg.Files[0].Rows[0].Columns[0], uint32(926))
	assert.Len(t, msg.Files[0].Rows[1].Columns, 2)
	assert.Equal(t, msg.Files[0].Rows[1].Columns[0], uint32(293))
	assert.Equal(t, msg.Files[0].Rows[1].Columns[1], uint32(250))
	assert.Len(t, msg.FilesOwnership, 2)
	assert.Equal(t, msg.FilesOwnership[0].Value, map[int32]int32{0: 293, -1: 250})
	assert.Equal(t, msg.FilesOwnership[1].Value, map[int32]int32{0: 171, -1: 119})
	assert.Len(t, msg.People, 2)
	assert.Equal(t, msg.People[0].Name, "one@srcd")
	assert.Equal(t, msg.People[1].Name, "two@srcd")
	assert.Len(t, msg.People[0].Rows, 2)
	assert.Len(t, msg.People[0].Rows[0].Columns, 1)
	assert.Len(t, msg.People[0].Rows[1].Columns, 1)
	assert.Equal(t, msg.People[0].Rows[0].Columns[0], uint32(1145))
	assert.Equal(t, msg.People[0].Rows[1].Columns[0], uint32(464))
	assert.Len(t, msg.People[1].Rows, 2)
	assert.Len(t, msg.People[1].Rows[0].Columns, 0)
	assert.Len(t, msg.People[1].Rows[1].Columns, 0)
	assert.Equal(t, msg.PeopleInteraction.NumberOfRows, int32(2))
	assert.Equal(t, msg.PeopleInteraction.NumberOfColumns, int32(4))
	data := [...]int64{1145, -681}
	assert.Equal(t, msg.PeopleInteraction.Data, data[:])
	indices := [...]int32{0, 1}
	assert.Equal(t, msg.PeopleInteraction.Indices, indices[:])
	indptr := [...]int64{0, 2, 2}
	assert.Equal(t, msg.PeopleInteraction.Indptr, indptr[:])
}

type panickingCloser struct {
}

func (c panickingCloser) Close() error {
	return io.EOF
}

func TestCheckClose(t *testing.T) {
	closer := panickingCloser{}
	assert.Panics(t, func() { checkClose(closer) })
}

func TestBurndownAddMatrix(t *testing.T) {
	size := 5*3 + 1
	daily := make([][]float32, size)
	for i := range daily {
		daily[i] = make([]float32, size)
	}
	added := make([][]int64, 5)
	for i := range added {
		added[i] = make([]int64, 3)
		switch i {
		case 0:
			added[i][0] = 10
		case 1:
			added[i][0] = 18
			added[i][1] = 2
		case 2:
			added[i][0] = 12
			added[i][1] = 14
		case 3:
			added[i][0] = 10
			added[i][1] = 12
			added[i][2] = 6
		case 4:
			added[i][0] = 8
			added[i][1] = 9
			added[i][2] = 13
		}
	}
	assert.Panics(t, func() {
		daily2 := make([][]float32, 16)
		for i := range daily2 {
			daily2[i] = make([]float32, 15)
		}
		addBurndownMatrix(added, 5, 3, daily2, 1)
	})
	assert.Panics(t, func() {
		daily2 := make([][]float32, 15)
		for i := range daily2 {
			daily2[i] = make([]float32, 16)
		}
		addBurndownMatrix(added, 5, 3, daily2, 1)
	})
	// yaml.PrintMatrix(os.Stdout, added, 0, "test", true)
	/*
			"test": |-
		  10  0  0
		  18  2  0
		  12 14  0
		  10 12  6
		   8  9 13
	*/
	addBurndownMatrix(added, 5, 3, daily, 1)
	for i := range daily[0] {
		assert.Equal(t, daily[0][i], float32(0))
	}
	for i := range daily {
		assert.Equal(t, daily[i][0], float32(0))
	}
	/*for _, row := range daily {
		fmt.Println(row)
	}*/
	// check pinned points
	for y := 0; y < 5; y++ {
		for x := 0; x < 3; x++ {
			var sum float32
			for i := x * 5; i < (x+1)*5; i++ {
				sum += daily[(y+1)*3][i+1]
			}
			assert.InDelta(t, sum, added[y][x], 0.00001)
		}
	}
	// check overall trend: 0 -> const -> peak -> decay
	for x := 0; x < 15; x++ {
		for y := 0; y < x; y++ {
			assert.Zero(t, daily[y+1][x+1])
		}
		var prev float32
		for y := x; y < ((x+3)/5)*5; y++ {
			if prev == 0 {
				prev = daily[y+1][x+1]
			}
			assert.Equal(t, daily[y+1][x+1], prev)
		}
		for y := ((x + 3) / 5) * 5; y < 15; y++ {
			if prev == 0 {
				prev = daily[y+1][x+1]
			}
			assert.True(t, daily[y+1][x+1] <= prev)
			prev = daily[y+1][x+1]
		}
	}
}

func TestBurndownAddMatrixCrazy(t *testing.T) {
	size := 5 * 3
	daily := make([][]float32, size)
	for i := range daily {
		daily[i] = make([]float32, size)
	}
	added := make([][]int64, 5)
	for i := range added {
		added[i] = make([]int64, 3)
		switch i {
		case 0:
			added[i][0] = 10
		case 1:
			added[i][0] = 9
			added[i][1] = 2
		case 2:
			added[i][0] = 8
			added[i][1] = 16
		case 3:
			added[i][0] = 7
			added[i][1] = 12
			added[i][2] = 6
		case 4:
			added[i][0] = 6
			added[i][1] = 9
			added[i][2] = 13
		}
	}
	// yaml.PrintMatrix(os.Stdout, added, 0, "test", true)
	/*
			"test": |-
		  10  0  0
		  9  2  0
		  8 16  0
		  7 12  6
		  6  9 13
	*/
	addBurndownMatrix(added, 5, 3, daily, 0)
	/*
		for _, row := range daily {
		  for _, v := range row {
			  fmt.Print(v, " ")
		  }
			fmt.Println()
		}
	*/
	// check pinned points
	for y := 0; y < 5; y++ {
		for x := 0; x < 3; x++ {
			var sum float32
			for i := x * 5; i < (x+1)*5; i++ {
				sum += daily[(y+1)*3-1][i]
			}
			assert.InDelta(t, sum, added[y][x], 0.00001)
		}
	}
	// check overall trend: 0 -> const -> peak -> decay
	for x := 0; x < 15; x++ {
		for y := 0; y < x; y++ {
			assert.Zero(t, daily[y][x])
		}
		var prev float32
		for y := x; y < ((x+3)/5)*5; y++ {
			if prev == 0 {
				prev = daily[y][x]
			}
			assert.Equal(t, daily[y][x], prev)
		}
		for y := ((x + 3) / 5) * 5; y < 15; y++ {
			if prev == 0 {
				prev = daily[y][x]
			}
			assert.True(t, daily[y][x] <= prev)
			prev = daily[y][x]
		}
	}
}

func TestBurndownAddMatrixNaNs(t *testing.T) {
	size := 4 * 4
	daily := make([][]float32, size)
	for i := range daily {
		daily[i] = make([]float32, size)
	}
	added := make([][]int64, 4)
	for i := range added {
		added[i] = make([]int64, 4)
		switch i {
		case 0:
			added[i][0] = 20
		case 1:
			added[i][0] = 18
			added[i][1] = 30
		case 2:
			added[i][0] = 15
			added[i][1] = 25
			added[i][2] = 28
		case 3:
			added[i][0] = 12
			added[i][1] = 20
			added[i][2] = 25
			added[i][3] = 40
		}
	}
	// yaml.PrintMatrix(os.Stdout, added, 0, "test", true)
	/*
			"test": |-
		  20 0  0  0
		  18 30 0  0
		  15 25 28 0
		  12 20 25 40
	*/
	addBurndownMatrix(added, 4, 4, daily, 0)
	/*
		for _, row := range daily {
		  for _, v := range row {
			  fmt.Print(v, " ")
		  }
			fmt.Println()
		}
	*/
	// check pinned points
	for y := 0; y < 4; y++ {
		for x := 0; x < 4; x++ {
			var sum float32
			for i := x * 4; i < (x+1)*4; i++ {
				sum += daily[(y+1)*4-1][i]
			}
			assert.InDelta(t, sum, added[y][x], 0.00001)
		}
	}
	// check overall trend: 0 -> const -> peak -> decay
	for x := 0; x < 16; x++ {
		for y := 0; y < x; y++ {
			assert.Zero(t, daily[y][x])
		}
		var prev float32
		for y := x - 4; y < x; y++ {
			if y < 0 {
				continue
			}
			if prev == 0 {
				prev = daily[y][x]
			}
			assert.Equal(t, daily[y][x], prev)
		}
		for y := x; y < 16; y++ {
			if prev == 0 {
				prev = daily[y][x]
			}
			assert.True(t, daily[y][x] <= prev)
			prev = daily[y][x]
		}
	}
}

func TestBurndownMergeGlobalHistory(t *testing.T) {
	people1 := [...]string{"one", "two"}
	res1 := BurndownResult{
		GlobalHistory:      [][]int64{},
		FileHistories:      map[string][][]int64{},
		PeopleHistories:    [][][]int64{},
		PeopleMatrix:       [][]int64{},
		reversedPeopleDict: people1[:],
		sampling:           15,
		granularity:        20,
		tickSize:           24 * time.Hour,
	}
	c1 := core.CommonAnalysisResult{
		BeginTime:     600566400, // 1989 Jan 12
		EndTime:       604713600, // 1989 March 1
		CommitsNumber: 10,
		RunTime:       100000,
	}
	// 48 days
	res1.GlobalHistory = make([][]int64, 48/15+1 /* 4 samples */)
	for i := range res1.GlobalHistory {
		res1.GlobalHistory[i] = make([]int64, 48/20+1 /* 3 bands */)
		switch i {
		case 0:
			res1.GlobalHistory[i][0] = 1000
		case 1:
			res1.GlobalHistory[i][0] = 1100
			res1.GlobalHistory[i][1] = 400
		case 2:
			res1.GlobalHistory[i][0] = 900
			res1.GlobalHistory[i][1] = 750
			res1.GlobalHistory[i][2] = 100
		case 3:
			res1.GlobalHistory[i][0] = 850
			res1.GlobalHistory[i][1] = 700
			res1.GlobalHistory[i][2] = 150
		}
	}
	res1.PeopleHistories = append(res1.PeopleHistories, res1.GlobalHistory)
	res1.PeopleHistories = append(res1.PeopleHistories, res1.GlobalHistory)
	res1.PeopleMatrix = append(res1.PeopleMatrix, make([]int64, 4))
	res1.PeopleMatrix = append(res1.PeopleMatrix, make([]int64, 4))
	res1.PeopleMatrix[0][0] = 10
	res1.PeopleMatrix[0][1] = 20
	res1.PeopleMatrix[0][2] = 30
	res1.PeopleMatrix[0][3] = 40
	res1.PeopleMatrix[1][0] = 50
	res1.PeopleMatrix[1][1] = 60
	res1.PeopleMatrix[1][2] = 70
	res1.PeopleMatrix[1][3] = 80
	people2 := [...]string{"two", "three"}
	res2 := BurndownResult{
		GlobalHistory:      nil,
		FileHistories:      map[string][][]int64{},
		PeopleHistories:    nil,
		PeopleMatrix:       nil,
		tickSize:           24 * time.Hour,
		reversedPeopleDict: people2[:],
		sampling:           14,
		granularity:        19,
	}
	c2 := core.CommonAnalysisResult{
		BeginTime:     601084800, // 1989 Jan 18
		EndTime:       605923200, // 1989 March 15
		CommitsNumber: 10,
		RunTime:       100000,
	}
	// 56 days
	res2.GlobalHistory = make([][]int64, 56/14 /* 4 samples */)
	for i := range res2.GlobalHistory {
		res2.GlobalHistory[i] = make([]int64, 56/19+1 /* 3 bands */)
		switch i {
		case 0:
			res2.GlobalHistory[i][0] = 900
		case 1:
			res2.GlobalHistory[i][0] = 1100
			res2.GlobalHistory[i][1] = 400
		case 2:
			res2.GlobalHistory[i][0] = 900
			res2.GlobalHistory[i][1] = 750
			res2.GlobalHistory[i][2] = 100
		case 3:
			res2.GlobalHistory[i][0] = 800
			res2.GlobalHistory[i][1] = 600
			res2.GlobalHistory[i][2] = 600
		}
	}
	res2.PeopleHistories = append(res2.PeopleHistories, res2.GlobalHistory)
	res2.PeopleHistories = append(res2.PeopleHistories, res2.GlobalHistory)
	res2.PeopleMatrix = append(res2.PeopleMatrix, make([]int64, 4))
	res2.PeopleMatrix = append(res2.PeopleMatrix, make([]int64, 4))
	res2.PeopleMatrix[0][0] = 100
	res2.PeopleMatrix[0][1] = 200
	res2.PeopleMatrix[0][2] = 300
	res2.PeopleMatrix[0][3] = 400
	res2.PeopleMatrix[1][0] = 500
	res2.PeopleMatrix[1][1] = 600
	res2.PeopleMatrix[1][2] = 700
	res2.PeopleMatrix[1][3] = 800
	bd := BurndownAnalysis{
		TickSize: 24 * time.Hour,
	}
	merged := bd.MergeResults(res1, res2, &c1, &c2).(BurndownResult)
	assert.Equal(t, merged.granularity, 19)
	assert.Equal(t, merged.sampling, 14)
	assert.Equal(t, merged.tickSize, 24*time.Hour)
	assert.Len(t, merged.GlobalHistory, 5)
	for _, row := range merged.GlobalHistory {
		assert.Len(t, row, 4)
	}
	assert.Nil(t, merged.FileHistories)
	assert.Len(t, merged.reversedPeopleDict, 3)
	assert.NotEqual(t, merged.PeopleHistories[0], res1.GlobalHistory)
	assert.Equal(t, merged.PeopleHistories[1], merged.GlobalHistory)
	assert.NotEqual(t, merged.PeopleHistories[2], res2.GlobalHistory)
	assert.Len(t, merged.PeopleMatrix, 3)
	for _, row := range merged.PeopleMatrix {
		assert.Len(t, row, 5)
	}
	assert.Equal(t, merged.PeopleMatrix[0][0], int64(10))
	assert.Equal(t, merged.PeopleMatrix[0][1], int64(20))
	assert.Equal(t, merged.PeopleMatrix[0][2], int64(30))
	assert.Equal(t, merged.PeopleMatrix[0][3], int64(40))
	assert.Equal(t, merged.PeopleMatrix[0][4], int64(0))

	assert.Equal(t, merged.PeopleMatrix[1][0], int64(150))
	assert.Equal(t, merged.PeopleMatrix[1][1], int64(260))
	assert.Equal(t, merged.PeopleMatrix[1][2], int64(70))
	assert.Equal(t, merged.PeopleMatrix[1][3], int64(380))
	assert.Equal(t, merged.PeopleMatrix[1][4], int64(400))

	assert.Equal(t, merged.PeopleMatrix[2][0], int64(500))
	assert.Equal(t, merged.PeopleMatrix[2][1], int64(600))
	assert.Equal(t, merged.PeopleMatrix[2][2], int64(0))
	assert.Equal(t, merged.PeopleMatrix[2][3], int64(700))
	assert.Equal(t, merged.PeopleMatrix[2][4], int64(800))
	assert.Nil(t, bd.serializeBinary(&merged, ioutil.Discard))
}

func TestBurndownMergeGlobalHistory_withDifferentTickSizes(t *testing.T) {
	res1 := BurndownResult{
		tickSize: 13 * time.Hour,
	}
	c1 := core.CommonAnalysisResult{
		BeginTime:     600566400, // 1989 Jan 12
		EndTime:       604713600, // 1989 March 1
		CommitsNumber: 10,
		RunTime:       100000,
	}
	res2 := BurndownResult{
		tickSize: 24 * time.Hour,
	}
	c2 := core.CommonAnalysisResult{
		BeginTime:     601084800, // 1989 Jan 18
		EndTime:       605923200, // 1989 March 15
		CommitsNumber: 10,
		RunTime:       100000,
	}
	bd := BurndownAnalysis{
		TickSize: 24 * time.Hour,
	}
	merged := bd.MergeResults(res1, res2, &c1, &c2)
	assert.IsType(t, errors.New(""), merged)
	assert.Contains(t, merged.(error).Error(), "mismatching tick sizes")
}

func TestBurndownMergeNils(t *testing.T) {
	res1 := BurndownResult{
		GlobalHistory:      nil,
		FileHistories:      map[string][][]int64{},
		PeopleHistories:    nil,
		PeopleMatrix:       nil,
		tickSize:           24 * time.Hour,
		reversedPeopleDict: nil,
		sampling:           15,
		granularity:        20,
	}
	c1 := core.CommonAnalysisResult{
		BeginTime:     600566400, // 1989 Jan 12
		EndTime:       604713600, // 1989 March 1
		CommitsNumber: 10,
		RunTime:       100000,
	}
	res2 := BurndownResult{
		GlobalHistory:      nil,
		FileHistories:      nil,
		PeopleHistories:    nil,
		PeopleMatrix:       nil,
		tickSize:           24 * time.Hour,
		reversedPeopleDict: nil,
		sampling:           14,
		granularity:        19,
	}
	c2 := core.CommonAnalysisResult{
		BeginTime:     601084800, // 1989 Jan 18
		EndTime:       605923200, // 1989 March 15
		CommitsNumber: 10,
		RunTime:       100000,
	}
	bd := BurndownAnalysis{
		TickSize: 24 * time.Hour,
	}
	merged := bd.MergeResults(res1, res2, &c1, &c2).(BurndownResult)
	assert.Equal(t, merged.granularity, 19)
	assert.Equal(t, merged.sampling, 14)
	assert.Equal(t, merged.tickSize, 24*time.Hour)
	assert.Nil(t, merged.GlobalHistory)
	assert.Nil(t, merged.FileHistories)
	assert.Nil(t, merged.PeopleHistories)
	assert.Nil(t, merged.PeopleMatrix)
	assert.Nil(t, bd.serializeBinary(&merged, ioutil.Discard))

	res2.GlobalHistory = [][]int64{
		{900, 0, 0},
		{1100, 400, 0},
		{900, 750, 100},
		{800, 600, 600},
	}
	res2.FileHistories = map[string]DenseHistory{"test": res2.GlobalHistory}
	people1 := [...]string{"one", "two"}
	res1.reversedPeopleDict = people1[:]
	res1.PeopleMatrix = append(res1.PeopleMatrix, make([]int64, 4))
	res1.PeopleMatrix = append(res1.PeopleMatrix, make([]int64, 4))
	res1.PeopleMatrix[0][0] = 10
	res1.PeopleMatrix[0][1] = 20
	res1.PeopleMatrix[0][2] = 30
	res1.PeopleMatrix[0][3] = 40
	res1.PeopleMatrix[1][0] = 50
	res1.PeopleMatrix[1][1] = 60
	res1.PeopleMatrix[1][2] = 70
	res1.PeopleMatrix[1][3] = 80
	people2 := [...]string{"two", "three"}
	res2.reversedPeopleDict = people2[:]
	merged = bd.MergeResults(res1, res2, &c1, &c2).(BurndownResult)
	// calculated in a spreadsheet
	mgh := [][]int64{
		{514, 0, 0, 0},
		{808, 506, 0, 0},
		{674, 889, 177, 0},
		{576, 720, 595, 0},
		{547, 663, 610, 178},
	}
	assert.Equal(t, mgh, merged.GlobalHistory)
	assert.Nil(t, merged.FileHistories)
	assert.Nil(t, merged.PeopleHistories)
	assert.Len(t, merged.PeopleMatrix, 3)
	for _, row := range merged.PeopleMatrix {
		assert.Len(t, row, 5)
	}
	assert.Equal(t, merged.PeopleMatrix[0][0], int64(10))
	assert.Equal(t, merged.PeopleMatrix[0][1], int64(20))
	assert.Equal(t, merged.PeopleMatrix[0][2], int64(30))
	assert.Equal(t, merged.PeopleMatrix[0][3], int64(40))
	assert.Equal(t, merged.PeopleMatrix[0][4], int64(0))

	assert.Equal(t, merged.PeopleMatrix[1][0], int64(50))
	assert.Equal(t, merged.PeopleMatrix[1][1], int64(60))
	assert.Equal(t, merged.PeopleMatrix[1][2], int64(70))
	assert.Equal(t, merged.PeopleMatrix[1][3], int64(80))
	assert.Equal(t, merged.PeopleMatrix[1][4], int64(0))

	assert.Equal(t, merged.PeopleMatrix[2][0], int64(0))
	assert.Equal(t, merged.PeopleMatrix[2][1], int64(0))
	assert.Equal(t, merged.PeopleMatrix[2][2], int64(0))
	assert.Equal(t, merged.PeopleMatrix[2][3], int64(0))
	assert.Equal(t, merged.PeopleMatrix[2][4], int64(0))
	assert.Nil(t, bd.serializeBinary(&merged, ioutil.Discard))
}

func TestBurndownDeserialize(t *testing.T) {
	allBuffer, err := ioutil.ReadFile(path.Join("..", "internal", "test_data", "burndown.pb"))
	assert.Nil(t, err)
	bd := BurndownAnalysis{}
	iresult, err := bd.Deserialize(allBuffer)
	assert.Nil(t, err)
	result := iresult.(BurndownResult)
	assert.True(t, len(result.GlobalHistory) > 0)
	assert.True(t, len(result.FileHistories) > 0)
	assert.Equal(t, len(result.FileOwnership), len(result.FileHistories))
	assert.True(t, len(result.reversedPeopleDict) > 0)
	assert.True(t, len(result.PeopleHistories) > 0)
	assert.True(t, len(result.PeopleMatrix) > 0)
	assert.Equal(t, result.granularity, 30)
	assert.Equal(t, result.sampling, 30)
	assert.Equal(t, result.tickSize, 24*time.Hour)
}

func TestBurndownEmptyFileHistory(t *testing.T) {
	bd := &BurndownAnalysis{
		Sampling:      30,
		Granularity:   30,
		globalHistory: sparseHistory{0: map[int]int64{0: 10}},
		fileHistories: map[string]sparseHistory{"test.go": {}},
	}
	res := bd.Finalize().(BurndownResult)
	assert.Len(t, res.GlobalHistory, 1)
	assert.Len(t, res.FileHistories, 0)
	assert.NotNil(t, res.FileHistories)
	assert.Len(t, res.PeopleHistories, 0)
	assert.NotNil(t, res.PeopleHistories)
}

func TestBurndownNegativePeople(t *testing.T) {
	bd := &BurndownAnalysis{
		Sampling:     30,
		Granularity:  30,
		PeopleNumber: -1,
	}
	err := bd.Initialize(test.Repository)
	assert.Equal(t, err.Error(), "PeopleNumber is negative: -1")
	facts := map[string]interface{}{
		ConfigBurndownTrackPeople:                true,
		identity.FactIdentityDetectorPeopleCount: -1,
	}
	err = bd.Configure(facts)
	assert.Equal(t, err.Error(), "PeopleNumber is negative: -1")
}

func TestBurndownHibernateBoot(t *testing.T) {
	_, bd := bakeBurndownForSerialization(t, 0, 1)
	assert.Equal(t, bd.fileAllocator.Size(), 157)
	assert.Equal(t, bd.fileAllocator.Used(), 155)
	assert.Nil(t, bd.Hibernate())
	assert.PanicsWithValue(t, "BurndownAnalysis.Consume() was called on a hibernated instance",
		func() { bd.Consume(nil) })
	assert.Equal(t, bd.fileAllocator.Size(), 0)
	assert.Nil(t, bd.Boot())
	assert.Equal(t, bd.fileAllocator.Size(), 157)
	assert.Equal(t, bd.fileAllocator.Used(), 155)
}

func TestBurndownHibernateBootSerialize(t *testing.T) {
	_, bd := bakeBurndownForSerialization(t, 0, 1)
	assert.Equal(t, bd.fileAllocator.Size(), 157)
	assert.Equal(t, bd.fileAllocator.Used(), 155)
	bd.HibernationToDisk = true
	assert.Nil(t, bd.Hibernate())
	assert.NotEmpty(t, bd.hibernatedFileName)
	assert.PanicsWithValue(t, "BurndownAnalysis.Consume() was called on a hibernated instance",
		func() { bd.Consume(nil) })
	assert.Equal(t, bd.fileAllocator.Size(), 0)
	assert.Nil(t, bd.Boot())
	assert.Equal(t, bd.fileAllocator.Size(), 157)
	assert.Equal(t, bd.fileAllocator.Used(), 155)
	assert.Empty(t, bd.hibernatedFileName)
}

func TestBurndownAddBurndownMatrix(t *testing.T) {
	h := DenseHistory{
		[]int64{13430, 0, 0, 0},
		[]int64{7698, 23316, 0, 0},
		[]int64{7181, 18750, 55841, 0},
		[]int64{6345, 16704, 17110, 55981},
	}
	perTick := make([][]float32, 4*30)
	for i := range perTick {
		perTick[i] = make([]float32, 4*30)
	}
	addBurndownMatrix(h, 30, 30, perTick, 0)
	sum := func(x, y int) int64 {
		var accum float32
		row := (y+1)*30 - 1
		offset := x * 30
		for i := offset; i < offset+30; i++ {
			accum += perTick[row][i]
		}
		return int64(accum)
	}
	for y, row := range h {
		for x, val := range row {
			assert.InDelta(t, sum(x, y), val, 1)
		}
	}
}

func TestBurndownMergeMatrices(t *testing.T) {
	h := DenseHistory{
		[]int64{13430, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{7698, 23316, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{7181, 18750, 55841, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{6345, 16704, 17110, 55981, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{6191, 15805, 15006, 41212, 26384, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{6020, 14760, 13000, 16292, 18157, 58615, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{5885, 14506, 11934, 15229, 16026, 54157, 27561, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{5684, 13997, 11588, 14939, 13034, 27032, 22242, 46431, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{5469, 13635, 11188, 13864, 12159, 25496, 20517, 42373, 62033, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{5431, 13088, 10608, 12546, 10615, 20405, 15111, 16412, 52677, 49573, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{4745, 12649, 9321, 11041, 9373, 12969, 11185, 14161, 38560, 43302, 24281, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{4546, 12540, 9205, 10621, 9038, 12728, 10760, 13651, 36806, 42229, 17719, 15903, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{3272, 7972, 4706, 8728, 4948, 11527, 4744, 7395, 29937, 38897, 8874, 7898, 46522, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{3136, 7653, 4434, 7760, 4113, 11325, 3855, 6988, 27395, 37709, 7983, 7467, 42685, 29844, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{2608, 5432, 4096, 7465, 3539, 11005, 3625, 5963, 19364, 36904, 7426, 6491, 36095, 25025, 22280, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{2157, 4033, 3000, 6968, 3186, 9687, 3191, 4955, 16729, 35998, 7200, 6372, 34196, 21592, 18757, 25304, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1887, 3847, 2939, 6573, 2829, 9496, 3050, 4829, 16312, 29070, 6910, 6270, 33138, 19577, 18101, 22819, 39223, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1869, 3660, 2634, 5744, 2478, 9265, 2876, 4442, 10362, 28338, 5908, 5266, 26172, 17293, 14834, 19263, 37511, 36830, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1860, 3642, 2589, 5717, 2410, 9237, 2836, 4278, 8712, 28152, 5458, 4970, 24725, 16106, 14158, 18201, 36032, 32884, 26193, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1840, 3622, 2533, 5545, 2274, 8955, 2783, 4247, 8467, 27810, 5068, 4864, 23757, 14822, 13453, 16199, 29994, 30955, 23038, 25745, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1782, 3429, 2459, 5362, 2160, 8526, 2473, 3237, 7238, 27376, 4899, 3839, 20857, 13491, 11719, 15045, 28905, 26343, 19202, 20732, 41048, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1535, 2608, 1800, 5012, 1882, 8261, 2373, 1846, 5039, 27180, 4522, 3464, 15816, 11562, 9868, 13729, 27709, 21367, 15626, 18095, 33529, 44821, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1524, 2598, 1798, 4847, 1862, 8233, 2354, 1791, 5005, 26743, 4218, 3358, 15241, 10329, 9304, 12594, 27478, 20230, 15011, 17382, 31331, 41415, 24488, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1474, 2532, 1795, 4260, 1756, 8125, 2344, 1627, 4879, 26543, 4134, 3198, 14132, 9776, 9175, 12243, 27019, 19818, 13999, 16697, 29687, 37543, 23669, 13238, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1427, 2507, 1778, 4192, 1718, 8102, 2326, 1554, 4818, 9726, 3963, 3099, 13642, 9523, 8975, 11940, 8967, 19035, 13584, 15627, 28388, 35931, 22954, 12218, 51230, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1414, 2266, 1759, 3924, 1613, 8083, 2302, 1522, 4590, 9396, 3835, 2987, 12616, 9076, 8538, 11603, 8664, 18267, 13011, 14926, 26676, 34225, 22091, 9581, 48080, 29792, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1394, 2243, 1731, 3911, 1594, 7911, 2284, 1518, 4544, 8452, 3832, 2975, 12533, 8875, 8238, 11274, 8467, 16358, 12471, 14468, 25468, 33459, 21417, 9267, 30708, 28383, 30913, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1393, 2243, 1728, 3902, 1591, 7899, 2248, 1463, 4503, 8445, 3677, 2872, 12271, 8779, 8127, 11118, 8436, 16271, 12229, 14177, 24719, 31578, 21036, 8874, 29685, 26663, 29919, 20499, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1390, 2131, 1663, 3863, 1564, 7873, 2242, 1422, 4476, 8385, 3669, 2856, 12197, 8650, 7932, 10844, 8202, 16149, 12065, 13529, 24289, 30669, 20806, 8701, 29238, 25926, 27111, 19383, 53864, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1390, 2116, 1663, 3848, 1531, 7852, 2232, 1417, 4472, 8335, 3544, 1524, 11920, 8635, 7860, 10726, 8064, 14483, 11369, 5956, 22559, 28467, 20308, 7767, 28403, 24070, 26682, 17395, 51966, 23389, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1388, 2113, 1626, 3832, 1524, 7840, 2232, 1407, 4460, 8292, 3534, 1489, 11307, 8602, 7794, 10671, 7996, 14393, 11339, 5915, 22253, 28291, 20214, 7729, 28344, 23585, 26486, 17152, 51532, 22845, 22435, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1386, 2110, 1558, 3823, 1518, 7745, 2232, 1087, 4404, 8082, 3382, 1316, 11080, 8229, 6774, 9887, 7855, 14086, 10997, 5158, 16647, 27042, 19173, 7345, 27367, 21983, 25194, 13957, 48597, 21030, 22008, 54558, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1379, 2105, 1557, 3740, 1488, 7621, 2200, 1080, 4370, 7820, 3338, 1293, 10279, 8180, 6417, 9686, 7767, 13410, 10762, 4678, 15603, 26465, 18850, 7169, 9580, 20556, 10501, 13019, 42837, 19989, 19586, 42354, 60288, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1379, 2103, 1557, 3740, 1482, 7615, 2082, 762, 4316, 7806, 3222, 1293, 10070, 7684, 5422, 8902, 7588, 13136, 10382, 3847, 9978, 25574, 17809, 6799, 8567, 18272, 9149, 9626, 40377, 18337, 19295, 41783, 58014, 33979, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1375, 2102, 1555, 3653, 1480, 7606, 2029, 756, 4312, 7795, 3222, 1291, 10011, 7669, 5390, 8851, 7580, 13132, 10376, 3768, 9898, 25298, 17522, 6415, 8526, 17475, 9113, 9269, 39856, 18230, 19197, 41134, 57566, 33320, 8047, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1375, 2102, 1555, 3651, 1480, 7606, 2029, 756, 4312, 7795, 3222, 1291, 10010, 7666, 5385, 8851, 7580, 13117, 10376, 3767, 9880, 25298, 17517, 6415, 8515, 17457, 9104, 9238, 39852, 18184, 19147, 41123, 57518, 33264, 8033, 941, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1370, 2099, 1498, 3523, 1474, 7605, 2009, 756, 4264, 7582, 3171, 1289, 9707, 7421, 5212, 8624, 7428, 12473, 10168, 3589, 9523, 24409, 17406, 6134, 8279, 16596, 9016, 9128, 39152, 17615, 19102, 36069, 56969, 32962, 7903, 927, 264665, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1109, 1678, 1236, 3409, 1440, 7406, 1974, 753, 4156, 7545, 3115, 1260, 9570, 7197, 4876, 7510, 6892, 11915, 9497, 2961, 8319, 23488, 15831, 5793, 7703, 8839, 8705, 7206, 36800, 16372, 17816, 34229, 55779, 26494, 7514, 814, 263077, 292742, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1099, 1662, 1226, 3151, 1399, 7369, 1912, 747, 4073, 7359, 3091, 1228, 9491, 6991, 4661, 7381, 6824, 11587, 9313, 2821, 7502, 22897, 15583, 5626, 7603, 8070, 8472, 6915, 36110, 16001, 17580, 33765, 55121, 26096, 7278, 761, 262798, 290965, 30113, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1045, 1658, 1222, 3124, 1365, 7133, 1854, 739, 3869, 7002, 3071, 1156, 8935, 6797, 4353, 6980, 6690, 11369, 8921, 2244, 6801, 22237, 14775, 5138, 7370, 6502, 8039, 6595, 34778, 14976, 16851, 32794, 54195, 24775, 6683, 748, 136421, 286458, 27127, 154181, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1042, 1658, 1222, 3108, 1300, 7126, 1853, 719, 3763, 6987, 3062, 1153, 8915, 6557, 4203, 6906, 6387, 11159, 8602, 2103, 5919, 20200, 14394, 3729, 6571, 5697, 7380, 5954, 32604, 13465, 16498, 28686, 53547, 24057, 6570, 690, 130726, 285240, 25497, 35819, 38609, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1042, 1657, 1209, 3088, 1295, 7114, 1849, 686, 3592, 4745, 2926, 1038, 8292, 5755, 3580, 6552, 6078, 10321, 7821, 1661, 5568, 19864, 13563, 3122, 6175, 5396, 6831, 5035, 32307, 13088, 16006, 27828, 50777, 23149, 6182, 601, 130329, 283925, 23394, 31912, 28622, 47146, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{1001, 1617, 1154, 2933, 1290, 7054, 1837, 676, 3487, 3203, 2268, 905, 7953, 5673, 3431, 4772, 5407, 9200, 7453, 939, 4947, 19334, 13054, 2401, 5316, 3768, 3949, 4580, 31317, 12444, 15323, 26602, 49590, 22753, 5702, 572, 26696, 275964, 22184, 27561, 26049, 24501, 194501, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{997, 1614, 1148, 2902, 1247, 6786, 1761, 672, 3401, 2938, 2248, 890, 7233, 5645, 3383, 4644, 5198, 8926, 7162, 208, 4231, 18575, 12876, 2012, 5196, 1806, 3731, 4451, 29976, 11199, 13122, 25866, 46032, 22122, 4663, 559, 24258, 274857, 21675, 26662, 24590, 21522, 191082, 39811, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{996, 1614, 1148, 2897, 1235, 6776, 1761, 672, 3400, 2937, 2248, 890, 7177, 5643, 3360, 4631, 5187, 8788, 7156, 202, 4205, 18484, 12861, 1969, 5183, 1578, 3674, 4281, 29948, 11147, 13094, 25801, 45910, 21985, 4647, 559, 24203, 274714, 20945, 26380, 24366, 20885, 190910, 37655, 35516, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{996, 1612, 1135, 2886, 1235, 6770, 1756, 672, 3398, 2927, 2246, 889, 7170, 5557, 3352, 4576, 5013, 8754, 7155, 149, 4163, 18382, 12834, 1937, 4732, 1459, 3633, 4270, 29914, 11131, 13086, 25774, 45895, 21946, 4617, 553, 24178, 274369, 20853, 25969, 23513, 20144, 188818, 34194, 33294, 24826, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{995, 1610, 1134, 2885, 1227, 6764, 1754, 672, 3385, 2921, 2231, 889, 7169, 5531, 3336, 4548, 4856, 8706, 7134, 126, 4152, 18327, 12772, 1912, 4720, 1449, 3600, 4246, 29899, 11081, 13037, 25513, 45806, 21900, 4613, 553, 24140, 274216, 20635, 25858, 23281, 19924, 188450, 33878, 33100, 24496, 8738, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{992, 1610, 1134, 2885, 1227, 6754, 1754, 670, 3384, 2920, 2230, 889, 7156, 5523, 3336, 4545, 4831, 8692, 7124, 113, 4137, 18316, 12758, 1907, 4711, 1447, 3598, 4181, 29892, 11042, 13029, 25345, 45768, 21865, 4587, 553, 24135, 274146, 20566, 25732, 23088, 19794, 188302, 33520, 32831, 24273, 8042, 8540, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{991, 1608, 1133, 2737, 1224, 6735, 1754, 670, 3377, 2918, 2228, 889, 7145, 5438, 3320, 4518, 4613, 8624, 7114, 76, 4088, 18155, 12692, 1906, 4686, 1401, 3551, 4112, 29826, 10930, 13006, 25072, 45665, 21819, 4567, 539, 24083, 274046, 20388, 25299, 22340, 19444, 188132, 32795, 31377, 22972, 7705, 8119, 80636, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{989, 1601, 1132, 2735, 1224, 6735, 1754, 669, 3377, 2918, 2220, 888, 7094, 5411, 3314, 4491, 4558, 8615, 7085, 39, 3625, 18062, 12620, 1904, 4622, 1359, 3523, 4060, 29711, 10795, 12978, 24990, 45607, 21774, 4499, 528, 23956, 272619, 20261, 25201, 21853, 18608, 176147, 32404, 30632, 22515, 7013, 6830, 78714, 37873, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{984, 1597, 1132, 2734, 1213, 6734, 1751, 667, 3374, 2917, 2215, 888, 7055, 5378, 3259, 4440, 4539, 8574, 7010, 2, 3567, 16954, 12516, 1823, 4468, 1264, 3471, 3967, 29669, 10711, 12929, 24918, 45543, 21645, 4487, 526, 14508, 272464, 19936, 22042, 21435, 18484, 80372, 31942, 30300, 20509, 6910, 6488, 76858, 36148, 93628, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{984, 1597, 1132, 2734, 1213, 6734, 1751, 667, 3373, 2915, 2214, 888, 7053, 5361, 3248, 4428, 4536, 8549, 6987, 0, 3557, 16871, 12496, 1820, 4457, 1236, 3453, 3966, 29667, 10670, 12881, 24880, 45531, 21638, 4485, 521, 14450, 272424, 19880, 21565, 20920, 18335, 80100, 31675, 30111, 20472, 6874, 6247, 76447, 35839, 93226, 10524, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{982, 1593, 1131, 2727, 1024, 6592, 1745, 666, 3370, 2883, 2068, 874, 6842, 5270, 3235, 4363, 4518, 8380, 6828, 0, 3417, 15931, 12442, 1783, 4434, 1036, 1692, 3810, 29535, 10015, 12697, 23628, 43199, 21571, 4350, 491, 14300, 272352, 19801, 21154, 20578, 16149, 78373, 26640, 27871, 19540, 6584, 5990, 40360, 31708, 91775, 10012, 54599, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{982, 1593, 1125, 2723, 1024, 6591, 1736, 660, 3348, 2688, 2063, 846, 6820, 5270, 3230, 4288, 4515, 8340, 6725, 0, 3340, 15868, 12307, 1538, 4425, 771, 1637, 3638, 29241, 9884, 12517, 23436, 43120, 21401, 4170, 484, 5707, 272273, 19447, 18178, 20176, 15941, 0, 25917, 27377, 16849, 6499, 5398, 35743, 28901, 89846, 8224, 50802, 107205, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int64{982, 1593, 1125, 2723, 1024, 6571, 1734, 660, 3347, 2683, 2063, 846, 6753, 5270, 3118, 4282, 4513, 8310, 6721, 0, 3240, 15836, 12304, 1538, 4421, 760, 1634, 3617, 29231, 9856, 12461, 23380, 43105, 21366, 3902, 484, 5701, 272248, 19163, 17637, 20087, 15799, 0, 25548, 27302, 16790, 6449, 5328, 35513, 28600, 89762, 7782, 50485, 103692, 11923, 0, 0, 0, 0, 0, 0, 0},
		[]int64{981, 1593, 1120, 2702, 1023, 5922, 1722, 660, 3344, 2681, 2061, 843, 6737, 5267, 3117, 4280, 4513, 8309, 6715, 0, 3180, 15800, 12268, 1535, 4419, 743, 1620, 3611, 29221, 9844, 12454, 23214, 43053, 21356, 3895, 479, 5700, 272209, 19102, 17525, 20046, 15771, 0, 25487, 27260, 16785, 6391, 5288, 35341, 27452, 89683, 7719, 50379, 102035, 11787, 48873, 0, 0, 0, 0, 0, 0},
		[]int64{975, 1593, 1120, 2686, 1019, 5920, 1718, 658, 3340, 2681, 2061, 843, 6733, 5266, 3117, 4269, 4499, 8306, 6713, 0, 3179, 15783, 12254, 1531, 4416, 739, 1612, 3592, 29217, 9839, 12452, 23202, 43043, 21350, 3885, 479, 5699, 272199, 18974, 17520, 19898, 15742, 0, 24718, 27197, 16765, 6377, 5281, 35306, 27384, 89531, 7502, 50238, 101815, 11720, 47787, 23800, 0, 0, 0, 0, 0},
		[]int64{975, 1593, 1117, 2686, 1019, 5920, 1717, 658, 3337, 2681, 2060, 842, 6723, 5240, 3116, 4256, 4494, 8296, 6701, 0, 3160, 15743, 12235, 1523, 4414, 693, 1602, 3572, 29206, 9821, 12433, 22983, 42992, 20999, 3855, 476, 5692, 272158, 18811, 17443, 19846, 15616, 0, 24558, 27098, 16618, 6313, 5255, 35056, 27196, 89341, 7400, 49814, 101672, 11526, 47117, 21002, 16664, 0, 0, 0, 0},
		[]int64{972, 1593, 1117, 2672, 1019, 5920, 1717, 658, 3337, 2681, 2059, 842, 6723, 5240, 3111, 4256, 4473, 8290, 6698, 0, 3152, 15716, 12172, 1523, 4412, 687, 1597, 3548, 29180, 9810, 12354, 22937, 42980, 20996, 3851, 475, 0, 271082, 18798, 14752, 19712, 15557, 0, 24487, 27006, 16132, 6213, 5095, 34845, 27103, 89281, 7268, 49516, 99034, 11501, 47105, 20886, 16499, 99277, 0, 0, 0},
		[]int64{967, 1593, 1117, 2672, 1019, 5920, 1717, 658, 3337, 2681, 2059, 842, 6723, 5240, 3106, 4256, 4396, 8285, 6691, 0, 3110, 15709, 12161, 1522, 4408, 680, 1592, 3392, 29167, 9804, 12352, 22927, 42979, 20994, 3849, 474, 0, 268871, 18740, 11751, 19601, 15451, 0, 24392, 8970, 14411, 4245, 4729, 33890, 26298, 88696, 7003, 49177, 33580, 11422, 46951, 20798, 15839, 85988, 6823, 0, 0},
		[]int64{967, 1592, 1116, 2660, 1018, 5920, 1714, 656, 3332, 2675, 2049, 842, 6679, 5204, 3091, 4139, 4322, 8206, 6644, 0, 2989, 15157, 11992, 1493, 4330, 634, 1553, 3320, 28555, 9724, 12317, 22700, 42501, 20936, 3835, 464, 0, 268531, 18694, 11669, 18754, 15247, 0, 21731, 8928, 14090, 4186, 4680, 30445, 25961, 88490, 6882, 48779, 33363, 11059, 46565, 19447, 14792, 85627, 6554, 17703, 0},
		[]int64{967, 1592, 1116, 2660, 1018, 5920, 1712, 656, 3332, 2674, 2049, 839, 6547, 5204, 3061, 4136, 4319, 8189, 6644, 0, 2863, 15098, 11958, 1491, 4314, 623, 1543, 3314, 28438, 9544, 12096, 22657, 42411, 20900, 3831, 459, 0, 268413, 17451, 11506, 18691, 15171, 0, 21575, 8912, 14042, 4173, 4663, 30235, 25574, 88256, 6823, 48510, 33297, 8623, 46286, 19276, 14663, 85617, 6410, 16838, 17004},
	}
	cr := &core.CommonAnalysisResult{
		BeginTime:     1390499270,
		EndTime:       1549992932,
		CommitsNumber: 6982,
		RunTime:       1567214,
	}
	bd := BurndownAnalysis{TickSize: 24 * time.Hour}
	nh := bd.mergeMatrices(h, nil, 30, 30, 30, 30, bd.TickSize, cr, cr)
	for y, row := range nh {
		for x, v := range row {
			assert.InDelta(t, v, h[y][x], 1, fmt.Sprintf("y=%d x=%d", y, x))
		}
	}
	nh = bd.mergeMatrices(h, h, 30, 30, 30, 30, bd.TickSize, cr, cr)
	for y, row := range nh {
		for x, v := range row {
			assert.InDelta(t, v, h[y][x]*2, 1, fmt.Sprintf("y=%d x=%d", y, x))
		}
	}
}

func TestBurndownMergePeopleHistories(t *testing.T) {
	h1 := [][]int64{
		{50, 0, 0},
		{40, 80, 0},
		{30, 50, 70},
	}
	h2 := [][]int64{
		{900, 0, 0},
		{1100, 400, 0},
		{900, 750, 100},
		{800, 600, 600},
	}
	res1 := BurndownResult{
		GlobalHistory:      h1,
		FileHistories:      map[string][][]int64{},
		PeopleHistories:    [][][]int64{h1, h1},
		PeopleMatrix:       nil,
		tickSize:           24 * time.Hour,
		reversedPeopleDict: []string{"one", "three"},
		sampling:           15, // 3
		granularity:        20, // 3
	}
	c1 := core.CommonAnalysisResult{
		BeginTime:     600566400, // 1989 Jan 12
		EndTime:       604540800, // 1989 February 27
		CommitsNumber: 10,
		RunTime:       100000,
	}
	res2 := BurndownResult{
		GlobalHistory:      h2,
		FileHistories:      nil,
		PeopleHistories:    [][][]int64{h2, h2},
		PeopleMatrix:       nil,
		tickSize:           24 * time.Hour,
		reversedPeopleDict: []string{"one", "two"},
		sampling:           14,
		granularity:        19,
	}
	c2 := core.CommonAnalysisResult{
		BeginTime:     601084800, // 1989 Jan 18
		EndTime:       605923200, // 1989 March 15
		CommitsNumber: 10,
		RunTime:       100000,
	}
	bd := BurndownAnalysis{
		TickSize: 24 * time.Hour,
	}
	merged := bd.MergeResults(res1, res2, &c1, &c2).(BurndownResult)
	mh := [][]int64{
		{560, 0, 0, 0},
		{851, 572, 0, 0},
		{704, 995, 217, 0},
		{605, 767, 670, 0},
		{575, 709, 685, 178},
	}
	assert.Equal(t, merged.reversedPeopleDict, []string{"one", "three", "two"})
	assert.Equal(t, merged.PeopleHistories[0], mh)
	mh = [][]int64{
		{46, 0, 0, 0},
		{43, 66, 0, 0},
		{30, 106, 39, 0},
		{28, 46, 75, 0},
		{28, 46, 75, 0},
	}
	assert.Equal(t, merged.PeopleHistories[1], mh)
	mh = [][]int64{
		{514, 0, 0, 0},
		{808, 506, 0, 0},
		{674, 889, 177, 0},
		{576, 720, 595, 0},
		{547, 663, 610, 178},
	}
	assert.Equal(t, merged.PeopleHistories[2], mh)
	assert.Nil(t, merged.PeopleMatrix)
	assert.Nil(t, bd.serializeBinary(&merged, ioutil.Discard))
}

func TestBurndownHandleRenameCycle(t *testing.T) {
	bd := BurndownAnalysis{
		TrackFiles: true,
		renames: map[string]string{
			"one":   "two",
			"two":   "three",
			"three": "one",
		},
		fileHistories: map[string]sparseHistory{
			"two": {},
		},
		files: map[string]*burndown.File{
			"one": {},
		},
	}
	assert.Nil(t, bd.handleRename("one", "three"))
	assert.Equal(t, bd.renames, map[string]string{
		"one":   "three",
		"two":   "three",
		"three": "one",
	})
	assert.Equal(t, bd.fileHistories, map[string]sparseHistory{
		"two":   {},
		"three": {},
	})
	assert.Equal(t, bd.files, map[string]*burndown.File{
		"three": {},
	})
}

func TestBurndownResultGetters(t *testing.T) {
	br := BurndownResult{tickSize: time.Hour, reversedPeopleDict: []string{"one", "two"}}
	assert.Equal(t, br.tickSize, br.GetTickSize())
	assert.Equal(t, br.GetIdentities(), br.reversedPeopleDict)
}
