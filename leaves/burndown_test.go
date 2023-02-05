package leaves

import (
	"bytes"
	"errors"
	"github.com/cyraxred/hercules/internal/linehistory"
	"github.com/cyraxred/hercules/internal/pb"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/cyraxred/hercules/internal/burndown"
	"github.com/cyraxred/hercules/internal/core"
	"github.com/cyraxred/hercules/internal/test/fixtures"

	items "github.com/cyraxred/hercules/internal/plumbing"
	"github.com/cyraxred/hercules/internal/plumbing/identity"
	"github.com/cyraxred/hercules/internal/test"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func LineHistoryAnalyser() *linehistory.LineHistoryAnalyser {
	lh := &linehistory.LineHistoryAnalyser{}
	_ = lh.Initialize(test.Repository)
	return lh
}

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
	required := [...]string{linehistory.DependencyLineHistory, identity.DependencyAuthor}
	for _, name := range required {
		assert.Contains(t, bd.Requires(), name)
	}
	opts := bd.ListConfigurationOptions()
	matches := 0
	for _, opt := range opts {
		switch opt.Name {
		case ConfigBurndownGranularity, ConfigBurndownSampling, ConfigBurndownTrackFiles,
			ConfigBurndownTrackPeople:
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
	facts[items.FactTickSize] = 24 * time.Hour
	facts[identity.FactIdentityDetectorPeopleCount] = 5
	facts[identity.FactIdentityDetectorReversedPeopleDict] = bd.Requires()
	assert.Nil(t, bd.Configure(facts))
	assert.Equal(t, bd.Granularity, 100)
	assert.Equal(t, bd.Sampling, 200)
	assert.Equal(t, bd.TrackFiles, true)
	assert.Equal(t, bd.PeopleNumber, 5)
	assert.Equal(t, bd.tickSize, 24*time.Hour)
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
	assert.Nil(t, bd.Initialize(test.Repository))
	assert.Equal(t, bd.Sampling, DefaultBurndownGranularity)
	assert.Equal(t, bd.Granularity, DefaultBurndownGranularity)
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
	{
		result, err := fd.Consume(deps)
		assert.Nil(t, err)
		deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]
		deps[core.DependencyCommit], _ = test.Repository.CommitObject(plumbing.NewHash(
			"cce947b98a050c6d356bc6ba95030254914027b1"))
	}

	lh := LineHistoryAnalyser()
	{
		result, err := lh.Consume(deps)
		assert.Nil(t, err)
		deps[linehistory.DependencyLineHistory] = result[linehistory.DependencyLineHistory]
	}

	bd := BurndownAnalysis{
		Granularity:  30,
		Sampling:     30,
		PeopleNumber: 2,
		TrackFiles:   true,
	}

	totalLines := int64(0)

	{
		assert.Nil(t, bd.Initialize(test.Repository))

		result, err := bd.Consume(deps)
		assert.Nil(t, result)
		assert.Nil(t, err)

		expectedFiles := map[string]int64{
			"cmd/hercules/main.go": 207,
			"analyser.go":          926,
			".travis.yml":          12,
		}

		for _, v := range expectedFiles {
			totalLines += v
		}
		assert.Len(t, bd.peopleHistories, 2)

		assert.Equal(t, bd.peopleHistories[0][0].deltas[0], totalLines)
		//assert.Equal(t, bd.peopleHistories[0][0].totalInsert, totalLines)
		//assert.Equal(t, bd.peopleHistories[0][0].totalDelete, int64(0))

		assert.Len(t, bd.globalHistory, 1)
		assert.Equal(t, bd.globalHistory[0].deltas[0], totalLines)
		//assert.Equal(t, bd.globalHistory[0].totalInsert, totalLines)
		//assert.Equal(t, bd.globalHistory[0].totalDelete, int64(0))

		assert.Len(t, bd.fileHistories, len(expectedFiles))
		//for k, v := range expectedFiles {
		//	assert.Equal(t, bd.fileHistories[k][0].totalInsert, v)
		//	assert.Equal(t, bd.fileHistories[k][0].totalDelete, int64(0))
		//}
	}

	deps[identity.DependencyAuthor] = 1

	{
		bd2 := BurndownAnalysis{
			Granularity: 30,
			Sampling:    0,
		}
		assert.Nil(t, bd2.Initialize(test.Repository))
		_, err := bd2.Consume(deps)
		assert.Nil(t, err)
		assert.Len(t, bd2.peopleHistories, 0)
		assert.Len(t, bd2.fileHistories, 0)
	}

	// stage 2
	// 2b1ed978194a94edeabbca6de7ff3b5771d4d665
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
	{
		fd = fixtures.FileDiff()
		result, err := fd.Consume(deps)
		assert.Nil(t, err)
		deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]
	}

	{
		result, err := lh.Consume(deps)
		assert.Nil(t, err)
		deps[linehistory.DependencyLineHistory] = result[linehistory.DependencyLineHistory]
	}

	{
		result, err := bd.Consume(deps)
		assert.Nil(t, result)
		assert.Nil(t, err)
	}

	assert.Len(t, bd.peopleHistories, 2)
	assert.Equal(t, bd.peopleHistories[0][0].deltas[0], totalLines)
	//assert.Equal(t, bd.peopleHistories[0][0].totalInsert, totalLines)
	//assert.Equal(t, bd.peopleHistories[0][0].totalDelete, int64(0))

	assert.Equal(t, len(bd.peopleHistories[0][30].deltas), 1)
	assert.Equal(t, bd.peopleHistories[0][30].deltas[0], int64(-681))
	//assert.Equal(t, bd.peopleHistories[0][30].totalInsert, int64(0))
	//assert.Equal(t, bd.peopleHistories[0][30].totalDelete, int64(-681))

	assert.Equal(t, len(bd.peopleHistories[1][30].deltas), 1)
	assert.Equal(t, bd.peopleHistories[1][30].deltas[30], int64(369))
	//assert.Equal(t, bd.peopleHistories[1][30].totalInsert, int64(369))
	//assert.Equal(t, bd.peopleHistories[1][30].totalDelete, int64(0))

	assert.Len(t, bd.globalHistory, 2)
	assert.Equal(t, len(bd.globalHistory[0].deltas), 1)
	assert.Equal(t, bd.globalHistory[0].deltas[0], totalLines)
	//assert.Equal(t, bd.globalHistory[0].totalInsert, totalLines)
	//assert.Equal(t, bd.globalHistory[0].totalDelete, int64(0))

	assert.Equal(t, len(bd.globalHistory[30].deltas), 2)
	assert.Equal(t, bd.globalHistory[30].deltas[0], int64(-681))
	assert.Equal(t, bd.globalHistory[30].deltas[30], int64(369))
	//assert.Equal(t, bd.globalHistory[30].totalInsert, int64(369))
	//assert.Equal(t, bd.globalHistory[30].totalDelete, int64(-681))

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

func prepareBDForSerialization(t *testing.T, firstAuthor, secondAuthor int) (
	BurndownResult, *BurndownAnalysis) {
	bd := BurndownAnalysis{
		Granularity:  30,
		Sampling:     30,
		PeopleNumber: 2,
		TrackFiles:   true,
		tickSize:     24 * time.Hour,
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

	fd := fixtures.FileDiff()
	{
		result, err := fd.Consume(deps)
		assert.Nil(t, err)
		deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]
	}

	lh := LineHistoryAnalyser()
	{
		result, err := lh.Consume(deps)
		assert.Nil(t, err)
		deps[linehistory.DependencyLineHistory] = result[linehistory.DependencyLineHistory]
	}

	{
		_, err := bd.Consume(deps)
		assert.Nil(t, err)
	}

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

	{
		result, err := fd.Consume(deps)
		assert.Nil(t, err)
		deps[items.DependencyFileDiff] = result[items.DependencyFileDiff]
	}

	{
		result, err := lh.Consume(deps)
		assert.Nil(t, err)
		deps[linehistory.DependencyLineHistory] = result[linehistory.DependencyLineHistory]
	}

	{
		bd.reversedPeopleDict = append([]string{}, "one@srcd", "two@srcd")
		_, err := bd.Consume(deps)
		assert.Nil(t, err)
	}

	out := bd.Finalize().(BurndownResult)
	return out, &bd
}

func TestBurndownSerialize(t *testing.T) {
	out, _ := prepareBDForSerialization(t, 0, 1)
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
	out, _ := prepareBDForSerialization(t, 0, core.AuthorMissing)
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
		tickSize: 24 * time.Hour,
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
		tickSize: 24 * time.Hour,
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
		tickSize: 24 * time.Hour,
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
	res2.FileHistories = map[string]burndown.DenseHistory{"test": res2.GlobalHistory}
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
		globalHistory: sparseHistory{0: sparseHistoryEntry{deltas: map[int]int64{0: 10}}},
		fileHistories: map[core.FileId]sparseHistory{1: {}},
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
		tickSize: 24 * time.Hour,
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

func TestBurndownResultGetters(t *testing.T) {
	br := BurndownResult{tickSize: time.Hour, reversedPeopleDict: []string{"one", "two"}}
	assert.Equal(t, br.tickSize, br.GetTickSize())
	assert.Equal(t, br.GetIdentities(), br.reversedPeopleDict)
}
