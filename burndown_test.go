package hercules

import (
	"bytes"
	"io"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v3/pb"
)

func TestBurndownMeta(t *testing.T) {
	burndown := BurndownAnalysis{}
	assert.Equal(t, burndown.Name(), "Burndown")
	assert.Equal(t, len(burndown.Provides()), 0)
	required := [...]string{"file_diff", "changes", "blob_cache", "day", "author"}
	for _, name := range required {
		assert.Contains(t, burndown.Requires(), name)
	}
	opts := burndown.ListConfigurationOptions()
	matches := 0
	for _, opt := range opts {
		switch opt.Name {
		case ConfigBurndownGranularity, ConfigBurndownSampling, ConfigBurndownTrackFiles,
			ConfigBurndownTrackPeople, ConfigBurndownDebug:
			matches++
		}
	}
	assert.Len(t, opts, matches)
	assert.Equal(t, burndown.Flag(), "burndown")
}

func TestBurndownConfigure(t *testing.T) {
	burndown := BurndownAnalysis{}
	facts := map[string]interface{}{}
	facts[ConfigBurndownGranularity] = 100
	facts[ConfigBurndownSampling] = 200
	facts[ConfigBurndownTrackFiles] = true
	facts[ConfigBurndownTrackPeople] = true
	facts[ConfigBurndownDebug] = true
	facts[FactIdentityDetectorPeopleCount] = 5
	facts[FactIdentityDetectorReversedPeopleDict] = burndown.Requires()
	burndown.Configure(facts)
	assert.Equal(t, burndown.Granularity, 100)
	assert.Equal(t, burndown.Sampling, 200)
	assert.Equal(t, burndown.TrackFiles, true)
	assert.Equal(t, burndown.PeopleNumber, 5)
	assert.Equal(t, burndown.Debug, true)
	assert.Equal(t, burndown.reversedPeopleDict, burndown.Requires())
	facts[ConfigBurndownTrackPeople] = false
	facts[FactIdentityDetectorPeopleCount] = 50
	burndown.Configure(facts)
	assert.Equal(t, burndown.PeopleNumber, 0)
	facts = map[string]interface{}{}
	burndown.Configure(facts)
	assert.Equal(t, burndown.Granularity, 100)
	assert.Equal(t, burndown.Sampling, 200)
	assert.Equal(t, burndown.TrackFiles, true)
	assert.Equal(t, burndown.PeopleNumber, 0)
	assert.Equal(t, burndown.Debug, true)
	assert.Equal(t, burndown.reversedPeopleDict, burndown.Requires())
}

func TestBurndownRegistration(t *testing.T) {
	tp, exists := Registry.registered[(&BurndownAnalysis{}).Name()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "BurndownAnalysis")
	tp, exists = Registry.flags[(&BurndownAnalysis{}).Flag()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "BurndownAnalysis")
}

func TestBurndownConsumeFinalize(t *testing.T) {
	burndown := BurndownAnalysis{
		Granularity:  30,
		Sampling:     30,
		PeopleNumber: 2,
		TrackFiles:   true,
	}
	burndown.Initialize(testRepository)
	deps := map[string]interface{}{}

	// stage 1
	deps["author"] = 0
	deps["day"] = 0
	cache := map[plumbing.Hash]*object.Blob{}
	hash := plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("dc248ba2b22048cc730c571a748e8ffcf7085ab9")
	cache[hash], _ = testRepository.BlobObject(hash)
	deps["blob_cache"] = cache
	changes := make(object.Changes, 3)
	treeFrom, _ := testRepository.TreeObject(plumbing.NewHash(
		"a1eb2ea76eb7f9bfbde9b243861474421000eb96"))
	treeTo, _ := testRepository.TreeObject(plumbing.NewHash(
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
	deps["changes"] = changes
	fd := fixtureFileDiff()
	result, err := fd.Consume(deps)
	assert.Nil(t, err)
	deps["file_diff"] = result["file_diff"]
	result, err = burndown.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Equal(t, burndown.previousDay, 0)
	assert.Equal(t, len(burndown.files), 3)
	assert.Equal(t, burndown.files["cmd/hercules/main.go"].Len(), 207)
	assert.Equal(t, burndown.files["analyser.go"].Len(), 926)
	assert.Equal(t, burndown.files[".travis.yml"].Len(), 12)
	assert.Equal(t, len(burndown.people), 2)
	assert.Equal(t, burndown.people[0][0], int64(12+207+926))
	assert.Equal(t, len(burndown.globalStatus), 1)
	assert.Equal(t, burndown.globalStatus[0], int64(12+207+926))
	assert.Equal(t, len(burndown.globalHistory), 0)
	assert.Equal(t, len(burndown.fileHistories), 0)
	burndown2 := BurndownAnalysis{
		Granularity: 30,
		Sampling:    0,
	}
	burndown2.Initialize(testRepository)
	_, err = burndown2.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(burndown2.people), 0)
	assert.Equal(t, len(burndown2.peopleHistories), 0)
	assert.Equal(t, len(burndown2.fileHistories), 0)

	// stage 2
	// 2b1ed978194a94edeabbca6de7ff3b5771d4d665
	deps["author"] = 1
	deps["day"] = 30
	cache = map[plumbing.Hash]*object.Blob{}
	hash = plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("29c9fafd6a2fae8cd20298c3f60115bc31a4c0f2")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("f7d918ec500e2f925ecde79b51cc007bac27de72")
	cache[hash], _ = testRepository.BlobObject(hash)
	deps["blob_cache"] = cache
	changes = make(object.Changes, 3)
	treeFrom, _ = testRepository.TreeObject(plumbing.NewHash(
		"96c6ece9b2f3c7c51b83516400d278dea5605100"))
	treeTo, _ = testRepository.TreeObject(plumbing.NewHash(
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
	deps["changes"] = changes
	fd = fixtureFileDiff()
	result, err = fd.Consume(deps)
	assert.Nil(t, err)
	deps["file_diff"] = result["file_diff"]
	result, err = burndown.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Equal(t, burndown.previousDay, 30)
	assert.Equal(t, len(burndown.files), 2)
	assert.Equal(t, burndown.files["cmd/hercules/main.go"].Len(), 290)
	assert.Equal(t, burndown.files["burndown.go"].Len(), 543)
	assert.Equal(t, len(burndown.people), 2)
	assert.Equal(t, len(burndown.globalStatus), 2)
	assert.Equal(t, burndown.globalStatus[0], int64(464))
	assert.Equal(t, burndown.globalStatus[1], int64(0))
	assert.Equal(t, len(burndown.globalHistory), 1)
	assert.Equal(t, len(burndown.globalHistory[0]), 2)
	assert.Equal(t, len(burndown.fileHistories), 3)
	out := burndown.Finalize().(BurndownResult)
	/*
		GlobalHistory   [][]int64
		FileHistories   map[string][][]int64
		PeopleHistories [][][]int64
		PeopleMatrix    [][]int64
	*/
	assert.Equal(t, len(out.GlobalHistory), 2)
	for i := 0; i < 2; i++ {
		assert.Equal(t, len(out.GlobalHistory[i]), 2)
	}
	assert.Equal(t, len(out.GlobalHistory), 2)
	assert.Equal(t, out.GlobalHistory[0][0], int64(1145))
	assert.Equal(t, out.GlobalHistory[0][1], int64(0))
	assert.Equal(t, out.GlobalHistory[1][0], int64(464))
	assert.Equal(t, out.GlobalHistory[1][1], int64(369))
	assert.Equal(t, len(out.FileHistories), 2)
	assert.Equal(t, len(out.FileHistories["cmd/hercules/main.go"]), 2)
	assert.Equal(t, len(out.FileHistories["burndown.go"]), 2)
	assert.Equal(t, len(out.FileHistories["cmd/hercules/main.go"][0]), 2)
	assert.Equal(t, len(out.FileHistories["burndown.go"][0]), 2)
	assert.Equal(t, len(out.PeopleMatrix), 2)
	assert.Equal(t, len(out.PeopleMatrix[0]), 4)
	assert.Equal(t, len(out.PeopleMatrix[1]), 4)
	assert.Equal(t, out.PeopleMatrix[0][0], int64(1145))
	assert.Equal(t, out.PeopleMatrix[0][1], int64(0))
	assert.Equal(t, out.PeopleMatrix[0][2], int64(0))
	assert.Equal(t, out.PeopleMatrix[0][3], int64(-681))
	assert.Equal(t, out.PeopleMatrix[1][0], int64(369))
	assert.Equal(t, out.PeopleMatrix[1][1], int64(0))
	assert.Equal(t, out.PeopleMatrix[1][2], int64(0))
	assert.Equal(t, out.PeopleMatrix[1][3], int64(0))
	assert.Equal(t, len(out.PeopleHistories), 2)
	for i := 0; i < 2; i++ {
		assert.Equal(t, len(out.PeopleHistories[i]), 2)
		assert.Equal(t, len(out.PeopleHistories[i][0]), 2)
		assert.Equal(t, len(out.PeopleHistories[i][1]), 2)
	}
}

func TestBurndownAnalysisSerialize(t *testing.T) {
	burndown := BurndownAnalysis{
		Granularity:  30,
		Sampling:     30,
		PeopleNumber: 2,
		TrackFiles:   true,
	}
	burndown.Initialize(testRepository)
	deps := map[string]interface{}{}
	// stage 1
	deps["author"] = 0
	deps["day"] = 0
	cache := map[plumbing.Hash]*object.Blob{}
	hash := plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("dc248ba2b22048cc730c571a748e8ffcf7085ab9")
	cache[hash], _ = testRepository.BlobObject(hash)
	deps["blob_cache"] = cache
	changes := make(object.Changes, 3)
	treeFrom, _ := testRepository.TreeObject(plumbing.NewHash(
		"a1eb2ea76eb7f9bfbde9b243861474421000eb96"))
	treeTo, _ := testRepository.TreeObject(plumbing.NewHash(
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
	deps["changes"] = changes
	fd := fixtureFileDiff()
	result, _ := fd.Consume(deps)
	deps["file_diff"] = result["file_diff"]
	burndown.Consume(deps)

	// stage 2
	// 2b1ed978194a94edeabbca6de7ff3b5771d4d665
	deps["author"] = 1
	deps["day"] = 30
	cache = map[plumbing.Hash]*object.Blob{}
	hash = plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("29c9fafd6a2fae8cd20298c3f60115bc31a4c0f2")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("f7d918ec500e2f925ecde79b51cc007bac27de72")
	cache[hash], _ = testRepository.BlobObject(hash)
	deps["blob_cache"] = cache
	changes = make(object.Changes, 3)
	treeFrom, _ = testRepository.TreeObject(plumbing.NewHash(
		"96c6ece9b2f3c7c51b83516400d278dea5605100"))
	treeTo, _ = testRepository.TreeObject(plumbing.NewHash(
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
	deps["changes"] = changes
	fd = fixtureFileDiff()
	result, _ = fd.Consume(deps)
	deps["file_diff"] = result["file_diff"]
	people := [...]string{"one@srcd", "two@srcd"}
	burndown.reversedPeopleDict = people[:]
	burndown.Consume(deps)
	out := burndown.Finalize().(BurndownResult)

	buffer := &bytes.Buffer{}
	burndown.Serialize(out, false, buffer)
	assert.Equal(t, buffer.String(), `  granularity: 30
  sampling: 30
  "project": |-
    1145    0
     464  369
  files:
    "burndown.go": |-
      0     0
      293 250
    "cmd/hercules/main.go": |-
      207   0
      171 119
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
	burndown.Serialize(out, true, buffer)
	msg := pb.BurndownAnalysisResults{}
	proto.Unmarshal(buffer.Bytes(), &msg)
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
	assert.Len(t, msg.Files[0].Rows[0].Columns, 0)
	assert.Len(t, msg.Files[0].Rows[1].Columns, 2)
	assert.Equal(t, msg.Files[0].Rows[1].Columns[0], uint32(293))
	assert.Equal(t, msg.Files[0].Rows[1].Columns[1], uint32(250))
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

type panickingCloser struct {
}

func (c panickingCloser) Close() error {
	return io.EOF
}

func TestCheckClose(t *testing.T) {
	closer := panickingCloser{}
	assert.Panics(t, func() { checkClose(closer) })
}
