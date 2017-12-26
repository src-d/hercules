package hercules

import (
	"bytes"
	"io"
	"io/ioutil"
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

func TestBurndownInitialize(t *testing.T) {
	burndown := BurndownAnalysis{}
	burndown.Sampling = -10
	burndown.Granularity = DefaultBurndownGranularity
	burndown.Initialize(testRepository)
	assert.Equal(t, burndown.Sampling, DefaultBurndownGranularity)
	assert.Equal(t, burndown.Granularity, DefaultBurndownGranularity)
	burndown.Sampling = 0
	burndown.Granularity = DefaultBurndownGranularity - 1
	burndown.Initialize(testRepository)
	assert.Equal(t, burndown.Sampling, DefaultBurndownGranularity-1)
	assert.Equal(t, burndown.Granularity, DefaultBurndownGranularity-1)
	burndown.Sampling = DefaultBurndownGranularity - 1
	burndown.Granularity = -10
	burndown.Initialize(testRepository)
	assert.Equal(t, burndown.Sampling, DefaultBurndownGranularity-1)
	assert.Equal(t, burndown.Granularity, DefaultBurndownGranularity)
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
	/*for _, row := range daily {
		fmt.Println(row)
	}*/
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
	}
	c1 := CommonAnalysisResult{
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
	res1.FileHistories["file1"] = res1.GlobalHistory
	res1.FileHistories["file2"] = res1.GlobalHistory
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
		GlobalHistory:      [][]int64{},
		FileHistories:      map[string][][]int64{},
		PeopleHistories:    [][][]int64{},
		PeopleMatrix:       [][]int64{},
		reversedPeopleDict: people2[:],
		sampling:           14,
		granularity:        19,
	}
	c2 := CommonAnalysisResult{
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
	res2.FileHistories["file2"] = res2.GlobalHistory
	res2.FileHistories["file3"] = res2.GlobalHistory
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
	burndown := BurndownAnalysis{}
	merged := burndown.MergeResults(res1, res2, &c1, &c2).(BurndownResult)
	assert.Equal(t, merged.granularity, 19)
	assert.Equal(t, merged.sampling, 14)
	assert.Len(t, merged.GlobalHistory, 5)
	for _, row := range merged.GlobalHistory {
		assert.Len(t, row, 4)
	}
	assert.Equal(t, merged.FileHistories["file1"], res1.GlobalHistory)
	assert.Equal(t, merged.FileHistories["file2"], merged.GlobalHistory)
	assert.Equal(t, merged.FileHistories["file3"], res2.GlobalHistory)
	assert.Len(t, merged.reversedPeopleDict, 3)
	assert.Equal(t, merged.PeopleHistories[0], res1.GlobalHistory)
	assert.Equal(t, merged.PeopleHistories[1], merged.GlobalHistory)
	assert.Equal(t, merged.PeopleHistories[2], res2.GlobalHistory)
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
	burndown.serializeBinary(&merged, ioutil.Discard)
}

func TestBurndownMergeNils(t *testing.T) {
	res1 := BurndownResult{
		GlobalHistory:      [][]int64{},
		FileHistories:      map[string][][]int64{},
		PeopleHistories:    [][][]int64{},
		PeopleMatrix:       [][]int64{},
		reversedPeopleDict: []string{},
		sampling:           15,
		granularity:        20,
	}
	c1 := CommonAnalysisResult{
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
		reversedPeopleDict: nil,
		sampling:           14,
		granularity:        19,
	}
	c2 := CommonAnalysisResult{
		BeginTime:     601084800, // 1989 Jan 18
		EndTime:       605923200, // 1989 March 15
		CommitsNumber: 10,
		RunTime:       100000,
	}
	burndown := BurndownAnalysis{}
	merged := burndown.MergeResults(res1, res2, &c1, &c2).(BurndownResult)
	assert.Equal(t, merged.granularity, 19)
	assert.Equal(t, merged.sampling, 14)
	assert.Nil(t, merged.GlobalHistory)
	assert.Nil(t, merged.FileHistories)
	assert.Nil(t, merged.PeopleHistories)
	assert.Nil(t, merged.PeopleMatrix)
	burndown.serializeBinary(&merged, ioutil.Discard)

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
	merged = burndown.MergeResults(res1, res2, &c1, &c2).(BurndownResult)
	mgh := [5][4]int64{
		{0, 0, 0, 0},
		{578, 0, 0, 0},
		{798, 546, 0, 0},
		{664, 884, 222, 0},
		{547, 663, 610, 178},
	}
	mgh2 := [...][]int64{
		mgh[0][:], mgh[1][:], mgh[2][:], mgh[3][:], mgh[4][:],
	}
	mgh3 := mgh2[:]
	assert.Equal(t, mgh3, merged.GlobalHistory)
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
	burndown.serializeBinary(&merged, ioutil.Discard)
}
