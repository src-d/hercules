package leaves

import (
	"bytes"
	"sort"
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

func TestCommitsMeta(t *testing.T) {
	ca := CommitsAnalysis{}
	assert.Equal(t, ca.Name(), "CommitsStat")
	assert.Len(t, ca.Provides(), 0)
	required := [...]string{identity.DependencyAuthor, items.DependencyLanguages, items.DependencyLineStats}
	for _, name := range required {
		assert.Contains(t, ca.Requires(), name)
	}
	opts := ca.ListConfigurationOptions()
	assert.Len(t, opts, 0)
	assert.Equal(t, ca.Flag(), "commits-stat")
	logger := core.NewLogger()
	assert.NoError(t, ca.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, ca.l)
}

func TestCommitsRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&CommitsAnalysis{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "CommitsStat")
	leaves := core.Registry.GetLeaves()
	matched := false
	for _, tp := range leaves {
		if tp.Flag() == (&CommitsAnalysis{}).Flag() {
			matched = true
			break
		}
	}
	assert.True(t, matched)
}

func TestCommitsConfigure(t *testing.T) {
	ca := CommitsAnalysis{}
	facts := map[string]interface{}{}
	facts[identity.FactIdentityDetectorReversedPeopleDict] = ca.Requires()
	assert.Nil(t, ca.Configure(facts))
	assert.Equal(t, ca.reversedPeopleDict, ca.Requires())
}

func TestCommitsConsume(t *testing.T) {
	ca := CommitsAnalysis{}
	assert.Nil(t, ca.Initialize(test.Repository))
	deps := map[string]interface{}{}

	// stage 1
	deps[identity.DependencyAuthor] = 0
	cache := map[plumbing.Hash]*items.CachedBlob{}
	AddHash(t, cache, "291286b4ac41952cbd1389fda66420ec03c1a9fe")
	AddHash(t, cache, "c29112dbd697ad9b401333b80c18a63951bc18d9")
	AddHash(t, cache, "baa64828831d174f40140e4b3cfa77d1e917a2c1")
	AddHash(t, cache, "dc248ba2b22048cc730c571a748e8ffcf7085ab9")
	deps[items.DependencyBlobCache] = cache
	deps[items.DependencyLanguages] = map[plumbing.Hash]string{
		plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe"): "Go",
		plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9"): "Go",
		plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1"): "Go",
		plumbing.NewHash("dc248ba2b22048cc730c571a748e8ffcf7085ab9"): "Go",
	}
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
	lsc := &items.LinesStatsCalculator{}
	lscres, err := lsc.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyLineStats] = lscres[items.DependencyLineStats]

	result, err = ca.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Len(t, ca.commits, 1)
	c := ca.commits[0]
	assert.Equal(t, "cce947b98a050c6d356bc6ba95030254914027b1", c.Hash)
	assert.Equal(t, int64(1481563829), c.When)
	assert.Equal(t, 0, c.Author)
	assert.Len(t, c.Files, 3)
	sort.Slice(c.Files, func(i, j int) bool { return c.Files[i].Name < c.Files[j].Name })
	assert.Equal(t, ".travis.yml", c.Files[0].Name)
	assert.Equal(t, "Go", c.Files[0].Language)
	assert.Equal(t, 12, c.Files[0].Added)
	assert.Equal(t, 0, c.Files[0].Removed)
	assert.Equal(t, 0, c.Files[0].Changed)
	assert.Equal(t, "analyser.go", c.Files[1].Name)
	assert.Equal(t, "Go", c.Files[1].Language)
	assert.Equal(t, 628, c.Files[1].Added)
	assert.Equal(t, 9, c.Files[1].Removed)
	assert.Equal(t, 67, c.Files[1].Changed)
	assert.Equal(t, "cmd/hercules/main.go", c.Files[2].Name)
	assert.Equal(t, "Go", c.Files[2].Language)
	assert.Equal(t, 207, c.Files[2].Added)
	assert.Equal(t, 0, c.Files[2].Removed)
	assert.Equal(t, 0, c.Files[2].Changed)

	deps[core.DependencyIsMerge] = true
	lscres, err = lsc.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyLineStats] = lscres[items.DependencyLineStats]
	result, err = ca.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Len(t, ca.commits, 1)
	c = ca.commits[0]
	assert.Equal(t, "cce947b98a050c6d356bc6ba95030254914027b1", c.Hash)
	assert.Equal(t, int64(1481563829), c.When)
	assert.Equal(t, 0, c.Author)
	assert.Len(t, c.Files, 3)
	sort.Slice(c.Files, func(i, j int) bool { return c.Files[i].Name < c.Files[j].Name })
	assert.Equal(t, ".travis.yml", c.Files[0].Name)
	assert.Equal(t, "Go", c.Files[0].Language)
	assert.Equal(t, 12, c.Files[0].Added)
	assert.Equal(t, 0, c.Files[0].Removed)
	assert.Equal(t, 0, c.Files[0].Changed)
	assert.Equal(t, "analyser.go", c.Files[1].Name)
	assert.Equal(t, "Go", c.Files[1].Language)
	assert.Equal(t, 628, c.Files[1].Added)
	assert.Equal(t, 9, c.Files[1].Removed)
	assert.Equal(t, 67, c.Files[1].Changed)
	assert.Equal(t, "cmd/hercules/main.go", c.Files[2].Name)
	assert.Equal(t, "Go", c.Files[2].Language)
	assert.Equal(t, 207, c.Files[2].Added)
	assert.Equal(t, 0, c.Files[2].Removed)
	assert.Equal(t, 0, c.Files[2].Changed)
}

func fixtureCommits() *CommitsAnalysis {
	ca := CommitsAnalysis{}
	ca.Initialize(test.Repository)
	ca.commits = []*CommitStat{
		{
			Hash:   "cce947b98a050c6d356bc6ba95030254914027b1",
			When:   1481563829,
			Author: 0,
			Files: []FileStat{
				{
					Name:     ".travis.yml",
					Language: "Yaml",
					LineStats: items.LineStats{
						Added:   12,
						Removed: 0,
						Changed: 0,
					},
				},
				{
					Name:     "analyser.go",
					Language: "Go",
					LineStats: items.LineStats{
						Added:   628,
						Removed: 9,
						Changed: 67,
					},
				},
			},
		},
		{
			Hash:   "c29112dbd697ad9b401333b80c18a63951bc18d9",
			When:   1481563999,
			Author: 1,
			Files: []FileStat{
				{
					Name:     "cmd/hercules/main.go",
					Language: "Go",
					LineStats: items.LineStats{
						Added:   1,
						Removed: 0,
						Changed: 0,
					},
				},
			},
		},
	}
	people := [...]string{"one@srcd", "two@srcd"}
	ca.reversedPeopleDict = people[:]

	return &ca
}

func TestCommitsFinalize(t *testing.T) {
	ca := fixtureCommits()
	x := ca.Finalize().(CommitsResult)
	assert.Equal(t, x.Commits, ca.commits)
	assert.Equal(t, x.reversedPeopleDict, ca.reversedPeopleDict)
}

func TestCommitsSerialize(t *testing.T) {
	ca := fixtureCommits()
	res := ca.Finalize().(CommitsResult)
	buffer := &bytes.Buffer{}
	err := ca.Serialize(res, false, buffer)
	assert.Nil(t, err)
	assert.Equal(t, `  commits:
    - hash: cce947b98a050c6d356bc6ba95030254914027b1
      when: 1481563829
      author: 0
      files:
       - name: .travis.yml
         language: Yaml
         stat: [12, 0, 0]
       - name: analyser.go
         language: Go
         stat: [628, 67, 9]
    - hash: c29112dbd697ad9b401333b80c18a63951bc18d9
      when: 1481563999
      author: 1
      files:
       - name: cmd/hercules/main.go
         language: Go
         stat: [1, 0, 0]
  people:
  - "one@srcd"
  - "two@srcd"
`, buffer.String())

	buffer = &bytes.Buffer{}
	err = ca.Serialize(res, true, buffer)
	assert.Nil(t, err)
	msg := pb.CommitsAnalysisResults{}
	assert.Nil(t, proto.Unmarshal(buffer.Bytes(), &msg))
	assert.Equal(t, msg.AuthorIndex, ca.reversedPeopleDict)
	assert.Len(t, msg.Commits, 2)
	assert.Equal(t, msg.Commits[0].Hash, "cce947b98a050c6d356bc6ba95030254914027b1")
	assert.Equal(t, msg.Commits[0].WhenUnixTime, int64(1481563829))
	assert.Equal(t, msg.Commits[0].Author, int32(0))
	assert.Len(t, msg.Commits[0].Files, 2)
	assert.Equal(t, msg.Commits[0].Files[0], &pb.CommitFile{
		Name:     ".travis.yml",
		Stats:    &pb.LineStats{Added: 12, Removed: 0, Changed: 0},
		Language: "Yaml"})
	assert.Equal(t, msg.Commits[0].Files[1], &pb.CommitFile{
		Name:     "analyser.go",
		Stats:    &pb.LineStats{Added: 628, Removed: 9, Changed: 67},
		Language: "Go"})
	assert.Equal(t, msg.Commits[1].Hash, "c29112dbd697ad9b401333b80c18a63951bc18d9")
	assert.Equal(t, msg.Commits[1].WhenUnixTime, int64(1481563999))
	assert.Equal(t, msg.Commits[1].Author, int32(1))
	assert.Len(t, msg.Commits[1].Files, 1)
	assert.Equal(t, msg.Commits[1].Files[0], &pb.CommitFile{
		Name:     "cmd/hercules/main.go",
		Stats:    &pb.LineStats{Added: 1, Removed: 0, Changed: 0},
		Language: "Go"})
}
