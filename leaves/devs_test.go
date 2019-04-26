package leaves

import (
	"bytes"
	"testing"
	"time"

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

func fixtureDevs() *DevsAnalysis {
	d := DevsAnalysis{}
	d.tickSize = 24 * time.Hour
	d.Initialize(test.Repository)
	people := [...]string{"one@srcd", "two@srcd"}
	d.reversedPeopleDict = people[:]
	return &d
}

func TestDevsMeta(t *testing.T) {
	d := fixtureDevs()
	assert.Equal(t, d.Name(), "Devs")
	assert.Equal(t, len(d.Provides()), 0)
	assert.Equal(t, len(d.Requires()), 5)
	assert.Equal(t, d.Requires()[0], identity.DependencyAuthor)
	assert.Equal(t, d.Requires()[1], items.DependencyTreeChanges)
	assert.Equal(t, d.Requires()[2], items.DependencyTick)
	assert.Equal(t, d.Requires()[3], items.DependencyLanguages)
	assert.Equal(t, d.Requires()[4], items.DependencyLineStats)
	assert.Equal(t, d.Flag(), "devs")
	assert.Len(t, d.ListConfigurationOptions(), 1)
	assert.Equal(t, d.ListConfigurationOptions()[0].Name, ConfigDevsConsiderEmptyCommits)
	assert.Equal(t, d.ListConfigurationOptions()[0].Flag, "empty-commits")
	assert.Equal(t, d.ListConfigurationOptions()[0].Type, core.BoolConfigurationOption)
	assert.Equal(t, d.ListConfigurationOptions()[0].Default, false)
	assert.True(t, len(d.Description()) > 0)
	logger := core.NewLogger()
	assert.NoError(t, d.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, d.l)
}

func TestDevsRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&DevsAnalysis{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "Devs")
	leaves := core.Registry.GetLeaves()
	matched := false
	for _, tp := range leaves {
		if tp.Flag() == (&DevsAnalysis{}).Flag() {
			matched = true
			break
		}
	}
	assert.True(t, matched)
}

func TestDevsConfigure(t *testing.T) {
	devs := DevsAnalysis{}
	facts := map[string]interface{}{}
	facts[ConfigDevsConsiderEmptyCommits] = true
	facts[items.FactTickSize] = 3 * time.Hour
	assert.NoError(t, devs.Configure(facts))
	assert.True(t, devs.ConsiderEmptyCommits)
	assert.Equal(t, 3*time.Hour, devs.tickSize)
}

func TestDevsInitialize(t *testing.T) {
	d := fixtureDevs()
	assert.NotNil(t, d.ticks)
	d = &DevsAnalysis{}
	assert.Error(t, d.Initialize(test.Repository))
}

func TestDevsConsumeFinalize(t *testing.T) {
	devs := fixtureDevs()
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

	result, err = devs.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Len(t, devs.ticks, 1)
	day := devs.ticks[0]
	assert.Len(t, day, 1)
	dev := day[0]
	assert.Equal(t, dev.Commits, 1)
	assert.Equal(t, dev.Added, 847)
	assert.Equal(t, dev.Removed, 9)
	assert.Equal(t, dev.Changed, 67)
	assert.Equal(t, dev.Languages["Go"].Added, 847)
	assert.Equal(t, dev.Languages["Go"].Removed, 9)
	assert.Equal(t, dev.Languages["Go"].Changed, 67)

	deps[core.DependencyIsMerge] = true
	lscres, err = lsc.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyLineStats] = lscres[items.DependencyLineStats]
	result, err = devs.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Len(t, devs.ticks, 1)
	day = devs.ticks[0]
	assert.Len(t, day, 1)
	dev = day[0]
	assert.Equal(t, dev.Commits, 2)
	assert.Equal(t, dev.Added, 847)
	assert.Equal(t, dev.Removed, 9)
	assert.Equal(t, dev.Changed, 67)
	assert.Equal(t, dev.Languages["Go"].Added, 847)
	assert.Equal(t, dev.Languages["Go"].Removed, 9)
	assert.Equal(t, dev.Languages["Go"].Changed, 67)

	deps[core.DependencyIsMerge] = false
	deps[identity.DependencyAuthor] = 1
	lscres, err = lsc.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyLineStats] = lscres[items.DependencyLineStats]
	result, err = devs.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Len(t, devs.ticks, 1)
	day = devs.ticks[0]
	assert.Len(t, day, 2)
	for i := 0; i < 2; i++ {
		dev = day[i]
		if i == 0 {
			assert.Equal(t, dev.Commits, 2)
		} else {
			assert.Equal(t, dev.Commits, 1)
		}
		assert.Equal(t, dev.Added, 847)
		assert.Equal(t, dev.Removed, 9)
		assert.Equal(t, dev.Changed, 67)
		assert.Equal(t, dev.Languages["Go"].Added, 847)
		assert.Equal(t, dev.Languages["Go"].Removed, 9)
		assert.Equal(t, dev.Languages["Go"].Changed, 67)
	}

	result, err = devs.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Len(t, devs.ticks, 1)
	day = devs.ticks[0]
	assert.Len(t, day, 2)
	dev = day[0]
	assert.Equal(t, dev.Commits, 2)
	assert.Equal(t, dev.Added, 847)
	assert.Equal(t, dev.Removed, 9)
	assert.Equal(t, dev.Changed, 67)
	assert.Equal(t, dev.Languages["Go"].Added, 847)
	assert.Equal(t, dev.Languages["Go"].Removed, 9)
	assert.Equal(t, dev.Languages["Go"].Changed, 67)
	dev = day[1]
	assert.Equal(t, dev.Commits, 2)
	assert.Equal(t, dev.Added, 847*2)
	assert.Equal(t, dev.Removed, 9*2)
	assert.Equal(t, dev.Changed, 67*2)
	assert.Equal(t, dev.Languages["Go"].Added, 847*2)
	assert.Equal(t, dev.Languages["Go"].Removed, 9*2)
	assert.Equal(t, dev.Languages["Go"].Changed, 67*2)

	deps[items.DependencyTick] = 1
	result, err = devs.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Len(t, devs.ticks, 2)
	day = devs.ticks[0]
	assert.Len(t, day, 2)
	dev = day[0]
	assert.Equal(t, dev.Commits, 2)
	assert.Equal(t, dev.Added, 847)
	assert.Equal(t, dev.Removed, 9)
	assert.Equal(t, dev.Changed, 67)
	assert.Equal(t, dev.Languages["Go"].Added, 847)
	assert.Equal(t, dev.Languages["Go"].Removed, 9)
	assert.Equal(t, dev.Languages["Go"].Changed, 67)
	dev = day[1]
	assert.Equal(t, dev.Commits, 2)
	assert.Equal(t, dev.Added, 847*2)
	assert.Equal(t, dev.Removed, 9*2)
	assert.Equal(t, dev.Changed, 67*2)
	assert.Equal(t, dev.Languages["Go"].Added, 847*2)
	assert.Equal(t, dev.Languages["Go"].Removed, 9*2)
	assert.Equal(t, dev.Languages["Go"].Changed, 67*2)
	day = devs.ticks[1]
	assert.Len(t, day, 1)
	dev = day[1]
	assert.Equal(t, dev.Commits, 1)
	assert.Equal(t, dev.Added, 847)
	assert.Equal(t, dev.Removed, 9)
	assert.Equal(t, dev.Changed, 67)
	assert.Equal(t, dev.Languages["Go"].Added, 847)
	assert.Equal(t, dev.Languages["Go"].Removed, 9)
	assert.Equal(t, dev.Languages["Go"].Changed, 67)
}

func ls(added, removed, changed int) items.LineStats {
	return items.LineStats{Added: added, Removed: removed, Changed: changed}
}

func TestDevsFinalize(t *testing.T) {
	devs := fixtureDevs()
	devs.ticks[1] = map[int]*DevTick{}
	devs.ticks[1][1] = &DevTick{10, ls(20, 30, 40), nil}
	x := devs.Finalize().(DevsResult)
	assert.Equal(t, x.Ticks, devs.ticks)
	assert.Equal(t, x.reversedPeopleDict, devs.reversedPeopleDict)
	assert.Equal(t, 24*time.Hour, devs.tickSize)
}

func TestDevsFork(t *testing.T) {
	devs := fixtureDevs()
	clone := devs.Fork(1)[0].(*DevsAnalysis)
	assert.True(t, devs == clone)
}

func TestDevsSerialize(t *testing.T) {
	devs := fixtureDevs()
	devs.ticks[1] = map[int]*DevTick{}
	devs.ticks[1][0] = &DevTick{10, ls(20, 30, 40), map[string]items.LineStats{"Go": ls(2, 3, 4)}}
	devs.ticks[1][1] = &DevTick{1, ls(2, 3, 4), map[string]items.LineStats{"Go": ls(25, 35, 45)}}
	devs.ticks[10] = map[int]*DevTick{}
	devs.ticks[10][0] = &DevTick{11, ls(21, 31, 41), map[string]items.LineStats{"": ls(12, 13, 14)}}
	devs.ticks[10][identity.AuthorMissing] = &DevTick{
		100, ls(200, 300, 400), map[string]items.LineStats{"Go": ls(32, 33, 34)}}
	res := devs.Finalize().(DevsResult)
	buffer := &bytes.Buffer{}
	err := devs.Serialize(res, false, buffer)
	assert.Nil(t, err)
	assert.Equal(t, `  ticks:
    1:
      0: [10, 20, 30, 40, {Go: [2, 3, 4]}]
      1: [1, 2, 3, 4, {Go: [25, 35, 45]}]
    10:
      0: [11, 21, 31, 41, {none: [12, 13, 14]}]
      -1: [100, 200, 300, 400, {Go: [32, 33, 34]}]
  people:
  - "one@srcd"
  - "two@srcd"
  tick_size: 86400
`, buffer.String())

	buffer = &bytes.Buffer{}
	err = devs.Serialize(res, true, buffer)
	assert.Nil(t, err)
	msg := pb.DevsAnalysisResults{}
	assert.Nil(t, proto.Unmarshal(buffer.Bytes(), &msg))
	assert.Equal(t, msg.DevIndex, devs.reversedPeopleDict)
	assert.Equal(t, int64(24*time.Hour), msg.TickSize)
	assert.Len(t, msg.Ticks, 2)
	assert.Len(t, msg.Ticks[1].Devs, 2)
	assert.Equal(t, msg.Ticks[1].Devs[0], &pb.DevTick{
		Commits: 10, Stats: &pb.LineStats{Added: 20, Removed: 30, Changed: 40},
		Languages: map[string]*pb.LineStats{"Go": {Added: 2, Removed: 3, Changed: 4}}})
	assert.Equal(t, msg.Ticks[1].Devs[1], &pb.DevTick{
		Commits: 1, Stats: &pb.LineStats{Added: 2, Removed: 3, Changed: 4},
		Languages: map[string]*pb.LineStats{"Go": {Added: 25, Removed: 35, Changed: 45}}})
	assert.Len(t, msg.Ticks[10].Devs, 2)
	assert.Equal(t, msg.Ticks[10].Devs[0], &pb.DevTick{
		Commits: 11, Stats: &pb.LineStats{Added: 21, Removed: 31, Changed: 41},
		Languages: map[string]*pb.LineStats{"": {Added: 12, Removed: 13, Changed: 14}}})
	assert.Equal(t, msg.Ticks[10].Devs[-1], &pb.DevTick{
		Commits: 100, Stats: &pb.LineStats{Added: 200, Removed: 300, Changed: 400},
		Languages: map[string]*pb.LineStats{"Go": {Added: 32, Removed: 33, Changed: 34}}})
}

func TestDevsDeserialize(t *testing.T) {
	devs := fixtureDevs()
	devs.ticks[1] = map[int]*DevTick{}
	devs.ticks[1][0] = &DevTick{10, ls(20, 30, 40), map[string]items.LineStats{"Go": ls(12, 13, 14)}}
	devs.ticks[1][1] = &DevTick{1, ls(2, 3, 4), map[string]items.LineStats{"Go": ls(22, 23, 24)}}
	devs.ticks[10] = map[int]*DevTick{}
	devs.ticks[10][0] = &DevTick{11, ls(21, 31, 41), map[string]items.LineStats{"Go": ls(32, 33, 34)}}
	devs.ticks[10][identity.AuthorMissing] = &DevTick{
		100, ls(200, 300, 400), map[string]items.LineStats{"Go": ls(42, 43, 44)}}
	res := devs.Finalize().(DevsResult)
	buffer := &bytes.Buffer{}
	err := devs.Serialize(res, true, buffer)
	assert.Nil(t, err)
	rawres2, err := devs.Deserialize(buffer.Bytes())
	assert.Nil(t, err)
	res2 := rawres2.(DevsResult)
	assert.Equal(t, res, res2)
}

func TestDevsMergeResults(t *testing.T) {
	people1 := [...]string{"1@srcd", "2@srcd"}
	people2 := [...]string{"3@srcd", "1@srcd"}
	r1 := DevsResult{
		Ticks:              map[int]map[int]*DevTick{},
		reversedPeopleDict: people1[:],
		tickSize:           24 * time.Hour,
	}
	r1.Ticks[1] = map[int]*DevTick{}
	r1.Ticks[1][0] = &DevTick{10, ls(20, 30, 40), map[string]items.LineStats{"Go": ls(12, 13, 14)}}
	r1.Ticks[1][1] = &DevTick{1, ls(2, 3, 4), map[string]items.LineStats{"Go": ls(22, 23, 24)}}
	r1.Ticks[10] = map[int]*DevTick{}
	r1.Ticks[10][0] = &DevTick{11, ls(21, 31, 41), nil}
	r1.Ticks[10][identity.AuthorMissing] = &DevTick{
		100, ls(200, 300, 400), map[string]items.LineStats{"Go": ls(32, 33, 34)}}
	r1.Ticks[11] = map[int]*DevTick{}
	r1.Ticks[11][1] = &DevTick{10, ls(20, 30, 40), map[string]items.LineStats{"Go": ls(42, 43, 44)}}
	r2 := DevsResult{
		Ticks:              map[int]map[int]*DevTick{},
		reversedPeopleDict: people2[:],
		tickSize:           22 * time.Hour,
	}
	r2.Ticks[1] = map[int]*DevTick{}
	r2.Ticks[1][0] = &DevTick{10, ls(20, 30, 40), map[string]items.LineStats{"Go": ls(12, 13, 14)}}
	r2.Ticks[1][1] = &DevTick{1, ls(2, 3, 4), map[string]items.LineStats{"Go": ls(22, 23, 24)}}
	r2.Ticks[2] = map[int]*DevTick{}
	r2.Ticks[2][0] = &DevTick{11, ls(21, 31, 41), map[string]items.LineStats{"Go": ls(32, 33, 34)}}
	r2.Ticks[2][identity.AuthorMissing] = &DevTick{
		100, ls(200, 300, 400), map[string]items.LineStats{"Go": ls(42, 43, 44)}}
	r2.Ticks[10] = map[int]*DevTick{}
	r2.Ticks[10][0] = &DevTick{11, ls(21, 31, 41), map[string]items.LineStats{"Go": ls(52, 53, 54)}}
	r2.Ticks[10][identity.AuthorMissing] = &DevTick{
		100, ls(200, 300, 400), map[string]items.LineStats{"Go": ls(62, 63, 64)}}

	devs := fixtureDevs()
	c1 := core.CommonAnalysisResult{BeginTime: 1556224895}
	assert.IsType(t, assert.AnError, devs.MergeResults(r1, r2, &c1, &c1))
	r2.tickSize = r1.tickSize
	rm := devs.MergeResults(r1, r2, &c1, &c1).(DevsResult)
	peoplerm := [...]string{"1@srcd", "2@srcd", "3@srcd"}
	assert.Equal(t, rm.reversedPeopleDict, peoplerm[:])
	assert.Len(t, rm.Ticks, 4)
	assert.Equal(t, rm.Ticks[11], map[int]*DevTick{
		1: {10, ls(20, 30, 40), map[string]items.LineStats{"Go": ls(42, 43, 44)}}})
	assert.Equal(t, rm.Ticks[2], map[int]*DevTick{
		identity.AuthorMissing: {100, ls(200, 300, 400), map[string]items.LineStats{"Go": ls(42, 43, 44)}},
		2:                      {11, ls(21, 31, 41), map[string]items.LineStats{"Go": ls(32, 33, 34)}},
	})
	assert.Equal(t, rm.Ticks[1], map[int]*DevTick{
		0: {11, ls(22, 33, 44), map[string]items.LineStats{"Go": ls(34, 36, 38)}},
		1: {1, ls(2, 3, 4), map[string]items.LineStats{"Go": ls(22, 23, 24)}},
		2: {10, ls(20, 30, 40), map[string]items.LineStats{"Go": ls(12, 13, 14)}},
	})
	assert.Equal(t, rm.Ticks[10], map[int]*DevTick{
		0: {11, ls(21, 31, 41), map[string]items.LineStats{}},
		2: {11, ls(21, 31, 41), map[string]items.LineStats{"Go": ls(52, 53, 54)}},
		identity.AuthorMissing: {
			100 * 2, ls(200*2, 300*2, 400*2), map[string]items.LineStats{"Go": ls(94, 96, 98)}},
	})

	c2 := core.CommonAnalysisResult{BeginTime: 1556224895 + 24*3600}
	rm = devs.MergeResults(r1, r2, &c1, &c2).(DevsResult)
	assert.Len(t, rm.Ticks, 5)
	assert.Equal(t, rm.Ticks[1], map[int]*DevTick{
		0: {10, ls(20, 30, 40), map[string]items.LineStats{"Go": ls(12, 13, 14)}},
		1: {1, ls(2, 3, 4), map[string]items.LineStats{"Go": ls(22, 23, 24)}},
	})
	assert.Equal(t, rm.Ticks[2], map[int]*DevTick{
		2: {10, ls(20, 30, 40), map[string]items.LineStats{"Go": ls(12, 13, 14)}},
		0: {1, ls(2, 3, 4), map[string]items.LineStats{"Go": ls(22, 23, 24)}},
	})
	assert.Equal(t, rm.Ticks[3], map[int]*DevTick{
		2:                      {11, ls(21, 31, 41), map[string]items.LineStats{"Go": ls(32, 33, 34)}},
		identity.AuthorMissing: {100, ls(200, 300, 400), map[string]items.LineStats{"Go": ls(42, 43, 44)}},
	})
	assert.Equal(t, rm.Ticks[10], map[int]*DevTick{
		0:                      {11, ls(21, 31, 41), map[string]items.LineStats{}},
		identity.AuthorMissing: {100, ls(200, 300, 400), map[string]items.LineStats{"Go": ls(32, 33, 34)}},
	})
	assert.Equal(t, rm.Ticks[11], map[int]*DevTick{
		1:                      {10, ls(20, 30, 40), map[string]items.LineStats{"Go": ls(42, 43, 44)}},
		2:                      {11, ls(21, 31, 41), map[string]items.LineStats{"Go": ls(52, 53, 54)}},
		identity.AuthorMissing: {100, ls(200, 300, 400), map[string]items.LineStats{"Go": ls(62, 63, 64)}},
	})
}

func TestDevsResultGetters(t *testing.T) {
	dr := DevsResult{tickSize: time.Hour, reversedPeopleDict: []string{"one", "two"}}
	assert.Equal(t, dr.tickSize, dr.GetTickSize())
	assert.Equal(t, dr.GetIdentities(), dr.reversedPeopleDict)
}
