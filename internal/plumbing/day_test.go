package plumbing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/hercules.v6/internal/core"
	"gopkg.in/src-d/hercules.v6/internal/test"
)

func fixtureDaysSinceStart() *DaysSinceStart {
	dss := DaysSinceStart{}
	dss.Configure(map[string]interface{}{})
	dss.Initialize(test.Repository)
	return &dss
}

func TestDaysSinceStartMeta(t *testing.T) {
	dss := fixtureDaysSinceStart()
	assert.Equal(t, dss.Name(), "DaysSinceStart")
	assert.Equal(t, len(dss.Provides()), 1)
	assert.Equal(t, dss.Provides()[0], DependencyDay)
	assert.Equal(t, len(dss.Requires()), 0)
	assert.Len(t, dss.ListConfigurationOptions(), 0)
	dss.Configure(map[string]interface{}{})
}

func TestDaysSinceStartRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&DaysSinceStart{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "DaysSinceStart")
	summoned = core.Registry.Summon((&DaysSinceStart{}).Provides()[0])
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "DaysSinceStart")
}

func TestDaysSinceStartConsume(t *testing.T) {
	dss := fixtureDaysSinceStart()
	deps := map[string]interface{}{}
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 0
	res, err := dss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res[DependencyDay].(int), 0)
	assert.Equal(t, dss.previousDay, 0)
	assert.Equal(t, dss.day0.Hour(), 1)   // 18 UTC+1
	assert.Equal(t, dss.day0.Minute(), 0) // 30
	assert.Equal(t, dss.day0.Second(), 0) // 29

	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"fc9ceecb6dabcb2aab60e8619d972e8d8208a7df"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 10
	res, err = dss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res[DependencyDay].(int), 1)
	assert.Equal(t, dss.previousDay, 1)

	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"a3ee37f91f0d705ec9c41ae88426f0ae44b2fbc3"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 20
	res, err = dss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res[DependencyDay].(int), 1)
	assert.Equal(t, dss.previousDay, 1)

	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"a8b665a65d7aced63f5ba2ff6d9b71dac227f8cf"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 20
	res, err = dss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res[DependencyDay].(int), 2)
	assert.Equal(t, dss.previousDay, 2)

	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"186ff0d7e4983637bb3762a24d6d0a658e7f4712"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 30
	res, err = dss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res[DependencyDay].(int), 2)
	assert.Equal(t, dss.previousDay, 2)

	assert.Len(t, dss.commits, 3)
	assert.Equal(t, dss.commits[0], []plumbing.Hash{plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1")})
	assert.Equal(t, dss.commits[1], []plumbing.Hash{
		plumbing.NewHash("fc9ceecb6dabcb2aab60e8619d972e8d8208a7df"),
		plumbing.NewHash("a3ee37f91f0d705ec9c41ae88426f0ae44b2fbc3")})
	assert.Equal(t, dss.commits[2], []plumbing.Hash{
		plumbing.NewHash("a8b665a65d7aced63f5ba2ff6d9b71dac227f8cf"),
		plumbing.NewHash("186ff0d7e4983637bb3762a24d6d0a658e7f4712")})
}

func TestDaysCommits(t *testing.T) {
	dss := fixtureDaysSinceStart()
	dss.commits[0] = []plumbing.Hash{plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1")}
	commits := dss.commits
	dss.Initialize(test.Repository)
	assert.Len(t, dss.commits, 0)
	assert.Equal(t, dss.commits, commits)
}

func TestDaysSinceStartFork(t *testing.T) {
	dss1 := fixtureDaysSinceStart()
	dss1.commits[0] = []plumbing.Hash{plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1")}
	clones := dss1.Fork(1)
	assert.Len(t, clones, 1)
	dss2 := clones[0].(*DaysSinceStart)
	assert.Equal(t, dss1.day0, dss2.day0)
	assert.Equal(t, dss1.previousDay, dss2.previousDay)
	assert.Equal(t, dss1.commits, dss2.commits)
	dss1.commits[0] = append(dss1.commits[0], plumbing.ZeroHash)
	assert.Len(t, dss2.commits[0], 2)
	assert.True(t, dss1 != dss2)
	// just for the sake of it
	dss1.Merge([]core.PipelineItem{dss2})
}
