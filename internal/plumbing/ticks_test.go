package plumbing

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func fixtureTicksSinceStart(config ...map[string]interface{}) *TicksSinceStart {
	tss := TicksSinceStart{
		TickSize: 24 * time.Hour,
	}
	if len(config) != 1 {
		config = []map[string]interface{}{{}}
	}
	tss.Configure(config[0])
	tss.Initialize(test.Repository)
	return &tss
}

func TestTicksSinceStartMeta(t *testing.T) {
	tss := fixtureTicksSinceStart()
	assert.Equal(t, tss.Name(), "TicksSinceStart")
	assert.Equal(t, len(tss.Provides()), 1)
	assert.Equal(t, tss.Provides()[0], DependencyTick)
	assert.Equal(t, len(tss.Requires()), 0)
	assert.Len(t, tss.ListConfigurationOptions(), 1)
	logger := core.NewLogger()
	assert.NoError(t, tss.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, tss.l)
}

func TestTicksSinceStartRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&TicksSinceStart{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "TicksSinceStart")
	summoned = core.Registry.Summon((&TicksSinceStart{}).Provides()[0])
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "TicksSinceStart")
}

func TestTicksSinceStartConsume(t *testing.T) {
	tss := fixtureTicksSinceStart()
	tss.TickSize = time.Second
	deps := map[string]interface{}{}
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 0
	res, err := tss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, 0, res[DependencyTick].(int))
	assert.Equal(t, 0, tss.previousTick)
	assert.Equal(t, 2016, tss.tick0.Year())
	assert.Equal(t, time.Month(12), tss.tick0.Month())
	assert.Equal(t, 12, tss.tick0.Day())
	assert.Equal(t, 18, tss.tick0.Hour())   // 18 UTC+1
	assert.Equal(t, 30, tss.tick0.Minute()) // 30
	assert.Equal(t, 29, tss.tick0.Second()) // 29

	tss = fixtureTicksSinceStart()
	res, err = tss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, 0, res[DependencyTick].(int))
	assert.Equal(t, 0, tss.previousTick)
	assert.Equal(t, 2016, tss.tick0.Year())
	assert.Equal(t, time.Month(12), tss.tick0.Month())
	assert.Equal(t, 12, tss.tick0.Day())
	assert.Equal(t, 1, tss.tick0.Hour()) // UTC+1
	assert.Equal(t, 0, tss.tick0.Minute())
	assert.Equal(t, 0, tss.tick0.Second())

	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"fc9ceecb6dabcb2aab60e8619d972e8d8208a7df"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 10
	res, err = tss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, 1, res[DependencyTick].(int))
	assert.Equal(t, 1, tss.previousTick)

	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"a3ee37f91f0d705ec9c41ae88426f0ae44b2fbc3"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 20
	res, err = tss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, 1, res[DependencyTick].(int))
	assert.Equal(t, 1, tss.previousTick)

	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"a8b665a65d7aced63f5ba2ff6d9b71dac227f8cf"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 20
	res, err = tss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, 2, res[DependencyTick].(int))
	assert.Equal(t, 2, tss.previousTick)

	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"186ff0d7e4983637bb3762a24d6d0a658e7f4712"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 30
	res, err = tss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, 2, res[DependencyTick].(int))
	assert.Equal(t, 2, tss.previousTick)

	assert.Len(t, tss.commits, 3)
	assert.Equal(t, tss.commits[0], []plumbing.Hash{plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1")})
	assert.Equal(t, tss.commits[1], []plumbing.Hash{
		plumbing.NewHash("fc9ceecb6dabcb2aab60e8619d972e8d8208a7df"),
		plumbing.NewHash("a3ee37f91f0d705ec9c41ae88426f0ae44b2fbc3")})
	assert.Equal(t, tss.commits[2], []plumbing.Hash{
		plumbing.NewHash("a8b665a65d7aced63f5ba2ff6d9b71dac227f8cf"),
		plumbing.NewHash("186ff0d7e4983637bb3762a24d6d0a658e7f4712")})
}

func TestTicksSinceStartConsumeWithTickSize(t *testing.T) {
	tss := fixtureTicksSinceStart(map[string]interface{}{
		ConfigTicksSinceStartTickSize: 1, // 1x hour
	})
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))
	deps := map[string]interface{}{
		core.DependencyCommit: commit,
		core.DependencyIndex:  0,
	}
	res, err := tss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, 0, res[DependencyTick].(int))
	assert.Equal(t, 0, tss.previousTick)
	assert.Equal(t, 18, tss.tick0.Hour())  // 18 UTC+1
	assert.Equal(t, 0, tss.tick0.Minute()) // 30
	assert.Equal(t, 0, tss.tick0.Second()) // 29

	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"fc9ceecb6dabcb2aab60e8619d972e8d8208a7df"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 10
	res, err = tss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, 24, res[DependencyTick].(int)) // 1 day later
	assert.Equal(t, 24, tss.previousTick)

	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"a3ee37f91f0d705ec9c41ae88426f0ae44b2fbc3"))
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 20
	res, err = tss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, 24, res[DependencyTick].(int)) // 1 day later
	assert.Equal(t, 24, tss.previousTick)

	assert.Len(t, tss.commits, 2)
	assert.Equal(t, []plumbing.Hash{plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1")},
		tss.commits[0])
	assert.Equal(t, []plumbing.Hash{
		plumbing.NewHash("fc9ceecb6dabcb2aab60e8619d972e8d8208a7df"),
		plumbing.NewHash("a3ee37f91f0d705ec9c41ae88426f0ae44b2fbc3")},
		tss.commits[24])
}

func TestTicksCommits(t *testing.T) {
	tss := fixtureTicksSinceStart()
	tss.commits[0] = []plumbing.Hash{plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1")}
	commits := tss.commits
	assert.NoError(t, tss.Initialize(test.Repository))
	assert.Len(t, tss.commits, 0)
	assert.Equal(t, tss.commits, commits)
}

func TestTicksSinceStartFork(t *testing.T) {
	tss1 := fixtureTicksSinceStart()
	tss1.commits[0] = []plumbing.Hash{plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1")}
	clones := tss1.Fork(1)
	assert.Len(t, clones, 1)
	tss2 := clones[0].(*TicksSinceStart)
	assert.Equal(t, tss1.tick0, tss2.tick0)
	assert.Equal(t, tss1.previousTick, tss2.previousTick)
	assert.Equal(t, tss1.commits, tss2.commits)
	tss1.commits[0] = append(tss1.commits[0], plumbing.ZeroHash)
	assert.Len(t, tss2.commits[0], 2)
	assert.True(t, tss1 != tss2)
	// just for the sake of it
	tss1.Merge([]core.PipelineItem{tss2})
}

func TestTicksSinceStartConsumeZero(t *testing.T) {
	tss := fixtureTicksSinceStart()
	deps := map[string]interface{}{}
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))
	commit.Committer.When = time.Unix(0, 0)
	deps[core.DependencyCommit] = commit
	deps[core.DependencyIndex] = 0
	// print warning to log
	var capture bytes.Buffer
	tss.l.(*core.DefaultLogger).W.SetOutput(&capture)
	res, err := tss.Consume(deps)
	assert.Nil(t, err)
	output := capture.String()
	assert.Contains(t, output, "cce947b98a050c6d356bc6ba95030254914027b1")
	assert.Contains(t, output, "hercules")
	// depending on where the contributor clones this project from, the remote
	// reported in the error could either be from gopkg.in or github.com
	if !strings.Contains(output, "github.com") && !strings.Contains(output, "gopkg.in") {
		assert.Failf(t, "output should contain either 'github.com' or 'gopkg.in'", "got: '%s'", output)
	}
	assert.Equal(t, res[DependencyTick].(int), 0)
	assert.Equal(t, tss.previousTick, 0)
	if (tss.tick0.Year() != 1969) && (tss.tick0.Year() != 1970) {
		assert.Failf(t, "tick0 should be unix-0 time (in either 1969 or 1970)", "got: '%v'", tss.tick0)
	}
	assert.Equal(t, tss.tick0.Minute(), 0)
	assert.Equal(t, tss.tick0.Second(), 0)
}
