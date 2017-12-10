package hercules

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func fixtureDaysSinceStart() *DaysSinceStart {
	dss := DaysSinceStart{}
	dss.Initialize(testRepository)
	return &dss
}

func TestDaysSinceStartMeta(t *testing.T) {
	dss := fixtureDaysSinceStart()
	assert.Equal(t, dss.Name(), "DaysSinceStart")
	assert.Equal(t, len(dss.Provides()), 1)
	assert.Equal(t, dss.Provides()[0], "day")
	assert.Equal(t, len(dss.Requires()), 0)
	assert.Len(t, dss.ListConfigurationOptions(), 0)
	dss.Configure(nil)
}

func TestDaysSinceStartRegistration(t *testing.T) {
	tp, exists := Registry.registered[(&DaysSinceStart{}).Name()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "DaysSinceStart")
	tps, exists := Registry.provided[(&DaysSinceStart{}).Provides()[0]]
	assert.True(t, exists)
	assert.Len(t, tps, 1)
	assert.Equal(t, tps[0].Elem().Name(), "DaysSinceStart")
}

func TestDaysSinceStartConsume(t *testing.T) {
	dss := fixtureDaysSinceStart()
	deps := map[string]interface{}{}
	commit, _ := testRepository.CommitObject(plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))
	deps["commit"] = commit
	deps["index"] = 0
	res, err := dss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res["day"].(int), 0)
	assert.Equal(t, dss.previousDay, 0)
	assert.Equal(t, dss.day0.Hour(), 1)   // 18 UTC+1
	assert.Equal(t, dss.day0.Minute(), 0) // 30
	assert.Equal(t, dss.day0.Second(), 0) // 29

	commit, _ = testRepository.CommitObject(plumbing.NewHash(
		"fc9ceecb6dabcb2aab60e8619d972e8d8208a7df"))
	deps["commit"] = commit
	deps["index"] = 10
	res, err = dss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res["day"].(int), 1)
	assert.Equal(t, dss.previousDay, 1)

	commit, _ = testRepository.CommitObject(plumbing.NewHash(
		"a3ee37f91f0d705ec9c41ae88426f0ae44b2fbc3"))
	deps["commit"] = commit
	deps["index"] = 20
	res, err = dss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res["day"].(int), 1)
	assert.Equal(t, dss.previousDay, 1)

	commit, _ = testRepository.CommitObject(plumbing.NewHash(
		"a8b665a65d7aced63f5ba2ff6d9b71dac227f8cf"))
	deps["commit"] = commit
	deps["index"] = 20
	res, err = dss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res["day"].(int), 2)
	assert.Equal(t, dss.previousDay, 2)

	commit, _ = testRepository.CommitObject(plumbing.NewHash(
		"186ff0d7e4983637bb3762a24d6d0a658e7f4712"))
	deps["commit"] = commit
	deps["index"] = 30
	res, err = dss.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res["day"].(int), 2)
	assert.Equal(t, dss.previousDay, 2)
}
