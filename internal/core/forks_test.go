package core

import (
	"testing"
	"gopkg.in/src-d/go-git.v4"
	"github.com/stretchr/testify/assert"
)

type testForkPipelineItem struct {
	NoopMerger
	Mutable map[int]bool
	Immutable string
}

func (item *testForkPipelineItem) Name() string {
	return "Test"
}

func (item *testForkPipelineItem) Provides() []string {
	arr := [...]string{"test"}
	return arr[:]
}

func (item *testForkPipelineItem) Requires() []string {
	return []string{}
}

func (item *testForkPipelineItem) Configure(facts map[string]interface{}) {
}

func (item *testForkPipelineItem) ListConfigurationOptions() []ConfigurationOption {
	return nil
}

func (item *testForkPipelineItem) Flag() string {
	return "mytest"
}

func (item *testForkPipelineItem) Features() []string {
	return nil
}

func (item *testForkPipelineItem) Initialize(repository *git.Repository) {
	item.Mutable = map[int]bool{}
}

func (item *testForkPipelineItem) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{"test": "foo"}, nil
}

func (item *testForkPipelineItem) Fork(n int) []PipelineItem {
	return ForkCopyPipelineItem(item, n)
}

func TestForkCopyPipelineItem(t *testing.T) {
	origin := &testForkPipelineItem{}
	origin.Initialize(nil)
	origin.Mutable[2] = true
	origin.Immutable = "before"
	clone := origin.Fork(1)[0].(*testForkPipelineItem)
	origin.Immutable = "after"
	origin.Mutable[1] = true
	assert.True(t, clone.Mutable[1])
	assert.True(t, clone.Mutable[2])
	assert.Equal(t, "before", clone.Immutable)
}
