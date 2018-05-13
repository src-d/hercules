package core

import (
	"reflect"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4"
)

func getRegistry() *PipelineItemRegistry {
	return &PipelineItemRegistry{
		provided:     map[string][]reflect.Type{},
		registered:   map[string]reflect.Type{},
		flags:        map[string]reflect.Type{},
		featureFlags: arrayFeatureFlags{Flags: []string{}, Choices: map[string]bool{}},
	}
}

type dummyPipelineItem struct{}

func (item *dummyPipelineItem) Name() string {
	return "dummy"
}

func (item *dummyPipelineItem) Provides() []string {
	arr := [...]string{"dummy"}
	return arr[:]
}

func (item *dummyPipelineItem) Requires() []string {
	return []string{}
}

func (item *dummyPipelineItem) Features() []string {
	arr := [...]string{"power"}
	return arr[:]
}

func (item *dummyPipelineItem) Configure(facts map[string]interface{}) {
}

func (item *dummyPipelineItem) ListConfigurationOptions() []ConfigurationOption {
	options := [...]ConfigurationOption{{
		Name:        "DummyOption",
		Description: "The option description.",
		Flag:        "dummy-option",
		Type:        BoolConfigurationOption,
		Default:     false,
	}}
	return options[:]
}

func (item *dummyPipelineItem) Initialize(repository *git.Repository) {}

func (item *dummyPipelineItem) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{"dummy": nil}, nil
}

type dummyPipelineItem2 struct{}

func (item *dummyPipelineItem2) Name() string {
	return "dummy2"
}

func (item *dummyPipelineItem2) Provides() []string {
	arr := [...]string{"dummy2"}
	return arr[:]
}

func (item *dummyPipelineItem2) Requires() []string {
	return []string{}
}

func (item *dummyPipelineItem2) Features() []string {
	arr := [...]string{"other"}
	return arr[:]
}

func (item *dummyPipelineItem2) Configure(facts map[string]interface{}) {
}

func (item *dummyPipelineItem2) ListConfigurationOptions() []ConfigurationOption {
	return []ConfigurationOption{}
}

func (item *dummyPipelineItem2) Initialize(repository *git.Repository) {}

func (item *dummyPipelineItem2) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{"dummy2": nil}, nil
}

func TestRegistrySummon(t *testing.T) {
	reg := getRegistry()
	reg.Register(&testPipelineItem{})
	summoned := reg.Summon((&testPipelineItem{}).Provides()[0])
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), (&testPipelineItem{}).Name())
	summoned = reg.Summon((&testPipelineItem{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), (&testPipelineItem{}).Name())
}

func TestRegistryAddFlags(t *testing.T) {
	reg := getRegistry()
	reg.Register(&testPipelineItem{})
	reg.Register(&dummyPipelineItem{})
	testCmd := &cobra.Command{
		Use:   "test",
		Short: "Temporary command to test the stuff.",
		Long:  ``,
		Args:  cobra.MaximumNArgs(0),
		Run:   func(cmd *cobra.Command, args []string) {},
	}
	facts, deployed := reg.AddFlags(testCmd.Flags())
	assert.Len(t, facts, 4)
	assert.IsType(t, 0, facts[(&testPipelineItem{}).ListConfigurationOptions()[0].Name])
	assert.IsType(t, true, facts[(&dummyPipelineItem{}).ListConfigurationOptions()[0].Name])
	assert.Contains(t, facts, ConfigPipelineDryRun)
	assert.Contains(t, facts, ConfigPipelineDumpPath)
	assert.Len(t, deployed, 1)
	assert.Contains(t, deployed, (&testPipelineItem{}).Name())
	assert.NotNil(t, testCmd.Flags().Lookup((&testPipelineItem{}).Flag()))
	assert.NotNil(t, testCmd.Flags().Lookup("feature"))
	assert.NotNil(t, testCmd.Flags().Lookup("dump-dag"))
	assert.NotNil(t, testCmd.Flags().Lookup("dry-run"))
	assert.NotNil(t, testCmd.Flags().Lookup(
		(&testPipelineItem{}).ListConfigurationOptions()[0].Flag))
	assert.NotNil(t, testCmd.Flags().Lookup(
		(&dummyPipelineItem{}).ListConfigurationOptions()[0].Flag))
	testCmd.UsageString() // to test that nothing is broken
}

func TestRegistryFeatures(t *testing.T) {
	reg := getRegistry()
	reg.Register(&dummyPipelineItem{})
	reg.Register(&dummyPipelineItem2{})
	testCmd := &cobra.Command{
		Use:   "test",
		Short: "Temporary command to test the stuff.",
		Long:  ``,
		Args:  cobra.MaximumNArgs(0),
		Run:   func(cmd *cobra.Command, args []string) {},
	}
	reg.AddFlags(testCmd.Flags())
	args := [...]string{"--feature", "other", "--feature", "power"}
	testCmd.ParseFlags(args[:])
	pipeline := NewPipeline(testRepository)
	val, _ := pipeline.GetFeature("power")
	assert.False(t, val)
	val, _ = pipeline.GetFeature("other")
	assert.False(t, val)
	pipeline.SetFeaturesFromFlags(reg)
	val, _ = pipeline.GetFeature("power")
	assert.True(t, val)
	val, _ = pipeline.GetFeature("other")
	assert.True(t, val)
}

func TestRegistryLeaves(t *testing.T) {
	reg := getRegistry()
	reg.Register(&testPipelineItem{})
	reg.Register(&dependingTestPipelineItem{})
	reg.Register(&dummyPipelineItem{})
	leaves := reg.GetLeaves()
	assert.Len(t, leaves, 2)
	assert.Equal(t, leaves[0].Name(), (&dependingTestPipelineItem{}).Name())
	assert.Equal(t, leaves[1].Name(), (&testPipelineItem{}).Name())
}

func TestRegistryPlumbingItems(t *testing.T) {
	reg := getRegistry()
	reg.Register(&testPipelineItem{})
	reg.Register(&dependingTestPipelineItem{})
	reg.Register(&dummyPipelineItem{})
	plumbing := reg.GetPlumbingItems()
	assert.Len(t, plumbing, 1)
	assert.Equal(t, plumbing[0].Name(), (&dummyPipelineItem{}).Name())
}

func TestRegistryFeaturedItems(t *testing.T) {
	reg := getRegistry()
	reg.Register(&testPipelineItem{})
	reg.Register(&dependingTestPipelineItem{})
	reg.Register(&dummyPipelineItem{})
	featured := reg.GetFeaturedItems()
	assert.Len(t, featured, 1)
	assert.Len(t, featured["power"], 2)
	assert.Equal(t, featured["power"][0].Name(), (&testPipelineItem{}).Name())
	assert.Equal(t, featured["power"][1].Name(), (&dummyPipelineItem{}).Name())
}
