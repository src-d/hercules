package hercules

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"testing"

	"flag"
	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/memory"
)

type testPipelineItem struct {
	Initialized   bool
	DepsConsumed  bool
	CommitMatches bool
	IndexMatches  bool
	TestError     bool
}

func (item *testPipelineItem) Name() string {
	return "Test"
}

func (item *testPipelineItem) Provides() []string {
	arr := [...]string{"test"}
	return arr[:]
}

func (item *testPipelineItem) Requires() []string {
	return []string{}
}

func (item *testPipelineItem) Configure(facts map[string]interface{}) {
}

func (item *testPipelineItem) ListConfigurationOptions() []ConfigurationOption {
	options := [...]ConfigurationOption{{
		Name:        "TestOption",
		Description: "The option description.",
		Flag:        "test-option",
		Type:        IntConfigurationOption,
		Default:     10,
	}}
	return options[:]
}

func (item *testPipelineItem) Flag() string {
	return "mytest"
}

func (item *testPipelineItem) Features() []string {
	f := [...]string{"power"}
	return f[:]
}

func (item *testPipelineItem) Initialize(repository *git.Repository) {
	item.Initialized = repository != nil
}

func (item *testPipelineItem) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if item.TestError {
		return nil, errors.New("error")
	}
	obj, exists := deps["commit"]
	item.DepsConsumed = exists
	if item.DepsConsumed {
		commit := obj.(*object.Commit)
		item.CommitMatches = commit.Hash == plumbing.NewHash(
			"af9ddc0db70f09f3f27b4b98e415592a7485171c")
		obj, item.DepsConsumed = deps["index"]
		if item.DepsConsumed {
			item.IndexMatches = obj.(int) == 0
		}
	}
	return map[string]interface{}{"test": item}, nil
}

func (item *testPipelineItem) Finalize() interface{} {
	return item
}

func (item *testPipelineItem) Serialize(result interface{}, binary bool, writer io.Writer) error {
	return nil
}

func getRegistry() *PipelineItemRegistry {
	return &PipelineItemRegistry{
		provided:   map[string][]reflect.Type{},
		registered: map[string]reflect.Type{},
		flags:      map[string]reflect.Type{},
	}
}

func TestPipelineItemRegistrySummon(t *testing.T) {
	reg := getRegistry()
	reg.Register(&testPipelineItem{})
	summoned := reg.Summon((&testPipelineItem{}).Provides()[0])
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), (&testPipelineItem{}).Name())
	summoned = reg.Summon((&testPipelineItem{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), (&testPipelineItem{}).Name())
}

func TestPipelineItemRegistryAddFlags(t *testing.T) {
	reg := getRegistry()
	reg.Register(&testPipelineItem{})
	facts, deployed := reg.AddFlags()
	assert.Len(t, facts, 1)
	assert.IsType(t, 0, facts[(&testPipelineItem{}).ListConfigurationOptions()[0].Name])
	assert.Len(t, deployed, 1)
	assert.Contains(t, deployed, (&testPipelineItem{}).Name())
	assert.NotNil(t, flag.Lookup((&testPipelineItem{}).Flag()))
}

type dependingTestPipelineItem struct {
	DependencySatisfied  bool
	TestNilConsumeReturn bool
}

func (item *dependingTestPipelineItem) Name() string {
	return "Test2"
}

func (item *dependingTestPipelineItem) Provides() []string {
	arr := [...]string{"test2"}
	return arr[:]
}

func (item *dependingTestPipelineItem) Requires() []string {
	arr := [...]string{"test"}
	return arr[:]
}

func (item *dependingTestPipelineItem) ListConfigurationOptions() []ConfigurationOption {
	options := [...]ConfigurationOption{{
		Name:        "TestOption",
		Description: "The option description.",
		Flag:        "test-option",
		Type:        IntConfigurationOption,
		Default:     10,
	}}
	return options[:]
}

func (item *dependingTestPipelineItem) Configure(facts map[string]interface{}) {
}

func (item *dependingTestPipelineItem) Initialize(repository *git.Repository) {
}

func (item *dependingTestPipelineItem) Flag() string {
	return "depflag"
}

func (item *dependingTestPipelineItem) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	_, exists := deps["test"]
	item.DependencySatisfied = exists
	if !item.TestNilConsumeReturn {
		return map[string]interface{}{"test2": item}, nil
	} else {
		return nil, nil
	}
}

func (item *dependingTestPipelineItem) Finalize() interface{} {
	return true
}

func (item *dependingTestPipelineItem) Serialize(result interface{}, binary bool, writer io.Writer) error {
	return nil
}

func TestPipelineFacts(t *testing.T) {
	pipeline := NewPipeline(testRepository)
	pipeline.SetFact("fact", "value")
	assert.Equal(t, pipeline.GetFact("fact"), "value")
}

func TestPipelineFeatures(t *testing.T) {
	pipeline := NewPipeline(testRepository)
	pipeline.SetFeature("feat")
	val, _ := pipeline.GetFeature("feat")
	assert.True(t, val)
	val, exists := pipeline.GetFeature("!")
	assert.False(t, exists)
	featureFlags.Set("777")
	defer func() {
		featureFlags = arrayFeatureFlags{Flags: []string{}, Choices: map[string]bool{}}
	}()
	pipeline.SetFeaturesFromFlags()
	_, exists = pipeline.GetFeature("777")
	assert.False(t, exists)
}

func TestPipelineRun(t *testing.T) {
	pipeline := NewPipeline(testRepository)
	item := &testPipelineItem{}
	pipeline.AddItem(item)
	pipeline.Initialize(map[string]interface{}{})
	assert.True(t, item.Initialized)
	commits := make([]*object.Commit, 1)
	commits[0], _ = testRepository.CommitObject(plumbing.NewHash(
		"af9ddc0db70f09f3f27b4b98e415592a7485171c"))
	result, err := pipeline.Run(commits)
	assert.Nil(t, err)
	assert.Equal(t, item, result[item].(*testPipelineItem))
	assert.True(t, item.DepsConsumed)
	assert.True(t, item.CommitMatches)
	assert.True(t, item.IndexMatches)
	pipeline.RemoveItem(item)
	result, err = pipeline.Run(commits)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(result))
}

func TestPipelineOnProgress(t *testing.T) {
	pipeline := NewPipeline(testRepository)
	var progressOk1, progressOk2 bool

	onProgress := func(step int, total int) {
		if step == 0 && total == 1 {
			progressOk1 = true
		}
		if step == 1 && total == 1 && progressOk1 {
			progressOk2 = true
		}
	}

	pipeline.OnProgress = onProgress
	commits := make([]*object.Commit, 1)
	commits[0], _ = testRepository.CommitObject(plumbing.NewHash(
		"af9ddc0db70f09f3f27b4b98e415592a7485171c"))
	result, err := pipeline.Run(commits)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(result))
	assert.True(t, progressOk1)
	assert.True(t, progressOk2)
}

func TestPipelineCommits(t *testing.T) {
	pipeline := NewPipeline(testRepository)
	commits := pipeline.Commits()
	assert.True(t, len(commits) >= 90)
	assert.Equal(t, commits[0].Hash, plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))
	assert.Equal(t, commits[89].Hash, plumbing.NewHash(
		"6db8065cdb9bb0758f36a7e75fc72ab95f9e8145"))
	assert.NotEqual(t, commits[len(commits)-1], commits[len(commits)-2])
}

func TestLoadCommitsFromFile(t *testing.T) {
	tmp, err := ioutil.TempFile("", "hercules-test-")
	assert.Nil(t, err)
	tmp.WriteString("cce947b98a050c6d356bc6ba95030254914027b1\n6db8065cdb9bb0758f36a7e75fc72ab95f9e8145")
	tmp.Close()
	defer os.Remove(tmp.Name())
	commits, err := LoadCommitsFromFile(tmp.Name(), testRepository)
	assert.Nil(t, err)
	assert.Equal(t, len(commits), 2)
	assert.Equal(t, commits[0].Hash, plumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))
	assert.Equal(t, commits[1].Hash, plumbing.NewHash(
		"6db8065cdb9bb0758f36a7e75fc72ab95f9e8145"))
	commits, err = LoadCommitsFromFile("/WAT?xxx!", testRepository)
	assert.Nil(t, commits)
	assert.NotNil(t, err)
	tmp, err = ioutil.TempFile("", "hercules-test-")
	assert.Nil(t, err)
	tmp.WriteString("WAT")
	tmp.Close()
	defer os.Remove(tmp.Name())
	commits, err = LoadCommitsFromFile(tmp.Name(), testRepository)
	assert.Nil(t, commits)
	assert.NotNil(t, err)
	tmp, err = ioutil.TempFile("", "hercules-test-")
	assert.Nil(t, err)
	tmp.WriteString("ffffffffffffffffffffffffffffffffffffffff")
	tmp.Close()
	defer os.Remove(tmp.Name())
	commits, err = LoadCommitsFromFile(tmp.Name(), testRepository)
	assert.Nil(t, commits)
	assert.NotNil(t, err)
}

func TestPipelineDeps(t *testing.T) {
	pipeline := NewPipeline(testRepository)
	item1 := &dependingTestPipelineItem{}
	item2 := &testPipelineItem{}
	pipeline.AddItem(item1)
	pipeline.AddItem(item2)
	assert.Equal(t, pipeline.Len(), 2)
	pipeline.Initialize(map[string]interface{}{})
	commits := make([]*object.Commit, 1)
	commits[0], _ = testRepository.CommitObject(plumbing.NewHash(
		"af9ddc0db70f09f3f27b4b98e415592a7485171c"))
	result, err := pipeline.Run(commits)
	assert.Nil(t, err)
	assert.True(t, result[item1].(bool))
	assert.Equal(t, result[item2], item2)
	item1.TestNilConsumeReturn = true
	assert.Panics(t, func() { pipeline.Run(commits) })
}

func TestPipelineError(t *testing.T) {
	pipeline := NewPipeline(testRepository)
	item := &testPipelineItem{}
	item.TestError = true
	pipeline.AddItem(item)
	pipeline.Initialize(map[string]interface{}{})
	commits := make([]*object.Commit, 1)
	commits[0], _ = testRepository.CommitObject(plumbing.NewHash(
		"af9ddc0db70f09f3f27b4b98e415592a7485171c"))
	result, err := pipeline.Run(commits)
	assert.Nil(t, result)
	assert.NotNil(t, err)
}

func TestPipelineSerialize(t *testing.T) {
	pipeline := NewPipeline(testRepository)
	pipeline.SetFeature("uast")
	pipeline.DeployItem(&BurndownAnalysis{})
	facts := map[string]interface{}{}
	facts["Pipeline.DryRun"] = true
	tmpdir, _ := ioutil.TempDir("", "hercules-")
	defer os.RemoveAll(tmpdir)
	dotpath := path.Join(tmpdir, "graph.dot")
	facts["Pipeline.DumpPath"] = dotpath
	pipeline.Initialize(facts)
	bdot, _ := ioutil.ReadFile(dotpath)
	dot := string(bdot)
	assert.Equal(t, `digraph Hercules {
  "6 BlobCache" -> "7 [blob_cache]"
  "0 DaysSinceStart" -> "3 [day]"
  "9 FileDiff" -> "11 [file_diff]"
  "15 FileDiffRefiner" -> "16 Burndown"
  "1 IdentityDetector" -> "4 [author]"
  "8 RenameAnalysis" -> "16 Burndown"
  "8 RenameAnalysis" -> "9 FileDiff"
  "8 RenameAnalysis" -> "10 UAST"
  "8 RenameAnalysis" -> "13 UASTChanges"
  "2 TreeDiff" -> "5 [changes]"
  "10 UAST" -> "12 [uasts]"
  "13 UASTChanges" -> "14 [changed_uasts]"
  "4 [author]" -> "16 Burndown"
  "7 [blob_cache]" -> "16 Burndown"
  "7 [blob_cache]" -> "9 FileDiff"
  "7 [blob_cache]" -> "8 RenameAnalysis"
  "7 [blob_cache]" -> "10 UAST"
  "14 [changed_uasts]" -> "15 FileDiffRefiner"
  "5 [changes]" -> "6 BlobCache"
  "5 [changes]" -> "8 RenameAnalysis"
  "3 [day]" -> "16 Burndown"
  "11 [file_diff]" -> "15 FileDiffRefiner"
  "12 [uasts]" -> "13 UASTChanges"
}`, dot)
}

func TestPipelineSerializeNoUast(t *testing.T) {
	pipeline := NewPipeline(testRepository)
	// pipeline.SetFeature("uast")
	pipeline.DeployItem(&BurndownAnalysis{})
	facts := map[string]interface{}{}
	facts["Pipeline.DryRun"] = true
	tmpdir, _ := ioutil.TempDir("", "hercules-")
	defer os.RemoveAll(tmpdir)
	dotpath := path.Join(tmpdir, "graph.dot")
	facts["Pipeline.DumpPath"] = dotpath
	pipeline.Initialize(facts)
	bdot, _ := ioutil.ReadFile(dotpath)
	dot := string(bdot)
	assert.Equal(t, `digraph Hercules {
  "6 BlobCache" -> "7 [blob_cache]"
  "0 DaysSinceStart" -> "3 [day]"
  "9 FileDiff" -> "10 [file_diff]"
  "1 IdentityDetector" -> "4 [author]"
  "8 RenameAnalysis" -> "11 Burndown"
  "8 RenameAnalysis" -> "9 FileDiff"
  "2 TreeDiff" -> "5 [changes]"
  "4 [author]" -> "11 Burndown"
  "7 [blob_cache]" -> "11 Burndown"
  "7 [blob_cache]" -> "9 FileDiff"
  "7 [blob_cache]" -> "8 RenameAnalysis"
  "5 [changes]" -> "6 BlobCache"
  "5 [changes]" -> "8 RenameAnalysis"
  "3 [day]" -> "11 Burndown"
  "10 [file_diff]" -> "11 Burndown"
}`, dot)
}

func init() {
	cwd, err := os.Getwd()
	if err == nil {
		testRepository, err = git.PlainOpen(cwd)
		if err == nil {
			iter, err := testRepository.CommitObjects()
			if err == nil {
				commits := -1
				for ; err != io.EOF; _, err = iter.Next() {
					if err != nil {
						panic(err)
					}
					commits++
					if commits >= 100 {
						return
					}
				}
			}
		}
	}
	testRepository, _ = git.Clone(memory.NewStorage(), nil, &git.CloneOptions{
		URL: "https://github.com/src-d/hercules",
	})
}
