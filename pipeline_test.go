package hercules

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"

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

func (item *dependingTestPipelineItem) Initialize(repository *git.Repository) {
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
	return item.DependencySatisfied
}

func TestPipelineRun(t *testing.T) {
	pipeline := NewPipeline(testRepository)
	item := &testPipelineItem{}
	pipeline.AddItem(item)
	pipeline.Initialize()
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
	pipeline.Initialize()
	commits := make([]*object.Commit, 1)
	commits[0], _ = testRepository.CommitObject(plumbing.NewHash(
		"af9ddc0db70f09f3f27b4b98e415592a7485171c"))
	result, err := pipeline.Run(commits)
	assert.Nil(t, err)
	assert.True(t, result[item1].(bool))
	item1.TestNilConsumeReturn = true
	assert.Panics(t, func() { pipeline.Run(commits) })
}

func TestPipelineError(t *testing.T) {
	pipeline := NewPipeline(testRepository)
	item := &testPipelineItem{}
	item.TestError = true
	pipeline.AddItem(item)
	pipeline.Initialize()
	commits := make([]*object.Commit, 1)
	commits[0], _ = testRepository.CommitObject(plumbing.NewHash(
		"af9ddc0db70f09f3f27b4b98e415592a7485171c"))
	result, err := pipeline.Run(commits)
	assert.Nil(t, result)
	assert.NotNil(t, err)
}

func init() {
	cwd, err := os.Getwd()
	if err == nil {
		testRepository, err = git.PlainOpen(cwd)
		if err == nil {
			iter, _ := testRepository.CommitObjects()
			commits := 0
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
	testRepository, _ = git.Clone(memory.NewStorage(), nil, &git.CloneOptions{
		URL: "https://github.com/src-d/hercules",
	})
}
