package imports

import (
	"runtime"
	"testing"

	"github.com/src-d/imports"
	"github.com/stretchr/testify/assert"
	gitplumbing "gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func fixtureExtractor() *Extractor {
	ex := &Extractor{}
	ex.Initialize(test.Repository)
	return ex
}

func TestExtractorConfigureInitialize(t *testing.T) {
	ex := fixtureExtractor()
	assert.Equal(t, runtime.NumCPU(), ex.Goroutines)
	facts := map[string]interface{}{}
	facts[ConfigImportsGoroutines] = 7
	facts[ConfigMaxFileSize] = 8
	assert.NoError(t, ex.Configure(facts))
	assert.Equal(t, 7, ex.Goroutines)
	assert.Equal(t, 8, ex.MaxFileSize)
	facts[ConfigImportsGoroutines] = -1
	facts[ConfigMaxFileSize] = -8
	assert.NoError(t, ex.Configure(facts))
	assert.Equal(t, runtime.NumCPU(), ex.Goroutines)
	assert.Equal(t, DefaultMaxFileSize, ex.MaxFileSize)
	assert.NotNil(t, ex.l)
}

func TestExtractorMetadata(t *testing.T) {
	ex := fixtureExtractor()
	assert.Equal(t, ex.Name(), "Imports")
	assert.Equal(t, len(ex.Provides()), 1)
	assert.Equal(t, ex.Provides()[0], DependencyImports)
	assert.Equal(t, []string{plumbing.DependencyTreeChanges, plumbing.DependencyBlobCache}, ex.Requires())
	opts := ex.ListConfigurationOptions()
	assert.Len(t, opts, 2)
	assert.Equal(t, opts[0].Name, ConfigImportsGoroutines)
	assert.Equal(t, opts[0].Default.(int), runtime.NumCPU())
	assert.Equal(t, opts[1].Name, ConfigMaxFileSize)
	assert.Equal(t, opts[1].Default.(int), DefaultMaxFileSize)
}

func TestExtractorRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&Extractor{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "Imports")
	summoned = core.Registry.Summon((&Extractor{}).Provides()[0])
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "Imports")
}

func TestExtractorConsumeModification(t *testing.T) {
	commit, _ := test.Repository.CommitObject(gitplumbing.NewHash(
		"af2d8db70f287b52d2428d9887a69a10bc4d1f46"))
	changes := make(object.Changes, 1)
	treeFrom, _ := test.Repository.TreeObject(gitplumbing.NewHash(
		"80fe25955b8e725feee25c08ea5759d74f8b670d"))
	treeTo, _ := test.Repository.TreeObject(gitplumbing.NewHash(
		"63076fa0dfd93e94b6d2ef0fc8b1fdf9092f83c4"))
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name: "labours.py",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "labours.py",
			Mode: 0100644,
			Hash: gitplumbing.NewHash("1cacfc1bf0f048eb2f31973750983ae5d8de647a"),
		},
	}, To: object.ChangeEntry{
		Name: "labours.py",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "labours.py",
			Mode: 0100644,
			Hash: gitplumbing.NewHash("c872b8d2291a5224e2c9f6edd7f46039b96b4742"),
		},
	}}
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	deps[plumbing.DependencyTreeChanges] = changes
	cache := &plumbing.BlobCache{}
	assert.NoError(t, cache.Initialize(test.Repository))
	blobs, err := cache.Consume(deps)
	assert.NoError(t, err)
	deps[plumbing.DependencyBlobCache] = blobs[plumbing.DependencyBlobCache]
	result, err := fixtureExtractor().Consume(deps)
	assert.NoError(t, err)
	assert.Equal(t, len(result), 1)
	exIface, exists := result[DependencyImports]
	assert.True(t, exists)
	ex := exIface.(map[gitplumbing.Hash]imports.File)
	assert.Len(t, ex, 1)
	file := ex[gitplumbing.NewHash("c872b8d2291a5224e2c9f6edd7f46039b96b4742")]
	assert.Equal(t, "labours.py", file.Path)
	assert.Equal(t, "Python", file.Lang)
	assert.Equal(t, []string{"argparse", "datetime", "matplotlib", "matplotlib.pyplot", "numpy",
		"pandas", "sys", "warnings"}, file.Imports)
}
