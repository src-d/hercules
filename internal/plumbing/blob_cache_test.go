package plumbing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func fixtureBlobCache() *BlobCache {
	cache := &BlobCache{}
	cache.Initialize(test.Repository)
	return cache
}

func AddHash(t *testing.T, cache map[plumbing.Hash]*CachedBlob, hash string) {
	objhash := plumbing.NewHash(hash)
	blob, err := test.Repository.BlobObject(objhash)
	assert.Nil(t, err)
	cb := &CachedBlob{Blob: *blob}
	err = cb.Cache()
	assert.Nil(t, err)
	cache[objhash] = cb
}

func TestBlobCacheConfigureInitialize(t *testing.T) {
	cache := fixtureBlobCache()
	assert.Equal(t, test.Repository, cache.repository)
	assert.False(t, cache.FailOnMissingSubmodules)
	facts := map[string]interface{}{}
	facts[ConfigBlobCacheFailOnMissingSubmodules] = true
	cache.Configure(facts)
	assert.True(t, cache.FailOnMissingSubmodules)
	facts = map[string]interface{}{}
	cache.Configure(facts)
	assert.True(t, cache.FailOnMissingSubmodules)
}

func TestBlobCacheMetadata(t *testing.T) {
	cache := fixtureBlobCache()
	assert.Equal(t, cache.Name(), "BlobCache")
	assert.Equal(t, len(cache.Provides()), 1)
	assert.Equal(t, cache.Provides()[0], DependencyBlobCache)
	assert.Equal(t, len(cache.Requires()), 1)
	changes := &TreeDiff{}
	assert.Equal(t, cache.Requires()[0], changes.Provides()[0])
	opts := cache.ListConfigurationOptions()
	assert.Len(t, opts, 1)
	assert.Equal(t, opts[0].Name, ConfigBlobCacheFailOnMissingSubmodules)
}

func TestBlobCacheRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&BlobCache{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "BlobCache")
	summoned = core.Registry.Summon((&BlobCache{}).Provides()[0])
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "BlobCache")
}

func TestBlobCacheConsumeModification(t *testing.T) {
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"af2d8db70f287b52d2428d9887a69a10bc4d1f46"))
	changes := make(object.Changes, 1)
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"80fe25955b8e725feee25c08ea5759d74f8b670d"))
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
		"63076fa0dfd93e94b6d2ef0fc8b1fdf9092f83c4"))
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name: "labours.py",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "labours.py",
			Mode: 0100644,
			Hash: plumbing.NewHash("1cacfc1bf0f048eb2f31973750983ae5d8de647a"),
		},
	}, To: object.ChangeEntry{
		Name: "labours.py",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "labours.py",
			Mode: 0100644,
			Hash: plumbing.NewHash("c872b8d2291a5224e2c9f6edd7f46039b96b4742"),
		},
	}}
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	deps[DependencyTreeChanges] = changes
	result, err := fixtureBlobCache().Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(result), 1)
	cacheIface, exists := result[DependencyBlobCache]
	assert.True(t, exists)
	cache := cacheIface.(map[plumbing.Hash]*CachedBlob)
	assert.Equal(t, len(cache), 2)
	blobFrom, exists := cache[plumbing.NewHash("1cacfc1bf0f048eb2f31973750983ae5d8de647a")]
	assert.True(t, exists)
	blobTo, exists := cache[plumbing.NewHash("c872b8d2291a5224e2c9f6edd7f46039b96b4742")]
	assert.True(t, exists)
	assert.Equal(t, blobFrom.Size, int64(8969))
	assert.Equal(t, blobTo.Size, int64(9481))
}

func TestBlobCacheConsumeInsertionDeletion(t *testing.T) {
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	changes := make(object.Changes, 2)
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"96c6ece9b2f3c7c51b83516400d278dea5605100"))
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
		"251f2094d7b523d5bcc60e663b6cf38151bf8844"))
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name: "analyser.go",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "analyser.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1"),
		},
	}, To: object.ChangeEntry{},
	}
	changes[1] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: "pipeline.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "pipeline.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("db99e1890f581ad69e1527fe8302978c661eb473"),
		},
	},
	}
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	deps[DependencyTreeChanges] = changes
	result, err := fixtureBlobCache().Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(result), 1)
	cacheIface, exists := result[DependencyBlobCache]
	assert.True(t, exists)
	cache := cacheIface.(map[plumbing.Hash]*CachedBlob)
	assert.Equal(t, len(cache), 2)
	blobFrom, exists := cache[plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1")]
	assert.True(t, exists)
	blobTo, exists := cache[plumbing.NewHash("db99e1890f581ad69e1527fe8302978c661eb473")]
	assert.True(t, exists)
	assert.Equal(t, blobFrom.Size, int64(26446))
	assert.Equal(t, blobTo.Size, int64(5576))
}

func TestBlobCacheConsumeNoAction(t *testing.T) {
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"af2d8db70f287b52d2428d9887a69a10bc4d1f46"))
	changes := make(object.Changes, 1)
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"80fe25955b8e725feee25c08ea5759d74f8b670d"))
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
		"63076fa0dfd93e94b6d2ef0fc8b1fdf9092f83c4"))
	changes[0] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{}}
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	deps[DependencyTreeChanges] = changes
	result, err := fixtureBlobCache().Consume(deps)
	assert.Nil(t, result)
	assert.NotNil(t, err)
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name:      "labours.py",
		Tree:      treeFrom,
		TreeEntry: object.TreeEntry{},
	}, To: object.ChangeEntry{
		Name:      "labours.py",
		Tree:      treeTo,
		TreeEntry: object.TreeEntry{},
	}}
	result, err = fixtureBlobCache().Consume(deps)
	assert.Nil(t, result)
	assert.NotNil(t, err)
}

func TestBlobCacheConsumeBadHashes(t *testing.T) {
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"af2d8db70f287b52d2428d9887a69a10bc4d1f46"))
	changes := make(object.Changes, 1)
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"80fe25955b8e725feee25c08ea5759d74f8b670d"))
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
		"63076fa0dfd93e94b6d2ef0fc8b1fdf9092f83c4"))
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name:      "labours.py",
		Tree:      treeFrom,
		TreeEntry: object.TreeEntry{},
	}, To: object.ChangeEntry{
		Name:      "labours.py",
		Tree:      treeTo,
		TreeEntry: object.TreeEntry{},
	}}
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	deps[DependencyTreeChanges] = changes
	result, err := fixtureBlobCache().Consume(deps)
	assert.Nil(t, result)
	assert.NotNil(t, err)
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name:      "labours.py",
		Tree:      treeFrom,
		TreeEntry: object.TreeEntry{},
	}, To: object.ChangeEntry{}}
	result, err = fixtureBlobCache().Consume(deps)
	// Deleting a missing blob is fine
	assert.NotNil(t, result)
	assert.Nil(t, err)
	changes[0] = &object.Change{From: object.ChangeEntry{},
		To: object.ChangeEntry{
			Name:      "labours.py",
			Tree:      treeTo,
			TreeEntry: object.TreeEntry{},
		}}
	result, err = fixtureBlobCache().Consume(deps)
	assert.Nil(t, result)
	assert.NotNil(t, err)
}

func TestBlobCacheConsumeInvalidHash(t *testing.T) {
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"af2d8db70f287b52d2428d9887a69a10bc4d1f46"))
	changes := make(object.Changes, 1)
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"80fe25955b8e725feee25c08ea5759d74f8b670d"))
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
		"63076fa0dfd93e94b6d2ef0fc8b1fdf9092f83c4"))
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name: "labours.py",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "labours.py",
			Mode: 0100644,
			Hash: plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff"),
		},
	}, To: object.ChangeEntry{
		Name:      "labours.py",
		Tree:      treeTo,
		TreeEntry: object.TreeEntry{},
	}}
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	deps[DependencyTreeChanges] = changes
	result, err := fixtureBlobCache().Consume(deps)
	assert.Nil(t, result)
	assert.NotNil(t, err)
}

func TestBlobCacheGetBlob(t *testing.T) {
	cache := fixtureBlobCache()
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"80fe25955b8e725feee25c08ea5759d74f8b670d"))
	entry := object.ChangeEntry{
		Name: "labours.py",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "labours.py",
			Mode: 0100644,
			Hash: plumbing.NewHash("80fe25955b8e725feee25c08ea5759d74f8b670d"),
		},
	}
	getter := func(path string) (*object.File, error) {
		assert.Equal(t, path, ".gitmodules")
		commit, _ := test.Repository.CommitObject(plumbing.NewHash(
			"13272b66c55e1ba1237a34104f30b84d7f6e4082"))
		return commit.File("test_data/gitmodules")
	}
	blob, err := cache.getBlob(&entry, getter)
	assert.Nil(t, blob)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), plumbing.ErrObjectNotFound.Error())
	getter = func(path string) (*object.File, error) {
		assert.Equal(t, path, ".gitmodules")
		commit, _ := test.Repository.CommitObject(plumbing.NewHash(
			"13272b66c55e1ba1237a34104f30b84d7f6e4082"))
		return commit.File("test_data/gitmodules_empty")
	}
	blob, err = cache.getBlob(&entry, getter)
	assert.Nil(t, blob)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), plumbing.ErrObjectNotFound.Error())
}

func TestBlobCacheDeleteInvalidBlob(t *testing.T) {
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	changes := make(object.Changes, 1)
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"96c6ece9b2f3c7c51b83516400d278dea5605100"))
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name: "analyser.go",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "analyser.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff"),
		},
	}, To: object.ChangeEntry{},
	}
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	deps[DependencyTreeChanges] = changes
	result, err := fixtureBlobCache().Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(result), 1)
	cacheIface, exists := result[DependencyBlobCache]
	assert.True(t, exists)
	cache := cacheIface.(map[plumbing.Hash]*CachedBlob)
	assert.Equal(t, len(cache), 1)
	blobFrom, exists := cache[plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff")]
	assert.True(t, exists)
	assert.Equal(t, blobFrom.Size, int64(0))
}

func TestBlobCacheInsertInvalidBlob(t *testing.T) {
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	changes := make(object.Changes, 1)
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
		"251f2094d7b523d5bcc60e663b6cf38151bf8844"))
	changes[0] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: "pipeline.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "pipeline.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff"),
		},
	},
	}
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	deps[DependencyTreeChanges] = changes
	result, err := fixtureBlobCache().Consume(deps)
	assert.NotNil(t, err)
	assert.Equal(t, len(result), 0)
}

func TestBlobCacheGetBlobIgnoreMissing(t *testing.T) {
	cache := fixtureBlobCache()
	cache.FailOnMissingSubmodules = false
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"80fe25955b8e725feee25c08ea5759d74f8b670d"))
	entry := object.ChangeEntry{
		Name: core.DependencyCommit,
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: core.DependencyCommit,
			Mode: 0160000,
			Hash: plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff"),
		},
	}
	getter := func(path string) (*object.File, error) {
		return nil, plumbing.ErrObjectNotFound
	}
	blob, err := cache.getBlob(&entry, getter)
	assert.NotNil(t, blob)
	assert.Nil(t, err)
	assert.Equal(t, blob.Size, int64(0))
	cache.FailOnMissingSubmodules = true
	getter = func(path string) (*object.File, error) {
		assert.Equal(t, path, ".gitmodules")
		commit, _ := test.Repository.CommitObject(plumbing.NewHash(
			"13272b66c55e1ba1237a34104f30b84d7f6e4082"))
		return commit.File("test_data/gitmodules")
	}
	blob, err = cache.getBlob(&entry, getter)
	assert.Nil(t, blob)
	assert.NotNil(t, err)
}

func TestBlobCacheGetBlobGitModulesErrors(t *testing.T) {
	cache := fixtureBlobCache()
	cache.FailOnMissingSubmodules = true
	entry := object.ChangeEntry{
		Name: "labours.py",
		TreeEntry: object.TreeEntry{
			Name: "labours.py",
			Mode: 0160000,
			Hash: plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff"),
		},
	}
	getter := func(path string) (*object.File, error) {
		return nil, plumbing.ErrInvalidType
	}
	blob, err := cache.getBlob(&entry, getter)
	assert.Nil(t, blob)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), plumbing.ErrInvalidType.Error())
	getter = func(path string) (*object.File, error) {
		blob, _ := internal.CreateDummyBlob(
			plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff"), true)
		return &object.File{Name: "fake", Blob: *blob}, nil
	}
	blob, err = cache.getBlob(&entry, getter)
	assert.Nil(t, blob)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "dummy failure")
	getter = func(path string) (*object.File, error) {
		blob, _ := test.Repository.BlobObject(plumbing.NewHash(
			"4434197c2b0509d990f09d53a3cabb910bfd34b7"))
		return &object.File{Name: ".gitmodules", Blob: *blob}, nil
	}
	blob, err = cache.getBlob(&entry, getter)
	assert.Nil(t, blob)
	assert.NotNil(t, err)
	assert.NotEqual(t, err.Error(), plumbing.ErrObjectNotFound.Error())
}

func TestBlobCacheFork(t *testing.T) {
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	changes := make(object.Changes, 1)
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
		"251f2094d7b523d5bcc60e663b6cf38151bf8844"))
	hash := plumbing.NewHash("db99e1890f581ad69e1527fe8302978c661eb473")
	changes[0] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: "pipeline.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "pipeline.go",
			Mode: 0100644,
			Hash: hash,
		},
	}}
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	deps[DependencyTreeChanges] = changes
	cache1 := fixtureBlobCache()
	cache1.FailOnMissingSubmodules = true
	cache1.Consume(deps)
	clones := cache1.Fork(1)
	assert.Len(t, clones, 1)
	cache2 := clones[0].(*BlobCache)
	assert.True(t, cache2.FailOnMissingSubmodules)
	assert.Equal(t, cache1.repository, cache2.repository)
	cache1.cache[plumbing.ZeroHash] = nil
	assert.Len(t, cache1.cache, 2)
	assert.Len(t, cache2.cache, 1)
	assert.Equal(t, cache1.cache[hash].Size, cache2.cache[hash].Size)
	// just for the sake of it
	cache1.Merge([]core.PipelineItem{cache2})
}
