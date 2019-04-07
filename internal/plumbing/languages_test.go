package plumbing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func TestLanguagesDetectionMeta(t *testing.T) {
	ls := &LanguagesDetection{}
	assert.Equal(t, ls.Name(), "LanguagesDetection")
	assert.Equal(t, len(ls.Provides()), 1)
	assert.Equal(t, ls.Provides()[0], DependencyLanguages)
	assert.Equal(t, len(ls.Requires()), 2)
	assert.Equal(t, ls.Requires()[0], DependencyTreeChanges)
	assert.Equal(t, ls.Requires()[1], DependencyBlobCache)
	opts := ls.ListConfigurationOptions()
	assert.Len(t, opts, 0)
	assert.NoError(t, ls.Configure(nil))
	logger := core.NewLogger()
	assert.NoError(t, ls.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, ls.l)
	assert.NoError(t, ls.Initialize(nil))
}

func TestLanguagesDetectionRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&LanguagesDetection{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "LanguagesDetection")
	summoned = core.Registry.Summon((&LanguagesDetection{}).Provides()[0])
	assert.True(t, len(summoned) >= 1)
	matched := false
	for _, tp := range summoned {
		matched = matched || tp.Name() == "LanguagesDetection"
	}
	assert.True(t, matched)
}

func TestLanguagesDetectionConsume(t *testing.T) {
	ls := &LanguagesDetection{}
	changes := make(object.Changes, 3)
	// 2b1ed978194a94edeabbca6de7ff3b5771d4d665
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
		Name: "burndown.bin",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "burndown.bin",
			Mode: 0100644,
			Hash: plumbing.NewHash("29c9fafd6a2fae8cd20298c3f60115bc31a4c0f2"),
		},
	},
	}
	changes[2] = &object.Change{From: object.ChangeEntry{
		Name: "cmd/hercules/main.go",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9"),
		},
	}, To: object.ChangeEntry{
		Name: "cmd/hercules/main2.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("f7d918ec500e2f925ecde79b51cc007bac27de72"),
		},
	},
	}
	cache := map[plumbing.Hash]*CachedBlob{}
	AddHash(t, cache, "baa64828831d174f40140e4b3cfa77d1e917a2c1")
	cache[plumbing.NewHash("29c9fafd6a2fae8cd20298c3f60115bc31a4c0f2")] =
		&CachedBlob{Data: make([]byte, 1000)}
	AddHash(t, cache, "c29112dbd697ad9b401333b80c18a63951bc18d9")
	AddHash(t, cache, "f7d918ec500e2f925ecde79b51cc007bac27de72")

	deps := map[string]interface{}{}
	deps[DependencyBlobCache] = cache
	deps[DependencyTreeChanges] = changes
	result, err := ls.Consume(deps)
	assert.Nil(t, err)
	langs := result[DependencyLanguages].(map[plumbing.Hash]string)
	assert.Equal(t, "Go", langs[plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1")])
	assert.Equal(t, "Go", langs[plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9")])
	assert.Equal(t, "Go", langs[plumbing.NewHash("f7d918ec500e2f925ecde79b51cc007bac27de72")])
	lang, exists := langs[plumbing.NewHash("29c9fafd6a2fae8cd20298c3f60115bc31a4c0f2")]
	assert.True(t, exists)
	assert.Equal(t, "", lang)
}
