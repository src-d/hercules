package hercules

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

func fixtureUASTExtractor() *UASTExtractor {
	exr := UASTExtractor{Endpoint: "0.0.0.0:9432"}
	exr.Initialize(testRepository)
	exr.Languages["Python"] = true
	return &exr
}

func TestUASTExtractorMeta(t *testing.T) {
	exr := fixtureUASTExtractor()
	assert.Equal(t, exr.Name(), "UAST")
	assert.Equal(t, len(exr.Provides()), 1)
	assert.Equal(t, exr.Provides()[0], "uasts")
	assert.Equal(t, len(exr.Requires()), 2)
	assert.Equal(t, exr.Requires()[0], "changes")
	assert.Equal(t, exr.Requires()[1], "blob_cache")
	opts := exr.ListConfigurationOptions()
	assert.Len(t, opts, 5)
	assert.Equal(t, opts[0].Name, ConfigUASTEndpoint)
	assert.Equal(t, opts[1].Name, ConfigUASTTimeout)
	assert.Equal(t, opts[2].Name, ConfigUASTPoolSize)
	assert.Equal(t, opts[3].Name, ConfigUASTFailOnErrors)
	assert.Equal(t, opts[4].Name, ConfigUASTLanguages)
	feats := exr.Features()
	assert.Len(t, feats, 1)
	assert.Equal(t, feats[0], "uast")
}

func TestUASTExtractorConfiguration(t *testing.T) {
	exr := fixtureUASTExtractor()
	facts := map[string]interface{}{}
	exr.Configure(facts)
	facts[ConfigUASTEndpoint] = "localhost:9432"
	facts[ConfigUASTTimeout] = 15
	facts[ConfigUASTPoolSize] = 7
	facts[ConfigUASTLanguages] = "C, Go"
	facts[ConfigUASTFailOnErrors] = true
	exr.Configure(facts)
	assert.Equal(t, exr.Endpoint, facts[ConfigUASTEndpoint])
	assert.NotNil(t, exr.Context)
	assert.Equal(t, exr.PoolSize, facts[ConfigUASTPoolSize])
	assert.True(t, exr.Languages["C"])
	assert.True(t, exr.Languages["Go"])
	assert.False(t, exr.Languages["Python"])
	assert.Equal(t, exr.FailOnErrors, true)
}

func TestUASTExtractorRegistration(t *testing.T) {
	tp, exists := Registry.registered[(&UASTExtractor{}).Name()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "UASTExtractor")
	tps, exists := Registry.provided[(&UASTExtractor{}).Provides()[0]]
	assert.True(t, exists)
	assert.Len(t, tps, 1)
	assert.Equal(t, tps[0].Elem().Name(), "UASTExtractor")
}

func TestUASTExtractorConsume(t *testing.T) {
	exr := fixtureUASTExtractor()
	changes := make(object.Changes, 2)
	// 2b1ed978194a94edeabbca6de7ff3b5771d4d665
	treeFrom, _ := testRepository.TreeObject(plumbing.NewHash(
		"96c6ece9b2f3c7c51b83516400d278dea5605100"))
	treeTo, _ := testRepository.TreeObject(plumbing.NewHash(
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
	changes[1] = &object.Change{From: object.ChangeEntry{
		Name: "cmd/hercules/main.go",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9"),
		},
	}, To: object.ChangeEntry{
		Name: "cmd/hercules/main.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("f7d918ec500e2f925ecde79b51cc007bac27de72"),
		},
	},
	}
	cache := map[plumbing.Hash]*object.Blob{}
	hash := plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("5d78f57d732aed825764347ec6f3ab74d50d0619")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("f7d918ec500e2f925ecde79b51cc007bac27de72")
	cache[hash], _ = testRepository.BlobObject(hash)
	deps := map[string]interface{}{}
	deps["blob_cache"] = cache
	deps["changes"] = changes
	res, err := exr.Consume(deps)
	// Language not enabled
	assert.Len(t, res["uasts"], 0)
	assert.Nil(t, err)
	exr.Languages["Go"] = true
	res, err = exr.Consume(deps)
	// No Go driver
	assert.Len(t, res["uasts"], 0)
	assert.Nil(t, err)

	hash = plumbing.NewHash("5d78f57d732aed825764347ec6f3ab74d50d0619")
	changes[1] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: "labours.py",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "labours.py",
			Mode: 0100644,
			Hash: hash,
		},
	},
	}

	res, err = exr.Consume(deps)
	assert.Nil(t, err)
	uasts := res["uasts"].(map[plumbing.Hash]*uast.Node)
	assert.Equal(t, len(uasts), 1)
	assert.Equal(t, len(uasts[hash].Children), 24)
}

func fixtureUASTChanges() *UASTChanges {
	ch := UASTChanges{}
	ch.Configure(nil)
	ch.Initialize(testRepository)
	return &ch
}

func TestUASTChangesMeta(t *testing.T) {
	ch := fixtureUASTChanges()
	assert.Equal(t, ch.Name(), "UASTChanges")
	assert.Equal(t, len(ch.Provides()), 1)
	assert.Equal(t, ch.Provides()[0], "changed_uasts")
	assert.Equal(t, len(ch.Requires()), 2)
	assert.Equal(t, ch.Requires()[0], "uasts")
	assert.Equal(t, ch.Requires()[1], "changes")
	opts := ch.ListConfigurationOptions()
	assert.Len(t, opts, 0)
	feats := ch.Features()
	assert.Len(t, feats, 1)
	assert.Equal(t, feats[0], "uast")
}

func TestUASTChangesRegistration(t *testing.T) {
	tp, exists := Registry.registered[(&UASTChanges{}).Name()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "UASTChanges")
	tps, exists := Registry.provided[(&UASTChanges{}).Provides()[0]]
	assert.True(t, exists)
	assert.True(t, len(tps) >= 1)
	matched := false
	for _, tp := range tps {
		matched = matched || tp.Elem().Name() == "UASTChanges"
	}
	assert.True(t, matched)
}

func TestUASTChangesConsume(t *testing.T) {
	uastsArray := []*uast.Node{}
	uasts := map[plumbing.Hash]*uast.Node{}
	hash := plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe")
	uasts[hash] = &uast.Node{}
	uasts[hash].InternalType = "uno"
	uastsArray = append(uastsArray, uasts[hash])
	hash = plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9")
	uasts[hash] = &uast.Node{}
	uasts[hash].InternalType = "dos"
	uastsArray = append(uastsArray, uasts[hash])
	hash = plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1")
	uasts[hash] = &uast.Node{}
	uasts[hash].InternalType = "tres"
	uastsArray = append(uastsArray, uasts[hash])
	hash = plumbing.NewHash("dc248ba2b22048cc730c571a748e8ffcf7085ab9")
	uasts[hash] = &uast.Node{}
	uasts[hash].InternalType = "quatro"
	uastsArray = append(uastsArray, uasts[hash])
	changes := make(object.Changes, 3)
	treeFrom, _ := testRepository.TreeObject(plumbing.NewHash(
		"a1eb2ea76eb7f9bfbde9b243861474421000eb96"))
	treeTo, _ := testRepository.TreeObject(plumbing.NewHash(
		"994eac1cd07235bb9815e547a75c84265dea00f5"))
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name: "analyser.go",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "analyser.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("dc248ba2b22048cc730c571a748e8ffcf7085ab9"),
		},
	}, To: object.ChangeEntry{
		Name: "analyser.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "analyser.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1"),
		},
	}}
	changes[1] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: "cmd/hercules/main.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9"),
		},
	},
	}
	changes[2] = &object.Change{To: object.ChangeEntry{}, From: object.ChangeEntry{
		Name: ".travis.yml",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: ".travis.yml",
			Mode: 0100644,
			Hash: plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe"),
		},
	},
	}
	deps := map[string]interface{}{}
	deps["uasts"] = uasts
	deps["changes"] = changes
	ch := fixtureUASTChanges()
	ch.cache[changes[0].From.TreeEntry.Hash] = uastsArray[3]
	ch.cache[changes[2].From.TreeEntry.Hash] = uastsArray[0]
	resultMap, err := ch.Consume(deps)
	assert.Nil(t, err)
	result := resultMap["changed_uasts"].([]UASTChange)
	assert.Len(t, result, 3)
	assert.Equal(t, result[0].Change, changes[0])
	assert.Equal(t, result[0].Before, uastsArray[3])
	assert.Equal(t, result[0].After, uastsArray[2])
	assert.Equal(t, result[1].Change, changes[1])
	assert.Nil(t, result[1].Before)
	assert.Equal(t, result[1].After, uastsArray[1])
	assert.Equal(t, result[2].Change, changes[2])
	assert.Equal(t, result[2].Before, uastsArray[0])
	assert.Nil(t, result[2].After)
}

func fixtureUASTChangesSaver() *UASTChangesSaver {
	ch := UASTChangesSaver{}
	ch.Initialize(testRepository)
	return &ch
}

func TestUASTChangesSaverMeta(t *testing.T) {
	ch := fixtureUASTChangesSaver()
	assert.Equal(t, ch.Name(), "UASTChangesSaver")
	assert.Equal(t, len(ch.Provides()), 0)
	assert.Equal(t, len(ch.Requires()), 1)
	assert.Equal(t, ch.Requires()[0], "changed_uasts")
	opts := ch.ListConfigurationOptions()
	assert.Len(t, opts, 1)
	assert.Equal(t, opts[0].Name, ConfigUASTChangesSaverOutputPath)
	feats := ch.Features()
	assert.Len(t, feats, 1)
	assert.Equal(t, feats[0], "uast")
	assert.Equal(t, ch.Flag(), "dump-uast-changes")
}

func TestUASTChangesSaverConfiguration(t *testing.T) {
	facts := map[string]interface{}{}
	ch := fixtureUASTChangesSaver()
	ch.Configure(facts)
	assert.Empty(t, ch.OutputPath)
	facts[ConfigUASTChangesSaverOutputPath] = "libre"
	ch.Configure(facts)
	assert.Equal(t, ch.OutputPath, "libre")
}

func TestUASTChangesSaverRegistration(t *testing.T) {
	tp, exists := Registry.registered[(&UASTChangesSaver{}).Name()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "UASTChangesSaver")
	tp, exists = Registry.flags[(&UASTChangesSaver{}).Flag()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "UASTChangesSaver")
}
