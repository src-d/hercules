package hercules

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/bblfsh/sdk.v0/uast"
)

func fixtureUASTExtractor() *UASTExtractor {
	exr := UASTExtractor{Endpoint: "0.0.0.0:9432"}
	exr.Initialize(testRepository)
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
}

func TestUASTExtractorFinalize(t *testing.T) {
	exr := fixtureUASTExtractor()
	r := exr.Finalize()
	assert.Nil(t, r)
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
	// No Go driver
	assert.Nil(t, res)
	assert.NotNil(t, err)

	changes[1] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: "labours.py",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "labours.py",
			Mode: 0100644,
			Hash: plumbing.NewHash("5d78f57d732aed825764347ec6f3ab74d50d0619"),
		},
	},
	}

	res, err = exr.Consume(deps)
	assert.Nil(t, err)
	uasts := res["uasts"].(map[string]*uast.Node)
	assert.Equal(t, len(uasts), 1)
	assert.Equal(t, len(uasts["labours.py"].Children), 24)
}
