package hercules

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func fixtureRenameAnalysis() *RenameAnalysis {
  ra := RenameAnalysis{SimilarityThreshold: 80}
	ra.Initialize(testRepository)
	return &ra
}

func TestRenameAnalysisMeta(t *testing.T) {
	ra := fixtureRenameAnalysis()
	assert.Equal(t, ra.Name(), "RenameAnalysis")
	assert.Equal(t, len(ra.Provides()), 1)
	assert.Equal(t, ra.Provides()[0], "renamed_changes")
	assert.Equal(t, len(ra.Requires()), 2)
	assert.Equal(t, ra.Requires()[0], "blob_cache")
	assert.Equal(t, ra.Requires()[1], "changes")
}

func TestRenameAnalysisInitializeInvalidThreshold(t *testing.T) {
	ra := RenameAnalysis{SimilarityThreshold: -10}
	assert.Panics(t, func() {ra.Initialize(testRepository)})
	ra = RenameAnalysis{SimilarityThreshold: 110}
	assert.Panics(t, func() {ra.Initialize(testRepository)})
	ra = RenameAnalysis{SimilarityThreshold: 0}
	ra.Initialize(testRepository)
	ra = RenameAnalysis{SimilarityThreshold: 100}
	ra.Initialize(testRepository)
}

func TestRenameAnalysisFinalize(t *testing.T) {
	ra := fixtureRenameAnalysis()
	r := ra.Finalize()
	assert.Nil(t, r)
}

func TestRenameAnalysisConsume(t *testing.T) {
	ra := fixtureRenameAnalysis()
	changes := make(object.Changes, 3)
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
	changes[1] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: "burndown.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "burndown.go",
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
	hash = plumbing.NewHash("29c9fafd6a2fae8cd20298c3f60115bc31a4c0f2")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("f7d918ec500e2f925ecde79b51cc007bac27de72")
	cache[hash], _ = testRepository.BlobObject(hash)
	deps := map[string]interface{}{}
	deps["blob_cache"] = cache
	deps["changes"] = changes
	ra.SimilarityThreshold = 33
	res, err := ra.Consume(deps)
	assert.Nil(t, err)
	renamed := res["renamed_changes"].(object.Changes)
	assert.Equal(t, len(renamed), 2)
	ra.SimilarityThreshold = 35
	res, err = ra.Consume(deps)
	assert.Nil(t, err)
	renamed = res["renamed_changes"].(object.Changes)
	assert.Equal(t, len(renamed), 3)
}
