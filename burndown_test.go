package hercules

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestBurndownMeta(t *testing.T) {
	burndown := BurndownAnalysis{}
	assert.Equal(t, burndown.Name(), "Burndown")
	assert.Equal(t, len(burndown.Provides()), 0)
	required := [...]string{"renamed_changes", "blob_cache", "day", "author"}
	for _, name := range required {
		assert.Contains(t, burndown.Requires(), name)
	}
}

func TestBurndownConsume(t *testing.T) {
	burndown := BurndownAnalysis{
		Granularity: 30,
		Sampling: 30,
		PeopleNumber: 1,
	}
	burndown.Initialize(testRepository)
	deps := map[string]interface{}{}
	deps["author"] = 0
	deps["day"] = 0
	cache := map[plumbing.Hash]*object.Blob{}
	hash := plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("334cde09da4afcb74f8d2b3e6fd6cce61228b485")
	cache[hash], _ = testRepository.BlobObject(hash)
	hash = plumbing.NewHash("dc248ba2b22048cc730c571a748e8ffcf7085ab9")
	cache[hash], _ = testRepository.BlobObject(hash)
	deps["blob_cache"] = cache
	changes := make(object.Changes, 2)
  treeFrom, _ := testRepository.TreeObject(plumbing.NewHash(
		"a1eb2ea76eb7f9bfbde9b243861474421000eb96"))
	treeTo, _ := testRepository.TreeObject(plumbing.NewHash(
		"4d3f9500c2b9dc10925ad1705926b67f0f9101ca"))
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
			Hash: plumbing.NewHash("334cde09da4afcb74f8d2b3e6fd6cce61228b485"),
		},
	}}
	changes[1] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: ".travis.yml",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: ".travis.yml",
			Mode: 0100644,
			Hash: plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe"),
		},
	},
	}
	deps["renamed_changes"] = changes
	result, err := burndown.Consume(deps)
	assert.Nil(t, result)
	assert.Nil(t, err)
	assert.Equal(t, burndown.previousDay, 0)
	assert.Equal(t, len(burndown.files), 2)
	assert.Equal(t, burndown.files[".travis.yml"].Len(), 12)
	assert.Equal(t, burndown.files["analyser.go"].Len(), 309)
	assert.Equal(t, len(burndown.people), 1)
	assert.Equal(t, burndown.people[0][0], int64(12 + 309))
	assert.Equal(t, len(burndown.globalStatus), 1)
	assert.Equal(t, burndown.globalStatus[0], int64(12 + 309))
	assert.Equal(t, len(burndown.globalHistory), 0)
	assert.Equal(t, len(burndown.fileHistories), 0)
	burndown = BurndownAnalysis{
		Granularity: 30,
		Sampling: 0,
		PeopleNumber: 0,
	}
	burndown.Initialize(testRepository)
	_, err = burndown.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(burndown.people), 0)
}
