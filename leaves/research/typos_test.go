// +build !disable_babelfish

package research

import (
	"bytes"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"gopkg.in/bblfsh/client-go.v3/tools"
	"gopkg.in/bblfsh/sdk.v2/uast"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	items "gopkg.in/src-d/hercules.v10/internal/plumbing"
	uast_items "gopkg.in/src-d/hercules.v10/internal/plumbing/uast"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func TestTyposDatasetMeta(t *testing.T) {
	tdb := TyposDatasetBuilder{}
	assert.Equal(t, tdb.Name(), "TyposDataset")
	assert.Len(t, tdb.Provides(), 0)
	required := [...]string{
		uast_items.DependencyUastChanges, items.DependencyFileDiff, items.DependencyBlobCache}
	for _, name := range required {
		assert.Contains(t, tdb.Requires(), name)
	}
	opts := tdb.ListConfigurationOptions()
	assert.Len(t, opts, 1)
	assert.Equal(t, opts[0].Name, ConfigTyposDatasetMaximumAllowedDistance)
	assert.Equal(t, opts[0].Type, core.IntConfigurationOption)
	assert.Equal(t, tdb.Flag(), "typos-dataset")
	logger := core.NewLogger()
	assert.NoError(t, tdb.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, tdb.l)
}

func TestTyposDatasetRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&TyposDatasetBuilder{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "TyposDataset")
	leaves := core.Registry.GetLeaves()
	matched := false
	for _, tp := range leaves {
		if tp.Flag() == (&TyposDatasetBuilder{}).Flag() {
			matched = true
			break
		}
	}
	assert.True(t, matched)
}

func TestTyposDatasetConfigure(t *testing.T) {
	tdb := TyposDatasetBuilder{}
	facts := map[string]interface{}{}
	facts[ConfigTyposDatasetMaximumAllowedDistance] = 5
	assert.Nil(t, tdb.Configure(facts))
	assert.Equal(t, tdb.MaximumAllowedDistance, 5)
	facts = map[string]interface{}{}
	assert.Nil(t, tdb.Configure(facts))
	assert.Equal(t, tdb.MaximumAllowedDistance, 5)
}

func AddHash(t *testing.T, cache map[plumbing.Hash]*items.CachedBlob, hash string) {
	objhash := plumbing.NewHash(hash)
	blob, err := test.Repository.BlobObject(objhash)
	assert.Nil(t, err)
	cb := &items.CachedBlob{Blob: *blob}
	err = cb.Cache()
	assert.Nil(t, err)
	cache[objhash] = cb
}

func bakeTyposDeps(t *testing.T) map[string]interface{} {
	deps := map[string]interface{}{}
	cache := map[plumbing.Hash]*items.CachedBlob{}
	AddHash(t, cache, "b9a12fd144274c99c7c9a0a32a0268f8b36d2f2c")
	AddHash(t, cache, "d5f8e61069136f3578457a3131800ede353527b8")
	AddHash(t, cache, "75bb0a09fc01db55d7322f0fae523453edba7846")
	deps[items.DependencyBlobCache] = cache
	changes := make(object.Changes, 2)
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"eac25f9126db00e38fa72a59d49773a84580d4ce"))
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
		"828467b465864b1f757dcec9a034be49030fc8b9"))
	changes[0] = &object.Change{From: object.ChangeEntry{
		Name: "file_test.go",
		Tree: treeFrom,
		TreeEntry: object.TreeEntry{
			Name: "file_test.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("75bb0a09fc01db55d7322f0fae523453edba7846"),
		},
	}, To: object.ChangeEntry{
		Name: "file_test.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "file_test.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("75bb0a09fc01db55d7322f0fae523453edba7846"),
		},
	}}
	changes[1] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: "blob_cache_test.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "blob_cache_test.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("b9a12fd144274c99c7c9a0a32a0268f8b36d2f2c"),
		},
	},
	}
	deps[items.DependencyTreeChanges] = changes
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(plumbing.NewHash(
		"84165d3b02647fae12cc026c7a580045246e8c98"))
	deps[core.DependencyIsMerge] = false
	uastItem := &uast_items.Extractor{}
	assert.Nil(t, uastItem.Initialize(test.Repository))
	uastResult, err := uastItem.Consume(deps)
	assert.Nil(t, err)
	deps[uast_items.DependencyUasts] = uastResult[uast_items.DependencyUasts]
	uastChanges := &uast_items.Changes{}
	assert.Nil(t, uastChanges.Initialize(test.Repository))
	_, err = uastChanges.Consume(deps)
	assert.Nil(t, err)
	changes[0].To.TreeEntry.Hash = plumbing.NewHash("d5f8e61069136f3578457a3131800ede353527b8")
	uastResult, err = uastItem.Consume(deps)
	assert.Nil(t, err)
	deps[uast_items.DependencyUasts] = uastResult[uast_items.DependencyUasts]
	changesResult, err := uastChanges.Consume(deps)
	assert.Nil(t, err)
	deps[uast_items.DependencyUastChanges] = changesResult[uast_items.DependencyUastChanges]
	fd := &items.FileDiff{}
	assert.Nil(t, fd.Initialize(test.Repository))
	diffResult, err := fd.Consume(deps)
	assert.Nil(t, err)
	deps[items.DependencyFileDiff] = diffResult[items.DependencyFileDiff]
	return deps
}

func TestTyposDatasetConsume(t *testing.T) {
	deps := bakeTyposDeps(t)
	tbd := &TyposDatasetBuilder{}
	assert.Nil(t, tbd.Initialize(test.Repository))
	res, err := tbd.Consume(deps)
	assert.Nil(t, res)
	assert.Nil(t, err)
	assert.Len(t, tbd.typos, 26)
	assert.Equal(t, tbd.typos[0].Wrong, "TestInitialize")
	assert.Equal(t, tbd.typos[0].Correct, "TestInitializeFile")
	assert.Equal(t, tbd.typos[0].Commit, plumbing.NewHash(
		"84165d3b02647fae12cc026c7a580045246e8c98"))
	assert.Equal(t, tbd.typos[0].File, "file_test.go")
	assert.Equal(t, tbd.typos[0].Line, 19)

	deps[core.DependencyIsMerge] = true
	res, err = tbd.Consume(deps)
	assert.Nil(t, res)
	assert.Nil(t, err)
	assert.Len(t, tbd.typos, 26)
}

func dropPositions(root nodes.Node, target string) {
	for element := range tools.Iterate(tools.NewIterator(root, tools.PreOrder)) {
		obj, isobj := element.(nodes.Object)
		if !isobj {
			continue
		}
		nameval, exists := obj["Name"]
		if !exists {
			continue
		}
		if name, isstr := nameval.(nodes.String); !isstr || string(name) != target {
			continue
		}
		posval, exists := obj[uast.KeyPos]
		if !exists {
			continue
		}
		posobj, isobj := posval.(nodes.Object)
		if !isobj {
			continue
		}
		for k, v := range posobj {
			po, _ := v.(nodes.Object)
			if uast.AsPosition(po) != nil {
				posobj[k] = nil
			}
		}
	}
}

func TestTyposDatasetConsumeMissingPosition(t *testing.T) {
	deps := bakeTyposDeps(t)
	uastChanges := deps[uast_items.DependencyUastChanges].([]uast_items.Change)
	dropPositions(uastChanges[0].Before, "TestZeroInitialize")
	dropPositions(uastChanges[0].After, "TestZeroInitializeFile")
	tbd := &TyposDatasetBuilder{}
	assert.Nil(t, tbd.Initialize(test.Repository))
	res, err := tbd.Consume(deps)
	assert.Nil(t, res)
	assert.Nil(t, err)
	assert.Len(t, tbd.typos, 25)
}

func fixtureTyposDataset() *TyposDatasetBuilder {
	tdb := TyposDatasetBuilder{}
	tdb.Initialize(test.Repository)
	tdb.typos = append(tdb.typos, Typo{
		Wrong:   "Fo",
		Correct: "Foo",
		Commit:  plumbing.ZeroHash,
		File:    "bar.go",
		Line:    7,
	})
	return &tdb
}

func TestTyposDatasetFinalize(t *testing.T) {
	tdb := fixtureTyposDataset()
	tdb.typos = append(tdb.typos, tdb.typos[0])
	x := tdb.Finalize().(TyposResult)
	assert.Len(t, x.Typos, 1)
	assert.Equal(t, x.Typos[0], Typo{
		Wrong:   "Fo",
		Correct: "Foo",
		Commit:  plumbing.ZeroHash,
		File:    "bar.go",
		Line:    7,
	})
}

func TestTyposDatasetSerialize(t *testing.T) {
	ca := fixtureTyposDataset()
	res := ca.Finalize().(TyposResult)
	buffer := &bytes.Buffer{}
	err := ca.Serialize(res, false, buffer)
	assert.Nil(t, err)
	assert.Equal(t, `  - wrong: "Fo"
    correct: "Foo"
    commit: 0000000000000000000000000000000000000000
    file: "bar.go"
    line: 7
`, buffer.String())

	buffer = &bytes.Buffer{}
	err = ca.Serialize(res, true, buffer)
	assert.Nil(t, err)
	msg := pb.TyposDataset{}
	assert.Nil(t, proto.Unmarshal(buffer.Bytes(), &msg))
	assert.Len(t, msg.Typos, 1)
	assert.Equal(t, *msg.Typos[0], pb.Typo{
		Wrong:   "Fo",
		Correct: "Foo",
		Commit:  "0000000000000000000000000000000000000000",
		File:    "bar.go",
		Line:    7,
	})
}
