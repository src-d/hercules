// +build !disable_babelfish

package uast

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"gopkg.in/bblfsh/sdk.v2/uast"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	items "gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func fixtureUASTExtractor() *Extractor {
	exr := Extractor{Endpoint: "0.0.0.0:9432"}
	err := exr.Initialize(test.Repository)
	if err != nil {
		panic(err)
	}
	return &exr
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

func TestUASTExtractorMeta(t *testing.T) {
	exr := fixtureUASTExtractor()
	defer exr.Dispose()
	assert.Equal(t, exr.Name(), "UAST")
	assert.Equal(t, len(exr.Provides()), 1)
	assert.Equal(t, exr.Provides()[0], DependencyUasts)
	assert.Equal(t, len(exr.Requires()), 2)
	assert.Equal(t, exr.Requires()[0], items.DependencyTreeChanges)
	assert.Equal(t, exr.Requires()[1], items.DependencyBlobCache)
	opts := exr.ListConfigurationOptions()
	assert.Len(t, opts, 5)
	assert.Equal(t, opts[0].Name, ConfigUASTEndpoint)
	assert.Equal(t, opts[1].Name, ConfigUASTTimeout)
	assert.Equal(t, opts[2].Name, ConfigUASTPoolSize)
	assert.Equal(t, opts[3].Name, ConfigUASTFailOnErrors)
	assert.Equal(t, opts[4].Name, ConfigUASTIgnoreMissingDrivers)
	feats := exr.Features()
	assert.Len(t, feats, 1)
	assert.Equal(t, feats[0], FeatureUast)
	logger := core.NewLogger()
	assert.NoError(t, exr.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, exr.l)
}

func TestUASTExtractorConfiguration(t *testing.T) {
	exr := fixtureUASTExtractor()
	defer exr.Dispose()
	facts := map[string]interface{}{}
	assert.Nil(t, exr.Configure(facts))
	facts[ConfigUASTEndpoint] = "localhost:9432"
	facts[ConfigUASTTimeout] = 15
	facts[ConfigUASTPoolSize] = 7
	facts[ConfigUASTFailOnErrors] = true
	facts[ConfigUASTIgnoreMissingDrivers] = []string{"test"}
	assert.Nil(t, exr.Configure(facts))
	assert.Equal(t, exr.Endpoint, facts[ConfigUASTEndpoint])
	assert.NotNil(t, exr.Context)
	assert.Equal(t, exr.PoolSize, facts[ConfigUASTPoolSize])
	assert.Equal(t, exr.FailOnErrors, true)
	assert.Equal(t, exr.IgnoredMissingDrivers, map[string]bool{"test": true})
}

func TestUASTExtractorRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&Extractor{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "UAST")
	summoned = core.Registry.Summon((&Extractor{}).Provides()[0])
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "UAST")
}

func TestUASTExtractorNoBabelfish(t *testing.T) {
	exr := Extractor{Endpoint: "0.0.0.0:56934"}
	err := exr.Initialize(test.Repository)
	assert.NotNil(t, err)
}

func TestUASTExtractorConsume(t *testing.T) {
	exr := fixtureUASTExtractor()
	defer exr.Dispose()
	changes := make(object.Changes, 4)
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
	changes[2] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: "linux.png",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "linux.png",
			Mode: 0100644,
			Hash: plumbing.NewHash("81f2b6d1fa5357f90e9dead150cd515720897545"),
		},
	},
	}
	changes[3] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: "README.md",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "README.md",
			Mode: 0100644,
			Hash: plumbing.NewHash("5248c86995f6d60eb57730da18b5e020a4341863"),
		},
	},
	}
	cache := map[plumbing.Hash]*items.CachedBlob{}
	for _, hash := range []string{
		"baa64828831d174f40140e4b3cfa77d1e917a2c1",
		"5d78f57d732aed825764347ec6f3ab74d50d0619",
		"c29112dbd697ad9b401333b80c18a63951bc18d9",
		"f7d918ec500e2f925ecde79b51cc007bac27de72",
		"81f2b6d1fa5357f90e9dead150cd515720897545",
		"5248c86995f6d60eb57730da18b5e020a4341863",
	} {
		AddHash(t, cache, hash)
	}
	deps := map[string]interface{}{}
	deps[items.DependencyBlobCache] = cache
	deps[items.DependencyTreeChanges] = changes
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(
		plumbing.NewHash("2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	res, err := exr.Consume(deps)
	assert.Len(t, res[DependencyUasts], 1)
	assert.Nil(t, err)
	res, err = exr.Consume(deps)
	assert.Len(t, res[DependencyUasts], 1)
	assert.Nil(t, err)

	exr.FailOnErrors = true
	res, err = exr.Consume(deps)
	assert.Nil(t, res)
	assert.NotNil(t, err)
	exr.FailOnErrors = false

	hash := plumbing.NewHash("5d78f57d732aed825764347ec6f3ab74d50d0619")
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
	deps[items.DependencyTreeChanges] = changes[:2]

	res, err = exr.Consume(deps)
	assert.Nil(t, err)
	uasts := res[DependencyUasts].(map[plumbing.Hash]nodes.Node)
	assert.Equal(t, len(uasts), 1)
	assert.Equal(t, len(uasts[hash].(nodes.Object)["body"].(nodes.Array)), 24)

	exr.IgnoredMissingDrivers = map[string]bool{}
	changes[2] = changes[3]
	deps[items.DependencyTreeChanges] = changes[:3]
	res, err = exr.Consume(deps)
	assert.Nil(t, err)
	exr.FailOnErrors = true
	res, err = exr.Consume(deps)
	assert.Nil(t, res)
	assert.NotNil(t, err)
	exr.FailOnErrors = false
}

func TestUASTExtractorFork(t *testing.T) {
	exr1 := fixtureUASTExtractor()
	defer exr1.Dispose()
	clones := exr1.Fork(1)
	assert.Len(t, clones, 1)
	exr2 := clones[0].(*Extractor)
	assert.True(t, exr1 == exr2)
	exr1.Merge([]core.PipelineItem{exr2})
}

func fixtureUASTChanges() *Changes {
	ch := Changes{}
	ch.Configure(nil)
	ch.Initialize(test.Repository)
	return &ch
}

func TestUASTChangesMeta(t *testing.T) {
	ch := fixtureUASTChanges()
	assert.Equal(t, ch.Name(), "UASTChanges")
	assert.Equal(t, len(ch.Provides()), 1)
	assert.Equal(t, ch.Provides()[0], DependencyUastChanges)
	assert.Equal(t, len(ch.Requires()), 2)
	assert.Equal(t, ch.Requires()[0], DependencyUasts)
	assert.Equal(t, ch.Requires()[1], items.DependencyTreeChanges)
	opts := ch.ListConfigurationOptions()
	assert.Len(t, opts, 0)
	logger := core.NewLogger()
	assert.NoError(t, ch.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, ch.l)
}

func TestUASTChangesRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&Changes{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "UASTChanges")
	summoned = core.Registry.Summon((&Changes{}).Provides()[0])
	assert.True(t, len(summoned) >= 1)
	matched := false
	for _, tp := range summoned {
		matched = matched || tp.Name() == "UASTChanges"
	}
	assert.True(t, matched)
}

func newNodeWithType(name string) nodes.Node {
	return nodes.Object{
		uast.KeyType:  nodes.String(name),
		uast.KeyToken: nodes.String("my_token"),
	}
}

func TestUASTChangesConsume(t *testing.T) {
	var uastsArray []nodes.Node
	uasts := map[plumbing.Hash]nodes.Node{}
	hash := plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe")
	uasts[hash] = newNodeWithType("uno")
	uastsArray = append(uastsArray, uasts[hash])
	hash = plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9")
	uasts[hash] = newNodeWithType("dos")
	uastsArray = append(uastsArray, uasts[hash])
	hash = plumbing.NewHash("baa64828831d174f40140e4b3cfa77d1e917a2c1")
	uasts[hash] = newNodeWithType("tres")
	uastsArray = append(uastsArray, uasts[hash])
	hash = plumbing.NewHash("dc248ba2b22048cc730c571a748e8ffcf7085ab9")
	uasts[hash] = newNodeWithType("quatro")
	uastsArray = append(uastsArray, uasts[hash])
	changes := make(object.Changes, 3)
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"a1eb2ea76eb7f9bfbde9b243861474421000eb96"))
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
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
	deps[DependencyUasts] = uasts
	deps[items.DependencyTreeChanges] = changes
	ch := fixtureUASTChanges()
	ch.cache[changes[0].From.TreeEntry.Hash] = uastsArray[3]
	ch.cache[changes[2].From.TreeEntry.Hash] = uastsArray[0]
	resultMap, err := ch.Consume(deps)
	assert.Nil(t, err)
	result := resultMap[DependencyUastChanges].([]Change)
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

func TestUASTChangesFork(t *testing.T) {
	changes1 := fixtureUASTChanges()
	changes1.cache[plumbing.ZeroHash] = nil
	clones := changes1.Fork(1)
	assert.Len(t, clones, 1)
	changes2 := clones[0].(*Changes)
	assert.False(t, changes1 == changes2)
	assert.Equal(t, changes1.cache, changes2.cache)
	delete(changes1.cache, plumbing.ZeroHash)
	assert.Len(t, changes2.cache, 1)
	changes1.Merge([]core.PipelineItem{changes2})
}

func fixtureUASTChangesSaver() *ChangesSaver {
	ch := ChangesSaver{}
	ch.Initialize(test.Repository)
	return &ch
}

func TestUASTChangesSaverMeta(t *testing.T) {
	chs := fixtureUASTChangesSaver()
	assert.Equal(t, chs.Name(), "UASTChangesSaver")
	assert.True(t, len(chs.Description()) > 0)
	assert.Equal(t, len(chs.Provides()), 0)
	assert.Equal(t, len(chs.Requires()), 1)
	assert.Equal(t, chs.Requires()[0], DependencyUastChanges)
	opts := chs.ListConfigurationOptions()
	assert.Len(t, opts, 1)
	assert.Equal(t, opts[0].Name, ConfigUASTChangesSaverOutputPath)
	assert.Equal(t, chs.Flag(), "dump-uast-changes")
	logger := core.NewLogger()
	assert.NoError(t, chs.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, chs.l)
}

func TestUASTChangesSaverConfiguration(t *testing.T) {
	facts := map[string]interface{}{}
	chs := fixtureUASTChangesSaver()
	chs.Configure(facts)
	assert.Empty(t, chs.OutputPath)
	facts[ConfigUASTChangesSaverOutputPath] = "libre"
	chs.Configure(facts)
	assert.Equal(t, chs.OutputPath, "libre")
}

func TestUASTChangesSaverRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&ChangesSaver{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "UASTChangesSaver")
	leaves := core.Registry.GetLeaves()
	matched := false
	for _, tp := range leaves {
		if tp.Flag() == (&ChangesSaver{}).Flag() {
			matched = true
			break
		}
	}
	assert.True(t, matched)
}

func TestUASTChangesSaverPayload(t *testing.T) {
	chs := fixtureUASTChangesSaver()
	deps := map[string]interface{}{}
	changes := make([]Change, 1)
	deps[DependencyUastChanges] = changes
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(
		plumbing.NewHash("2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"a1eb2ea76eb7f9bfbde9b243861474421000eb96"))
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
		"994eac1cd07235bb9815e547a75c84265dea00f5"))
	changes[0] = Change{Before: nodes.Object{}, After: nodes.Object{},
		Change: &object.Change{From: object.ChangeEntry{
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
		}}}
	chs.Consume(deps)
	res := chs.Finalize()
	tmpdir, err := ioutil.TempDir("", "hercules-test-")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpdir)
	chs.OutputPath = tmpdir
	buffer := &bytes.Buffer{}
	chs.Serialize(res, true, buffer)
	pbResults := &pb.UASTChangesSaverResults{}
	proto.Unmarshal(buffer.Bytes(), pbResults)
	assert.Len(t, pbResults.Changes, 1)
	assert.Equal(t, pbResults.Changes[0].FileName, "analyser.go")
	assert.Equal(t, pbResults.Changes[0].SrcAfter,
		path.Join(tmpdir, "0_0_after_334cde09da4afcb74f8d2b3e6fd6cce61228b485.src"))
	assert.Equal(t, pbResults.Changes[0].SrcBefore,
		path.Join(tmpdir, "0_0_before_dc248ba2b22048cc730c571a748e8ffcf7085ab9.src"))
	assert.Equal(t, pbResults.Changes[0].UastAfter,
		path.Join(tmpdir, "0_0_after_334cde09da4afcb74f8d2b3e6fd6cce61228b485.pb"))
	assert.Equal(t, pbResults.Changes[0].UastBefore,
		path.Join(tmpdir, "0_0_before_dc248ba2b22048cc730c571a748e8ffcf7085ab9.pb"))
	checkFiles := func() {
		files, err := ioutil.ReadDir(tmpdir)
		assert.Nil(t, err)
		assert.Len(t, files, 4)
		names := map[string]int{
			"0_0_after_334cde09da4afcb74f8d2b3e6fd6cce61228b485.src":  1,
			"0_0_before_dc248ba2b22048cc730c571a748e8ffcf7085ab9.src": 1,
			"0_0_after_334cde09da4afcb74f8d2b3e6fd6cce61228b485.pb":   1,
			"0_0_before_dc248ba2b22048cc730c571a748e8ffcf7085ab9.pb":  1,
		}
		matches := 0
		for _, fi := range files {
			matches += names[fi.Name()]
			os.Remove(fi.Name())
		}
		assert.Equal(t, matches, len(names))
	}
	checkFiles()
	buffer.Truncate(0)
	chs.Serialize(res, false, buffer)
	assert.Equal(t, buffer.String(), fmt.Sprintf(`  - {file: analyser.go, src0: %s/0_0_before_dc248ba2b22048cc730c571a748e8ffcf7085ab9.src, src1: %s/0_0_after_334cde09da4afcb74f8d2b3e6fd6cce61228b485.src, uast0: %s/0_0_before_dc248ba2b22048cc730c571a748e8ffcf7085ab9.pb, uast1: %s/0_0_after_334cde09da4afcb74f8d2b3e6fd6cce61228b485.pb}
`, tmpdir, tmpdir, tmpdir, tmpdir))
	checkFiles()
}

func TestUASTChangesSaverConsumeMerge(t *testing.T) {
	chs := fixtureUASTChangesSaver()
	deps := map[string]interface{}{}
	changes := make([]Change, 1)
	deps[DependencyUastChanges] = changes
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(
		plumbing.NewHash("2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	treeFrom, _ := test.Repository.TreeObject(plumbing.NewHash(
		"a1eb2ea76eb7f9bfbde9b243861474421000eb96"))
	treeTo, _ := test.Repository.TreeObject(plumbing.NewHash(
		"994eac1cd07235bb9815e547a75c84265dea00f5"))
	changes[0] = Change{Before: nodes.Object{}, After: nodes.Object{},
		Change: &object.Change{From: object.ChangeEntry{
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
		}}}
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(
		plumbing.NewHash("cce947b98a050c6d356bc6ba95030254914027b1"))
	chs.Consume(deps)
	assert.Len(t, chs.result, 1)
	chs.Consume(deps)
	assert.Len(t, chs.result, 2)
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(
		plumbing.NewHash("dd9dd084d5851d7dc4399fc7dbf3d8292831ebc5"))
	chs.Consume(deps)
	assert.Len(t, chs.result, 3)
	chs.Consume(deps)
	assert.Len(t, chs.result, 3)
}

func TestUASTChangesSaverFork(t *testing.T) {
	saver1 := fixtureUASTChangesSaver()
	clones := saver1.Fork(1)
	assert.Len(t, clones, 1)
	saver2 := clones[0].(*ChangesSaver)
	assert.True(t, saver1 == saver2)
	saver1.Merge([]core.PipelineItem{saver2})
}
