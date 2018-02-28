package hercules

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v3/pb"
	"path"
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
	assert.Equal(t, exr.Provides()[0], DependencyUasts)
	assert.Equal(t, len(exr.Requires()), 2)
	assert.Equal(t, exr.Requires()[0], DependencyTreeChanges)
	assert.Equal(t, exr.Requires()[1], DependencyBlobCache)
	opts := exr.ListConfigurationOptions()
	assert.Len(t, opts, 5)
	assert.Equal(t, opts[0].Name, ConfigUASTEndpoint)
	assert.Equal(t, opts[1].Name, ConfigUASTTimeout)
	assert.Equal(t, opts[2].Name, ConfigUASTPoolSize)
	assert.Equal(t, opts[3].Name, ConfigUASTFailOnErrors)
	assert.Equal(t, opts[4].Name, ConfigUASTLanguages)
	feats := exr.Features()
	assert.Len(t, feats, 1)
	assert.Equal(t, feats[0], FeatureUast)
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
	deps[DependencyBlobCache] = cache
	deps[DependencyTreeChanges] = changes
	res, err := exr.Consume(deps)
	// Language not enabled
	assert.Len(t, res[DependencyUasts], 0)
	assert.Nil(t, err)
	exr.Languages["Go3000"] = true
	res, err = exr.Consume(deps)
	// No Go driver
	assert.Len(t, res[DependencyUasts], 0)
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
	uasts := res[DependencyUasts].(map[plumbing.Hash]*uast.Node)
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
	assert.Equal(t, ch.Provides()[0], DependencyUastChanges)
	assert.Equal(t, len(ch.Requires()), 2)
	assert.Equal(t, ch.Requires()[0], DependencyUasts)
	assert.Equal(t, ch.Requires()[1], DependencyTreeChanges)
	opts := ch.ListConfigurationOptions()
	assert.Len(t, opts, 0)
	feats := ch.Features()
	assert.Len(t, feats, 1)
	assert.Equal(t, feats[0], FeatureUast)
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
	deps[DependencyUasts] = uasts
	deps[DependencyTreeChanges] = changes
	ch := fixtureUASTChanges()
	ch.cache[changes[0].From.TreeEntry.Hash] = uastsArray[3]
	ch.cache[changes[2].From.TreeEntry.Hash] = uastsArray[0]
	resultMap, err := ch.Consume(deps)
	assert.Nil(t, err)
	result := resultMap[DependencyUastChanges].([]UASTChange)
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
	chs := fixtureUASTChangesSaver()
	assert.Equal(t, chs.Name(), "UASTChangesSaver")
	assert.Equal(t, len(chs.Provides()), 0)
	assert.Equal(t, len(chs.Requires()), 1)
	assert.Equal(t, chs.Requires()[0], DependencyUastChanges)
	opts := chs.ListConfigurationOptions()
	assert.Len(t, opts, 1)
	assert.Equal(t, opts[0].Name, ConfigUASTChangesSaverOutputPath)
	feats := chs.Features()
	assert.Len(t, feats, 1)
	assert.Equal(t, feats[0], FeatureUast)
	assert.Equal(t, chs.Flag(), "dump-uast-changes")
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
	tp, exists := Registry.registered[(&UASTChangesSaver{}).Name()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "UASTChangesSaver")
	tp, exists = Registry.flags[(&UASTChangesSaver{}).Flag()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "UASTChangesSaver")
}

func TestUASTChangesSaverPayload(t *testing.T) {
	chs := fixtureUASTChangesSaver()
	deps := map[string]interface{}{}
	changes := make([]UASTChange, 1)
	deps[DependencyUastChanges] = changes
	treeFrom, _ := testRepository.TreeObject(plumbing.NewHash(
		"a1eb2ea76eb7f9bfbde9b243861474421000eb96"))
	treeTo, _ := testRepository.TreeObject(plumbing.NewHash(
		"994eac1cd07235bb9815e547a75c84265dea00f5"))
	changes[0] = UASTChange{Before: &uast.Node{}, After: &uast.Node{},
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
