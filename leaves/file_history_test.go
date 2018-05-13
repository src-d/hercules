package leaves

import (
	"bytes"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v4/internal/pb"
)

func fixtureFileHistory() *FileHistory {
	fh := FileHistory{}
	fh.Initialize(testRepository)
	return &fh
}

func TestFileHistoryMeta(t *testing.T) {
	fh := fixtureFileHistory()
	assert.Equal(t, fh.Name(), "FileHistory")
	assert.Equal(t, len(fh.Provides()), 0)
	assert.Equal(t, len(fh.Requires()), 1)
	assert.Equal(t, fh.Requires()[0], DependencyTreeChanges)
	assert.Len(t, fh.ListConfigurationOptions(), 0)
	fh.Configure(nil)
}

func TestFileHistoryRegistration(t *testing.T) {
	tp, exists := Registry.registered[(&FileHistory{}).Name()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "FileHistory")
	tp, exists = Registry.flags[(&FileHistory{}).Flag()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "FileHistory")
}

func TestFileHistoryConsume(t *testing.T) {
	fh := fixtureFileHistory()
	deps := map[string]interface{}{}
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
	changes[1] = &object.Change{To: object.ChangeEntry{}, From: object.ChangeEntry{
		Name: "cmd/hercules/main.go",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: "cmd/hercules/main.go",
			Mode: 0100644,
			Hash: plumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9"),
		},
	},
	}
	changes[2] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: ".travis.yml",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: ".travis.yml",
			Mode: 0100644,
			Hash: plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe"),
		},
	},
	}
	deps[DependencyTreeChanges] = changes
	commit, _ := testRepository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	deps["commit"] = commit
	fh.files["cmd/hercules/main.go"] = []plumbing.Hash{plumbing.NewHash(
		"0000000000000000000000000000000000000000")}
	fh.files["analyser.go"] = []plumbing.Hash{plumbing.NewHash(
		"ffffffffffffffffffffffffffffffffffffffff")}
	fh.Consume(deps)
	assert.Len(t, fh.files, 2)
	assert.Nil(t, fh.files["cmd/hercules/main.go"])
	assert.Len(t, fh.files["analyser.go"], 2)
	assert.Equal(t, fh.files["analyser.go"][0], plumbing.NewHash(
		"ffffffffffffffffffffffffffffffffffffffff"))
	assert.Equal(t, fh.files["analyser.go"][1], plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	assert.Len(t, fh.files[".travis.yml"], 1)
	assert.Equal(t, fh.files[".travis.yml"][0], plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	res := fh.Finalize().(FileHistoryResult)
	assert.Equal(t, fh.files, res.Files)
}

func TestFileHistorySerializeText(t *testing.T) {
	fh := fixtureFileHistory()
	deps := map[string]interface{}{}
	changes := make(object.Changes, 1)
	treeTo, _ := testRepository.TreeObject(plumbing.NewHash(
		"994eac1cd07235bb9815e547a75c84265dea00f5"))
	changes[0] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: ".travis.yml",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: ".travis.yml",
			Mode: 0100644,
			Hash: plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe"),
		},
	},
	}
	deps[DependencyTreeChanges] = changes
	commit, _ := testRepository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	deps["commit"] = commit
	fh.Consume(deps)
	res := fh.Finalize().(FileHistoryResult)
	buffer := &bytes.Buffer{}
	fh.Serialize(res, false, buffer)
	assert.Equal(t, buffer.String(), "  - .travis.yml: [\"2b1ed978194a94edeabbca6de7ff3b5771d4d665\"]\n")
}

func TestFileHistorySerializeBinary(t *testing.T) {
	fh := fixtureFileHistory()
	deps := map[string]interface{}{}
	changes := make(object.Changes, 1)
	treeTo, _ := testRepository.TreeObject(plumbing.NewHash(
		"994eac1cd07235bb9815e547a75c84265dea00f5"))
	changes[0] = &object.Change{From: object.ChangeEntry{}, To: object.ChangeEntry{
		Name: ".travis.yml",
		Tree: treeTo,
		TreeEntry: object.TreeEntry{
			Name: ".travis.yml",
			Mode: 0100644,
			Hash: plumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe"),
		},
	},
	}
	deps[DependencyTreeChanges] = changes
	commit, _ := testRepository.CommitObject(plumbing.NewHash(
		"2b1ed978194a94edeabbca6de7ff3b5771d4d665"))
	deps["commit"] = commit
	fh.Consume(deps)
	res := fh.Finalize().(FileHistoryResult)
	buffer := &bytes.Buffer{}
	fh.Serialize(res, true, buffer)
	msg := pb.FileHistoryResultMessage{}
	proto.Unmarshal(buffer.Bytes(), &msg)
	assert.Len(t, msg.Files, 1)
	assert.Len(t, msg.Files[".travis.yml"].Commits, 1)
	assert.Equal(t, msg.Files[".travis.yml"].Commits[0], "2b1ed978194a94edeabbca6de7ff3b5771d4d665")
}
