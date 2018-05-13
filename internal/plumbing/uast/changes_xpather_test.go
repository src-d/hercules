// +build !disable_babelfish

package uast

import (
	"io/ioutil"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/bblfsh/client-go.v2"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

func TestChangesXPatherExtractChanged(t *testing.T) {
	client, err := bblfsh.NewClient("0.0.0.0:9432")
	if err != nil {
		log.Panicf("Failed to connect to the Babelfish server at 0.0.0.0:9432: %v", err)
	}
	hash1 := "a98a6940eb4cfb1eb635c3232485a75c4b63fff3"
	hash2 := "42457dc695fa73ec9621b47832d5711f6325410d"
	root1 := parseBlobFromTestRepo(hash1, "burndown.go", client)
	root2 := parseBlobFromTestRepo(hash2, "burndown.go", client)
	gitChange := fakeChangeForName("burndown.go", hash1, hash2)
	uastChanges := []UASTChange{
		{Before: root1, After: root2, Change: gitChange},
		{Before: nil, After: root2, Change: gitChange},
		{Before: root1, After: nil, Change: gitChange},
	}
	xpather := ChangesXPather{XPath: "//*[@roleComment]"}
	nodes := xpather.Extract(uastChanges)
	assert.True(t, len(nodes) > 0)
}

func parseBlobFromTestRepo(hash, name string, client *bblfsh.Client) *uast.Node {
	blob, err := testRepository.BlobObject(plumbing.NewHash(hash))
	if err != nil {
		panic(err)
	}
	reader, err := blob.Reader()
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	request := client.NewParseRequest()
	request.Content(string(data))
	request.Filename(name)
	response, err := request.Do()
	if err != nil {
		panic(err)
	}
	return response.UAST
}

func fakeChangeForName(name string, hashFrom string, hashTo string) *object.Change {
	return &object.Change{
		From: object.ChangeEntry{Name: name, TreeEntry: object.TreeEntry{
			Name: name, Hash: plumbing.NewHash(hashFrom),
		}},
		To: object.ChangeEntry{Name: name, TreeEntry: object.TreeEntry{
			Name: name, Hash: plumbing.NewHash(hashTo),
		}},
	}
}
