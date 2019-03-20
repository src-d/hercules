// +build !disable_babelfish

package uast

import (
	"log"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/bblfsh/client-go.v3"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	uast_test "gopkg.in/src-d/hercules.v10/internal/plumbing/uast/test"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func TestChangesXPatherExtractChanged(t *testing.T) {
	client, err := bblfsh.NewClient("0.0.0.0:9432")
	if err != nil {
		log.Panicf("Failed to connect to the Babelfish server at 0.0.0.0:9432: %v", err)
	}
	hash1 := "a98a6940eb4cfb1eb635c3232485a75c4b63fff3"
	hash2 := "42457dc695fa73ec9621b47832d5711f6325410d"
	root1 := uast_test.ParseBlobFromTestRepo(hash1, "burndown.go", client)
	root2 := uast_test.ParseBlobFromTestRepo(hash2, "burndown.go", client)
	gitChange := test.FakeChangeForName("burndown.go", hash1, hash2)
	uastChanges := []Change{
		{Before: root1, After: root2, Change: gitChange},
		{Before: nil, After: root2, Change: gitChange},
		{Before: root1, After: nil, Change: gitChange},
	}
	xpather := ChangesXPather{XPath: "//uast:Comment"}
	nodesAdded, nodesRemoved := xpather.Extract(uastChanges)
	sort.Slice(nodesRemoved, func(i, j int) bool {
		return nodesRemoved[i].(nodes.Object)["Text"].(nodes.String) <
			nodesRemoved[j].(nodes.Object)["Text"].(nodes.String)
	})
	for _, n := range nodesAdded {
		assert.True(t, len(n.(nodes.Object)["Text"].(nodes.String)) > 0)
	}
	for _, n := range nodesRemoved[1:] {
		assert.True(t, len(n.(nodes.Object)["Text"].(nodes.String)) > 0)
	}
	assert.True(t, len(nodesAdded) > 0)
	assert.True(t, len(nodesRemoved) > 0)
}
