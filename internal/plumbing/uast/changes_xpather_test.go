// +build !disable_babelfish

package uast

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/bblfsh/client-go.v2"
	uast_test "gopkg.in/src-d/hercules.v5/internal/plumbing/uast/test"
	"gopkg.in/src-d/hercules.v5/internal/test"
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
	xpather := ChangesXPather{XPath: "//*[@roleComment]"}
	nodes := xpather.Extract(uastChanges)
	assert.True(t, len(nodes) > 0)
}
