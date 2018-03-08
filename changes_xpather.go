package hercules

import (
	"log"

	"github.com/minio/highwayhash"
	"gopkg.in/bblfsh/client-go.v2/tools"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"io"
	"bytes"
)

// ChangesXPather extracts changed UAST nodes from files changed in the current commit.
type ChangesXPather struct {
	XPath string
}

var hashKey = []byte{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
	16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
}

// Extract returns the list of new or changed UAST nodes filtered by XPath.
func (xpather ChangesXPather) Extract(changes []UASTChange) []*uast.Node {
	result := []*uast.Node{}
	for _, change := range changes {
		if change.After == nil {
			continue
		}
		oldNodes := xpather.filter(change.Before, change.Change.From.TreeEntry.Hash)
		newNodes := xpather.filter(change.After, change.Change.To.TreeEntry.Hash)
		oldHashes := xpather.hash(oldNodes)
		newHashes := xpather.hash(newNodes)
		// remove any untouched nodes
		for hash := range oldHashes {
			delete(newHashes, hash)
		}
		// there can be hash collisions; we ignore them
		for _, node := range newHashes {
			result = append(result, node)
		}
	}
	return result
}

func (xpather ChangesXPather) filter(root *uast.Node, origin plumbing.Hash) []*uast.Node {
	if root != nil {
		nodes, err := tools.Filter(root, xpather.XPath)
		if err != nil {
			log.Printf("libuast filter error on object %s: %v", origin.String(), err)
			return []*uast.Node{}
		}
		return nodes
	}
	return []*uast.Node{}
}

func (xpather ChangesXPather) hash(nodes []*uast.Node) map[uint64]*uast.Node {
	result := map[uint64]*uast.Node{}
	for _, node := range nodes {
		buffer := &bytes.Buffer{}
		stringifyUASTNode(node, buffer)
		result[highwayhash.Sum64(buffer.Bytes(), hashKey)] = node
	}
	return result
}

func stringifyUASTNode(node *uast.Node, writer io.Writer) {
	writer.Write([]byte(node.Token + "|" + node.InternalType + ">"))
	for _, child := range node.Children {
		stringifyUASTNode(child, writer)
	}
}
