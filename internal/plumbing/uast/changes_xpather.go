package uast

import (
	"bytes"
	"io"
	"log"
	"sort"

	"github.com/minio/highwayhash"
	"gopkg.in/bblfsh/client-go.v3/tools"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

// ChangesXPather extracts changed UAST nodes from files changed in the current commit.
type ChangesXPather struct {
	XPath string
}

var hashKey = []byte{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
	16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
}

// Extract returns the list of (inserted, removed) UAST nodes filtered by XPath.
func (xpather ChangesXPather) Extract(changes []Change) ([]nodes.Node, []nodes.Node) {
	var resultAdded, resultRemoved []nodes.Node
	for _, change := range changes {
		if change.After == nil {
			continue
		}
		oldNodes := xpather.filter(change.Before, change.Change.From.TreeEntry.Hash)
		newNodes := xpather.filter(change.After, change.Change.To.TreeEntry.Hash)
		oldHashes := xpather.hash(oldNodes)
		newHashes := xpather.hash(newNodes)
		// there can be hash collisions; we ignore them
		for hash, node := range newHashes {
			if _, exists := oldHashes[hash]; !exists {
				resultAdded = append(resultAdded, node)
			}
		}
		for hash, node := range oldHashes {
			if _, exists := newHashes[hash]; !exists {
				resultRemoved = append(resultRemoved, node)
			}
		}
	}
	return resultAdded, resultRemoved
}

func (xpather ChangesXPather) filter(root nodes.Node, origin plumbing.Hash) []nodes.Node {
	if root == nil {
		return nil
	}
	filtered, err := tools.Filter(root, xpather.XPath)
	if err != nil {
		log.Printf("libuast filter error on object %s: %v", origin.String(), err)
		return []nodes.Node{}
	}
	var result []nodes.Node
	for filtered.Next() {
		result = append(result, filtered.Node().(nodes.Node))
	}
	return result
}

func (xpather ChangesXPather) hash(nodesToHash []nodes.Node) map[uint64]nodes.Node {
	result := map[uint64]nodes.Node{}
	for _, node := range nodesToHash {
		buffer := &bytes.Buffer{}
		stringifyUASTNode(node, buffer)
		result[highwayhash.Sum64(buffer.Bytes(), hashKey)] = node
	}
	return result
}

func stringifyUASTNode(node nodes.Node, writer io.Writer) {
	for element := range tools.Iterate(tools.NewIterator(node, tools.PositionOrder)) {
		var keys []string
		obj := element.(nodes.Object)
		for key, val := range obj {
			if _, ok := val.(nodes.String); ok {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			writer.Write([]byte(key + "=" + string(obj[key].(nodes.String)) + ";"))
		}
	}
}
