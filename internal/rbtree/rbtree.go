package rbtree

import (
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/gogo/protobuf/sortkeys"
	"gopkg.in/src-d/go-git.v4/utils/binary"
)

//
// Public definitions
//

// Item is the object stored in each tree node.
type Item struct {
	Key   uint32
	Value uint32
}

// Allocator is the allocator for nodes in a RBTree.
type Allocator struct {
	HibernationThreshold int

	storage              []node
	gaps                 map[uint32]bool
	hibernatedData       [7][]byte
	hibernatedStorageLen int
	hibernatedGapsLen    int
}

// NewAllocator creates a new allocator for RBTree's nodes.
func NewAllocator() *Allocator {
	return &Allocator{
		storage: []node{},
		gaps:    map[uint32]bool{},
	}
}

// Size returns the currently allocated size.
func (allocator Allocator) Size() int {
	return len(allocator.storage)
}

// Used returns the number of nodes contained in the allocator.
func (allocator Allocator) Used() int {
	if allocator.storage == nil {
		panic("hibernated allocators cannot be used")
	}
	return len(allocator.storage) - len(allocator.gaps)
}

// Clone copies an existing RBTree allocator.
func (allocator Allocator) Clone() *Allocator {
	if allocator.storage == nil {
		panic("cannot clone a hibernated allocator")
	}
	newAllocator := &Allocator{
		HibernationThreshold: allocator.HibernationThreshold,
		storage:              make([]node, len(allocator.storage), cap(allocator.storage)),
		gaps:                 map[uint32]bool{},
	}
	copy(newAllocator.storage, allocator.storage)
	for key, val := range allocator.gaps {
		newAllocator.gaps[key] = val
	}
	return newAllocator
}

// Hibernate compresses the allocated memory.
func (allocator *Allocator) Hibernate() {
	if allocator.hibernatedStorageLen > 0 {
		panic("cannot hibernate an already hibernated Allocator")
	}
	if len(allocator.storage) < allocator.HibernationThreshold {
		return
	}
	allocator.hibernatedStorageLen = len(allocator.storage)
	if allocator.hibernatedStorageLen == 0 {
		return
	}
	buffers := [6][]uint32{}
	for i := 0; i < len(buffers); i++ {
		buffers[i] = make([]uint32, len(allocator.storage))
	}
	// we deinterleave to achieve a better compression ratio
	for i, n := range allocator.storage {
		buffers[0][i] = n.item.Key
		buffers[1][i] = n.item.Value
		buffers[2][i] = n.left
		buffers[3][i] = n.parent
		buffers[4][i] = n.right
		if n.color {
			buffers[5][i] = 1
		}
	}
	allocator.storage = nil
	wg := &sync.WaitGroup{}
	wg.Add(len(buffers) + 1)
	for i, buffer := range buffers {
		go func(i int, buffer []uint32) {
			allocator.hibernatedData[i] = CompressUInt32Slice(buffer)
			buffers[i] = nil
			wg.Done()
		}(i, buffer)
	}
	// compress gaps
	go func() {
		if len(allocator.gaps) > 0 {
			allocator.hibernatedGapsLen = len(allocator.gaps)
			gapsBuffer := make([]uint32, len(allocator.gaps))
			i := 0
			for key := range allocator.gaps {
				gapsBuffer[i] = key
				i++
			}
			sortkeys.Uint32s(gapsBuffer)
			allocator.hibernatedData[len(buffers)] = CompressUInt32Slice(gapsBuffer)
		}
		allocator.gaps = nil
		wg.Done()
	}()
	wg.Wait()
}

// Boot performs the opposite of Hibernate() - decompresses and restores the allocated memory.
func (allocator *Allocator) Boot() {
	if allocator.hibernatedStorageLen == 0 {
		// not hibernated
		return
	}
	if allocator.hibernatedData[0] == nil {
		panic("cannot boot a serialized Allocator")
	}
	allocator.gaps = map[uint32]bool{}
	buffers := [6][]uint32{}
	wg := &sync.WaitGroup{}
	wg.Add(len(buffers) + 1)
	for i := 0; i < len(buffers); i++ {
		go func(i int) {
			buffers[i] = make([]uint32, allocator.hibernatedStorageLen)
			DecompressUInt32Slice(allocator.hibernatedData[i], buffers[i])
			allocator.hibernatedData[i] = nil
			wg.Done()
		}(i)
	}
	go func() {
		if allocator.hibernatedGapsLen > 0 {
			gapData := allocator.hibernatedData[len(buffers)]
			buffer := make([]uint32, allocator.hibernatedGapsLen)
			DecompressUInt32Slice(gapData, buffer)
			for _, key := range buffer {
				allocator.gaps[key] = true
			}
			allocator.hibernatedData[len(buffers)] = nil
			allocator.hibernatedGapsLen = 0
		}
		wg.Done()
	}()
	wg.Wait()
	allocator.storage = make([]node, allocator.hibernatedStorageLen, (allocator.hibernatedStorageLen*3)/2)
	for i := range allocator.storage {
		n := &allocator.storage[i]
		n.item.Key = buffers[0][i]
		n.item.Value = buffers[1][i]
		n.left = buffers[2][i]
		n.parent = buffers[3][i]
		n.right = buffers[4][i]
		n.color = buffers[5][i] > 0
	}
	allocator.hibernatedStorageLen = 0
}

// Serialize writes the hibernated allocator on disk.
func (allocator *Allocator) Serialize(path string) error {
	if allocator.storage != nil {
		panic("serialization requires the hibernated state")
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	err = binary.WriteVariableWidthInt(file, int64(allocator.hibernatedStorageLen))
	if err != nil {
		return err
	}
	err = binary.WriteVariableWidthInt(file, int64(allocator.hibernatedGapsLen))
	if err != nil {
		return err
	}
	for i, hse := range allocator.hibernatedData {
		err = binary.WriteVariableWidthInt(file, int64(len(hse)))
		if err != nil {
			return err
		}
		_, err = file.Write(hse)
		if err != nil {
			return err
		}
		allocator.hibernatedData[i] = nil
	}
	return nil
}

// Deserialize reads a hibernated allocator from disk.
func (allocator *Allocator) Deserialize(path string) error {
	if allocator.storage != nil {
		panic("deserialization requires the hibernated state")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	x, err := binary.ReadVariableWidthInt(file)
	if err != nil {
		return err
	}
	allocator.hibernatedStorageLen = int(x)
	x, err = binary.ReadVariableWidthInt(file)
	if err != nil {
		return err
	}
	allocator.hibernatedGapsLen = int(x)
	for i := range allocator.hibernatedData {
		x, err = binary.ReadVariableWidthInt(file)
		if err != nil {
			return err
		}
		allocator.hibernatedData[i] = make([]byte, int(x))
		n, err := file.Read(allocator.hibernatedData[i])
		if err != nil {
			return err
		}
		if n != int(x) {
			return fmt.Errorf("incomplete read %d: %d instead of %d", i, n, x)
		}
	}
	return nil
}

func (allocator *Allocator) malloc() uint32 {
	if allocator.storage == nil {
		panic("hibernated allocators cannot be used")
	}
	if len(allocator.gaps) > 0 {
		var key uint32
		for key = range allocator.gaps {
			break
		}
		delete(allocator.gaps, key)
		return key
	}
	n := len(allocator.storage)
	if n == 0 {
		// zero is reserved
		allocator.storage = append(allocator.storage, node{})
		n = 1
	}
	if n == negativeLimitNode-1 {
		// math.MaxUint32 is reserved
		panic("the size of my RBTree allocator has reached the maximum value for uint32, sorry")
	}
	doAssert(n < negativeLimitNode)
	allocator.storage = append(allocator.storage, node{})
	return uint32(n)
}

func (allocator *Allocator) free(n uint32) {
	if allocator.storage == nil {
		panic("hibernated allocators cannot be used")
	}
	if n == 0 {
		panic("node #0 is special and cannot be deallocated")
	}
	_, exists := allocator.gaps[n]
	doAssert(!exists)
	allocator.storage[n] = node{}
	allocator.gaps[n] = true
}

// RBTree is a red-black tree with an API similar to C++ STL's.
//
// The implementation is inspired (read: stolen) from:
// http://en.literateprograms.org/Red-black_tree_(C)#chunk use:private function prototypes.
//
// The code was optimized for the simple integer types of Key and Value.
// The code was further optimized for using allocators.
// Credits: Yaz Saito.
type RBTree struct {
	// Root of the tree
	root uint32

	// The minimum and maximum nodes under the tree.
	minNode, maxNode uint32

	// Number of nodes under root, including the root
	count int32

	// Nodes allocator
	allocator *Allocator
}

// NewRBTree creates a new red-black binary tree.
func NewRBTree(allocator *Allocator) *RBTree {
	return &RBTree{allocator: allocator}
}

func (tree RBTree) storage() []node {
	return tree.allocator.storage
}

// Allocator returns the bound nodes allocator.
func (tree RBTree) Allocator() *Allocator {
	return tree.allocator
}

// Len returns the number of elements in the tree.
func (tree RBTree) Len() int {
	return int(tree.count)
}

// CloneShallow performs a shallow copy of the tree - the nodes are assumed to already exist in the allocator.
func (tree RBTree) CloneShallow(allocator *Allocator) *RBTree {
	clone := tree
	clone.allocator = allocator
	return &clone
}

// CloneDeep performs a deep copy of the tree - the nodes are created from scratch.
func (tree RBTree) CloneDeep(allocator *Allocator) *RBTree {
	clone := &RBTree{
		count:     tree.count,
		allocator: allocator,
	}
	nodeMap := map[uint32]uint32{}
	originStorage := tree.storage()
	for iter := tree.Min(); !iter.Limit(); iter = iter.Next() {
		newNode := allocator.malloc()
		cloneNode := &allocator.storage[newNode]
		cloneNode.item = *iter.Item()
		cloneNode.color = originStorage[iter.node].color
		nodeMap[iter.node] = newNode
	}
	cloneStorage := allocator.storage
	for iter := tree.Min(); !iter.Limit(); iter = iter.Next() {
		cloneNode := &cloneStorage[nodeMap[iter.node]]
		originNode := originStorage[iter.node]
		cloneNode.left = nodeMap[originNode.left]
		cloneNode.right = nodeMap[originNode.right]
		cloneNode.parent = nodeMap[originNode.parent]
	}
	clone.root = nodeMap[tree.root]
	clone.minNode = nodeMap[tree.minNode]
	clone.maxNode = nodeMap[tree.maxNode]
	return clone
}

// Erase removes all the nodes from the tree.
func (tree *RBTree) Erase() {
	nodes := make([]uint32, 0, tree.count)
	for iter := tree.Min(); !iter.Limit(); iter = iter.Next() {
		nodes = append(nodes, iter.node)
	}
	for _, node := range nodes {
		tree.allocator.free(node)
	}
	tree.root = 0
	tree.minNode = 0
	tree.maxNode = 0
	tree.count = 0
}

// Get is a convenience function for finding an element equal to Key. Returns
// nil if not found.
func (tree RBTree) Get(key uint32) *uint32 {
	n, exact := tree.findGE(key)
	if exact {
		return &tree.storage()[n].item.Value
	}
	return nil
}

// Min creates an iterator that points to the minimum item in the tree.
// If the tree is empty, returns Limit()
func (tree *RBTree) Min() Iterator {
	return Iterator{tree, tree.minNode}
}

// Max creates an iterator that points at the maximum item in the tree.
//
// If the tree is empty, returns NegativeLimit().
func (tree *RBTree) Max() Iterator {
	if tree.maxNode == 0 {
		return Iterator{tree, negativeLimitNode}
	}
	return Iterator{tree, tree.maxNode}
}

// Limit creates an iterator that points beyond the maximum item in the tree.
func (tree *RBTree) Limit() Iterator {
	return Iterator{tree, 0}
}

// NegativeLimit creates an iterator that points before the minimum item in the tree.
func (tree *RBTree) NegativeLimit() Iterator {
	return Iterator{tree, negativeLimitNode}
}

// FindGE finds the smallest element N such that N >= Key, and returns the
// iterator pointing to the element. If no such element is found,
// returns tree.Limit().
func (tree *RBTree) FindGE(key uint32) Iterator {
	n, _ := tree.findGE(key)
	return Iterator{tree, n}
}

// FindLE finds the largest element N such that N <= Key, and returns the
// iterator pointing to the element. If no such element is found,
// returns iter.NegativeLimit().
func (tree *RBTree) FindLE(key uint32) Iterator {
	n, exact := tree.findGE(key)
	if exact {
		return Iterator{tree, n}
	}
	if n != 0 {
		return Iterator{tree, doPrev(n, tree.storage())}
	}
	if tree.maxNode == 0 {
		return Iterator{tree, negativeLimitNode}
	}
	return Iterator{tree, tree.maxNode}
}

// Insert an item. If the item is already in the tree, do nothing and
// return false. Else return true.
func (tree *RBTree) Insert(item Item) (bool, Iterator) {
	// TODO: delay creating n until it is found to be inserted
	n := tree.doInsert(item)
	if n == 0 {
		return false, Iterator{}
	}
	alloc := tree.storage()
	insN := n

	alloc[n].color = red

	for true {
		// Case 1: N is at the root
		if alloc[n].parent == 0 {
			alloc[n].color = black
			break
		}

		// Case 2: The parent is black, so the tree already
		// satisfies the RB properties
		if alloc[alloc[n].parent].color == black {
			break
		}

		// Case 3: parent and uncle are both red.
		// Then paint both black and make grandparent red.
		grandparent := alloc[alloc[n].parent].parent
		var uncle uint32
		if isLeftChild(alloc[n].parent, alloc) {
			uncle = alloc[grandparent].right
		} else {
			uncle = alloc[grandparent].left
		}
		if uncle != 0 && alloc[uncle].color == red {
			alloc[alloc[n].parent].color = black
			alloc[uncle].color = black
			alloc[grandparent].color = red
			n = grandparent
			continue
		}

		// Case 4: parent is red, uncle is black (1)
		if isRightChild(n, alloc) && isLeftChild(alloc[n].parent, alloc) {
			tree.rotateLeft(alloc[n].parent)
			n = alloc[n].left
			continue
		}
		if isLeftChild(n, alloc) && isRightChild(alloc[n].parent, alloc) {
			tree.rotateRight(alloc[n].parent)
			n = alloc[n].right
			continue
		}

		// Case 5: parent is read, uncle is black (2)
		alloc[alloc[n].parent].color = black
		alloc[grandparent].color = red
		if isLeftChild(n, alloc) {
			tree.rotateRight(grandparent)
		} else {
			tree.rotateLeft(grandparent)
		}
		break
	}
	return true, Iterator{tree, insN}
}

// DeleteWithKey deletes an item with the given Key. Returns true iff the item was
// found.
func (tree *RBTree) DeleteWithKey(key uint32) bool {
	n, exact := tree.findGE(key)
	if exact {
		tree.doDelete(n)
		return true
	}
	return false
}

// DeleteWithIterator deletes the current item.
//
// REQUIRES: !iter.Limit() && !iter.NegativeLimit()
func (tree *RBTree) DeleteWithIterator(iter Iterator) {
	doAssert(!iter.Limit() && !iter.NegativeLimit())
	tree.doDelete(iter.node)
}

// Iterator allows scanning tree elements in sort order.
//
// Iterator invalidation rule is the same as C++ std::map<>'s. That
// is, if you delete the element that an iterator points to, the
// iterator becomes invalid. For other operation types, the iterator
// remains valid.
type Iterator struct {
	tree *RBTree
	node uint32
}

// Equal checks for the underlying nodes equality.
func (iter Iterator) Equal(other Iterator) bool {
	return iter.node == other.node
}

// Limit checks if the iterator points beyond the max element in the tree.
func (iter Iterator) Limit() bool {
	return iter.node == 0
}

// Min checks if the iterator points to the minimum element in the tree.
func (iter Iterator) Min() bool {
	return iter.node == iter.tree.minNode
}

// Max checks if the iterator points to the maximum element in the tree.
func (iter Iterator) Max() bool {
	return iter.node == iter.tree.maxNode
}

// NegativeLimit checks if the iterator points before the minimum element in the tree.
func (iter Iterator) NegativeLimit() bool {
	return iter.node == negativeLimitNode
}

// Item returns the current element. Allows mutating the node
// (key to be changed with care!).
//
// The result is nil if iter.Limit() || iter.NegativeLimit().
func (iter Iterator) Item() *Item {
	if iter.Limit() || iter.NegativeLimit() {
		return nil
	}
	return &iter.tree.storage()[iter.node].item
}

// Next creates a new iterator that points to the successor of the current element.
//
// REQUIRES: !iter.Limit()
func (iter Iterator) Next() Iterator {
	doAssert(!iter.Limit())
	if iter.NegativeLimit() {
		return Iterator{iter.tree, iter.tree.minNode}
	}
	return Iterator{iter.tree, doNext(iter.node, iter.tree.storage())}
}

// Prev creates a new iterator that points to the predecessor of the current
// node.
//
// REQUIRES: !iter.NegativeLimit()
func (iter Iterator) Prev() Iterator {
	doAssert(!iter.NegativeLimit())
	if !iter.Limit() {
		return Iterator{iter.tree, doPrev(iter.node, iter.tree.storage())}
	}
	if iter.tree.maxNode == 0 {
		return Iterator{iter.tree, negativeLimitNode}
	}
	return Iterator{iter.tree, iter.tree.maxNode}
}

func doAssert(b bool) {
	if !b {
		panic("rbtree internal assertion failed")
	}
}

const (
	red               = false
	black             = true
	negativeLimitNode = math.MaxUint32
)

type node struct {
	item                Item
	parent, left, right uint32
	color               bool // black or red
}

//
// Internal node attribute accessors
//
func getColor(n uint32, allocator []node) bool {
	if n == 0 {
		return black
	}
	return allocator[n].color
}

func isLeftChild(n uint32, allocator []node) bool {
	return n == allocator[allocator[n].parent].left
}

func isRightChild(n uint32, allocator []node) bool {
	return n == allocator[allocator[n].parent].right
}

func sibling(n uint32, allocator []node) uint32 {
	doAssert(allocator[n].parent != 0)
	if isLeftChild(n, allocator) {
		return allocator[allocator[n].parent].right
	}
	return allocator[allocator[n].parent].left
}

// Return the minimum node that's larger than N. Return nil if no such
// node is found.
func doNext(n uint32, allocator []node) uint32 {
	if allocator[n].right != 0 {
		m := allocator[n].right
		for allocator[m].left != 0 {
			m = allocator[m].left
		}
		return m
	}

	for n != 0 {
		p := allocator[n].parent
		if p == 0 {
			return 0
		}
		if isLeftChild(n, allocator) {
			return p
		}
		n = p
	}
	return 0
}

// Return the maximum node that's smaller than N. Return nil if no
// such node is found.
func doPrev(n uint32, allocator []node) uint32 {
	if allocator[n].left != 0 {
		return maxPredecessor(n, allocator)
	}

	for n != 0 {
		p := allocator[n].parent
		if p == 0 {
			break
		}
		if isRightChild(n, allocator) {
			return p
		}
		n = p
	}
	return negativeLimitNode
}

// Return the predecessor of "n".
func maxPredecessor(n uint32, allocator []node) uint32 {
	doAssert(allocator[n].left != 0)
	m := allocator[n].left
	for allocator[m].right != 0 {
		m = allocator[m].right
	}
	return m
}

//
// Tree methods
//

//
// Private methods
//

func (tree *RBTree) recomputeMinNode() {
	alloc := tree.storage()
	tree.minNode = tree.root
	if tree.minNode != 0 {
		for alloc[tree.minNode].left != 0 {
			tree.minNode = alloc[tree.minNode].left
		}
	}
}

func (tree *RBTree) recomputeMaxNode() {
	alloc := tree.storage()
	tree.maxNode = tree.root
	if tree.maxNode != 0 {
		for alloc[tree.maxNode].right != 0 {
			tree.maxNode = alloc[tree.maxNode].right
		}
	}
}

func (tree *RBTree) maybeSetMinNode(n uint32) {
	alloc := tree.storage()
	if tree.minNode == 0 {
		tree.minNode = n
		tree.maxNode = n
	} else if alloc[n].item.Key < alloc[tree.minNode].item.Key {
		tree.minNode = n
	}
}

func (tree *RBTree) maybeSetMaxNode(n uint32) {
	alloc := tree.storage()
	if tree.maxNode == 0 {
		tree.minNode = n
		tree.maxNode = n
	} else if alloc[n].item.Key > alloc[tree.maxNode].item.Key {
		tree.maxNode = n
	}
}

// Try inserting "item" into the tree. Return nil if the item is
// already in the tree. Otherwise return a new (leaf) node.
func (tree *RBTree) doInsert(item Item) uint32 {
	if tree.root == 0 {
		n := tree.allocator.malloc()
		tree.storage()[n].item = item
		tree.root = n
		tree.minNode = n
		tree.maxNode = n
		tree.count++
		return n
	}
	parent := tree.root
	storage := tree.storage()
	for true {
		parentNode := storage[parent]
		comp := int(item.Key) - int(parentNode.item.Key)
		if comp == 0 {
			return 0
		} else if comp < 0 {
			if parentNode.left == 0 {
				n := tree.allocator.malloc()
				storage = tree.storage()
				newNode := &storage[n]
				newNode.item = item
				newNode.parent = parent
				storage[parent].left = n
				tree.count++
				tree.maybeSetMinNode(n)
				return n
			}
			parent = parentNode.left
		} else {
			if parentNode.right == 0 {
				n := tree.allocator.malloc()
				storage = tree.storage()
				newNode := &storage[n]
				newNode.item = item
				newNode.parent = parent
				storage[parent].right = n
				tree.count++
				tree.maybeSetMaxNode(n)
				return n
			}
			parent = parentNode.right
		}
	}
	panic("should not reach here")
}

// Find a node whose item >= Key. The 2nd return Value is true iff the
// node.item==Key. Returns (nil, false) if all nodes in the tree are <
// Key.
func (tree RBTree) findGE(key uint32) (uint32, bool) {
	alloc := tree.storage()
	n := tree.root
	for true {
		if n == 0 {
			return 0, false
		}
		comp := int(key) - int(alloc[n].item.Key)
		if comp == 0 {
			return n, true
		} else if comp < 0 {
			if alloc[n].left != 0 {
				n = alloc[n].left
			} else {
				return n, false
			}
		} else {
			if alloc[n].right != 0 {
				n = alloc[n].right
			} else {
				succ := doNext(n, alloc)
				if succ == 0 {
					return 0, false
				}
				return succ, key == alloc[succ].item.Key
			}
		}
	}
	panic("should not reach here")
}

// Delete N from the tree.
func (tree *RBTree) doDelete(n uint32) {
	alloc := tree.storage()
	if alloc[n].left != 0 && alloc[n].right != 0 {
		pred := maxPredecessor(n, alloc)
		tree.swapNodes(n, pred)
	}

	doAssert(alloc[n].left == 0 || alloc[n].right == 0)
	child := alloc[n].right
	if child == 0 {
		child = alloc[n].left
	}
	if alloc[n].color == black {
		alloc[n].color = getColor(child, alloc)
		tree.deleteCase1(n)
	}
	tree.replaceNode(n, child)
	if alloc[n].parent == 0 && child != 0 {
		alloc[child].color = black
	}
	tree.allocator.free(n)
	tree.count--
	if tree.count == 0 {
		tree.minNode = 0
		tree.maxNode = 0
	} else {
		if tree.minNode == n {
			tree.recomputeMinNode()
		}
		if tree.maxNode == n {
			tree.recomputeMaxNode()
		}
	}
}

// Move n to the pred's place, and vice versa
//
func (tree *RBTree) swapNodes(n, pred uint32) {
	doAssert(pred != n)
	alloc := tree.storage()
	isLeft := isLeftChild(pred, alloc)
	tmp := alloc[pred]
	tree.replaceNode(n, pred)
	alloc[pred].color = alloc[n].color

	if tmp.parent == n {
		// swap the positions of n and pred
		if isLeft {
			alloc[pred].left = n
			alloc[pred].right = alloc[n].right
			if alloc[pred].right != 0 {
				alloc[alloc[pred].right].parent = pred
			}
		} else {
			alloc[pred].left = alloc[n].left
			if alloc[pred].left != 0 {
				alloc[alloc[pred].left].parent = pred
			}
			alloc[pred].right = n
		}
		alloc[n].item = tmp.item
		alloc[n].parent = pred

		alloc[n].left = tmp.left
		if alloc[n].left != 0 {
			alloc[alloc[n].left].parent = n
		}
		alloc[n].right = tmp.right
		if alloc[n].right != 0 {
			alloc[alloc[n].right].parent = n
		}
	} else {
		alloc[pred].left = alloc[n].left
		if alloc[pred].left != 0 {
			alloc[alloc[pred].left].parent = pred
		}
		alloc[pred].right = alloc[n].right
		if alloc[pred].right != 0 {
			alloc[alloc[pred].right].parent = pred
		}
		if isLeft {
			alloc[tmp.parent].left = n
		} else {
			alloc[tmp.parent].right = n
		}
		alloc[n].item = tmp.item
		alloc[n].parent = tmp.parent
		alloc[n].left = tmp.left
		if alloc[n].left != 0 {
			alloc[alloc[n].left].parent = n
		}
		alloc[n].right = tmp.right
		if alloc[n].right != 0 {
			alloc[alloc[n].right].parent = n
		}
	}
	alloc[n].color = tmp.color
}

func (tree *RBTree) deleteCase1(n uint32) {
	alloc := tree.storage()
	for true {
		if alloc[n].parent != 0 {
			if getColor(sibling(n, alloc), alloc) == red {
				alloc[alloc[n].parent].color = red
				alloc[sibling(n, alloc)].color = black
				if n == alloc[alloc[n].parent].left {
					tree.rotateLeft(alloc[n].parent)
				} else {
					tree.rotateRight(alloc[n].parent)
				}
			}
			if getColor(alloc[n].parent, alloc) == black &&
				getColor(sibling(n, alloc), alloc) == black &&
				getColor(alloc[sibling(n, alloc)].left, alloc) == black &&
				getColor(alloc[sibling(n, alloc)].right, alloc) == black {
				alloc[sibling(n, alloc)].color = red
				n = alloc[n].parent
				continue
			} else {
				// case 4
				if getColor(alloc[n].parent, alloc) == red &&
					getColor(sibling(n, alloc), alloc) == black &&
					getColor(alloc[sibling(n, alloc)].left, alloc) == black &&
					getColor(alloc[sibling(n, alloc)].right, alloc) == black {
					alloc[sibling(n, alloc)].color = red
					alloc[alloc[n].parent].color = black
				} else {
					tree.deleteCase5(n)
				}
			}
		}
		break
	}
}

func (tree *RBTree) deleteCase5(n uint32) {
	alloc := tree.storage()
	if n == alloc[alloc[n].parent].left &&
		getColor(sibling(n, alloc), alloc) == black &&
		getColor(alloc[sibling(n, alloc)].left, alloc) == red &&
		getColor(alloc[sibling(n, alloc)].right, alloc) == black {
		alloc[sibling(n, alloc)].color = red
		alloc[alloc[sibling(n, alloc)].left].color = black
		tree.rotateRight(sibling(n, alloc))
	} else if n == alloc[alloc[n].parent].right &&
		getColor(sibling(n, alloc), alloc) == black &&
		getColor(alloc[sibling(n, alloc)].right, alloc) == red &&
		getColor(alloc[sibling(n, alloc)].left, alloc) == black {
		alloc[sibling(n, alloc)].color = red
		alloc[alloc[sibling(n, alloc)].right].color = black
		tree.rotateLeft(sibling(n, alloc))
	}

	// case 6
	alloc[sibling(n, alloc)].color = getColor(alloc[n].parent, alloc)
	alloc[alloc[n].parent].color = black
	if n == alloc[alloc[n].parent].left {
		doAssert(getColor(alloc[sibling(n, alloc)].right, alloc) == red)
		alloc[alloc[sibling(n, alloc)].right].color = black
		tree.rotateLeft(alloc[n].parent)
	} else {
		doAssert(getColor(alloc[sibling(n, alloc)].left, alloc) == red)
		alloc[alloc[sibling(n, alloc)].left].color = black
		tree.rotateRight(alloc[n].parent)
	}
}

func (tree *RBTree) replaceNode(oldn, newn uint32) {
	alloc := tree.storage()
	if alloc[oldn].parent == 0 {
		tree.root = newn
	} else {
		if oldn == alloc[alloc[oldn].parent].left {
			alloc[alloc[oldn].parent].left = newn
		} else {
			alloc[alloc[oldn].parent].right = newn
		}
	}
	if newn != 0 {
		alloc[newn].parent = alloc[oldn].parent
	}
}

/*
    X		     Y
  A   Y	    =>     X   C
     B C 	  A B
*/
func (tree *RBTree) rotateLeft(x uint32) {
	alloc := tree.storage()
	y := alloc[x].right
	alloc[x].right = alloc[y].left
	if alloc[y].left != 0 {
		alloc[alloc[y].left].parent = x
	}
	alloc[y].parent = alloc[x].parent
	if alloc[x].parent == 0 {
		tree.root = y
	} else {
		if isLeftChild(x, alloc) {
			alloc[alloc[x].parent].left = y
		} else {
			alloc[alloc[x].parent].right = y
		}
	}
	alloc[y].left = x
	alloc[x].parent = y
}

/*
     Y           X
   X   C  =>   A   Y
  A B             B C
*/
func (tree *RBTree) rotateRight(y uint32) {
	alloc := tree.storage()
	x := alloc[y].left

	// Move "B"
	alloc[y].left = alloc[x].right
	if alloc[x].right != 0 {
		alloc[alloc[x].right].parent = y
	}

	alloc[x].parent = alloc[y].parent
	if alloc[y].parent == 0 {
		tree.root = x
	} else {
		if isLeftChild(y, alloc) {
			alloc[alloc[y].parent].left = x
		} else {
			alloc[alloc[y].parent].right = x
		}
	}
	alloc[x].right = y
	alloc[y].parent = x
}
