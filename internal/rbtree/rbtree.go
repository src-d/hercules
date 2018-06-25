package rbtree

//
// Public definitions
//

// Item is the object stored in each tree node.
type Item struct {
	Key   int
	Value int
}

// RBTree created by Yaz Saito on 06/10/12.
//
// A red-black tree with an API similar to C++ STL's.
//
// The implementation is inspired (read: stolen) from:
// http://en.literateprograms.org/Red-black_tree_(C)#chunk use:private function prototypes.
//
// The code was optimized for the simple integer types of Key and Value.
type RBTree struct {
	// Root of the tree
	root *node

	// The minimum and maximum nodes under the tree.
	minNode, maxNode *node

	// Number of nodes under root, including the root
	count int
}

// Len returns the number of elements in the tree.
func (tree *RBTree) Len() int {
	return tree.count
}

// Clone performs a deep copy of the tree.
func (tree *RBTree) Clone() *RBTree {
	clone := &RBTree{}
	clone.count = tree.count
	nodeMap := map[*node]*node{}
	queue := []*node{tree.root}
	for len(queue) > 0 {
		head := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		headCopy := *head
		nodeMap[head] = &headCopy
		if head.left != nil {
			queue = append(queue, head.left)
		}
		if head.right != nil {
			queue = append(queue, head.right)
		}
	}
	for _, mapped := range nodeMap {
		if mapped.parent != nil {
			mapped.parent = nodeMap[mapped.parent]
		}
		if mapped.left != nil {
			mapped.left = nodeMap[mapped.left]
		}
		if mapped.right != nil {
			mapped.right = nodeMap[mapped.right]
		}
	}
	clone.root = nodeMap[tree.root]
	clone.minNode = nodeMap[tree.minNode]
	clone.maxNode = nodeMap[tree.maxNode]
	return clone
}

// Get is a convenience function for finding an element equal to Key. Returns
// nil if not found.
func (tree *RBTree) Get(key int) *int {
	n, exact := tree.findGE(key)
	if exact {
		return &n.item.Value
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
	if tree.maxNode == nil {
		return Iterator{tree, negativeLimitNode}
	}
	return Iterator{tree, tree.maxNode}
}

// Limit creates an iterator that points beyond the maximum item in the tree.
func (tree *RBTree) Limit() Iterator {
	return Iterator{tree, nil}
}

// NegativeLimit creates an iterator that points before the minimum item in the tree.
func (tree *RBTree) NegativeLimit() Iterator {
	return Iterator{tree, negativeLimitNode}
}

// FindGE finds the smallest element N such that N >= Key, and returns the
// iterator pointing to the element. If no such element is found,
// returns tree.Limit().
func (tree *RBTree) FindGE(key int) Iterator {
	n, _ := tree.findGE(key)
	return Iterator{tree, n}
}

// FindLE finds the largest element N such that N <= Key, and returns the
// iterator pointing to the element. If no such element is found,
// returns iter.NegativeLimit().
func (tree *RBTree) FindLE(key int) Iterator {
	n, exact := tree.findGE(key)
	if exact {
		return Iterator{tree, n}
	}
	if n != nil {
		return Iterator{tree, n.doPrev()}
	}
	if tree.maxNode == nil {
		return Iterator{tree, negativeLimitNode}
	}
	return Iterator{tree, tree.maxNode}
}

// Insert an item. If the item is already in the tree, do nothing and
// return false. Else return true.
func (tree *RBTree) Insert(item Item) (bool, Iterator) {
	// TODO: delay creating n until it is found to be inserted
	n := tree.doInsert(item)
	if n == nil {
		return false, Iterator{}
	}
	insN := n

	n.color = red

	for true {
		// Case 1: N is at the root
		if n.parent == nil {
			n.color = black
			break
		}

		// Case 2: The parent is black, so the tree already
		// satisfies the RB properties
		if n.parent.color == black {
			break
		}

		// Case 3: parent and uncle are both red.
		// Then paint both black and make grandparent red.
		grandparent := n.parent.parent
		var uncle *node
		if n.parent.isLeftChild() {
			uncle = grandparent.right
		} else {
			uncle = grandparent.left
		}
		if uncle != nil && uncle.color == red {
			n.parent.color = black
			uncle.color = black
			grandparent.color = red
			n = grandparent
			continue
		}

		// Case 4: parent is red, uncle is black (1)
		if n.isRightChild() && n.parent.isLeftChild() {
			tree.rotateLeft(n.parent)
			n = n.left
			continue
		}
		if n.isLeftChild() && n.parent.isRightChild() {
			tree.rotateRight(n.parent)
			n = n.right
			continue
		}

		// Case 5: parent is read, uncle is black (2)
		n.parent.color = black
		grandparent.color = red
		if n.isLeftChild() {
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
func (tree *RBTree) DeleteWithKey(key int) bool {
	iter := tree.FindGE(key)
	if iter.node != nil {
		tree.DeleteWithIterator(iter)
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
	node *node
}

// Equal checks for the underlying nodes equality.
func (iter Iterator) Equal(other Iterator) bool {
	return iter.node == other.node
}

// Limit checks if the iterator points beyond the max element in the tree.
func (iter Iterator) Limit() bool {
	return iter.node == nil
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
// REQUIRES: !iter.Limit() && !iter.NegativeLimit()
func (iter Iterator) Item() *Item {
	return &iter.node.item
}

// Next creates a new iterator that points to the successor of the current element.
//
// REQUIRES: !iter.Limit()
func (iter Iterator) Next() Iterator {
	doAssert(!iter.Limit())
	if iter.NegativeLimit() {
		return Iterator{iter.tree, iter.tree.minNode}
	}
	return Iterator{iter.tree, iter.node.doNext()}
}

// Prev creates a new iterator that points to the predecessor of the current
// node.
//
// REQUIRES: !iter.NegativeLimit()
func (iter Iterator) Prev() Iterator {
	doAssert(!iter.NegativeLimit())
	if !iter.Limit() {
		return Iterator{iter.tree, iter.node.doPrev()}
	}
	if iter.tree.maxNode == nil {
		return Iterator{iter.tree, negativeLimitNode}
	}
	return Iterator{iter.tree, iter.tree.maxNode}
}

func doAssert(b bool) {
	if !b {
		panic("rbtree internal assertion failed")
	}
}

const red = iota
const black = 1 + iota

type node struct {
	item                Item
	parent, left, right *node
	color               int // black or red
}

var negativeLimitNode *node

//
// Internal node attribute accessors
//
func getColor(n *node) int {
	if n == nil {
		return black
	}
	return n.color
}

func (n *node) isLeftChild() bool {
	return n == n.parent.left
}

func (n *node) isRightChild() bool {
	return n == n.parent.right
}

func (n *node) sibling() *node {
	doAssert(n.parent != nil)
	if n.isLeftChild() {
		return n.parent.right
	}
	return n.parent.left
}

// Return the minimum node that's larger than N. Return nil if no such
// node is found.
func (n *node) doNext() *node {
	if n.right != nil {
		m := n.right
		for m.left != nil {
			m = m.left
		}
		return m
	}

	for n != nil {
		p := n.parent
		if p == nil {
			return nil
		}
		if n.isLeftChild() {
			return p
		}
		n = p
	}
	return nil
}

// Return the maximum node that's smaller than N. Return nil if no
// such node is found.
func (n *node) doPrev() *node {
	if n.left != nil {
		return maxPredecessor(n)
	}

	for n != nil {
		p := n.parent
		if p == nil {
			break
		}
		if n.isRightChild() {
			return p
		}
		n = p
	}
	return negativeLimitNode
}

// Return the predecessor of "n".
func maxPredecessor(n *node) *node {
	doAssert(n.left != nil)
	m := n.left
	for m.right != nil {
		m = m.right
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
	tree.minNode = tree.root
	if tree.minNode != nil {
		for tree.minNode.left != nil {
			tree.minNode = tree.minNode.left
		}
	}
}

func (tree *RBTree) recomputeMaxNode() {
	tree.maxNode = tree.root
	if tree.maxNode != nil {
		for tree.maxNode.right != nil {
			tree.maxNode = tree.maxNode.right
		}
	}
}

func (tree *RBTree) maybeSetMinNode(n *node) {
	if tree.minNode == nil {
		tree.minNode = n
		tree.maxNode = n
	} else if n.item.Key < tree.minNode.item.Key {
		tree.minNode = n
	}
}

func (tree *RBTree) maybeSetMaxNode(n *node) {
	if tree.maxNode == nil {
		tree.minNode = n
		tree.maxNode = n
	} else if n.item.Key > tree.maxNode.item.Key {
		tree.maxNode = n
	}
}

// Try inserting "item" into the tree. Return nil if the item is
// already in the tree. Otherwise return a new (leaf) node.
func (tree *RBTree) doInsert(item Item) *node {
	if tree.root == nil {
		n := &node{item: item}
		tree.root = n
		tree.minNode = n
		tree.maxNode = n
		tree.count++
		return n
	}
	parent := tree.root
	for true {
		comp := item.Key - parent.item.Key
		if comp == 0 {
			return nil
		} else if comp < 0 {
			if parent.left == nil {
				n := &node{item: item, parent: parent}
				parent.left = n
				tree.count++
				tree.maybeSetMinNode(n)
				return n
			}
			parent = parent.left
		} else {
			if parent.right == nil {
				n := &node{item: item, parent: parent}
				parent.right = n
				tree.count++
				tree.maybeSetMaxNode(n)
				return n
			}
			parent = parent.right
		}
	}
	panic("should not reach here")
}

// Find a node whose item >= Key. The 2nd return Value is true iff the
// node.item==Key. Returns (nil, false) if all nodes in the tree are <
// Key.
func (tree *RBTree) findGE(key int) (*node, bool) {
	n := tree.root
	for true {
		if n == nil {
			return nil, false
		}
		comp := key - n.item.Key
		if comp == 0 {
			return n, true
		} else if comp < 0 {
			if n.left != nil {
				n = n.left
			} else {
				return n, false
			}
		} else {
			if n.right != nil {
				n = n.right
			} else {
				succ := n.doNext()
				if succ == nil {
					return nil, false
				}
				return succ, key == succ.item.Key
			}
		}
	}
	panic("should not reach here")
}

// Delete N from the tree.
func (tree *RBTree) doDelete(n *node) {
	if n.left != nil && n.right != nil {
		pred := maxPredecessor(n)
		tree.swapNodes(n, pred)
	}

	doAssert(n.left == nil || n.right == nil)
	child := n.right
	if child == nil {
		child = n.left
	}
	if n.color == black {
		n.color = getColor(child)
		tree.deleteCase1(n)
	}
	tree.replaceNode(n, child)
	if n.parent == nil && child != nil {
		child.color = black
	}
	tree.count--
	if tree.count == 0 {
		tree.minNode = nil
		tree.maxNode = nil
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
func (tree *RBTree) swapNodes(n, pred *node) {
	doAssert(pred != n)
	isLeft := pred.isLeftChild()
	tmp := *pred
	tree.replaceNode(n, pred)
	pred.color = n.color

	if tmp.parent == n {
		// swap the positions of n and pred
		if isLeft {
			pred.left = n
			pred.right = n.right
			if pred.right != nil {
				pred.right.parent = pred
			}
		} else {
			pred.left = n.left
			if pred.left != nil {
				pred.left.parent = pred
			}
			pred.right = n
		}
		n.item = tmp.item
		n.parent = pred

		n.left = tmp.left
		if n.left != nil {
			n.left.parent = n
		}
		n.right = tmp.right
		if n.right != nil {
			n.right.parent = n
		}
	} else {
		pred.left = n.left
		if pred.left != nil {
			pred.left.parent = pred
		}
		pred.right = n.right
		if pred.right != nil {
			pred.right.parent = pred
		}
		if isLeft {
			tmp.parent.left = n
		} else {
			tmp.parent.right = n
		}
		n.item = tmp.item
		n.parent = tmp.parent
		n.left = tmp.left
		if n.left != nil {
			n.left.parent = n
		}
		n.right = tmp.right
		if n.right != nil {
			n.right.parent = n
		}
	}
	n.color = tmp.color
}

func (tree *RBTree) deleteCase1(n *node) {
	for true {
		if n.parent != nil {
			if getColor(n.sibling()) == red {
				n.parent.color = red
				n.sibling().color = black
				if n == n.parent.left {
					tree.rotateLeft(n.parent)
				} else {
					tree.rotateRight(n.parent)
				}
			}
			if getColor(n.parent) == black &&
				getColor(n.sibling()) == black &&
				getColor(n.sibling().left) == black &&
				getColor(n.sibling().right) == black {
				n.sibling().color = red
				n = n.parent
				continue
			} else {
				// case 4
				if getColor(n.parent) == red &&
					getColor(n.sibling()) == black &&
					getColor(n.sibling().left) == black &&
					getColor(n.sibling().right) == black {
					n.sibling().color = red
					n.parent.color = black
				} else {
					tree.deleteCase5(n)
				}
			}
		}
		break
	}
}

func (tree *RBTree) deleteCase5(n *node) {
	if n == n.parent.left &&
		getColor(n.sibling()) == black &&
		getColor(n.sibling().left) == red &&
		getColor(n.sibling().right) == black {
		n.sibling().color = red
		n.sibling().left.color = black
		tree.rotateRight(n.sibling())
	} else if n == n.parent.right &&
		getColor(n.sibling()) == black &&
		getColor(n.sibling().right) == red &&
		getColor(n.sibling().left) == black {
		n.sibling().color = red
		n.sibling().right.color = black
		tree.rotateLeft(n.sibling())
	}

	// case 6
	n.sibling().color = getColor(n.parent)
	n.parent.color = black
	if n == n.parent.left {
		doAssert(getColor(n.sibling().right) == red)
		n.sibling().right.color = black
		tree.rotateLeft(n.parent)
	} else {
		doAssert(getColor(n.sibling().left) == red)
		n.sibling().left.color = black
		tree.rotateRight(n.parent)
	}
}

func (tree *RBTree) replaceNode(oldn, newn *node) {
	if oldn.parent == nil {
		tree.root = newn
	} else {
		if oldn == oldn.parent.left {
			oldn.parent.left = newn
		} else {
			oldn.parent.right = newn
		}
	}
	if newn != nil {
		newn.parent = oldn.parent
	}
}

/*
    X		     Y
  A   Y	    =>     X   C
     B C 	  A B
*/
func (tree *RBTree) rotateLeft(x *node) {
	y := x.right
	x.right = y.left
	if y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		tree.root = y
	} else {
		if x.isLeftChild() {
			x.parent.left = y
		} else {
			x.parent.right = y
		}
	}
	y.left = x
	x.parent = y
}

/*
     Y           X
   X   C  =>   A   Y
  A B             B C
*/
func (tree *RBTree) rotateRight(y *node) {
	x := y.left

	// Move "B"
	y.left = x.right
	if x.right != nil {
		x.right.parent = y
	}

	x.parent = y.parent
	if y.parent == nil {
		tree.root = x
	} else {
		if y.isLeftChild() {
			y.parent.left = x
		} else {
			y.parent.right = x
		}
	}
	x.right = y
	y.parent = x
}

func init() {
	negativeLimitNode = &node{}
}
