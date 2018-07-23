package burndown

import (
	"fmt"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/hercules.v4/internal"
	"gopkg.in/src-d/hercules.v4/internal/rbtree"
)

// Status is the something we would like to keep track of in File.Update().
type Status struct {
	data   interface{}
	update func(interface{}, int, int, int)
}

// File encapsulates a balanced binary tree to store line intervals and
// a cumulative mapping of values to the corresponding length counters. Users
// are not supposed to create File-s directly; instead, they should call NewFile().
// NewFileFromTree() is the special constructor which is useful in the tests.
//
// Len() returns the number of lines in File.
//
// Update() mutates File by introducing tree structural changes and updating the
// length mapping.
//
// Dump() writes the tree to a string and Validate() checks the tree integrity.
type File struct {
	// Git hash of the contents.
	Hash     plumbing.Hash

	tree     *rbtree.RBTree
	statuses []Status
}

// NewStatus initializes a new instance of Status struct. It is needed to set the only two
// private fields which are not supposed to be replaced during the whole lifetime.
func NewStatus(data interface{}, update func(interface{}, int, int, int)) Status {
	return Status{data: data, update: update}
}

// TreeEnd denotes the value of the last leaf in the tree.
const TreeEnd = -1
// TreeMaxBinPower is the binary power value which corresponds to the maximum day which
// can be stored in the tree.
const TreeMaxBinPower = 14
// TreeMergeMark is the special day which disables the status updates and is used in File.Merge().
const TreeMergeMark = (1 << TreeMaxBinPower) - 1

func (file *File) updateTime(currentTime int, previousTime int, delta int) {
	if currentTime & TreeMergeMark == TreeMergeMark {
		// merge mode
		return
	}
	for _, status := range file.statuses {
		status.update(status.data, currentTime, previousTime, delta)
	}
}

// NewFile initializes a new instance of File struct.
//
// time is the starting value of the first node;
//
// length is the starting length of the tree (the key of the second and the
// last node);
//
// statuses are the attached interval length mappings.
func NewFile(hash plumbing.Hash, time int, length int, statuses ...Status) *File {
	file := &File{Hash: hash, tree: new(rbtree.RBTree), statuses: statuses}
	if length > 0 {
		file.updateTime(time, time, length)
		file.tree.Insert(rbtree.Item{Key: 0, Value: time})
	}
	file.tree.Insert(rbtree.Item{Key: length, Value: TreeEnd})
	return file
}

// NewFileFromTree is an alternative constructor for File which is used in tests.
// The resulting tree is validated with Validate() to ensure the initial integrity.
//
// keys is a slice with the starting tree keys.
//
// vals is a slice with the starting tree values. Must match the size of keys.
//
// statuses are the attached interval length mappings.
func NewFileFromTree(hash plumbing.Hash, keys []int, vals []int, statuses ...Status) *File {
	file := &File{Hash: hash, tree: new(rbtree.RBTree), statuses: statuses}
	if len(keys) != len(vals) {
		panic("keys and vals must be of equal length")
	}
	for i := 0; i < len(keys); i++ {
		file.tree.Insert(rbtree.Item{Key: keys[i], Value: vals[i]})
	}
	file.Validate()
	return file
}

// Clone copies the file. It performs a deep copy of the tree;
// depending on `clearStatuses` the original statuses are removed or not.
// Any new `statuses` are appended.
func (file *File) Clone(clearStatuses bool, statuses ...Status) *File {
	clone := &File{Hash: file.Hash, tree: file.tree.Clone(), statuses: file.statuses}
	if clearStatuses {
		clone.statuses = []Status{}
	}
	for _, status := range statuses {
		clone.statuses = append(clone.statuses, status)
	}
	return clone
}

// Len returns the File's size - that is, the maximum key in the tree of line
// intervals.
func (file *File) Len() int {
	return file.tree.Max().Item().Key
}

// Update modifies the underlying tree to adapt to the specified line changes.
//
// time is the time when the requested changes are made. Sets the values of the
// inserted nodes.
//
// pos is the index of the line at which the changes are introduced.
//
// ins_length is the number of inserted lines after pos.
//
// del_length is the number of removed lines after pos. Deletions come before
// the insertions.
//
// The code inside this function is probably the most important one throughout
// the project. It is extensively covered with tests. If you find a bug, please
// add the corresponding case in file_test.go.
func (file *File) Update(time int, pos int, insLength int, delLength int) {
	if time < 0 {
		panic("time may not be negative")
	}
	if pos < 0 {
		panic("attempt to insert/delete at a negative position")
	}
	if insLength < 0 || delLength < 0 {
		panic("insLength and delLength must be non-negative")
	}
	if insLength|delLength == 0 {
		return
	}
	tree := file.tree
	if tree.Len() < 2 && tree.Min().Item().Key != 0 {
		panic("invalid tree state")
	}
	if pos > tree.Max().Item().Key {
		panic(fmt.Sprintf("attempt to insert after the end of the file: %d < %d",
			tree.Max().Item().Key, pos))
	}
	iter := tree.FindLE(pos)
	origin := *iter.Item()
	file.updateTime(time, time, insLength)
	if delLength == 0 {
		// simple case with insertions only
		if origin.Key < pos || (origin.Value == time && pos == 0) {
			iter = iter.Next()
		}
		for ; !iter.Limit(); iter = iter.Next() {
			iter.Item().Key += insLength
		}
		if origin.Value != time {
			tree.Insert(rbtree.Item{Key: pos, Value: time})
			if origin.Key < pos {
				tree.Insert(rbtree.Item{Key: pos + insLength, Value: origin.Value})
			}
		}
		return
	}

	// delete nodes
	for true {
		node := iter.Item()
		nextIter := iter.Next()
		if nextIter.Limit() {
			if pos+delLength > node.Key {
				panic("attempt to delete after the end of the file")
			}
			break
		}
		delta := internal.Min(nextIter.Item().Key, pos+delLength) - internal.Max(node.Key, pos)
		if delta <= 0 {
			break
		}
		file.updateTime(time, node.Value, -delta)
		if node.Key >= pos {
			origin = *node
			tree.DeleteWithIterator(iter)
		}
		iter = nextIter
	}

	// prepare for the keys update
	var previous *rbtree.Item
	if insLength > 0 && (origin.Value != time || origin.Key == pos) {
		// insert our new interval
		if iter.Item().Value == time {
			prev := iter.Prev()
			if prev.Item().Value != time {
				iter.Item().Key = pos
			} else {
				tree.DeleteWithIterator(iter)
				iter = prev
			}
			origin.Value = time // cancels the insertion after applying the delta
		} else {
			_, iter = tree.Insert(rbtree.Item{Key: pos, Value: time})
		}
	} else {
		// rollback 1 position back, see "for true" deletion cycle ^
		iter = iter.Prev()
		previous = iter.Item()
	}

	// update the keys of all subsequent nodes
	delta := insLength - delLength
	if delta != 0 {
		for iter = iter.Next(); !iter.Limit(); iter = iter.Next() {
			// we do not need to re-balance the tree
			iter.Item().Key += delta
		}
		// have to adjust origin in case insLength == 0
		if origin.Key > pos {
			origin.Key += delta
		}
	}

	if insLength > 0 {
		if origin.Value != time {
			tree.Insert(rbtree.Item{Key: pos + insLength, Value: origin.Value})
		} else if pos == 0 {
			// recover the beginning
			tree.Insert(rbtree.Item{Key: pos, Value: time})
		}
	} else if (pos > origin.Key && previous.Value != origin.Value) || pos == origin.Key || pos == 0 {
		// continue the original interval
		tree.Insert(rbtree.Item{Key: pos, Value: origin.Value})
	}
}

// Merge combines several prepared File-s together. Returns the value
// indicating whether at least one File required merging.
func (file *File) Merge(day int, others... *File) bool {
	dirty := false
	for _, other := range others {
		if file.Hash != other.Hash {
			dirty = true
			break
		}
	}
	if !dirty {
		return false
	}
	myself := file.flatten()
	for _, other := range others {
		lines := other.flatten()
		if len(myself) != len(lines) {
			panic("file corruption, lines number mismatch during merge")
		}
		for i, l := range myself {
			ol := lines[i]
			if ol & TreeMergeMark == TreeMergeMark {
				continue
			}
			if l & TreeMergeMark == TreeMergeMark {
				myself[i] = ol
			} else if l != ol {
				// the same line introduced in different branches
				// consider the oldest version as the ground truth
				if l > ol {
					myself[i] = ol
					// subtract from the newer day l
					file.updateTime(ol, l, -1)
				} else {
					// subtract from the newer day ol
					file.updateTime(l, ol, -1)
				}
			}
		}
	}
	for i, l := range myself {
		if l & TreeMergeMark == TreeMergeMark {
			myself[i] = day
			file.updateTime(day, day, 1)
		}
	}
	// now we need to reconstruct the tree from the discrete values
	tree := &rbtree.RBTree{}
	for i, v := range myself {
		if i == 0 || v != myself[i - 1] {
			tree.Insert(rbtree.Item{Key: i, Value: v})
		}
	}
	tree.Insert(rbtree.Item{Key: len(myself), Value: TreeEnd})
	file.tree = tree
	return true
}

// Status returns the bound status object by the specified index.
func (file *File) Status(index int) interface{} {
	if index < 0 || index >= len(file.statuses) {
		panic(fmt.Sprintf("status index %d is out of bounds [0, %d)",
			index, len(file.statuses)))
	}
	return file.statuses[index].data
}

// Dump formats the underlying line interval tree into a string.
// Useful for error messages, panic()-s and debugging.
func (file *File) Dump() string {
	buffer := ""
	for iter := file.tree.Min(); !iter.Limit(); iter = iter.Next() {
		node := iter.Item()
		buffer += fmt.Sprintf("%d %d\n", node.Key, node.Value)
	}
	return buffer
}

// Validate checks the underlying line interval tree integrity.
// The checks are as follows:
//
// 1. The minimum key must be 0 because the first line index is always 0.
//
// 2. The last node must carry TreeEnd value. This is the maintained invariant
// which marks the ending of the last line interval.
//
// 3. Node keys must monotonically increase and never duplicate.
func (file *File) Validate() {
	if file.tree.Min().Item().Key != 0 {
		panic("the tree must start with key 0")
	}
	if file.tree.Max().Item().Value != TreeEnd {
		panic(fmt.Sprintf("the last value in the tree must be %d", TreeEnd))
	}
	prevKey := -1
	for iter := file.tree.Min(); !iter.Limit(); iter = iter.Next() {
		node := iter.Item()
		if node.Key == prevKey {
			panic(fmt.Sprintf("duplicate tree key: %d", node.Key))
		}
		prevKey = node.Key
	}
}

// flatten represents the file as a slice of lines, each line's value being the corresponding day.
func (file *File) flatten() []int {
	lines := make([]int, 0, file.Len())
	val := -1
	for iter := file.tree.Min(); !iter.Limit(); iter = iter.Next() {
		for i := len(lines); i < iter.Item().Key; i++ {
			lines = append(lines, val)
		}
		val = iter.Item().Value
	}
	return lines
}
