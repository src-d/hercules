package hercules

import (
	"fmt"
	"gopkg.in/src-d/hercules.v3/rbtree"
)

// A status is the something we would like to update during File.Update().
type Status struct {
	data   interface{}
	update func(interface{}, int, int, int)
}

// A file encapsulates a balanced binary tree to store line intervals and
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
	tree     *rbtree.RBTree
	statuses []Status
}

func NewStatus(data interface{}, update func(interface{}, int, int, int)) Status {
	return Status{data: data, update: update}
}

// TreeEnd denotes the value of the last leaf in the tree.
const TreeEnd int = -1

// The ugly side of Go.
// template <typename T> please!

// min calculates the minimum of two 32-bit integers.
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// min64 calculates the minimum of two 64-bit integers.
func min64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// max calculates the maximum of two 32-bit integers.
func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

// max64 calculates the maximum of two 64-bit integers.
func max64(a int64, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

// abs64 calculates the absolute value of a 64-bit integer.
func abs64(v int64) int64 {
	if v <= 0 {
		return -v
	}
	return v
}

func (file *File) updateTime(current_time int, previous_time int, delta int) {
	for _, status := range file.statuses {
		status.update(status.data, current_time, previous_time, delta)
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
func NewFile(time int, length int, statuses ...Status) *File {
	file := new(File)
	file.statuses = statuses
	file.tree = new(rbtree.RBTree)
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
func NewFileFromTree(keys []int, vals []int, statuses ...Status) *File {
	file := new(File)
	file.statuses = statuses
	file.tree = new(rbtree.RBTree)
	if len(keys) != len(vals) {
		panic("keys and vals must be of equal length")
	}
	for i := 0; i < len(keys); i++ {
		file.tree.Insert(rbtree.Item{Key: keys[i], Value: vals[i]})
	}
	file.Validate()
	return file
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
func (file *File) Update(time int, pos int, ins_length int, del_length int) {
	if time < 0 {
		panic("time may not be negative")
	}
	if pos < 0 {
		panic("attempt to insert/delete at a negative position")
	}
	if ins_length < 0 || del_length < 0 {
		panic("ins_length and del_length must be nonnegative")
	}
	if ins_length|del_length == 0 {
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
	file.updateTime(time, time, ins_length)
	if del_length == 0 {
		// simple case with insertions only
		if origin.Key < pos || (origin.Value == time && pos == 0) {
			iter = iter.Next()
		}
		for ; !iter.Limit(); iter = iter.Next() {
			iter.Item().Key += ins_length
		}
		if origin.Value != time {
			tree.Insert(rbtree.Item{Key: pos, Value: time})
			if origin.Key < pos {
				tree.Insert(rbtree.Item{Key: pos + ins_length, Value: origin.Value})
			}
		}
		return
	}

	// delete nodes
	for true {
		node := iter.Item()
		next_iter := iter.Next()
		if next_iter.Limit() {
			if pos+del_length > node.Key {
				panic("attempt to delete after the end of the file")
			}
			break
		}
		delta := min(next_iter.Item().Key, pos+del_length) - max(node.Key, pos)
		if delta <= 0 {
			break
		}
		file.updateTime(time, node.Value, -delta)
		if node.Key >= pos {
			origin = *node
			tree.DeleteWithIterator(iter)
		}
		iter = next_iter
	}

	// prepare for the keys update
	var previous *rbtree.Item
	if ins_length > 0 && (origin.Value != time || origin.Key == pos) {
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
	delta := ins_length - del_length
	if delta != 0 {
		for iter = iter.Next(); !iter.Limit(); iter = iter.Next() {
			// we do not need to re-balance the tree
			iter.Item().Key += delta
		}
		// have to adjust origin in case ins_length == 0
		if origin.Key > pos {
			origin.Key += delta
		}
	}

	if ins_length > 0 {
		if origin.Value != time {
			tree.Insert(rbtree.Item{pos + ins_length, origin.Value})
		} else if pos == 0 {
			// recover the beginning
			tree.Insert(rbtree.Item{pos, time})
		}
	} else if (pos > origin.Key && previous.Value != origin.Value) || pos == origin.Key || pos == 0 {
		// continue the original interval
		tree.Insert(rbtree.Item{pos, origin.Value})
	}
}

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
	prev_key := -1
	for iter := file.tree.Min(); !iter.Limit(); iter = iter.Next() {
		node := iter.Item()
		if node.Key == prev_key {
			panic(fmt.Sprintf("duplicate tree key: %d", node.Key))
		}
		prev_key = node.Key
	}
}
