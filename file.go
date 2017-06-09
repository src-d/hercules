package hercules

import "fmt"

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
	tree   *RBTree
	statuses []map[int]int64
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

func (file *File) updateTime(time int, delta int) {
	for _, status := range file.statuses {
		status[time] += int64(delta)
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
func NewFile(time int, length int, statuses ...map[int]int64) *File {
	file := new(File)
	file.statuses = statuses
	file.tree = new(RBTree)
	if length > 0 {
		file.updateTime(time, length)
		file.tree.Insert(Item{key: 0, value: time})
	}
	file.tree.Insert(Item{key: length, value: TreeEnd})
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
func NewFileFromTree(keys []int, vals []int, statuses ...map[int]int64) *File {
	file := new(File)
	file.statuses = statuses
	file.tree = new(RBTree)
	if len(keys) != len(vals) {
		panic("keys and vals must be of equal length")
	}
	for i := 0; i < len(keys); i++ {
		file.tree.Insert(Item{key: keys[i], value: vals[i]})
	}
	file.Validate()
	return file
}

// Len returns the File's size - that is, the maximum key in the tree of line
// intervals.
func (file *File) Len() int {
	return file.tree.Max().Item().key
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
	if pos > tree.Max().Item().key {
		panic(fmt.Sprintf("attempt to insert after the end of the file: %d < %d",
			tree.Max().Item().key, pos))
	}
	if tree.Len() < 2 && tree.Min().Item().key != 0 {
		panic("invalid tree state")
	}
	iter := tree.FindLE(pos)
	origin := *iter.Item()
	file.updateTime(time, ins_length)
	if del_length == 0 {
		// simple case with insertions only
		if origin.key < pos || (origin.value == time && pos == 0) {
			iter = iter.Next()
		}
		for ; !iter.Limit(); iter = iter.Next() {
			iter.Item().key += ins_length
		}
		if origin.value != time {
			tree.Insert(Item{key: pos, value: time})
			if origin.key < pos {
				tree.Insert(Item{key: pos + ins_length, value: origin.value})
			}
		}
		return
	}

	// delete nodes
	for true {
		node := iter.Item()
		next_iter := iter.Next()
		if next_iter.Limit() {
			if pos+del_length > node.key {
				panic("attempt to delete after the end of the file")
			}
			break
		}
		delta := min(next_iter.Item().key, pos+del_length) - max(node.key, pos)
		if delta <= 0 {
			break
		}
		file.updateTime(node.value, -delta)
		if node.key >= pos {
			origin = *node
			tree.DeleteWithIterator(iter)
		}
		iter = next_iter
	}

	// prepare for the keys update
	var previous *Item
	if ins_length > 0 && (origin.value != time || origin.key == pos) {
		// insert our new interval
		if iter.Item().value == time {
			prev := iter.Prev()
			if prev.Item().value != time {
				iter.Item().key = pos
			} else {
				tree.DeleteWithIterator(iter)
				iter = prev
			}
			origin.value = time // cancels the insertion after applying the delta
		} else {
			_, iter = tree.Insert(Item{key: pos, value: time})
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
			iter.Item().key += delta
		}
		// have to adjust origin in case ins_length == 0
		if origin.key > pos {
			origin.key += delta
		}
	}

	if ins_length > 0 {
		if origin.value != time {
			tree.Insert(Item{pos + ins_length, origin.value})
		} else if pos == 0 {
			// recover the beginning
			tree.Insert(Item{pos, time})
		}
	} else if (pos > origin.key && previous.value != origin.value) || pos == origin.key || pos == 0 {
		// continue the original interval
		tree.Insert(Item{pos, origin.value})
	}
}

func (file *File) Status(index int) map[int]int64 {
	if index < 0 || index >= len(file.statuses) {
		panic(fmt.Sprintf("status index %d is out of bounds [0, %d)",
		                  index, len(file.statuses)))
	}
	return file.statuses[index]
}

// Dump formats the underlying line interval tree into a string.
// Useful for error messages, panic()-s and debugging.
func (file *File) Dump() string {
	buffer := ""
	for iter := file.tree.Min(); !iter.Limit(); iter = iter.Next() {
		node := iter.Item()
		buffer += fmt.Sprintf("%d %d\n", node.key, node.value)
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
	if file.tree.Min().Item().key != 0 {
		panic("the tree must start with key 0")
	}
	if file.tree.Max().Item().value != TreeEnd {
		panic(fmt.Sprintf("the last value in the tree must be %d", TreeEnd))
	}
	prev_key := -1
	for iter := file.tree.Min(); !iter.Limit(); iter = iter.Next() {
		node := iter.Item()
		if node.key == prev_key {
			panic(fmt.Sprintf("duplicate tree key: %d", node.key))
		}
		prev_key = node.key
	}
}
