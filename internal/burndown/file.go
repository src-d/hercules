package burndown

import (
	"fmt"
	"log"
	"math"

	"gopkg.in/src-d/hercules.v10/internal"
	"gopkg.in/src-d/hercules.v10/internal/rbtree"
)

// Updater is the function which is called back on File.Update().
type Updater = func(currentTime, previousTime, delta int)

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
	tree     *rbtree.RBTree
	updaters []Updater
}

// TreeEnd denotes the value of the last leaf in the tree.
const TreeEnd = math.MaxUint32

// TreeMaxBinPower is the binary power value which corresponds to the maximum tick which
// can be stored in the tree.
const TreeMaxBinPower = 14

// TreeMergeMark is the special day which disables the status updates and is used in File.Merge().
const TreeMergeMark = (1 << TreeMaxBinPower) - 1

func (file *File) updateTime(currentTime, previousTime, delta int) {
	if previousTime&TreeMergeMark == TreeMergeMark {
		if currentTime == previousTime {
			return
		}
		panic("previousTime cannot be TreeMergeMark")
	}
	if currentTime&TreeMergeMark == TreeMergeMark {
		// merge mode - we have already updated in one of the branches
		return
	}
	for _, update := range file.updaters {
		update(currentTime, previousTime, delta)
	}
}

// NewFile initializes a new instance of File struct.
//
// time is the starting value of the first node;
//
// length is the starting length of the tree (the key of the second and the
// last node);
//
// updaters are the attached interval length mappings.
func NewFile(time int, length int, allocator *rbtree.Allocator, updaters ...Updater) *File {
	file := &File{tree: rbtree.NewRBTree(allocator), updaters: updaters}
	file.updateTime(time, time, length)
	if time < 0 || time > math.MaxUint32 {
		log.Panicf("time is out of allowed range: %d", time)
	}
	if length > math.MaxUint32 {
		log.Panicf("length is out of allowed range: %d", length)
	}
	if length > 0 {
		file.tree.Insert(rbtree.Item{Key: 0, Value: uint32(time)})
	}
	file.tree.Insert(rbtree.Item{Key: uint32(length), Value: TreeEnd})
	return file
}

// NewFileFromTree is an alternative constructor for File which is used in tests.
// The resulting tree is validated with Validate() to ensure the initial integrity.
//
// keys is a slice with the starting tree keys.
//
// vals is a slice with the starting tree values. Must match the size of keys.
//
// updaters are the attached interval length mappings.
func NewFileFromTree(keys []int, vals []int, allocator *rbtree.Allocator, updaters ...Updater) *File {
	file := &File{tree: rbtree.NewRBTree(allocator), updaters: updaters}
	if len(keys) != len(vals) {
		panic("keys and vals must be of equal length")
	}
	for i, key := range keys {
		val := vals[i]
		if key < 0 || key >= math.MaxUint32 {
			log.Panicf("key is out of allowed range: [%d]=%d", i, key)
		}
		if val < 0 || val > math.MaxUint32 {
			log.Panicf("val is out of allowed range: [%d]=%d", i, val)
		}
		file.tree.Insert(rbtree.Item{Key: uint32(key), Value: uint32(val)})
	}
	file.Validate()
	return file
}

// CloneShallow copies the file. It performs a shallow copy of the tree: the allocator
// must be Clone()-d beforehand.
func (file *File) CloneShallow(allocator *rbtree.Allocator) *File {
	return &File{tree: file.tree.CloneShallow(allocator), updaters: file.updaters}
}

// CloneDeep copies the file. It performs a deep copy of the tree.
func (file *File) CloneDeep(allocator *rbtree.Allocator) *File {
	return &File{tree: file.tree.CloneDeep(allocator), updaters: file.updaters}
}

// Delete deallocates the file.
func (file *File) Delete() {
	file.tree.Erase()
}

// Len returns the File's size - that is, the maximum key in the tree of line
// intervals.
func (file File) Len() int {
	return int(file.tree.Max().Item().Key)
}

// Nodes returns the number of RBTree nodes in the file.
func (file File) Nodes() int {
	return file.tree.Len()
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
	if time >= math.MaxUint32 {
		panic("time may not be >= MaxUint32")
	}
	if pos < 0 {
		panic("attempt to insert/delete at a negative position")
	}
	if pos > math.MaxUint32 {
		panic("pos may not be > MaxUint32")
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
	if uint32(pos) > tree.Max().Item().Key {
		panic(fmt.Sprintf("attempt to insert after the end of the file: %d < %d",
			tree.Max().Item().Key, pos))
	}
	iter := tree.FindLE(uint32(pos))
	origin := *iter.Item()
	prevOrigin := origin
	{
		prevIter := iter.Prev()
		if prevIter.Item() != nil {
			prevOrigin = *prevIter.Item()
		}
	}
	if insLength > 0 {
		file.updateTime(time, time, insLength)
	}
	if delLength == 0 {
		// simple case with insertions only
		if origin.Key < uint32(pos) || (origin.Value == uint32(time) && (pos == 0 || uint32(pos) == origin.Key)) {
			iter = iter.Next()
		}
		for ; !iter.Limit(); iter = iter.Next() {
			iter.Item().Key += uint32(insLength)
		}
		if origin.Value != uint32(time) {
			tree.Insert(rbtree.Item{Key: uint32(pos), Value: uint32(time)})
			if origin.Key < uint32(pos) {
				tree.Insert(rbtree.Item{Key: uint32(pos + insLength), Value: origin.Value})
			}
		}
		return
	}

	// delete nodes
	for true {
		node := iter.Item()
		nextIter := iter.Next()
		if nextIter.Limit() {
			if uint32(pos+delLength) > node.Key {
				panic("attempt to delete after the end of the file")
			}
			break
		}
		delta := internal.Min(int(nextIter.Item().Key), pos+delLength) - internal.Max(int(node.Key), pos)
		if delta == 0 && insLength == 0 && origin.Key == uint32(pos) && prevOrigin.Value == node.Value {
			origin = *node
			tree.DeleteWithIterator(iter)
			iter = nextIter
		}
		if delta <= 0 {
			break
		}
		file.updateTime(time, int(node.Value), -delta)
		if node.Key >= uint32(pos) {
			origin = *node
			tree.DeleteWithIterator(iter)
		}
		iter = nextIter
	}

	// prepare for the keys update
	var previous *rbtree.Item
	if insLength > 0 && (origin.Value != uint32(time) || origin.Key == uint32(pos)) {
		// insert our new interval
		if iter.Item().Value == uint32(time) && int(iter.Item().Key)-delLength == pos {
			prev := iter.Prev()
			if prev.NegativeLimit() || prev.Item().Value != uint32(time) {
				iter.Item().Key = uint32(pos)
			} else {
				tree.DeleteWithIterator(iter)
				iter = prev
			}
			origin.Value = uint32(time) // cancels the insertion after applying the delta
		} else {
			_, iter = tree.Insert(rbtree.Item{Key: uint32(pos), Value: uint32(time)})
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
			iter.Item().Key = uint32(int(iter.Item().Key) + delta)
		}
		// have to adjust origin in case insLength == 0
		if origin.Key > uint32(pos) {
			origin.Key = uint32(int(origin.Key) + delta)
		}
	}

	if insLength > 0 {
		if origin.Value != uint32(time) {
			tree.Insert(rbtree.Item{Key: uint32(pos + insLength), Value: origin.Value})
		} else if pos == 0 {
			// recover the beginning
			tree.Insert(rbtree.Item{Key: uint32(pos), Value: uint32(time)})
		}
	} else if (uint32(pos) > origin.Key && previous != nil && previous.Value != origin.Value) ||
		(uint32(pos) == origin.Key && origin.Value != prevOrigin.Value) ||
		pos == 0 {
		// continue the original interval
		tree.Insert(rbtree.Item{Key: uint32(pos), Value: origin.Value})
	}
}

// Merge combines several prepared File-s together.
func (file *File) Merge(day int, others ...*File) {
	myself := file.flatten()
	for _, other := range others {
		if other == nil {
			log.Panic("merging with a nil file")
		}
		lines := other.flatten()
		if len(myself) != len(lines) {
			log.Panicf("file corruption, lines number mismatch during merge %d != %d",
				len(myself), len(lines))
		}
		for i, l := range myself {
			ol := lines[i]
			if ol&TreeMergeMark == TreeMergeMark {
				continue
			}
			if l&TreeMergeMark == TreeMergeMark || l&TreeMergeMark > ol&TreeMergeMark {
				// the line is merged in myself and exists in other
				// OR the same line introduced in different branches
				// consider the oldest version as the ground truth in that case
				myself[i] = ol
				continue
			}
		}
	}
	for i, l := range myself {
		if l&TreeMergeMark == TreeMergeMark {
			// original merge conflict resolution
			myself[i] = day
			file.updateTime(day, day, 1)
		}
	}
	// now we need to reconstruct the tree from the discrete values
	file.tree.Erase()
	tree := rbtree.NewRBTree(file.tree.Allocator())
	for i, v := range myself {
		if i == 0 || v != myself[i-1] {
			tree.Insert(rbtree.Item{Key: uint32(i), Value: uint32(v)})
		}
	}
	tree.Insert(rbtree.Item{Key: uint32(len(myself)), Value: TreeEnd})
	file.tree = tree
}

// Dump formats the underlying line interval tree into a string.
// Useful for error messages, panic()-s and debugging.
func (file File) Dump() string {
	buffer := ""
	file.ForEach(func(line, value int) {
		buffer += fmt.Sprintf("%d %d\n", line, value)
	})
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
func (file File) Validate() {
	if file.tree.Min().Item().Key != 0 {
		log.Panic("the tree must start with key 0")
	}
	if file.tree.Max().Item().Value != TreeEnd {
		log.Panicf("the last value in the tree must be %d", TreeEnd)
	}
	prevKey := uint32(math.MaxUint32)
	for iter := file.tree.Min(); !iter.Limit(); iter = iter.Next() {
		node := iter.Item()
		if node.Key == prevKey {
			log.Panicf("duplicate tree key: %d", node.Key)
		}
		if node.Value == TreeMergeMark {
			log.Panicf("unmerged lines left: %d", node.Key)
		}
		prevKey = node.Key
	}
}

// ForEach visits each node in the underlying tree, in ascending key order.
func (file File) ForEach(callback func(line, value int)) {
	for iter := file.tree.Min(); !iter.Limit(); iter = iter.Next() {
		item := iter.Item()
		key := int(item.Key)
		var value int
		if item.Value == math.MaxUint32 {
			value = -1
		} else {
			value = int(item.Value)
		}
		callback(key, value)
	}
}

// flatten represents the file as a slice of lines, each line's value being the corresponding day.
func (file *File) flatten() []int {
	lines := make([]int, 0, file.Len())
	val := uint32(math.MaxUint32)
	for iter := file.tree.Min(); !iter.Limit(); iter = iter.Next() {
		for i := uint32(len(lines)); i < iter.Item().Key; i++ {
			lines = append(lines, int(val))
		}
		val = iter.Item().Value
	}
	return lines
}
