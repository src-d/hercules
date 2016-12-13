package hercules

import "fmt"

type File struct {
	tree   *RBTree
	status map[int]int64
}

const TreeEnd int = -1

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

func NewFile(time int, length int, status map[int]int64) *File {
	file := new(File)
	file.status = status
	file.tree = new(RBTree)
	if length > 0 {
		status[time] += int64(length)
		file.tree.Insert(Item{key: 0, value: time})
	}
	file.tree.Insert(Item{key: length, value: TreeEnd})
	return file
}

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
		panic("attempt to insert after the end of the file")
	}
	status := file.status
	iter := tree.FindLE(pos)
	origin := *iter.Item()
	status[time] += int64(ins_length)
	if del_length == 0 {
		// simple case with insertions only
		if origin.key < pos {
			iter = iter.Next()
		}
		for ; !iter.Limit(); iter = iter.Next() {
			iter.Item().key += ins_length
		}
		tree.Insert(Item{key: pos, value: time})
		if origin.key < pos {
			tree.Insert(Item{key: pos + ins_length, value: origin.value})
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
		status[node.value] -= int64(delta)
		if node.key >= pos {
			origin = *node
			tree.DeleteWithIterator(iter)
		}
		iter = next_iter
	}

	// prepare for the keys update
	var previous *Item
	if ins_length > 0 {
		if origin.value != time {
			// insert our new interval
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
		tree.Insert(Item{pos + ins_length, origin.value})
	} else if (pos > origin.key && previous.value != origin.value) || pos == origin.key {
		// continue the original interval
		tree.Insert(Item{pos, origin.value})
	}
}

func (file *File) Dump() string {
	buffer := ""
	for iter := file.tree.Min(); !iter.Limit(); iter = iter.Next() {
		node := iter.Item()
		buffer += fmt.Sprintf("%d %d\n", node.key, node.value)
	}
	return buffer
}
