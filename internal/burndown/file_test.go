package burndown

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/hercules.v4/internal/rbtree"
)

func updateStatusFile(
	status interface{}, _ int, previousTime int, delta int) {
	status.(map[int]int64)[previousTime] += int64(delta)
}

func fixtureFile() (*File, map[int]int64) {
	status := map[int]int64{}
	file := NewFile(0, 100, NewStatus(status, updateStatusFile))
	return file, status
}

func TestInitializeFile(t *testing.T) {
	file, status := fixtureFile()
	dump := file.Dump()
	// Output:
	// 0 0
	// 100 -1
	assert.Equal(t, "0 0\n100 -1\n", dump)
	assert.Equal(t, int64(100), status[0])
}

func testPanicFile(t *testing.T, method func(*File), msg string) {
	defer func() {
		r := recover()
		assert.NotNil(t, r, "not panic()-ed")
		assert.IsType(t, "", r)
		assert.Contains(t, r.(string), msg)
	}()
	file, _ := fixtureFile()
	method(file)
}

func TestBullshitFile(t *testing.T) {
	testPanicFile(t, func(file *File) { file.Update(1, -10, 10, 0) }, "insert")
	testPanicFile(t, func(file *File) { file.Update(1, 110, 10, 0) }, "insert")
	testPanicFile(t, func(file *File) { file.Update(1, -10, 0, 10) }, "delete")
	testPanicFile(t, func(file *File) { file.Update(1, 100, 0, 10) }, "delete")
	testPanicFile(t, func(file *File) { file.Update(1, 0, -10, 0) }, "Length")
	testPanicFile(t, func(file *File) { file.Update(1, 0, 0, -10) }, "Length")
	testPanicFile(t, func(file *File) { file.Update(1, 0, -10, -10) }, "Length")
	testPanicFile(t, func(file *File) { file.Update(-1, 0, 10, 10) }, "time")
	file, status := fixtureFile()
	file.Update(1, 10, 0, 0)
	assert.Equal(t, int64(100), status[0])
	assert.Equal(t, int64(0), status[1])
}

func TestLenFile(t *testing.T) {
	file, _ := fixtureFile()
	assert.Equal(t, 100, file.Len())
}

func TestInsertFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(1, 10, 10, 0)
	dump := file.Dump()
	// Output:
	// 0 0
	// 10 1
	// 20 0
	// 110 -1
	assert.Equal(t, "0 0\n10 1\n20 0\n110 -1\n", dump)
	assert.Equal(t, int64(100), status[0])
	assert.Equal(t, int64(10), status[1])
}

func TestZeroInitializeFile(t *testing.T) {
	status := map[int]int64{}
	file := NewFile(0, 0, NewStatus(status, updateStatusFile))
	assert.NotContains(t, status, 0)
	dump := file.Dump()
	// Output:
	// 0 -1
	assert.Equal(t, "0 -1\n", dump)
	file.Update(1, 0, 10, 0)
	dump = file.Dump()
	// Output:
	// 0 1
	// 10 -1
	assert.Equal(t, "0 1\n10 -1\n", dump)
	assert.Equal(t, int64(10), status[1])
}

func TestDeleteFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(1, 10, 0, 10)
	dump := file.Dump()
	// Output:
	// 0 0
	// 90 -1
	assert.Equal(t, "0 0\n90 -1\n", dump)
	assert.Equal(t, int64(90), status[0])
	assert.Equal(t, int64(0), status[1])
}

func TestFusedFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(1, 10, 6, 7)
	dump := file.Dump()
	// Output:
	// 0 0
	// 10 1
	// 16 0
	// 99 -1
	assert.Equal(t, "0 0\n10 1\n16 0\n99 -1\n", dump)
	assert.Equal(t, int64(93), status[0])
	assert.Equal(t, int64(6), status[1])
}

func TestInsertSameTimeFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(0, 5, 10, 0)
	dump := file.Dump()
	// Output:
	// 0 0
	// 110 -1
	assert.Equal(t, "0 0\n110 -1\n", dump)
	assert.Equal(t, int64(110), status[0])
}

func TestInsertSameStartFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(1, 10, 10, 0)
	file.Update(2, 10, 10, 0)
	dump := file.Dump()
	// Output:
	// 0 0
	// 10 2
	// 20 1
	// 30 0
	// 120 -1
	assert.Equal(t, "0 0\n10 2\n20 1\n30 0\n120 -1\n", dump)
	assert.Equal(t, int64(100), status[0])
	assert.Equal(t, int64(10), status[1])
	assert.Equal(t, int64(10), status[2])
}

func TestInsertEndFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(1, 100, 10, 0)
	dump := file.Dump()
	// Output:
	// 0 0
	// 100 1
	// 110 -1
	assert.Equal(t, "0 0\n100 1\n110 -1\n", dump)
	assert.Equal(t, int64(100), status[0])
	assert.Equal(t, int64(10), status[1])
}

func TestDeleteSameStart0File(t *testing.T) {
	file, status := fixtureFile()
	file.Update(1, 0, 0, 10)
	dump := file.Dump()
	// Output:
	// 0 0
	// 90 -1
	assert.Equal(t, "0 0\n90 -1\n", dump)
	assert.Equal(t, int64(90), status[0])
	assert.Equal(t, int64(0), status[1])
}

func TestDeleteSameStartMiddleFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(1, 10, 10, 0)
	file.Update(2, 10, 0, 5)
	dump := file.Dump()
	// Output:
	// 0 0
	// 10 1
	// 15 0
	// 105 -1
	assert.Equal(t, "0 0\n10 1\n15 0\n105 -1\n", dump)
	assert.Equal(t, int64(100), status[0])
	assert.Equal(t, int64(5), status[1])
}

func TestDeleteIntersectionFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(1, 10, 10, 0)
	file.Update(2, 15, 0, 10)
	dump := file.Dump()
	// Output:
	// 0 0
	// 10 1
	// 15 0
	// 100 -1
	assert.Equal(t, "0 0\n10 1\n15 0\n100 -1\n", dump)
	assert.Equal(t, int64(95), status[0])
	assert.Equal(t, int64(5), status[1])
}

func TestDeleteAllFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(1, 0, 0, 100)
	// Output:
	// 0 -1
	dump := file.Dump()
	assert.Equal(t, "0 -1\n", dump)
	assert.Equal(t, int64(0), status[0])
	assert.Equal(t, int64(0), status[1])
}

func TestFusedIntersectionFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(1, 10, 10, 0)
	file.Update(2, 15, 3, 10)
	dump := file.Dump()
	// Output:
	// 0 0
	// 10 1
	// 15 2
	// 18 0
	// 103 -1
	assert.Equal(t, "0 0\n10 1\n15 2\n18 0\n103 -1\n", dump)
	assert.Equal(t, int64(95), status[0])
	assert.Equal(t, int64(5), status[1])
	assert.Equal(t, int64(3), status[2])
}

func TestTortureFile(t *testing.T) {
	file, status := fixtureFile()
	// 0 0 | 100 -1                             [0]: 100
	file.Update(1, 20, 30, 0)
	// 0 0 | 20 1 | 50 0 | 130 -1               [0]: 100, [1]: 30
	file.Update(2, 20, 0, 5)
	// 0 0 | 20 1 | 45 0 | 125 -1               [0]: 100, [1]: 25
	file.Update(3, 20, 0, 5)
	// 0 0 | 20 1 | 40 0 | 120 -1               [0]: 100, [1]: 20
	file.Update(4, 20, 10, 0)
	// 0 0 | 20 4 | 30 1 | 50 0 | 130 -1        [0]: 100, [1]: 20, [4]: 10
	file.Update(5, 45, 0, 10)
	// 0 0 | 20 4 | 30 1 | 45 0 | 120 -1        [0]: 95, [1]: 15, [4]: 10
	file.Update(6, 45, 5, 0)
	// 0 0 | 20 4 | 30 1 | 45 6 | 50 0 | 125 -1 [0]: 95, [1]: 15, [4]: 10, [6]: 5
	file.Update(7, 10, 0, 50)
	// 0 0 | 75 -1                              [0]: 75
	file.Update(8, 0, 10, 10)
	// 0 8 | 10 0 | 75 -1                       [0]: 65, [8]: 10
	dump := file.Dump()
	assert.Equal(t, "0 8\n10 0\n75 -1\n", dump)
	assert.Equal(t, int64(65), status[0])
	assert.Equal(t, int64(0), status[1])
	assert.Equal(t, int64(0), status[2])
	assert.Equal(t, int64(0), status[3])
	assert.Equal(t, int64(0), status[4])
	assert.Equal(t, int64(0), status[5])
	assert.Equal(t, int64(0), status[6])
	assert.Equal(t, int64(0), status[7])
	assert.Equal(t, int64(10), status[8])
}

func TestInsertDeleteSameTimeFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(0, 10, 10, 20)
	dump := file.Dump()
	assert.Equal(t, "0 0\n90 -1\n", dump)
	assert.Equal(t, int64(90), status[0])
	file.Update(0, 10, 20, 10)
	dump = file.Dump()
	assert.Equal(t, "0 0\n100 -1\n", dump)
	assert.Equal(t, int64(100), status[0])
}

func TestBug1File(t *testing.T) {
	file, status := fixtureFile()
	file.Update(316, 1, 86, 0)
	file.Update(316, 87, 0, 99)
	file.Update(251, 0, 1, 0)
	file.Update(251, 1, 0, 1)
	dump := file.Dump()
	assert.Equal(t, "0 251\n1 316\n87 -1\n", dump)
	assert.Equal(t, int64(1), status[251])
	assert.Equal(t, int64(86), status[316])
	file.Update(316, 0, 0, 1)
	file.Update(316, 0, 1, 0)
	dump = file.Dump()
	assert.Equal(t, "0 316\n87 -1\n", dump)
	assert.Equal(t, int64(0), status[251])
	assert.Equal(t, int64(87), status[316])
}

func TestBug2File(t *testing.T) {
	file, status := fixtureFile()
	file.Update(316, 1, 86, 0)
	file.Update(316, 87, 0, 99)
	file.Update(251, 0, 1, 0)
	file.Update(251, 1, 0, 1)
	dump := file.Dump()
	assert.Equal(t, "0 251\n1 316\n87 -1\n", dump)
	file.Update(316, 0, 1, 1)
	dump = file.Dump()
	assert.Equal(t, "0 316\n87 -1\n", dump)
	assert.Equal(t, int64(0), status[251])
	assert.Equal(t, int64(87), status[316])
}

func TestJoinFile(t *testing.T) {
	file, status := fixtureFile()
	file.Update(1, 10, 10, 0)
	file.Update(1, 30, 10, 0)
	file.Update(1, 20, 10, 10)
	dump := file.Dump()
	assert.Equal(t, "0 0\n10 1\n40 0\n120 -1\n", dump)
	assert.Equal(t, int64(90), status[0])
	assert.Equal(t, int64(30), status[1])
}

func TestBug3File(t *testing.T) {
	file, status := fixtureFile()
	file.Update(0, 1, 0, 99)
	file.Update(0, 0, 1, 1)
	dump := file.Dump()
	assert.Equal(t, "0 0\n1 -1\n", dump)
	assert.Equal(t, int64(1), status[0])
}

func TestBug4File(t *testing.T) {
	status := map[int]int64{}
	file := NewFile(0, 10, NewStatus(status, updateStatusFile))
	file.Update(125, 0, 20, 9)
	file.Update(125, 0, 20, 20)
	file.Update(166, 12, 1, 1)
	file.Update(214, 2, 1, 1)
	file.Update(214, 4, 9, 0)
	file.Update(214, 27, 1, 1)
	file.Update(215, 3, 1, 1)
	file.Update(215, 13, 1, 1)
	file.Update(215, 17, 1, 1)
	file.Update(215, 19, 5, 0)
	file.Update(215, 25, 0, 1)
	file.Update(215, 31, 6, 1)
	file.Update(215, 27, 15, 0)
	file.Update(215, 2, 25, 4)
	file.Update(215, 28, 1, 1)
	file.Update(215, 30, 7, 2)
	file.Update(215, 38, 1, 0)
	file.Update(215, 40, 4, 2)
	file.Update(215, 46, 1, 0)
	file.Update(215, 49, 1, 0)
	file.Update(215, 52, 2, 6)
	dump := file.Dump()
	assert.Equal(t, "0 125\n2 215\n48 125\n50 215\n69 125\n73 215\n79 125\n80 0\n81 -1\n", dump)
}

func TestBug5File(t *testing.T) {
	status := map[int]int64{}
	keys := []int{0, 2, 4, 7, 10}
	vals := []int{24, 28, 24, 28, -1}
	file := NewFileFromTree(keys, vals, NewStatus(status, updateStatusFile))
	file.Update(28, 0, 1, 3)
	dump := file.Dump()
	assert.Equal(t, "0 28\n2 24\n5 28\n8 -1\n", dump)

	keys = []int{0, 1, 16, 18}
	vals = []int{305, 0, 157, -1}
	file = NewFileFromTree(keys, vals, NewStatus(status, updateStatusFile))
	file.Update(310, 0, 0, 2)
	dump = file.Dump()
	assert.Equal(t, "0 0\n14 157\n16 -1\n", dump)
}

func TestNewFileFromTreeInvalidSize(t *testing.T) {
	keys := [...]int{1, 2, 3}
	vals := [...]int{4, 5}
	assert.Panics(t, func() { NewFileFromTree(keys[:], vals[:]) })
}

func TestUpdatePanic(t *testing.T) {
	keys := [...]int{0}
	vals := [...]int{-1}
	file := NewFileFromTree(keys[:], vals[:])
	file.tree.DeleteWithKey(0)
	file.tree.Insert(rbtree.Item{Key: -1, Value: -1})
	var paniced interface{}
	func() {
		defer func() {
			paniced = recover()
		}()
		file.Update(1, 0, 1, 0)
	}()
	assert.Contains(t, paniced, "invalid tree state")
}

func TestFileStatus(t *testing.T) {
	f, _ := fixtureFile()
	assert.Panics(t, func() { f.Status(1) })
	assert.NotNil(t, f.Status(0))
}

func TestFileValidate(t *testing.T) {
	keys := [...]int{0}
	vals := [...]int{-1}
	file := NewFileFromTree(keys[:], vals[:])
	file.tree.DeleteWithKey(0)
	file.tree.Insert(rbtree.Item{Key: -1, Value: -1})
	assert.Panics(t, func() { file.Validate() })
	file.tree.DeleteWithKey(-1)
	file.tree.Insert(rbtree.Item{Key: 0, Value: -1})
	file.Validate()
	file.tree.DeleteWithKey(0)
	file.tree.Insert(rbtree.Item{Key: 0, Value: 0})
	assert.Panics(t, func() { file.Validate() })
	file.tree.DeleteWithKey(0)
	file.tree.Insert(rbtree.Item{Key: 0, Value: 1})
	file.tree.Insert(rbtree.Item{Key: 1, Value: 1})
	file.tree.Insert(rbtree.Item{Key: 2, Value: -1})
	file.Validate()
	file.tree.FindGE(2).Item().Key = 1
	assert.Panics(t, func() { file.Validate() })
}
