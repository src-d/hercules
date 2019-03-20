package burndown

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/hercules.v10/internal/rbtree"
)

func updateStatusFile(status map[int]int64, _, previousTime, delta int) {
	status[previousTime] += int64(delta)
}

func fixtureFile() (*File, map[int]int64, *rbtree.Allocator) {
	status := map[int]int64{}
	alloc := rbtree.NewAllocator()
	file := NewFile(0, 100, alloc, func(a, b, c int) {
		updateStatusFile(status, a, b, c)
	})
	return file, status, alloc
}

func TestInitializeFile(t *testing.T) {
	file, status, _ := fixtureFile()
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
	file, _, _ := fixtureFile()
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
	file, status, alloc := fixtureFile()
	file.Update(1, 10, 0, 0)
	assert.Equal(t, int64(100), status[0])
	assert.Equal(t, int64(0), status[1])
	assert.Equal(t, alloc.Size(), 3) // 1 + 2 nodes
}

func TestCloneFileShallow(t *testing.T) {
	file, status, alloc := fixtureFile()
	// 0 0 | 100 -1                             [0]: 100
	file.Update(1, 20, 30, 0)
	// 0 0 | 20 1 | 50 0 | 130 -1               [0]: 100, [1]: 30
	file.Update(2, 20, 0, 5)
	// 0 0 | 20 1 | 45 0 | 125 -1               [0]: 100, [1]: 25
	file.Update(3, 20, 0, 5)
	// 0 0 | 20 1 | 40 0 | 120 -1               [0]: 100, [1]: 20
	file.Update(4, 20, 10, 0)
	// 0 0 | 20 4 | 30 1 | 50 0 | 130 -1        [0]: 100, [1]: 20, [4]: 10
	assert.Equal(t, alloc.Size(), 6)
	clone := file.CloneShallow(alloc.Clone())
	clone.Update(5, 45, 0, 10)
	// 0 0 | 20 4 | 30 1 | 45 0 | 120 -1        [0]: 95, [1]: 15, [4]: 10
	clone.Update(6, 45, 5, 0)
	// 0 0 | 20 4 | 30 1 | 45 6 | 50 0 | 125 -1 [0]: 95, [1]: 15, [4]: 10, [6]: 5
	assert.Equal(t, int64(95), status[0])
	assert.Equal(t, int64(15), status[1])
	assert.Equal(t, int64(0), status[2])
	assert.Equal(t, int64(0), status[3])
	assert.Equal(t, int64(10), status[4])
	assert.Equal(t, int64(0), status[5])
	assert.Equal(t, int64(5), status[6])
	dump := file.Dump()
	// Output:
	// 0 0
	// 20 4
	// 30 1
	// 50 0
	// 130 -1
	assert.Equal(t, "0 0\n20 4\n30 1\n50 0\n130 -1\n", dump)
	dump = clone.Dump()
	// Output:
	// 0 0
	// 20 4
	// 30 1
	// 45 6
	// 50 0
	// 125 -1
	assert.Equal(t, "0 0\n20 4\n30 1\n45 6\n50 0\n125 -1\n", dump)
}

func TestCloneFileDeep(t *testing.T) {
	file, status, alloc := fixtureFile()
	// 0 0 | 100 -1                             [0]: 100
	file.Update(1, 20, 30, 0)
	// 0 0 | 20 1 | 50 0 | 130 -1               [0]: 100, [1]: 30
	file.Update(2, 20, 0, 5)
	// 0 0 | 20 1 | 45 0 | 125 -1               [0]: 100, [1]: 25
	file.Update(3, 20, 0, 5)
	// 0 0 | 20 1 | 40 0 | 120 -1               [0]: 100, [1]: 20
	file.Update(4, 20, 10, 0)
	// 0 0 | 20 4 | 30 1 | 50 0 | 130 -1        [0]: 100, [1]: 20, [4]: 10
	assert.Equal(t, alloc.Size(), 6)
	clone := file.CloneDeep(rbtree.NewAllocator())
	clone.Update(5, 45, 0, 10)
	// 0 0 | 20 4 | 30 1 | 45 0 | 120 -1        [0]: 95, [1]: 15, [4]: 10
	clone.Update(6, 45, 5, 0)
	// 0 0 | 20 4 | 30 1 | 45 6 | 50 0 | 125 -1 [0]: 95, [1]: 15, [4]: 10, [6]: 5
	assert.Equal(t, int64(95), status[0])
	assert.Equal(t, int64(15), status[1])
	assert.Equal(t, int64(0), status[2])
	assert.Equal(t, int64(0), status[3])
	assert.Equal(t, int64(10), status[4])
	assert.Equal(t, int64(0), status[5])
	assert.Equal(t, int64(5), status[6])
	dump := file.Dump()
	// Output:
	// 0 0
	// 20 4
	// 30 1
	// 50 0
	// 130 -1
	assert.Equal(t, "0 0\n20 4\n30 1\n50 0\n130 -1\n", dump)
	dump = clone.Dump()
	// Output:
	// 0 0
	// 20 4
	// 30 1
	// 45 6
	// 50 0
	// 125 -1
	assert.Equal(t, "0 0\n20 4\n30 1\n45 6\n50 0\n125 -1\n", dump)
}

func TestLenFile(t *testing.T) {
	file, _, _ := fixtureFile()
	assert.Equal(t, 100, file.Len())
}

func TestInsertFile(t *testing.T) {
	file, status, _ := fixtureFile()
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
	file := NewFile(0, 0, rbtree.NewAllocator(), func(a, b, c int) {
		updateStatusFile(status, a, b, c)
	})
	assert.Contains(t, status, 0)
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
	file, status, alloc := fixtureFile()
	file.Update(1, 10, 0, 10)
	dump := file.Dump()
	// Output:
	// 0 0
	// 90 -1
	assert.Equal(t, "0 0\n90 -1\n", dump)
	assert.Equal(t, int64(90), status[0])
	assert.Equal(t, int64(0), status[1])
	assert.Equal(t, alloc.Size(), 3)
}

func TestFusedFile(t *testing.T) {
	file, status, alloc := fixtureFile()
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
	file.Update(3, 10, 0, 6)
	dump = file.Dump()
	assert.Equal(t, "0 0\n93 -1\n", dump)
	assert.Equal(t, alloc.Size(), 5)
	file.Update(3, 10, 6, 0)         // +2 nodes
	assert.Equal(t, alloc.Size(), 5) // using gaps
	file.Update(4, 10, 6, 0)
	assert.Equal(t, alloc.Size(), 6)
}

func TestDeleteSameBeginning(t *testing.T) {
	file, _, _ := fixtureFile()
	file.Update(1, 0, 5, 0)
	dump := file.Dump()
	// Output:
	// 0 0
	// 10 1
	// 16 0
	// 99 -1
	assert.Equal(t, "0 1\n5 0\n105 -1\n", dump)
	file.Update(3, 0, 0, 5)
	dump = file.Dump()
	assert.Equal(t, "0 0\n100 -1\n", dump)
}

func TestInsertSameTimeFile(t *testing.T) {
	file, status, _ := fixtureFile()
	file.Update(0, 5, 10, 0)
	dump := file.Dump()
	// Output:
	// 0 0
	// 110 -1
	assert.Equal(t, "0 0\n110 -1\n", dump)
	assert.Equal(t, int64(110), status[0])
}

func TestInsertSameStartFile(t *testing.T) {
	file, status, _ := fixtureFile()
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
	file, status, _ := fixtureFile()
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
	file, status, _ := fixtureFile()
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
	file, status, _ := fixtureFile()
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
	file, status, _ := fixtureFile()
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
	file, status, _ := fixtureFile()
	file.Update(1, 0, 0, 100)
	// Output:
	// 0 -1
	dump := file.Dump()
	assert.Equal(t, "0 -1\n", dump)
	assert.Equal(t, int64(0), status[0])
	assert.Equal(t, int64(0), status[1])
}

func TestFusedIntersectionFile(t *testing.T) {
	file, status, _ := fixtureFile()
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
	file, status, _ := fixtureFile()
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
	file, status, _ := fixtureFile()
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
	file, status, _ := fixtureFile()
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
	file, status, _ := fixtureFile()
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
	file, status, _ := fixtureFile()
	file.Update(1, 10, 10, 0)
	file.Update(1, 30, 10, 0)
	file.Update(1, 20, 10, 10)
	dump := file.Dump()
	assert.Equal(t, "0 0\n10 1\n40 0\n120 -1\n", dump)
	assert.Equal(t, int64(90), status[0])
	assert.Equal(t, int64(30), status[1])
}

func TestBug3File(t *testing.T) {
	file, status, _ := fixtureFile()
	file.Update(0, 1, 0, 99)
	file.Update(0, 0, 1, 1)
	dump := file.Dump()
	assert.Equal(t, "0 0\n1 -1\n", dump)
	assert.Equal(t, int64(1), status[0])
}

func TestBug4File(t *testing.T) {
	status := map[int]int64{}
	alloc := rbtree.NewAllocator()
	file := NewFile(0, 10, alloc, func(a, b, c int) {
		updateStatusFile(status, a, b, c)
	})
	// 0 0 | 10 -1
	file.Update(125, 0, 20, 9)
	// 0 125 | 20 0 | 21 -1
	file.Update(125, 0, 20, 20)
	// 0 125 | 20 0 | 21 -1
	file.Update(166, 12, 1, 1)
	// 0 125 | 12 166 | 13 125 | 20 0 | 21 -1
	file.Update(214, 2, 1, 1)
	// 0 125 | 2 214 | 3 125 | 12 166 | 13 125 | 20 0 | 21 -1
	file.Update(214, 4, 9, 0)
	// 0 125 | 2 214 | 3 125 | 4 214 | 13 125 | 21 166 | 22 125 | 29 0 | 30 -1
	file.Update(214, 27, 1, 1)
	// 0 125 | 2 214 | 3 125 | 4 214 | 13 125 | 21 166 | 22 125 | 27 214 | 28 125 | 29 0 | 30 -1
	file.Update(215, 3, 1, 1)
	// 0 125 | 2 214 | 3 215 | 4 214 | 13 125 | 21 166 | 22 125 | 27 214 | 28 125 | 29 0 | 30 -1
	file.Update(215, 13, 1, 1)
	// 0 125 | 2 214 | 3 215 | 4 214 | 13 215 | 14 125 | 21 166 | 22 125 | 27 214 | 28 125 | 29 0 | 30 -1
	file.Update(215, 17, 1, 1)
	// 0 125 | 2 214 | 3 215 | 4 214 | 13 215 | 14 125 | 17 215 | 18 125 | 21 166 | 22 125 | 27 214 | 28 125 | 29 0 | 30 -1
	file.Update(215, 19, 5, 0)
	// 0 125 | 2 214 | 3 215 | 4 214 | 13 215 | 14 125 | 17 215 | 18 125 | 19 215 | 24 125 | 26 166 | 27 125 | 32 214 | 33 125 | 34 0 | 35 -1
	file.Update(215, 25, 0, 1)
	// 0 125 | 2 214 | 3 215 | 4 214 | 13 215 | 14 125 | 17 215 | 18 125 | 19 215 | 24 125 | 25 166 | 26 125 | 31 214 | 32 125 | 33 0 | 34 -1
	file.Update(215, 31, 6, 1)
	// 0 125 | 2 214 | 3 215 | 4 214 | 13 215 | 14 125 | 17 215 | 18 125 | 19 215 | 24 125 | 25 166 | 26 125 | 31 215 | 37 125 | 38 0 | 39 -1
	file.Update(215, 27, 15, 0)
	// 0 125 | 2 214 | 3 215 | 4 214 | 13 215 | 14 125 | 17 215 | 18 125 | 19 215 | 24 125 | 25 166 | 26 125 | 27 215 | 42 125 | 46 215 | 52 125 | 53 0 | 54 -1
	file.Update(215, 2, 25, 4)
	// 0 125 | 2 215 | 27 214 | 34 215 | 35 125 | 38 215 | 39 125 | 40 215 | 45 125 | 46 166 | 47 125 | 48 215 | 63 125 | 67 215 | 73 125 | 74 0 | 75 -1
	file.Update(215, 28, 1, 1)
	// 0 125 | 2 215 | 27 214 | 28 215 | 29 214 | 34 215 | 35 125 | 38 215 | 39 125 | 40 215 | 45 125 | 46 166 | 47 125 | 48 215 | 63 125 | 67 215 | 73 125 | 74 0 | 75 -1
	file.Update(215, 30, 7, 2)
	// 0 125 | 2 215 | 27 214 | 28 215 | 29 214 | 30 215 | 37 214 | 39 215 | 40 125 | 43 215 | 44 125 | 45 215 | 50 125 | 51 166 | 52 125 | 53 215 | 68 125 | 72 215 | 78 125 | 79 0 | 80 -1
	file.Update(215, 38, 1, 0)
	// 0 125 | 2 215 | 27 214 | 28 215 | 29 214 | 30 215 | 37 214 | 38 215 | 39 214 | 40 215 | 41 125 | 44 215 | 45 125 | 46 215 | 51 125 | 52 166 | 53 125 | 54 215 | 69 125 | 73 215 | 79 125 | 80 0 | 81 -1
	file.Update(215, 40, 4, 2)
	// 0 125 | 2 215 | 27 214 | 28 215 | 29 214 | 30 215 | 37 214 | 38 215 | 39 214 | 40 215 | 44 125 | 46 215 | 47 125 | 48 215 | 53 125 | 54 166 | 55 125 | 56 215 | 71 125 | 75 215 | 81 125 | 82 0 | 83 -1
	file.Update(215, 46, 1, 0)
	// 0 125 | 2 215 | 27 214 | 28 215 | 29 214 | 30 215 | 37 214 | 38 215 | 39 214 | 40 215 | 44 125 | 46 215 | 48 125 | 49 215 | 54 125 | 55 166 | 56 125 | 57 215 | 72 125 | 76 215 | 82 125 | 83 0 | 84 -1
	file.Update(215, 49, 1, 0)
	// 0 125 | 2 215 | 27 214 | 28 215 | 29 214 | 30 215 | 37 214 | 38 215 | 39 214 | 40 215 | 44 125 | 46 215 | 48 125 | 49 215 | 55 125 | 56 166 | 57 125 | 58 215 | 73 125 | 77 215 | 83 125 | 84 0 | 85 -1
	file.Update(215, 52, 2, 6)
	// 0 125 | 2 215 | 27 214 | 28 215 | 29 214 | 30 215 | 37 214 | 38 215 | 39 214 | 40 215 | 44 125 | 46 215 | 48 125 | 49 215 | 69 125 | 73 215 | 79 125 | 80 0 | 81 -1
	dump := file.Dump()
	assert.Equal(t, "0 125\n2 215\n27 214\n28 215\n29 214\n30 215\n37 214\n38 215\n39 214\n40 215\n44 125\n46 215\n48 125\n49 215\n69 125\n73 215\n79 125\n80 0\n81 -1\n", dump)

	file.Update(214, 38, 1, 1)
	// 0 125 | 2 215 | 27 214 | 28 215 | 29 214 | 30 215 | 37 214 | 40 215 | 44 125 | 46 215 | 48 125 | 49 215 | 69 125 | 73 215 | 79 125 | 80 0 | 81 -1
	dump = file.Dump()
	assert.Equal(t, "0 125\n2 215\n27 214\n28 215\n29 214\n30 215\n37 214\n40 215\n44 125\n46 215\n48 125\n49 215\n69 125\n73 215\n79 125\n80 0\n81 -1\n", dump)

	file.Update(300, 28, 1, 1)
	// 0 125 | 2 215 | 27 214 | 28 300 | 29 214 | 30 215 | 37 214 | 40 215 | 44 125 | 46 215 | 48 125 | 49 215 | 69 125 | 73 215 | 79 125 | 80 0 | 81 -1
	dump = file.Dump()
	assert.Equal(t, "0 125\n2 215\n27 214\n28 300\n29 214\n30 215\n37 214\n40 215\n44 125\n46 215\n48 125\n49 215\n69 125\n73 215\n79 125\n80 0\n81 -1\n", dump)
	assert.Equal(t, 1+file.tree.Len(), alloc.Used())
	assert.Equal(t, file.Nodes(), file.tree.Len())
}

func TestBug5File(t *testing.T) {
	status := map[int]int64{}
	keys := []int{0, 2, 4, 7, 10}
	vals := []int{24, 28, 24, 28, math.MaxUint32}
	file := NewFileFromTree(keys, vals, rbtree.NewAllocator(), func(a, b, c int) {
		updateStatusFile(status, a, b, c)
	})
	file.Update(28, 0, 1, 3)
	dump := file.Dump()
	assert.Equal(t, "0 28\n2 24\n5 28\n8 -1\n", dump)

	keys = []int{0, 1, 16, 18}
	vals = []int{305, 0, 157, math.MaxUint32}
	file = NewFileFromTree(keys, vals, rbtree.NewAllocator(), func(a, b, c int) {
		updateStatusFile(status, a, b, c)
	})
	file.Update(310, 0, 0, 2)
	dump = file.Dump()
	assert.Equal(t, "0 0\n14 157\n16 -1\n", dump)
}

func TestNewFileFromTreeInvalidSize(t *testing.T) {
	keys := [...]int{1, 2, 3}
	vals := [...]int{4, 5}
	assert.Panics(t, func() { NewFileFromTree(keys[:], vals[:], rbtree.NewAllocator()) })
}

func TestUpdatePanic(t *testing.T) {
	keys := [...]int{0}
	vals := [...]int{math.MaxUint32}
	file := NewFileFromTree(keys[:], vals[:], rbtree.NewAllocator())
	file.tree.DeleteWithKey(0)
	file.tree.Insert(rbtree.Item{Key: 1, Value: math.MaxUint32})
	assert.PanicsWithValue(t, "invalid tree state", func() { file.Update(1, 0, 1, 0) })
	file.tree.Insert(rbtree.Item{Key: 0, Value: math.MaxUint32})
	assert.PanicsWithValue(
		t, "time may not be >= MaxUint32", func() { file.Update(math.MaxUint32, 0, 1, 0) })
}

func TestFileValidate(t *testing.T) {
	keys := [...]int{0}
	vals := [...]int{math.MaxUint32}
	file := NewFileFromTree(keys[:], vals[:], rbtree.NewAllocator())
	file.tree.DeleteWithKey(0)
	file.tree.Insert(rbtree.Item{Key: 1, Value: math.MaxUint32})
	assert.Panics(t, func() { file.Validate() })
	file.tree.DeleteWithKey(1)
	file.tree.Insert(rbtree.Item{Key: 0, Value: math.MaxUint32})
	file.Validate()
	file.tree.DeleteWithKey(0)
	file.tree.Insert(rbtree.Item{Key: 0, Value: 0})
	assert.Panics(t, func() { file.Validate() })
	file.tree.DeleteWithKey(0)
	file.tree.Insert(rbtree.Item{Key: 0, Value: 1})
	file.tree.Insert(rbtree.Item{Key: 1, Value: 1})
	file.tree.Insert(rbtree.Item{Key: 2, Value: math.MaxUint32})
	file.Validate()
	file.tree.FindGE(2).Item().Key = 1
	assert.Panics(t, func() { file.Validate() })
}

func TestFileFlatten(t *testing.T) {
	file, _, _ := fixtureFile()
	// 0 0 | 100 -1                             [0]: 100
	file.Update(1, 20, 30, 0)
	// 0 0 | 20 1 | 50 0 | 130 -1               [0]: 100, [1]: 30
	file.Update(2, 20, 0, 5)
	// 0 0 | 20 1 | 45 0 | 125 -1               [0]: 100, [1]: 25
	file.Update(3, 20, 0, 5)
	// 0 0 | 20 1 | 40 0 | 120 -1               [0]: 100, [1]: 20
	file.Update(4, 20, 10, 0)
	// 0 0 | 20 4 | 30 1 | 50 0 | 130 -1        [0]: 100, [1]: 20, [4]: 10
	lines := file.flatten()
	for i := 0; i < 20; i++ {
		assert.Equal(t, 0, lines[i], fmt.Sprintf("line %d", i))
	}
	for i := 20; i < 30; i++ {
		assert.Equal(t, 4, lines[i], fmt.Sprintf("line %d", i))
	}
	for i := 30; i < 50; i++ {
		assert.Equal(t, 1, lines[i], fmt.Sprintf("line %d", i))
	}
	for i := 50; i < 130; i++ {
		assert.Equal(t, 0, lines[i], fmt.Sprintf("line %d", i))
	}
	assert.Len(t, lines, 130)
}

func TestFileMergeMark(t *testing.T) {
	file, status, _ := fixtureFile()
	// 0 0 | 100 -1                             [0]: 100
	file.Update(1, 20, 30, 0)
	// 0 0 | 20 1 | 50 0 | 130 -1               [0]: 100, [1]: 30
	file.Update(2, 20, 0, 5)
	// 0 0 | 20 1 | 45 0 | 125 -1               [0]: 100, [1]: 25
	file.Update(3, 20, 0, 5)
	// 0 0 | 20 1 | 40 0 | 120 -1               [0]: 100, [1]: 20
	file.Update(4, 20, 10, 0)
	// 0 0 | 20 4 | 30 1 | 50 0 | 130 -1        [0]: 100, [1]: 20, [4]: 10
	file.Update(TreeMergeMark, 60, 20, 20)
	// 0 0 | 20 4 | 30 1 | 50 0 | 60 M | 80 0 | 130 -1
	// [0]: 100, [1]: 20, [4]: 10
	dump := file.Dump()
	assert.Equal(t, "0 0\n20 4\n30 1\n50 0\n60 16383\n80 0\n130 -1\n", dump)
	assert.Contains(t, status, 0)
	assert.Equal(t, int64(100), status[0])
	assert.Equal(t, int64(20), status[1])
	assert.Equal(t, int64(0), status[2])
	assert.Equal(t, int64(0), status[3])
	assert.Equal(t, int64(10), status[4])
	assert.NotContains(t, status, TreeMergeMark)
}

func TestFileMergeShallow(t *testing.T) {
	file1, status, alloc := fixtureFile()
	// 0 0 | 100 -1                             [0]: 100
	file1.Update(1, 20, 30, 0)
	// 0 0 | 20 1 | 50 0 | 130 -1               [0]: 100, [1]: 30
	file1.Update(2, 20, 0, 5)
	// 0 0 | 20 1 | 45 0 | 125 -1               [0]: 100, [1]: 25
	file1.Update(3, 20, 0, 5)
	// 0 0 | 20 1 | 40 0 | 120 -1               [0]: 100, [1]: 20
	file1.Update(4, 20, 10, 0)
	// 0 0 | 20 4 | 30 1 | 50 0 | 130 -1        [0]: 100, [1]: 20, [4]: 10
	file2 := file1.CloneShallow(alloc.Clone())
	file1.Update(TreeMergeMark, 60, 30, 30)
	// 0 0 | 20 4 | 30 1 | 50 0 | 60 M | 90 0 | 130 -1
	// [0]: 70, [1]: 20, [4]: 10
	file2.Update(5, 60, 20, 20)
	// 0 0 | 20 4 | 30 1 | 50 0 | 60 5 | 80 0 | 130 -1
	// [0]: 80, [1]: 20, [4]: 10, [5]: 20
	file2.Update(TreeMergeMark, 80, 10, 10)
	// 0 0 | 20 4 | 30 1 | 50 0 | 60 5 | 80 M | 90 0 | 130 -1
	// [0]: 70, [1]: 20, [4]: 10, [5]: 20
	file2.Update(6, 0, 10, 10)
	// 0 6 | 10 0 | 20 4 | 30 1 | 50 0 | 60 5 | 80 M | 90 0 | 130 -1
	// [0]: 60, [1]: 20, [4]: 10, [5]: 20, [6]: 10
	file1.Merge(7, file2)
	// 0 0 | 20 4 | 30 1 | 50 0 | 60 5 | 80 7 | 90 0 | 130 -1
	// [0]: 70, [1]: 20, [4]: 10, [5]: 20, [6]: 0, [7]: 10
	dump := file1.Dump()
	assert.Equal(t, "0 0\n20 4\n30 1\n50 0\n60 5\n80 7\n90 0\n130 -1\n", dump)
	assert.Equal(t, int64(70), status[0])
	assert.Equal(t, int64(20), status[1])
	assert.Equal(t, int64(0), status[2])
	assert.Equal(t, int64(0), status[3])
	assert.Equal(t, int64(10), status[4])
	assert.Equal(t, int64(20), status[5])
	assert.Equal(t, int64(10), status[6])
	assert.Equal(t, int64(10), status[7])
}

func TestFileMergeDeep(t *testing.T) {
	file1, status, _ := fixtureFile()
	// 0 0 | 100 -1                             [0]: 100
	file1.Update(1, 20, 30, 0)
	// 0 0 | 20 1 | 50 0 | 130 -1               [0]: 100, [1]: 30
	file1.Update(2, 20, 0, 5)
	// 0 0 | 20 1 | 45 0 | 125 -1               [0]: 100, [1]: 25
	file1.Update(3, 20, 0, 5)
	// 0 0 | 20 1 | 40 0 | 120 -1               [0]: 100, [1]: 20
	file1.Update(4, 20, 10, 0)
	// 0 0 | 20 4 | 30 1 | 50 0 | 130 -1        [0]: 100, [1]: 20, [4]: 10
	file2 := file1.CloneDeep(rbtree.NewAllocator())
	file1.Update(TreeMergeMark, 60, 30, 30)
	// 0 0 | 20 4 | 30 1 | 50 0 | 60 M | 90 0 | 130 -1
	// [0]: 70, [1]: 20, [4]: 10
	file2.Update(5, 60, 20, 20)
	// 0 0 | 20 4 | 30 1 | 50 0 | 60 5 | 80 0 | 130 -1
	// [0]: 80, [1]: 20, [4]: 10, [5]: 20
	file2.Update(TreeMergeMark, 80, 10, 10)
	// 0 0 | 20 4 | 30 1 | 50 0 | 60 5 | 80 M | 90 0 | 130 -1
	// [0]: 70, [1]: 20, [4]: 10, [5]: 20
	file2.Update(6, 0, 10, 10)
	// 0 6 | 10 0 | 20 4 | 30 1 | 50 0 | 60 5 | 80 M | 90 0 | 130 -1
	// [0]: 60, [1]: 20, [4]: 10, [5]: 20, [6]: 10
	file1.Merge(7, file2)
	// 0 0 | 20 4 | 30 1 | 50 0 | 60 5 | 80 7 | 90 0 | 130 -1
	// [0]: 70, [1]: 20, [4]: 10, [5]: 20, [6]: 0, [7]: 10
	dump := file1.Dump()
	assert.Equal(t, "0 0\n20 4\n30 1\n50 0\n60 5\n80 7\n90 0\n130 -1\n", dump)
	assert.Equal(t, int64(70), status[0])
	assert.Equal(t, int64(20), status[1])
	assert.Equal(t, int64(0), status[2])
	assert.Equal(t, int64(0), status[3])
	assert.Equal(t, int64(10), status[4])
	assert.Equal(t, int64(20), status[5])
	assert.Equal(t, int64(10), status[6])
	assert.Equal(t, int64(10), status[7])
}

func TestFileMergeNoop(t *testing.T) {
	file1, _, _ := fixtureFile()
	// 0 0 | 100 -1                             [0]: 100
	assert.Panics(t, func() { file1.Merge(3, nil) })
}

func TestFileMergeNil(t *testing.T) {
	file, _, _ := fixtureFile()
	assert.Panics(t, func() {
		file.Merge(1, nil)
	})
}

func TestBug6File(t *testing.T) {
	status := map[int]int64{}
	keys := []int{0, 113, 153, 154}
	vals := []int{7, 10, 7, math.MaxUint32}
	file := NewFileFromTree(keys, vals, rbtree.NewAllocator(), func(a, b, c int) {
		updateStatusFile(status, a, b, c)
	})
	// 0 7 | 113 10 | 153 7 | 154 -1
	file.Update(10, 99, 1, 1)
	// 0 7 | 99 10 | 100 7 | 113 10 | 153 7 | 154 -1
	file.Update(10, 104, 1, 1)
	// 0 7 | 99 10 | 100 7 | 104 10 | 105 7 | 113 10 | 153 7 | 154 -1
	file.Update(10, 106, 1, 1)
	// 0 7 | 99 10 | 100 7 | 104 10 | 105 7 | 106 10 | 107 7 | 113 10 | 153 7 | 154 -1
	file.Update(10, 108, 1, 1)
	// 0 7 | 99 10 | 100 7 | 104 10 | 105 7 | 106 10 | 107 7 | 108 10 | 109 7 | 113 10 | 153 7 | 154 -1
	file.Update(10, 115, 2, 0)
	// 0 7 | 99 10 | 100 7 | 104 10 | 105 7 | 106 10 | 107 7 | 108 10 | 109 7 | 113 10 | 155 7 | 156 -1
	file.Update(10, 125, 4, 2)
	// 0 7 | 99 10 | 100 7 | 104 10 | 105 7 | 106 10 | 107 7 | 108 10 | 109 7 | 113 10 | 157 7 | 158 -1

	dump := file.Dump()
	assert.Equal(t, "0 7\n99 10\n100 7\n104 10\n105 7\n106 10\n107 7\n108 10\n109 7\n113 10\n157 7\n158 -1\n", dump)

	file = NewFileFromTree(keys, vals, rbtree.NewAllocator(), func(a, b, c int) {
		updateStatusFile(status, a, b, c)
	})
	// 0 7 | 113 10 | 153 7 | 154 -1
	file.Update(10, 112, 1, 1)
	// 0 7 | 112 10 | 153 7 | 154 -1
	dump = file.Dump()
	assert.Equal(t, "0 7\n112 10\n153 7\n154 -1\n", dump)
}
