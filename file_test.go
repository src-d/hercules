package hercules

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func fixture() (*File, map[int]int64) {
	status := map[int]int64{}
	file := NewFile(0, 100, status)
	return file, status
}

func TestInitialize(t *testing.T) {
	file, status := fixture()
	dump := file.Dump()
	// Output:
	// 0 0
	// 100 -1
	assert.Equal(t, "0 0\n100 -1\n", dump)
	assert.Equal(t, int64(100), status[0])
}

func testPanic(t *testing.T, method func(*File), msg string) {
	defer func() {
		r := recover()
		assert.NotNil(t, r, "not panic()-ed")
		assert.IsType(t, "", r)
		assert.Contains(t, r.(string), msg)
	}()
	file, _ := fixture()
	method(file)
}

func TestBullshit(t *testing.T) {
	testPanic(t, func(file *File) { file.Update(1, -10, 10, 0) }, "insert")
	testPanic(t, func(file *File) { file.Update(1, 110, 10, 0) }, "insert")
	testPanic(t, func(file *File) { file.Update(1, -10, 0, 10) }, "delete")
	testPanic(t, func(file *File) { file.Update(1, 100, 0, 10) }, "delete")
	testPanic(t, func(file *File) { file.Update(1, 0, -10, 0) }, "length")
	testPanic(t, func(file *File) { file.Update(1, 0, 0, -10) }, "length")
	testPanic(t, func(file *File) { file.Update(1, 0, -10, -10) }, "length")
	testPanic(t, func(file *File) { file.Update(-1, 0, 10, 10) }, "time")
	file, status := fixture()
	file.Update(1, 10, 0, 0)
	assert.Equal(t, int64(100), status[0])
	assert.Equal(t, int64(0), status[1])
}

func TestLen(t *testing.T) {
	file, _ := fixture()
	assert.Equal(t, 100, file.Len())
}

func TestInsert(t *testing.T) {
	file, status := fixture()
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

func TestZeroInitialize(t *testing.T) {
	status := map[int]int64{}
	file := NewFile(0, 0, status)
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

func TestDelete(t *testing.T) {
	file, status := fixture()
	file.Update(1, 10, 0, 10)
	dump := file.Dump()
	// Output:
	// 0 0
	// 90 -1
	assert.Equal(t, "0 0\n90 -1\n", dump)
	assert.Equal(t, int64(90), status[0])
	assert.Equal(t, int64(0), status[1])
}

func TestFused(t *testing.T) {
	file, status := fixture()
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

func TestInsertSameStart(t *testing.T) {
	file, status := fixture()
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

func TestInsertEnd(t *testing.T) {
	file, status := fixture()
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

func TestDeleteSameStart0(t *testing.T) {
	file, status := fixture()
	file.Update(1, 0, 0, 10)
	dump := file.Dump()
	// Output:
	// 0 0
	// 90 -1
	assert.Equal(t, "0 0\n90 -1\n", dump)
	assert.Equal(t, int64(90), status[0])
	assert.Equal(t, int64(0), status[1])
}

func TestDeleteSameStartMiddle(t *testing.T) {
	file, status := fixture()
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

func TestDeleteIntersection(t *testing.T) {
	file, status := fixture()
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

func TestDeleteAll(t *testing.T) {
	file, status := fixture()
	file.Update(1, 0, 0, 100)
	// Output:
	// 0 -1
	dump := file.Dump()
	assert.Equal(t, "0 -1\n", dump)
	assert.Equal(t, int64(0), status[0])
	assert.Equal(t, int64(0), status[1])
}

func TestFusedIntersection(t *testing.T) {
	file, status := fixture()
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

func TestTorture(t *testing.T) {
	file, status := fixture()
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
