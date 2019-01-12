package rbtree

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Create a tree storing a set of integers
func testNewIntSet() *RBTree {
	return NewRBTree(NewAllocator())
}

func testAssert(t *testing.T, b bool, message string) {
	assert.True(t, b, message)
}

func boolInsert(tree *RBTree, item int) bool {
	status, _ := tree.Insert(Item{uint32(item), uint32(item)})
	return status
}

func TestEmpty(t *testing.T) {
	tree := testNewIntSet()
	testAssert(t, tree.Len() == 0, "len!=0")
	testAssert(t, tree.Max().NegativeLimit(), "neglimit")
	testAssert(t, tree.Min().Limit(), "limit")
	testAssert(t, tree.FindGE(10).Limit(), "Not empty")
	testAssert(t, tree.FindLE(10).NegativeLimit(), "Not empty")
	testAssert(t, tree.Get(10) == nil, "Not empty")
	testAssert(t, tree.Limit().Equal(tree.Min()), "iter")
}

func TestFindGE(t *testing.T) {
	tree := testNewIntSet()
	testAssert(t, boolInsert(tree, 10), "Insert1")
	testAssert(t, !boolInsert(tree, 10), "Insert2")
	testAssert(t, tree.Len() == 1, "len==1")
	testAssert(t, tree.FindGE(10).Item().Key == 10, "FindGE 10")
	testAssert(t, tree.FindGE(11).Limit(), "FindGE 11")
	assert.Equal(t, tree.FindGE(9).Item().Key, uint32(10), "FindGE 10")
}

func TestFindLE(t *testing.T) {
	tree := testNewIntSet()
	testAssert(t, boolInsert(tree, 10), "insert1")
	testAssert(t, tree.FindLE(10).Item().Key == 10, "FindLE 10")
	testAssert(t, tree.FindLE(11).Item().Key == 10, "FindLE 11")
	testAssert(t, tree.FindLE(9).NegativeLimit(), "FindLE 9")
}

func TestGet(t *testing.T) {
	tree := testNewIntSet()
	testAssert(t, boolInsert(tree, 10), "insert1")
	assert.Equal(t, *tree.Get(10), uint32(10), "Get 10")
	testAssert(t, tree.Get(9) == nil, "Get 9")
	testAssert(t, tree.Get(11) == nil, "Get 11")
}

func TestDelete(t *testing.T) {
	tree := testNewIntSet()
	testAssert(t, !tree.DeleteWithKey(10), "del")
	testAssert(t, tree.Len() == 0, "dellen")
	testAssert(t, boolInsert(tree, 10), "ins")
	testAssert(t, tree.DeleteWithKey(10), "del")
	testAssert(t, tree.Len() == 0, "dellen")

	// delete was deleting after the request if request not found
	// ensure this does not regress:
	testAssert(t, boolInsert(tree, 10), "ins")
	testAssert(t, !tree.DeleteWithKey(9), "del")
	testAssert(t, tree.Len() == 1, "dellen")

}

func iterToString(i Iterator) string {
	s := ""
	for ; !i.Limit(); i = i.Next() {
		if s != "" {
			s = s + ","
		}
		s = s + fmt.Sprintf("%d", i.Item().Key)
	}
	return s
}

func reverseIterToString(i Iterator) string {
	s := ""
	for ; !i.NegativeLimit(); i = i.Prev() {
		if s != "" {
			s = s + ","
		}
		s = s + fmt.Sprintf("%d", i.Item().Key)
	}
	return s
}

func TestIterator(t *testing.T) {
	tree := testNewIntSet()
	for i := 0; i < 10; i = i + 2 {
		boolInsert(tree, i)
	}
	assert.Equal(t, iterToString(tree.FindGE(3)), "4,6,8")
	assert.Equal(t, iterToString(tree.FindGE(4)), "4,6,8")
	assert.Equal(t, iterToString(tree.FindGE(8)), "8")
	assert.Equal(t, iterToString(tree.FindGE(9)), "")
	assert.Equal(t, reverseIterToString(tree.FindLE(3)), "2,0")
	assert.Equal(t, reverseIterToString(tree.FindLE(2)), "2,0")
	assert.Equal(t, reverseIterToString(tree.FindLE(0)), "0")
}

//
// Randomized tests
//

// oracle stores provides an interface similar to rbtree, but stores
// data in an sorted array
type oracle struct {
	data []int
}

func newOracle() *oracle {
	return &oracle{data: make([]int, 0)}
}

func (o *oracle) Len() int {
	return len(o.data)
}

// interface needed for sorting
func (o *oracle) Less(i, j int) bool {
	return o.data[i] < o.data[j]
}

func (o *oracle) Swap(i, j int) {
	e := o.data[j]
	o.data[j] = o.data[i]
	o.data[i] = e
}

func (o *oracle) Insert(key int) bool {
	for _, e := range o.data {
		if e == key {
			return false
		}
	}

	n := len(o.data) + 1
	newData := make([]int, n)
	copy(newData, o.data)
	newData[n-1] = key
	o.data = newData
	sort.Sort(o)
	return true
}

func (o *oracle) RandomExistingKey(rand *rand.Rand) int {
	index := rand.Int31n(int32(len(o.data)))
	return o.data[index]
}

func (o *oracle) FindGE(t *testing.T, key int) oracleIterator {
	prev := int(-1)
	for i, e := range o.data {
		if e <= prev {
			t.Fatal("Nonsorted oracle ", e, prev)
		}
		if e >= key {
			return oracleIterator{o: o, index: i}
		}
	}
	return oracleIterator{o: o, index: len(o.data)}
}

func (o *oracle) FindLE(t *testing.T, key int) oracleIterator {
	iter := o.FindGE(t, key)
	if !iter.Limit() && o.data[iter.index] == key {
		return iter
	}
	return oracleIterator{o, iter.index - 1}
}

func (o *oracle) Delete(key int) bool {
	for i, e := range o.data {
		if e == key {
			newData := make([]int, len(o.data)-1)
			copy(newData, o.data[0:i])
			copy(newData[i:], o.data[i+1:])
			o.data = newData
			return true
		}
	}
	return false
}

//
// Test iterator
//
type oracleIterator struct {
	o     *oracle
	index int
}

func (oiter oracleIterator) Limit() bool {
	return oiter.index >= len(oiter.o.data)
}

func (oiter oracleIterator) Min() bool {
	return oiter.index == 0
}

func (oiter oracleIterator) NegativeLimit() bool {
	return oiter.index < 0
}

func (oiter oracleIterator) Max() bool {
	return oiter.index == len(oiter.o.data)-1
}

func (oiter oracleIterator) Item() int {
	return oiter.o.data[oiter.index]
}

func (oiter oracleIterator) Next() oracleIterator {
	return oracleIterator{oiter.o, oiter.index + 1}
}

func (oiter oracleIterator) Prev() oracleIterator {
	return oracleIterator{oiter.o, oiter.index - 1}
}

func compareContents(t *testing.T, oiter oracleIterator, titer Iterator) {
	oi := oiter
	ti := titer

	// Test forward iteration
	testAssert(t, oi.NegativeLimit() == ti.NegativeLimit(), "rend")
	if oi.NegativeLimit() {
		oi = oi.Next()
		ti = ti.Next()
	}

	for !oi.Limit() && !ti.Limit() {
		// log.Print("Item: ", oi.Item(), ti.Item())
		if ti.Item().Key != uint32(oi.Item()) {
			t.Fatal("Wrong item", ti.Item(), oi.Item())
		}
		oi = oi.Next()
		ti = ti.Next()
	}
	if !ti.Limit() {
		t.Fatal("!ti.done", ti.Item())
	}
	if !oi.Limit() {
		t.Fatal("!oi.done", oi.Item())
	}

	// Test reverse iteration
	oi = oiter
	ti = titer
	testAssert(t, oi.Limit() == ti.Limit(), "end")
	if oi.Limit() {
		oi = oi.Prev()
		ti = ti.Prev()
	}

	for !oi.NegativeLimit() && !ti.NegativeLimit() {
		if ti.Item().Key != uint32(oi.Item()) {
			t.Fatal("Wrong item", ti.Item(), oi.Item())
		}
		oi = oi.Prev()
		ti = ti.Prev()
	}
	if !ti.NegativeLimit() {
		t.Fatal("!ti.done", ti.Item())
	}
	if !oi.NegativeLimit() {
		t.Fatal("!oi.done", oi.Item())
	}
}

func compareContentsFull(t *testing.T, o *oracle, tree *RBTree) {
	compareContents(t, o.FindGE(t, -1), tree.FindGE(0))
}

func TestRandomized(t *testing.T) {
	const numKeys = 1000

	o := newOracle()
	tree := testNewIntSet()
	r := rand.New(rand.NewSource(0))
	for i := 0; i < 10000; i++ {
		op := r.Int31n(100)
		if op < 50 {
			key := r.Int31n(numKeys)
			o.Insert(int(key))
			boolInsert(tree, int(key))
			compareContentsFull(t, o, tree)
		} else if op < 90 && o.Len() > 0 {
			key := o.RandomExistingKey(r)
			o.Delete(key)
			if !tree.DeleteWithKey(uint32(key)) {
				t.Fatal("DeleteExisting", key)
			}
			compareContentsFull(t, o, tree)
		} else if op < 95 {
			key := int(r.Int31n(numKeys))
			compareContents(t, o.FindGE(t, key), tree.FindGE(uint32(key)))
		} else {
			key := int(r.Int31n(numKeys))
			compareContents(t, o.FindLE(t, key), tree.FindLE(uint32(key)))
		}
	}
}

func TestAllocatorFreeZero(t *testing.T) {
	alloc := NewAllocator()
	alloc.malloc()
	assert.Panics(t, func() { alloc.free(0) })
}

func TestCloneShallow(t *testing.T) {
	alloc1 := NewAllocator()
	alloc1.malloc()
	tree := NewRBTree(alloc1)
	tree.Insert(Item{7, 7})
	tree.Insert(Item{8, 8})
	tree.DeleteWithKey(8)
	assert.Equal(t, alloc1.storage, []node{{}, {}, {color: black, item: Item{7, 7}}, {}})
	assert.Equal(t, tree.minNode, uint32(2))
	assert.Equal(t, tree.maxNode, uint32(2))
	alloc2 := alloc1.Clone()
	clone := tree.CloneShallow(alloc2)
	assert.Equal(t, alloc2.storage, []node{{}, {}, {color: black, item: Item{7, 7}}, {}})
	assert.Equal(t, clone.minNode, uint32(2))
	assert.Equal(t, clone.maxNode, uint32(2))
	assert.Equal(t, alloc2.Size(), 4)
	tree.Insert(Item{10, 10})
	alloc3 := alloc1.Clone()
	clone = tree.CloneShallow(alloc3)
	assert.Equal(t, alloc3.storage, []node{
		{}, {},
		{right: 3, color: black, item: Item{7, 7}},
		{parent: 2, color: red, item: Item{10, 10}}})
	assert.Equal(t, clone.minNode, uint32(2))
	assert.Equal(t, clone.maxNode, uint32(3))
	assert.Equal(t, alloc3.Size(), 4)
	assert.Equal(t, alloc2.Size(), 4)
}

func TestCloneDeep(t *testing.T) {
	alloc1 := NewAllocator()
	alloc1.malloc()
	tree := NewRBTree(alloc1)
	tree.Insert(Item{7, 7})
	assert.Equal(t, alloc1.storage, []node{{}, {}, {color: black, item: Item{7, 7}}})
	assert.Equal(t, tree.minNode, uint32(2))
	assert.Equal(t, tree.maxNode, uint32(2))
	alloc2 := NewAllocator()
	clone := tree.CloneDeep(alloc2)
	assert.Equal(t, alloc2.storage, []node{{}, {color: black, item: Item{7, 7}}})
	assert.Equal(t, clone.minNode, uint32(1))
	assert.Equal(t, clone.maxNode, uint32(1))
	assert.Equal(t, alloc2.Size(), 2)
	tree.Insert(Item{10, 10})
	alloc2 = NewAllocator()
	clone = tree.CloneDeep(alloc2)
	assert.Equal(t, alloc2.storage, []node{
		{},
		{right: 2, color: black, item: Item{7, 7}},
		{parent: 1, color: red, item: Item{10, 10}}})
	assert.Equal(t, clone.minNode, uint32(1))
	assert.Equal(t, clone.maxNode, uint32(2))
	assert.Equal(t, alloc2.Size(), 3)
}

func TestErase(t *testing.T) {
	alloc := NewAllocator()
	tree := NewRBTree(alloc)
	for i := 0; i < 10; i++ {
		tree.Insert(Item{uint32(i), uint32(i)})
	}
	assert.Equal(t, alloc.Used(), 11)
	tree.Erase()
	assert.Equal(t, alloc.Used(), 1)
	assert.Equal(t, alloc.Size(), 11)
}

func TestAllocatorHibernateBoot(t *testing.T) {
	alloc := NewAllocator()
	for i := 0; i < 10000; i++ {
		n := alloc.malloc()
		alloc.storage[n].item.Key = uint32(i)
		alloc.storage[n].item.Value = uint32(i)
		alloc.storage[n].left = uint32(i)
		alloc.storage[n].right = uint32(i)
		alloc.storage[n].parent = uint32(i)
		alloc.storage[n].color = i%2 == 0
	}
	for i := 0; i < 10000; i++ {
		alloc.gaps[uint32(i)] = true // makes no sense, only to test
	}
	alloc.Hibernate()
	assert.PanicsWithValue(t, "cannot hibernate an already hibernated Allocator", alloc.Hibernate)
	assert.Nil(t, alloc.storage)
	assert.Nil(t, alloc.gaps)
	assert.Equal(t, alloc.Size(), 0)
	assert.Equal(t, alloc.hibernatedStorageLen, 10001)
	assert.Equal(t, alloc.hibernatedGapsLen, 10000)
	assert.PanicsWithValue(t, "hibernated allocators cannot be used", func() { alloc.Used() })
	assert.PanicsWithValue(t, "hibernated allocators cannot be used", func() { alloc.malloc() })
	assert.PanicsWithValue(t, "hibernated allocators cannot be used", func() { alloc.free(0) })
	assert.PanicsWithValue(t, "cannot clone a hibernated allocator", func() { alloc.Clone() })
	alloc.Boot()
	assert.Equal(t, alloc.hibernatedStorageLen, 0)
	assert.Equal(t, alloc.hibernatedGapsLen, 0)
	for n := 1; n <= 10000; n++ {
		assert.Equal(t, alloc.storage[n].item.Key, uint32(n-1))
		assert.Equal(t, alloc.storage[n].item.Value, uint32(n-1))
		assert.Equal(t, alloc.storage[n].left, uint32(n-1))
		assert.Equal(t, alloc.storage[n].right, uint32(n-1))
		assert.Equal(t, alloc.storage[n].parent, uint32(n-1))
		assert.Equal(t, alloc.storage[n].color, (n-1)%2 == 0)
		assert.True(t, alloc.gaps[uint32(n-1)])
	}
}

func TestAllocatorHibernateBootEmpty(t *testing.T) {
	alloc := NewAllocator()
	alloc.Hibernate()
	alloc.Boot()
	assert.NotNil(t, alloc.gaps)
	assert.Equal(t, alloc.Size(), 0)
	assert.Equal(t, alloc.Used(), 0)
}

func TestAllocatorHibernateBootThreshold(t *testing.T) {
	alloc := NewAllocator()
	alloc.malloc()
	alloc.HibernationThreshold = 3
	assert.Equal(t, 3, alloc.Clone().HibernationThreshold)
	alloc.Hibernate()
	assert.Equal(t, alloc.hibernatedStorageLen, 0)
	alloc.Boot()
	alloc.malloc()
	alloc.Hibernate()
	assert.Equal(t, alloc.hibernatedGapsLen, 0)
	assert.Equal(t, alloc.hibernatedStorageLen, 3)
	alloc.Boot()
	assert.Equal(t, alloc.Size(), 3)
	assert.Equal(t, alloc.Used(), 3)
	assert.NotNil(t, alloc.gaps)
}

func TestAllocatorSerializeDeserialize(t *testing.T) {
	alloc := NewAllocator()
	for i := 0; i < 10000; i++ {
		n := alloc.malloc()
		alloc.storage[n].item.Key = uint32(i)
		alloc.storage[n].item.Value = uint32(i)
		alloc.storage[n].left = uint32(i)
		alloc.storage[n].right = uint32(i)
		alloc.storage[n].parent = uint32(i)
		alloc.storage[n].color = i%2 == 0
	}
	for i := 0; i < 10000; i++ {
		alloc.gaps[uint32(i)] = true // makes no sense, only to test
	}
	assert.PanicsWithValue(t, "serialization requires the hibernated state",
		func() { alloc.Serialize("...") })
	assert.PanicsWithValue(t, "deserialization requires the hibernated state",
		func() { alloc.Deserialize("...") })
	alloc.Hibernate()
	file, err := ioutil.TempFile("", "")
	assert.Nil(t, err)
	name := file.Name()
	defer os.Remove(name)
	assert.Nil(t, file.Close())
	assert.NotNil(t, alloc.Serialize("/tmp/xxx/yyy"))
	assert.Nil(t, alloc.Serialize(name))
	assert.Nil(t, alloc.storage)
	assert.Nil(t, alloc.gaps)
	for _, d := range alloc.hibernatedData {
		assert.Nil(t, d)
	}
	assert.Equal(t, alloc.hibernatedStorageLen, 10001)
	assert.Equal(t, alloc.hibernatedGapsLen, 10000)
	assert.PanicsWithValue(t, "cannot boot a serialized Allocator", alloc.Boot)
	assert.NotNil(t, alloc.Deserialize("/tmp/xxx/yyy"))
	assert.Nil(t, alloc.Deserialize(name))
	for _, d := range alloc.hibernatedData {
		assert.True(t, len(d) > 0)
	}
	alloc.Boot()
	assert.Equal(t, alloc.hibernatedStorageLen, 0)
	assert.Equal(t, alloc.hibernatedGapsLen, 0)
	for _, d := range alloc.hibernatedData {
		assert.Nil(t, d)
	}
	for n := 1; n <= 10000; n++ {
		assert.Equal(t, alloc.storage[n].item.Key, uint32(n-1))
		assert.Equal(t, alloc.storage[n].item.Value, uint32(n-1))
		assert.Equal(t, alloc.storage[n].left, uint32(n-1))
		assert.Equal(t, alloc.storage[n].right, uint32(n-1))
		assert.Equal(t, alloc.storage[n].parent, uint32(n-1))
		assert.Equal(t, alloc.storage[n].color, (n-1)%2 == 0)
		assert.True(t, alloc.gaps[uint32(n-1)])
	}
	alloc.Hibernate()
	assert.Nil(t, os.Truncate(name, 100))
	assert.NotNil(t, alloc.Deserialize(name))
	assert.Nil(t, os.Truncate(name, 4))
	assert.NotNil(t, alloc.Deserialize(name))
	assert.Nil(t, os.Truncate(name, 0))
	assert.NotNil(t, alloc.Deserialize(name))
}
