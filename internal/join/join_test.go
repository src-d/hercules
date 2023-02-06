package join

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIdentityJoinLiterals(t *testing.T) {
	pa1 := [...]string{"one|one@one", "two|aaa@two"}
	pa2 := [...]string{"two|aaa@two", "three|one@one"}
	people, merged := LiteralIdentities(pa1[:], pa2[:])
	assert.Len(t, people, 3)
	assert.Len(t, merged, 3)
	assert.Equal(t, people["one|one@one"], JoinedIndex{0, 0, -1})
	assert.Equal(t, people["two|aaa@two"], JoinedIndex{1, 1, 0})
	assert.Equal(t, people["three|one@one"], JoinedIndex{2, -1, 1})
	assert.Equal(t, merged, []string{"one|one@one", "two|aaa@two", "three|one@one"})
	pa1 = [...]string{"two|aaa@two", "one|one@one"}
	people, merged = LiteralIdentities(pa1[:], pa2[:])
	assert.Len(t, people, 3)
	assert.Len(t, merged, 3)
	assert.Equal(t, people["one|one@one"], JoinedIndex{1, 1, -1})
	assert.Equal(t, people["two|aaa@two"], JoinedIndex{0, 0, 0})
	assert.Equal(t, people["three|one@one"], JoinedIndex{2, -1, 1})
	assert.Equal(t, merged, []string{"two|aaa@two", "one|one@one", "three|one@one"})
}

func TestIdentityJoinPeoples(t *testing.T) {
	pa1 := [...]string{"one|one@one", "two|aaa@two"}
	pa2 := [...]string{"two|aaa@two", "three|one@one"}
	people, merged := PeopleIdentities(pa1[:], pa2[:])
	assert.Len(t, people, 3)
	assert.Len(t, merged, 2)
	assert.Equal(t, people["one|one@one"], JoinedIndex{0, 0, -1})
	assert.Equal(t, people["two|aaa@two"], JoinedIndex{1, 1, 0})
	assert.Equal(t, people["three|one@one"], JoinedIndex{0, -1, 1})
	assert.Equal(t, merged, []string{"one|three|one@one", "two|aaa@two"})
}

func TestIdentityJoinReversedDictsIdentitiesStrikeBack(t *testing.T) {
	pa1 := [...]string{"one|one@one", "two|aaa@two", "three|three@three"}
	pa2 := [...]string{"two|aaa@two", "three|one@one"}
	people, merged := PeopleIdentities(pa1[:], pa2[:])
	assert.Len(t, people, 4)
	assert.Len(t, merged, 2)
	assert.Equal(t, people["one|one@one"], JoinedIndex{0, 0, -1})
	assert.Equal(t, people["two|aaa@two"], JoinedIndex{1, 1, 0})
	assert.Equal(t, people["three|one@one"], JoinedIndex{0, -1, 1})
	assert.Equal(t, people["three|three@three"], JoinedIndex{0, 2, -1})
	assert.Equal(t, merged, []string{"one|three|one@one|three@three", "two|aaa@two"})

	pa1 = [...]string{"one|one@one", "two|aaa@two", "three|aaa@two"}
	people, merged = PeopleIdentities(pa1[:], pa2[:])
	assert.Len(t, people, 4)
	assert.Len(t, merged, 1)
	assert.Equal(t, people["one|one@one"], JoinedIndex{0, 0, -1})
	assert.Equal(t, people["two|aaa@two"], JoinedIndex{0, 1, 0})
	assert.Equal(t, people["three|one@one"], JoinedIndex{0, -1, 1})
	assert.Equal(t, people["three|aaa@two"], JoinedIndex{0, 2, -1})
	assert.Equal(t, merged, []string{"one|three|two|aaa@two|one@one"})
}
