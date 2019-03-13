// Copyright (c) 2015, Arbo von Monkiewitsch All rights reserved.
// Use of this source code is governed by a BSD-style
// license.

package levenshtein

// Context is the object which allows to calculate the Levenshtein distance
// with Distance() method. It is needed to ensure 0 memory allocations.
type Context struct {
	intSlice []int
}

func (c *Context) getIntSlice(l int) []int {
	if cap(c.intSlice) < l {
		c.intSlice = make([]int, l)
	}
	return c.intSlice[:l]
}

// Distance calculates the Levenshtein distance between two strings which
// is defined as the minimum number of edits needed to transform one string
// into the other, with the allowable edit operations being insertion, deletion,
// or substitution of a single character
// http://en.wikipedia.org/wiki/Levenshtein_distance
//
// This implementation is optimized to use O(min(m,n)) space.
// It is based on the optimized C version found here:
// http://en.wikibooks.org/wiki/Algorithm_implementation/Strings/Levenshtein_distance#C
func (c *Context) Distance(str1, str2 string) int {
	s1 := []rune(str1)
	s2 := []rune(str2)

	lenS1 := len(s1)
	lenS2 := len(s2)

	if lenS2 == 0 {
		return lenS1
	}

	column := c.getIntSlice(lenS1 + 1)
	// Column[0] will be initialised at the start of the first loop before it
	// is read, unless lenS2 is zero, which we deal with above
	for i := 1; i <= lenS1; i++ {
		column[i] = i
	}

	for x := 0; x < lenS2; x++ {
		s2Rune := s2[x]
		column[0] = x + 1
		lastdiag := x

		for y := 0; y < lenS1; y++ {
			olddiag := column[y+1]
			cost := 0
			if s1[y] != s2Rune {
				cost = 1
			}
			column[y+1] = min(
				column[y+1]+1,
				column[y]+1,
				lastdiag+cost,
			)
			lastdiag = olddiag
		}
	}

	return column[lenS1]
}

func min(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
	} else {
		if b < c {
			return b
		}
	}
	return c
}
