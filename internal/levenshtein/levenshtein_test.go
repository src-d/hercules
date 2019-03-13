// Copyright (c) 2015, Arbo von Monkiewitsch All rights reserved.
// Use of this source code is governed by a BSD-style
// license.

package levenshtein

import (
	"fmt"
	"testing"
)

var distanceTests = []struct {
	first  string
	second string
	wanted int
}{
	{"a", "a", 0},
	{"ab", "ab", 0},
	{"ab", "aa", 1},
	{"ab", "aa", 1},
	{"ab", "aaa", 2},
	{"bbb", "a", 3},
	{"kitten", "sitting", 3},
	{"a", "", 1},
	{"", "a", 1},
	{"aa", "aü", 1},
	{"Fön", "Föm", 1},
}

func TestDistance(t *testing.T) {

	lev := &Context{}

	for index, distanceTest := range distanceTests {
		result := lev.Distance(distanceTest.first, distanceTest.second)
		if result != distanceTest.wanted {
			output := fmt.Sprintf("%v \t distance of %v and %v should be %v but was %v.",
				index, distanceTest.first, distanceTest.second, distanceTest.wanted, result)
			t.Errorf(output)
		}
	}
}

func BenchmarkDistance(b *testing.B) {
	s1 := "frederick"
	s2 := "fredelstick"
	total := 0

	b.ReportAllocs()
	b.ResetTimer()

	c := &Context{}

	for i := 0; i < b.N; i++ {
		total += c.Distance(s1, s2)
	}

	if total == 0 {
		b.Logf("total is %d", total)
	}
}

func BenchmarkDistanceOriginal(b *testing.B) {
	s1 := "frederick"
	s2 := "fredelstick"
	total := 0

	b.ReportAllocs()
	b.ResetTimer()

	ctx := Context{}
	for i := 0; i < b.N; i++ {
		total += ctx.Distance(s1, s2)
	}

	if total == 0 {
		b.Logf("total is %d", total)
	}
}
