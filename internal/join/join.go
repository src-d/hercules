package join

import (
	"sort"
	"strings"
)

// JoinedIndex is the result of merging `rd1[First]` and `rd2[Second]`: the index in the final reversed
// dictionary. -1 for `First` or `Second` means that the corresponding string does not exist
// in respectively `rd1` and `rd2`.
// See also:
// * LiteralIdentities()
// * PeopleIdentities()
type JoinedIndex struct {
	Final  int
	First  int
	Second int
}

// LiteralIdentities joins two string lists together, excluding duplicates, in-order.
// The string comparisons are the usual ones.
// The returned mapping's keys are the unique strings in `rd1 ∪ rd2`, and the values are:
// 1. Index after merging.
// 2. Corresponding index in the first array - `rd1`. -1 means that it does not exist.
// 3. Corresponding index in the second array - `rd2`. -1 means that it does not exist.
func LiteralIdentities(rd1, rd2 []string) (map[string]JoinedIndex, []string) {

	people := map[string]JoinedIndex{}
	for i, pid := range rd1 {
		people[pid] = JoinedIndex{len(people), i, -1}
	}
	for i, pid := range rd2 {
		if ptrs, exists := people[pid]; !exists {
			people[pid] = JoinedIndex{len(people), -1, i}
		} else {
			people[pid] = JoinedIndex{ptrs.Final, ptrs.First, i}
		}
	}
	mrd := make([]string, len(people))
	for name, ptrs := range people {
		mrd[ptrs.Final] = name
	}
	return people, mrd
}

type identityPair struct {
	Index1 int
	Index2 int
}

// PeopleIdentities joins two identity lists together, excluding duplicates.
// The strings are split by "|" and we find the connected components..
// The returned mapping's keys are the unique strings in `rd1 ∪ rd2`, and the values are:
// 1. Index after merging.
// 2. Corresponding index in the first array - `rd1`. -1 means that it does not exist.
// 3. Corresponding index in the second array - `rd2`. -1 means that it does not exist.
func PeopleIdentities(rd1, rd2 []string) (map[string]JoinedIndex, []string) {

	vocabulary := map[string]identityPair{}
	vertices1 := make([][]string, len(rd1))
	for i, s := range rd1 {
		parts := strings.Split(s, "|")
		vertices1[i] = parts
		for _, p := range parts {
			vocabulary[p] = identityPair{i, -1}
		}
	}
	vertices2 := make([][]string, len(rd2))
	for i, s := range rd2 {
		parts := strings.Split(s, "|")
		vertices2[i] = parts
		for _, p := range parts {
			if ip, exists := vocabulary[p]; !exists {
				vocabulary[p] = identityPair{-1, i}
			} else {
				ip.Index2 = i
				vocabulary[p] = ip
			}
		}
	}

	// find the connected components by walking the graph
	var walks []map[string]bool
	visited := map[string]bool{}

	walkFromVertex := func(root []string) {
		walk := map[string]bool{}
		pending := map[string]bool{}
		for _, p := range root {
			pending[p] = true
		}
		for len(pending) > 0 {
			var element string
			for e := range pending {
				element = e
				delete(pending, e)
				break
			}
			if !walk[element] {
				walk[element] = true
				ip := vocabulary[element]
				if ip.Index1 >= 0 {
					for _, p := range vertices1[ip.Index1] {
						if !walk[p] {
							pending[p] = true
						}
					}
				}
				if ip.Index2 >= 0 {
					for _, p := range vertices2[ip.Index2] {
						if !walk[p] {
							pending[p] = true
						}
					}
				}
			}
		}
		for e := range walk {
			visited[e] = true
		}
		walks = append(walks, walk)
	}

	for i1 := range rd1 {
		var skip bool
		for _, p := range vertices1[i1] {
			if visited[p] {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		walkFromVertex(vertices1[i1])
	}
	for i2 := range rd2 {
		var skip bool
		for _, p := range vertices2[i2] {
			if visited[p] {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		walkFromVertex(vertices2[i2])
	}

	mergedStrings := make([]string, 0, len(walks))
	mergedIndex := map[string]JoinedIndex{}
	// convert each walk from strings to indexes
	for walkIndex, walk := range walks {
		ids := make([]string, 0, len(walk))
		for key := range walk {
			ids = append(ids, key)
		}
		// place emails after names
		sort.Slice(ids, func(i, j int) bool {
			iid := ids[i]
			jid := ids[j]
			iHasAt := strings.ContainsRune(iid, '@')
			jHasAt := strings.ContainsRune(jid, '@')
			if iHasAt == jHasAt {
				return iid < jid
			}
			return jHasAt
		})
		mergedStrings = append(mergedStrings, strings.Join(ids, "|"))
		for _, key := range ids {
			ipair := vocabulary[key]
			if ipair.Index1 >= 0 {
				s1 := rd1[ipair.Index1]
				if mi, exists := mergedIndex[s1]; !exists {
					mergedIndex[s1] = JoinedIndex{walkIndex, ipair.Index1, -1}
				} else {
					mergedIndex[s1] = JoinedIndex{walkIndex, ipair.Index1, mi.Second}
				}
			}
			if ipair.Index2 >= 0 {
				s2 := rd2[ipair.Index2]
				if mi, exists := mergedIndex[s2]; !exists {
					mergedIndex[s2] = JoinedIndex{walkIndex, -1, ipair.Index2}
				} else {
					mergedIndex[s2] = JoinedIndex{walkIndex, mi.First, ipair.Index2}
				}
			}
		}
	}
	return mergedIndex, mergedStrings
}
