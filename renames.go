package hercules

import (
	"sort"
	"unicode/utf8"

	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
)

type RenameAnalysis struct {
	// SimilarityThreshold adjusts the heuristic to determine file renames.
	// It has the same units as cgit's -X rename-threshold or -M. Better to
	// set it to the default value of 90 (90%).
	SimilarityThreshold int

	repository *git.Repository
}

func (ra *RenameAnalysis) Name() string {
	return "RenameAnalysis"
}

func (ra *RenameAnalysis) Provides() []string {
	arr := [...]string{"renamed_changes"}
	return arr[:]
}

func (ra *RenameAnalysis) Requires() []string {
	arr := [...]string{"blob_cache", "changes"}
	return arr[:]
}

func (ra *RenameAnalysis) Initialize(repository *git.Repository) {
	if ra.SimilarityThreshold < 0 || ra.SimilarityThreshold > 100 {
		panic("hercules.RenameAnalysis: an invalid SimilarityThreshold was specified")
	}
	ra.repository = repository
}

func (ra *RenameAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	changes := deps["changes"].(object.Changes)
	cache := deps["blob_cache"].(map[plumbing.Hash]*object.Blob)

	reduced_changes := make(object.Changes, 0, changes.Len())

	// Stage 1 - find renames by matching the hashes
	// n log(n)
	// We sort additions and deletions by hash and then do the single scan along
	// both slices.
	deleted := make(sortableChanges, 0, changes.Len())
	added := make(sortableChanges, 0, changes.Len())
	for _, change := range changes {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		switch action {
		case merkletrie.Insert:
			added = append(added, sortableChange{change, change.To.TreeEntry.Hash})
		case merkletrie.Delete:
			deleted = append(deleted, sortableChange{change, change.From.TreeEntry.Hash})
		case merkletrie.Modify:
			reduced_changes = append(reduced_changes, change)
		}
	}
	sort.Sort(deleted)
	sort.Sort(added)
	a := 0
	d := 0
	still_deleted := make(object.Changes, 0, deleted.Len())
	still_added := make(object.Changes, 0, added.Len())
	for a < added.Len() && d < deleted.Len() {
		if added[a].hash == deleted[d].hash {
			reduced_changes = append(
				reduced_changes,
				&object.Change{From: deleted[d].change.From, To: added[a].change.To})
			a++; d++
		} else if added[a].Less(&deleted[d]) {
			still_added = append(still_added, added[a].change)
			a++
		} else {
			still_deleted = append(still_deleted, deleted[d].change)
			d++
		}
	}
	for ; a < added.Len(); a++ {
		still_added = append(still_added, added[a].change)
	}
	for ; d < deleted.Len(); d++ {
		still_deleted = append(still_deleted, deleted[d].change)
	}

	// Stage 2 - apply the similarity threshold
	// n^2 but actually linear
	// We sort the blobs by size and do the single linear scan.
	added_blobs := make(sortableBlobs, 0, still_added.Len())
	deleted_blobs := make(sortableBlobs, 0, still_deleted.Len())
	for _, change := range still_added {
		blob := cache[change.To.TreeEntry.Hash]
		added_blobs = append(
			added_blobs, sortableBlob{change: change, size: blob.Size})
	}
	for _, change := range still_deleted {
		blob := cache[change.From.TreeEntry.Hash]
		deleted_blobs = append(
			deleted_blobs, sortableBlob{change: change, size: blob.Size})
	}
	sort.Sort(added_blobs)
	sort.Sort(deleted_blobs)
	d_start := 0
	for a = 0; a < added_blobs.Len(); a++ {
		my_blob := cache[added_blobs[a].change.To.TreeEntry.Hash]
		my_size := added_blobs[a].size
		for d = d_start; d < deleted_blobs.Len() && !ra.sizesAreClose(my_size, deleted_blobs[d].size); d++ {
		}
		d_start = d
		found_match := false
		for d = d_start; d < deleted_blobs.Len() && ra.sizesAreClose(my_size, deleted_blobs[d].size); d++ {
			blobsAreClose, err := ra.blobsAreClose(
				my_blob, cache[deleted_blobs[d].change.From.TreeEntry.Hash])
			if err != nil {
				return nil, err
			}
			if blobsAreClose {
				found_match = true
				reduced_changes = append(
					reduced_changes,
					&object.Change{From: deleted_blobs[d].change.From,
						To: added_blobs[a].change.To})
				break
			}
		}
		if found_match {
			added_blobs = append(added_blobs[:a], added_blobs[a+1:]...)
			a--
			deleted_blobs = append(deleted_blobs[:d], deleted_blobs[d+1:]...)
		}
	}

	// Stage 3 - we give up, everything left are independent additions and deletions
	for _, blob := range added_blobs {
		reduced_changes = append(reduced_changes, blob.change)
	}
	for _, blob := range deleted_blobs {
		reduced_changes = append(reduced_changes, blob.change)
	}
	return map[string]interface{}{"renamed_changes": reduced_changes}, nil
}

func (ra *RenameAnalysis) Finalize() interface{} {
	return nil
}

func (ra *RenameAnalysis) sizesAreClose(size1 int64, size2 int64) bool {
	return abs64(size1-size2)*100/max64(1, min64(size1, size2)) <=
		int64(100-ra.SimilarityThreshold)
}

func (ra *RenameAnalysis) blobsAreClose(
	blob1 *object.Blob, blob2 *object.Blob) (bool, error) {
	str_from, err := blobToString(blob1)
	if err != nil {
		return false, err
	}
	str_to, err := blobToString(blob2)
	if err != nil {
		return false, err
	}
	dmp := diffmatchpatch.New()
	src, dst, _ := dmp.DiffLinesToRunes(str_from, str_to)
	diffs := dmp.DiffMainRunes(src, dst, false)
	common := 0
	for _, edit := range diffs {
		if edit.Type == diffmatchpatch.DiffEqual {
			common += utf8.RuneCountInString(edit.Text)
		}
	}
	return common*100/max(1, min(len(src), len(dst))) >= ra.SimilarityThreshold, nil
}

type sortableChange struct {
	change *object.Change
	hash   plumbing.Hash
}

type sortableChanges []sortableChange

func (change *sortableChange) Less(other *sortableChange) bool {
	for x := 0; x < 20; x++ {
		if change.hash[x] < other.hash[x] {
			return true
		}
	}
	return false
}

func (slice sortableChanges) Len() int {
	return len(slice)
}

func (slice sortableChanges) Less(i, j int) bool {
	return slice[i].Less(&slice[j])
}

func (slice sortableChanges) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

type sortableBlob struct {
	change *object.Change
	size   int64
}

type sortableBlobs []sortableBlob

func (change *sortableBlob) Less(other *sortableBlob) bool {
	return change.size < other.size
}

func (slice sortableBlobs) Len() int {
	return len(slice)
}

func (slice sortableBlobs) Less(i, j int) bool {
	return slice[i].Less(&slice[j])
}

func (slice sortableBlobs) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}
