package plumbing

import (
	"log"
	"path/filepath"
	"sort"
	"sync"
	"unicode/utf8"

	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v6/internal"
	"gopkg.in/src-d/hercules.v6/internal/core"
)

// RenameAnalysis improves TreeDiff's results by searching for changed blobs under different
// paths which are likely to be the result of a rename with subsequent edits.
// RenameAnalysis is a PipelineItem.
type RenameAnalysis struct {
	core.NoopMerger
	// SimilarityThreshold adjusts the heuristic to determine file renames.
	// It has the same units as cgit's -X rename-threshold or -M. Better to
	// set it to the default value of 80 (80%).
	SimilarityThreshold int

	repository *git.Repository
}

const (
	// RenameAnalysisDefaultThreshold specifies the default percentage of common lines in a pair
	// of files to consider them linked. The exact code of the decision is sizesAreClose().
	// CGit's default is 50%. Ours is 80% because 50% can be too computationally expensive.
	RenameAnalysisDefaultThreshold = 80

	// ConfigRenameAnalysisSimilarityThreshold is the name of the configuration option
	// (RenameAnalysis.Configure()) which sets the similarity threshold.
	ConfigRenameAnalysisSimilarityThreshold = "RenameAnalysis.SimilarityThreshold"

	// RenameAnalysisMinimumSize is the minimum size of a blob to be considered.
	RenameAnalysisMinimumSize = 32

	// RenameAnalysisMaxCandidates is the maximum number of rename candidates to consider per file.
	RenameAnalysisMaxCandidates = 50
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (ra *RenameAnalysis) Name() string {
	return "RenameAnalysis"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (ra *RenameAnalysis) Provides() []string {
	arr := [...]string{DependencyTreeChanges}
	return arr[:]
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (ra *RenameAnalysis) Requires() []string {
	arr := [...]string{DependencyBlobCache, DependencyTreeChanges}
	return arr[:]
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (ra *RenameAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigRenameAnalysisSimilarityThreshold,
		Description: "The threshold on the similarity index used to detect renames.",
		Flag:        "M",
		Type:        core.IntConfigurationOption,
		Default:     RenameAnalysisDefaultThreshold},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (ra *RenameAnalysis) Configure(facts map[string]interface{}) error {
	if val, exists := facts[ConfigRenameAnalysisSimilarityThreshold].(int); exists {
		ra.SimilarityThreshold = val
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (ra *RenameAnalysis) Initialize(repository *git.Repository) error {
	if ra.SimilarityThreshold < 0 || ra.SimilarityThreshold > 100 {
		log.Printf("Warning: adjusted the similarity threshold to %d\n",
			RenameAnalysisDefaultThreshold)
		ra.SimilarityThreshold = RenameAnalysisDefaultThreshold
	}
	ra.repository = repository
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (ra *RenameAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	changes := deps[DependencyTreeChanges].(object.Changes)
	cache := deps[DependencyBlobCache].(map[plumbing.Hash]*CachedBlob)

	reducedChanges := make(object.Changes, 0, changes.Len())

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
			reducedChanges = append(reducedChanges, change)
		}
	}
	sort.Sort(deleted)
	sort.Sort(added)
	stillDeleted := make(object.Changes, 0, deleted.Len())
	stillAdded := make(object.Changes, 0, added.Len())
	{
		a := 0
		d := 0
		for a < added.Len() && d < deleted.Len() {
			if added[a].hash == deleted[d].hash {
				reducedChanges = append(
					reducedChanges,
					&object.Change{From: deleted[d].change.From, To: added[a].change.To})
				a++
				d++
			} else if added[a].Less(&deleted[d]) {
				stillAdded = append(stillAdded, added[a].change)
				a++
			} else {
				stillDeleted = append(stillDeleted, deleted[d].change)
				d++
			}
		}
		for ; a < added.Len(); a++ {
			stillAdded = append(stillAdded, added[a].change)
		}
		for ; d < deleted.Len(); d++ {
			stillDeleted = append(stillDeleted, deleted[d].change)
		}
	}

	// Stage 2 - apply the similarity threshold
	// n^2 but actually linear
	// We sort the blobs by size and do the single linear scan.
	addedBlobs := make(sortableBlobs, 0, stillAdded.Len())
	deletedBlobs := make(sortableBlobs, 0, stillDeleted.Len())
	var smallChanges []*object.Change
	for _, change := range stillAdded {
		blob := cache[change.To.TreeEntry.Hash]
		if blob.Size < RenameAnalysisMinimumSize {
			smallChanges = append(smallChanges, change)
		} else {
			addedBlobs = append(
				addedBlobs, sortableBlob{change: change, size: blob.Size})
		}
	}
	for _, change := range stillDeleted {
		blob := cache[change.From.TreeEntry.Hash]
		if blob.Size < RenameAnalysisMinimumSize {
			smallChanges = append(smallChanges, change)
		} else {
			deletedBlobs = append(
				deletedBlobs, sortableBlob{change: change, size: blob.Size})
		}
	}
	sort.Sort(addedBlobs)
	sort.Sort(deletedBlobs)

	var finished, finishedA, finishedB bool
	matchesA := make(object.Changes, 0, changes.Len())
	matchesB := make(object.Changes, 0, changes.Len())
	addedBlobsA := addedBlobs
	addedBlobsB := make(sortableBlobs, len(addedBlobs))
	copy(addedBlobsB, addedBlobs)
	deletedBlobsA := deletedBlobs
	deletedBlobsB := make(sortableBlobs, len(deletedBlobs))
	copy(deletedBlobsB, deletedBlobs)
	wg := sync.WaitGroup{}
	matchA := func() error {
		defer func() {
			finished = true
			wg.Done()
		}()
		aStart := 0
		// we will try to find a matching added blob for each deleted blob
		for d := 0; d < deletedBlobsA.Len(); d++ {
			myBlob := cache[deletedBlobsA[d].change.From.TreeEntry.Hash]
			mySize := deletedBlobsA[d].size
			myName := filepath.Base(deletedBlobsA[d].change.From.Name)
			var a int
			for a = aStart; a < addedBlobsA.Len() && !ra.sizesAreClose(mySize, addedBlobsA[a].size); a++ {
			}
			aStart = a
			foundMatch := false
			// get the list of possible candidates and sort by file name similarity
			var candidates []int
			for a = aStart; a < addedBlobsA.Len() && ra.sizesAreClose(mySize, addedBlobsA[a].size); a++ {
				candidates = append(candidates, a)
			}
			sortRenameCandidates(candidates, myName, func(a int) string {
				return addedBlobsA[a].change.To.Name
			})
			var ci int
			for ci, a = range candidates {
				if finished {
					return nil
				}
				if ci > RenameAnalysisMaxCandidates {
					break
				}
				blobsAreClose, err := ra.blobsAreClose(
					myBlob, cache[addedBlobsA[a].change.To.TreeEntry.Hash])
				if err != nil {
					return err
				}
				if blobsAreClose {
					foundMatch = true
					matchesA = append(
						matchesA,
						&object.Change{
							From: deletedBlobsA[d].change.From,
							To:   addedBlobsA[a].change.To})
					break
				}
			}
			if foundMatch {
				deletedBlobsA = append(deletedBlobsA[:d], deletedBlobsA[d+1:]...)
				d--
				addedBlobsA = append(addedBlobsA[:a], addedBlobsA[a+1:]...)
			}
		}
		finishedA = true
		return nil
	}
	matchB := func() error {
		defer func() {
			finished = true
			wg.Done()
		}()
		dStart := 0
		for a := 0; a < addedBlobsB.Len(); a++ {
			myBlob := cache[addedBlobsB[a].change.To.TreeEntry.Hash]
			mySize := addedBlobsB[a].size
			myName := filepath.Base(addedBlobsB[a].change.To.Name)
			var d int
			for d = dStart; d < deletedBlobsB.Len() && !ra.sizesAreClose(mySize, deletedBlobsB[d].size); d++ {
			}
			dStart = d
			foundMatch := false
			// get the list of possible candidates and sort by file name similarity
			var candidates []int
			for d = dStart; d < deletedBlobsB.Len() && ra.sizesAreClose(mySize, deletedBlobsB[d].size); d++ {
				candidates = append(candidates, d)
			}
			sortRenameCandidates(candidates, myName, func(d int) string {
				return deletedBlobsB[d].change.From.Name
			})
			var ci int
			for ci, d = range candidates {
				if finished {
					return nil
				}
				if ci > RenameAnalysisMaxCandidates {
					break
				}
				blobsAreClose, err := ra.blobsAreClose(
					myBlob, cache[deletedBlobsB[d].change.From.TreeEntry.Hash])
				if err != nil {
					return err
				}
				if blobsAreClose {
					foundMatch = true
					matchesB = append(
						matchesB,
						&object.Change{
							From: deletedBlobsB[d].change.From,
							To:   addedBlobsB[a].change.To})
					break
				}
			}
			if foundMatch {
				addedBlobsB = append(addedBlobsB[:a], addedBlobsB[a+1:]...)
				a--
				deletedBlobsB = append(deletedBlobsB[:d], deletedBlobsB[d+1:]...)
			}
		}
		finishedB = true
		return nil
	}
	// run two functions in parallel, and take the result from the one which finished earlier
	wg.Add(2)
	var err error
	go func() { err = matchA() }()
	go func() { err = matchB() }()
	wg.Wait()
	if err != nil {
		return nil, err
	}
	var matches object.Changes
	if finishedA {
		addedBlobs = addedBlobsA
		deletedBlobs = deletedBlobsA
		matches = matchesA
	} else {
		if !finishedB {
			panic("Impossible happened: two functions returned without an error " +
				"but no results from both")
		}
		addedBlobs = addedBlobsB
		deletedBlobs = deletedBlobsB
		matches = matchesB
	}

	// Stage 3 - we give up, everything left are independent additions and deletions
	for _, change := range matches {
		reducedChanges = append(reducedChanges, change)
	}
	for _, blob := range addedBlobs {
		reducedChanges = append(reducedChanges, blob.change)
	}
	for _, blob := range deletedBlobs {
		reducedChanges = append(reducedChanges, blob.change)
	}
	for _, change := range smallChanges {
		reducedChanges = append(reducedChanges, change)
	}
	return map[string]interface{}{DependencyTreeChanges: reducedChanges}, nil
}

// Fork clones this PipelineItem.
func (ra *RenameAnalysis) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(ra, n)
}

func (ra *RenameAnalysis) sizesAreClose(size1 int64, size2 int64) bool {
	size := internal.Max64(1, internal.Max64(size1, size2))
	return (internal.Abs64(size1-size2)*10000)/size <= int64(100-ra.SimilarityThreshold)*100
}

func (ra *RenameAnalysis) blobsAreClose(blob1 *CachedBlob, blob2 *CachedBlob) (bool, error) {
	defer func() {
		if err := recover(); err != nil {
			log.Println()
			log.Println(blob1.Hash.String())
			log.Println(blob2.Hash.String())
			panic(err)
		}
	}()
	_, err1 := blob1.CountLines()
	_, err2 := blob2.CountLines()
	if err1 == ErrorBinary || err2 == ErrorBinary {
		// binary mode
		bsdifflen := DiffBytes(blob1.Data, blob2.Data)
		delta := int((int64(bsdifflen) * 100) / internal.Max64(
			internal.Min64(blob1.Size, blob2.Size), 1))
		return 100-delta >= ra.SimilarityThreshold, nil
	}
	src, dst := string(blob1.Data), string(blob2.Data)
	maxSize := internal.Max(1, internal.Max(utf8.RuneCountInString(src), utf8.RuneCountInString(dst)))

	// compute the line-by-line diff, then the char-level diffs of the del-ins blocks
	// yes, this algorithm is greedy and not exact
	dmp := diffmatchpatch.New()
	srcLines, dstLines, lines := dmp.DiffLinesToRunes(src, dst)
	diffs := dmp.DiffMainRunes(srcLines, dstLines, false)
	var common, posSrc, prevPosSrc, posDst int
	possibleDelInsBlock := false
	for _, edit := range diffs {
		switch edit.Type {
		case diffmatchpatch.DiffDelete:
			possibleDelInsBlock = true
			prevPosSrc = posSrc
			for _, lineno := range edit.Text {
				posSrc += len(lines[lineno])
			}
		case diffmatchpatch.DiffInsert:
			nextPosDst := posDst
			for _, lineno := range edit.Text {
				nextPosDst += len(lines[lineno])
			}
			if possibleDelInsBlock {
				possibleDelInsBlock = false
				localDmp := diffmatchpatch.New()
				localSrc := src[prevPosSrc:posSrc]
				localDst := dst[posDst:nextPosDst]
				localDiffs := localDmp.DiffMainRunes([]rune(localSrc), []rune(localDst), false)
				for _, localEdit := range localDiffs {
					if localEdit.Type == diffmatchpatch.DiffEqual {
						common += utf8.RuneCountInString(localEdit.Text)
					}
				}
			}
			posDst = nextPosDst
		case diffmatchpatch.DiffEqual:
			possibleDelInsBlock = false
			for _, lineno := range edit.Text {
				common += utf8.RuneCountInString(lines[lineno])
				step := len(lines[lineno])
				posSrc += step
				posDst += step
			}
		}
		if possibleDelInsBlock {
			continue
		}
		// supposing that the rest of the lines are the same (they are not - too optimistic),
		// estimate the maximum similarity and exit the loop if it lower than our threshold
		maxCommon := common + internal.Min(
			utf8.RuneCountInString(src[posSrc:]),
			utf8.RuneCountInString(dst[posDst:]))
		similarity := (maxCommon * 100) / maxSize
		if similarity < ra.SimilarityThreshold {
			return false, nil
		}
	}
	// the very last "overly optimistic" estimate was actually precise, so since we are still here
	// the blobs are similar
	return true, nil
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

type candidateDistance struct {
	Candidate int
	Distance  int
}

func sortRenameCandidates(candidates []int, origin string, nameGetter func(int) string) {
	distances := make([]candidateDistance, len(candidates))
	ctx := LevenshteinContext{}
	for i, x := range candidates {
		name := filepath.Base(nameGetter(x))
		distances[i] = candidateDistance{x, ctx.Distance(origin, name)}
	}
	sort.Slice(distances, func(i, j int) bool {
		return distances[i].Distance < distances[j].Distance
	})
	for i, cd := range distances {
		candidates[i] = cd.Candidate
	}
}

func init() {
	core.Registry.Register(&RenameAnalysis{})
}
