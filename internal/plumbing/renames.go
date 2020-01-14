package plumbing

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v10/internal"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/levenshtein"
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

	// Timeout is the maximum time allowed to spend computing renames in a single commit.
	Timeout time.Duration

	repository *git.Repository

	l core.Logger
}

const (
	// RenameAnalysisDefaultThreshold specifies the default percentage of common lines in a pair
	// of files to consider them linked. The exact code of the decision is sizesAreClose().
	// CGit's default is 50%. Ours is 80% because 50% can be too computationally expensive.
	RenameAnalysisDefaultThreshold = 80

	// RenameAnalysisDefaultTimeout is the default value of RenameAnalysis.Timeout (in milliseconds).
	RenameAnalysisDefaultTimeout = 60000

	// ConfigRenameAnalysisSimilarityThreshold is the name of the configuration option
	// (RenameAnalysis.Configure()) which sets the similarity threshold.
	ConfigRenameAnalysisSimilarityThreshold = "RenameAnalysis.SimilarityThreshold"

	// ConfigRenameAnalysisTimeout is the name of the configuration option
	// (RenameAnalysis.Configure()) which sets the maximum time allowed to spend
	// computing renames in a single commit.
	ConfigRenameAnalysisTimeout = "RenameAnalysis.Timeout"

	// RenameAnalysisMinimumSize is the minimum size of a blob to be considered.
	RenameAnalysisMinimumSize = 32

	// RenameAnalysisMaxCandidates is the maximum number of rename candidates to consider per file.
	RenameAnalysisMaxCandidates = 50

	// RenameAnalysisSetSizeLimit is the maximum number of added + removed files for
	// RenameAnalysisMaxCandidates to be active; the bigger numbers set it to 1.
	RenameAnalysisSetSizeLimit = 1000

	// RenameAnalysisByteDiffSizeThreshold is the maximum size of each of the compared parts
	// to be diff-ed on byte level.
	RenameAnalysisByteDiffSizeThreshold = 100000
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (ra *RenameAnalysis) Name() string {
	return "RenameAnalysis"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (ra *RenameAnalysis) Provides() []string {
	return []string{DependencyTreeChanges}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (ra *RenameAnalysis) Requires() []string {
	return []string{DependencyBlobCache, DependencyTreeChanges}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (ra *RenameAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigRenameAnalysisSimilarityThreshold,
		Description: "The threshold on the similarity index used to detect renames.",
		Flag:        "M",
		Type:        core.IntConfigurationOption,
		Default:     RenameAnalysisDefaultThreshold}, {
		Name: ConfigRenameAnalysisTimeout,
		Description: "The maximum time (milliseconds) allowed to spend computing " +
			"renames in a single commit. 0 sets the default.",
		Flag:    "renames-timeout",
		Type:    core.IntConfigurationOption,
		Default: RenameAnalysisDefaultTimeout},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (ra *RenameAnalysis) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		ra.l = l
	}
	if val, exists := facts[ConfigRenameAnalysisSimilarityThreshold].(int); exists {
		ra.SimilarityThreshold = val
	}
	if val, exists := facts[ConfigRenameAnalysisTimeout].(int); exists {
		if val < 0 {
			return fmt.Errorf("negative renames detection timeout is not allowed: %d", val)
		}
		ra.Timeout = time.Duration(val) * time.Millisecond
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (ra *RenameAnalysis) Initialize(repository *git.Repository) error {
	ra.l = core.NewLogger()
	if ra.SimilarityThreshold < 0 || ra.SimilarityThreshold > 100 {
		ra.l.Warnf("adjusted the similarity threshold to %d\n",
			RenameAnalysisDefaultThreshold)
		ra.SimilarityThreshold = RenameAnalysisDefaultThreshold
	}
	if ra.Timeout == 0 {
		ra.Timeout = time.Duration(RenameAnalysisDefaultTimeout) * time.Millisecond
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
	beginTime := time.Now()
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
	maxCandidates := RenameAnalysisMaxCandidates
	if len(stillAdded)+len(stillDeleted) > RenameAnalysisSetSizeLimit {
		maxCandidates = 1
	}
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

	finished := make(chan bool, 2)
	finishedA := make(chan bool, 1)
	finishedB := make(chan bool, 1)
	errs := make(chan error)
	matchesA := make(object.Changes, 0, changes.Len())
	matchesB := make(object.Changes, 0, changes.Len())
	addedBlobsA := addedBlobs
	addedBlobsB := make(sortableBlobs, len(addedBlobs))
	copy(addedBlobsB, addedBlobs)
	deletedBlobsA := deletedBlobs
	deletedBlobsB := make(sortableBlobs, len(deletedBlobs))
	copy(deletedBlobsB, deletedBlobs)
	wg := sync.WaitGroup{}
	matchA := func() {
		defer func() {
			finished <- true
			wg.Done()
		}()
		aStart := 0
		// we will try to find a matching added blob for each deleted blob
		for d := 0; d < deletedBlobsA.Len() && time.Now().Sub(beginTime) < ra.Timeout; d++ {
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
				select {
				case <-finished:
					return
				default:
					break
				}
				if ci > maxCandidates {
					break
				}
				blobsAreClose, err := ra.blobsAreClose(
					myBlob, cache[addedBlobsA[a].change.To.TreeEntry.Hash])
				if err != nil {
					errs <- err
					return
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
		finishedA <- true
	}
	matchB := func() {
		defer func() {
			finished <- true
			wg.Done()
		}()
		dStart := 0
		for a := 0; a < addedBlobsB.Len() && time.Now().Sub(beginTime) < ra.Timeout; a++ {
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
				select {
				case <-finished:
					return
				default:
					break
				}
				if ci > maxCandidates {
					break
				}
				blobsAreClose, err := ra.blobsAreClose(
					myBlob, cache[deletedBlobsB[d].change.From.TreeEntry.Hash])
				if err != nil {
					errs <- err
					return
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
		finishedB <- true
	}
	// run two functions in parallel, and take the result from the one which finished earlier
	wg.Add(2)
	go matchA()
	go matchB()
	wg.Wait()
	var matches object.Changes
	select {
	case err := <-errs:
		return nil, err
	case <-finishedA:
		addedBlobs = addedBlobsA
		deletedBlobs = deletedBlobsA
		matches = matchesA
	case <-finishedB:
		addedBlobs = addedBlobsB
		deletedBlobs = deletedBlobsB
		matches = matchesB
	default:
		panic("Impossible happened: two functions returned without an error " +
			"but no results from both")
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
	cleanReturn := false
	defer func() {
		if !cleanReturn {
			ra.l.Warnf("\nunclean return detected for blobs '%s' and '%s'\n",
				blob1.Hash.String(), blob2.Hash.String())
		}
	}()
	_, err1 := blob1.CountLines()
	_, err2 := blob2.CountLines()
	if err1 == ErrorBinary || err2 == ErrorBinary {
		// binary mode
		bsdifflen := DiffBytes(blob1.Data, blob2.Data)
		delta := int((int64(bsdifflen) * 100) / internal.Max64(
			internal.Min64(blob1.Size, blob2.Size), 1))
		cleanReturn = true
		return 100-delta >= ra.SimilarityThreshold, nil
	}
	src, dst := string(blob1.Data), string(blob2.Data)
	maxSize := internal.Max(1, internal.Max(utf8.RuneCountInString(src), utf8.RuneCountInString(dst)))

	// compute the line-by-line diff, then the char-level diffs of the del-ins blocks
	// yes, this algorithm is greedy and not exact
	dmp := diffmatchpatch.New()
	dmp.DiffTimeout = time.Hour
	srcLineRunes, dstLineRunes, _ := dmp.DiffLinesToRunes(src, dst)
	// the third returned value, []string, is the mapping from runes to lines
	// we cannot use it because it is approximate and has string collisions
	// that is, the mapping is wrong for huge files
	diffs := dmp.DiffMainRunes(srcLineRunes, dstLineRunes, false)

	srcPositions := calcLinePositions(src)
	dstPositions := calcLinePositions(dst)
	var common, posSrc, prevPosSrc, posDst int
	possibleDelInsBlock := false
	for _, edit := range diffs {
		switch edit.Type {
		case diffmatchpatch.DiffDelete:
			possibleDelInsBlock = true
			prevPosSrc = posSrc
			posSrc += utf8.RuneCountInString(edit.Text)
		case diffmatchpatch.DiffInsert:
			nextPosDst := posDst + utf8.RuneCountInString(edit.Text)
			if possibleDelInsBlock {
				possibleDelInsBlock = false
				if internal.Max(srcPositions[posSrc]-srcPositions[prevPosSrc],
					dstPositions[nextPosDst]-dstPositions[posDst]) < RenameAnalysisByteDiffSizeThreshold {
					localDmp := diffmatchpatch.New()
					localDmp.DiffTimeout = time.Hour
					localSrc := src[srcPositions[prevPosSrc]:srcPositions[posSrc]]
					localDst := dst[dstPositions[posDst]:dstPositions[nextPosDst]]
					localDiffs := localDmp.DiffMainRunes(
						strToLiteralRunes(localSrc), strToLiteralRunes(localDst), false)
					for _, localEdit := range localDiffs {
						if localEdit.Type == diffmatchpatch.DiffEqual {
							common += utf8.RuneCountInString(localEdit.Text)
						}
					}
				}
			}
			posDst = nextPosDst
		case diffmatchpatch.DiffEqual:
			possibleDelInsBlock = false
			step := utf8.RuneCountInString(edit.Text)
			// for i := range edit.Text does *not* work
			// idk why, but `i` appears to be bigger than the number of runes
			for i := 0; i < step; i++ {
				common += srcPositions[posSrc+i+1] - srcPositions[posSrc+i]
			}
			posSrc += step
			posDst += step
		}
		if possibleDelInsBlock {
			continue
		}
		// supposing that the rest of the lines are the same (they are not - too optimistic),
		// estimate the maximum similarity and exit the loop if it lower than our threshold
		var srcPendingSize, dstPendingSize int
		srcPendingSize = len(src) - srcPositions[posSrc]
		dstPendingSize = len(dst) - dstPositions[posDst]
		maxCommon := common + internal.Min(srcPendingSize, dstPendingSize)
		similarity := (maxCommon * 100) / maxSize
		if similarity < ra.SimilarityThreshold {
			cleanReturn = true
			return false, nil
		}
		similarity = (common * 100) / maxSize
		if similarity >= ra.SimilarityThreshold {
			cleanReturn = true
			return true, nil
		}
	}
	// the very last "overly optimistic" estimate was actually precise, so since we are still here
	// the blobs are similar
	cleanReturn = true
	return true, nil
}

func calcLinePositions(text string) []int {
	if text == "" {
		return []int{0}
	}
	lines := strings.Split(text, "\n")
	positions := make([]int, len(lines)+1)
	accum := 0
	for i, l := range lines {
		positions[i] = accum
		accum += len(l) + 1 // +1 for \n
	}
	if len(lines) > 0 && lines[len(lines)-1] != "\n" {
		accum--
	}
	positions[len(lines)] = accum
	return positions
}

func strToLiteralRunes(s string) []rune {
	lrunes := make([]rune, len(s))
	for i, b := range []byte(s) {
		lrunes[i] = rune(b)
	}
	return lrunes
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
	ctx := levenshtein.Context{}
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
