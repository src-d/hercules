package hercules

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v3/pb"
	"gopkg.in/src-d/hercules.v3/yaml"
)

// BurndownAnalysis allows to gather the line burndown statistics for a Git repository.
// It is a LeafPipelineItem.
// Reference: https://erikbern.com/2016/12/05/the-half-life-of-code.html
type BurndownAnalysis struct {
	// Granularity sets the size of each band - the number of days it spans.
	// Smaller values provide better resolution but require more work and eat more
	// memory. 30 days is usually enough.
	Granularity int
	// Sampling sets how detailed is the statistic - the size of the interval in
	// days between consecutive measurements. It may not be greater than Granularity. Try 15 or 30.
	Sampling int

	// TrackFiles enables or disables the fine-grained per-file burndown analysis.
	// It does not change the project level burndown results.
	TrackFiles bool

	// The number of developers for which to collect the burndown stats. 0 disables it.
	PeopleNumber int

	// Debug activates the debugging mode. Analyse() runs slower in this mode
	// but it accurately checks all the intermediate states for invariant
	// violations.
	Debug bool

	// Repository points to the analysed Git repository struct from go-git.
	repository *git.Repository
	// globalStatus is the current daily alive number of lines; key is the number
	// of days from the beginning of the history.
	globalStatus map[int]int64
	// globalHistory is the periodic snapshots of globalStatus.
	globalHistory [][]int64
	// fileHistories is the periodic snapshots of each file's status.
	fileHistories map[string][][]int64
	// peopleHistories is the periodic snapshots of each person's status.
	peopleHistories [][][]int64
	// files is the mapping <file path> -> *File.
	files map[string]*File
	// matrix is the mutual deletions and self insertions.
	matrix []map[int]int64
	// people is the people's individual time stats.
	people []map[int]int64
	// day is the most recent day index processed.
	day int
	// previousDay is the day from the previous sample period -
	// different from DaysSinceStart.previousDay.
	previousDay int
	// references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
}

// BurndownResult carries the result of running BurndownAnalysis - it is returned by
// BurndownAnalysis.Finalize().
type BurndownResult struct {
	// [number of samples][number of bands]
	// The number of samples depends on Sampling: the less Sampling, the bigger the number.
	// The number of bands depends on Granularity: the less Granularity, the bigger the number.
	GlobalHistory [][]int64
	// The key is the path inside the Git repository. The value's dimensions are the same as
	// in GlobalHistory.
	FileHistories map[string][][]int64
	// [number of people][number of samples][number of bands]
	PeopleHistories [][][]int64
	// [number of people][number of people + 2]
	// The first element is the total number of lines added by the author.
	// The second element is the number of removals by unidentified authors (outside reversedPeopleDict).
	// The rest of the elements are equal the number of line removals by the corresponding
	// authors in reversedPeopleDict: 2 -> 0, 3 -> 1, etc.
	PeopleMatrix [][]int64

	// The following members are private.

	// reversedPeopleDict is borrowed from IdentityDetector and becomes available after
	// Pipeline.Initialize(facts map[string]interface{}). Thus it can be obtained via
	// facts[FactIdentityDetectorReversedPeopleDict].
	reversedPeopleDict []string
	// sampling and granularity are copied from BurndownAnalysis and stored for service purposes
	// such as merging several results together.
	sampling    int
	granularity int
}

const (
	// ConfigBurndownGranularity is the name of the option to set BurndownAnalysis.Granularity.
	ConfigBurndownGranularity = "Burndown.Granularity"
	// ConfigBurndownSampling is the name of the option to set BurndownAnalysis.Sampling.
	ConfigBurndownSampling = "Burndown.Sampling"
	// ConfigBurndownTrackFiles enables burndown collection for files.
	ConfigBurndownTrackFiles = "Burndown.TrackFiles"
	// ConfigBurndownTrackPeople enables burndown collection for authors.
	ConfigBurndownTrackPeople = "Burndown.TrackPeople"
	// ConfigBurndownDebug enables some extra debug assertions.
	ConfigBurndownDebug = "Burndown.Debug"
	// DefaultBurndownGranularity is the default number of days for BurndownAnalysis.Granularity
	// and BurndownAnalysis.Sampling.
	DefaultBurndownGranularity = 30
	// authorSelf is the internal author index which is used in BurndownAnalysis.Finalize() to
	// format the author overwrites matrix.
	authorSelf = (1 << 18) - 2
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (analyser *BurndownAnalysis) Name() string {
	return "Burndown"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by hercules.Registry to build the global map of providers.
func (analyser *BurndownAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (analyser *BurndownAnalysis) Requires() []string {
	arr := [...]string{
		DependencyFileDiff, DependencyTreeChanges, DependencyBlobCache, DependencyDay, DependencyAuthor}
	return arr[:]
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (analyser *BurndownAnalysis) ListConfigurationOptions() []ConfigurationOption {
	options := [...]ConfigurationOption{{
		Name:        ConfigBurndownGranularity,
		Description: "How many days there are in a single band.",
		Flag:        "granularity",
		Type:        IntConfigurationOption,
		Default:     DefaultBurndownGranularity}, {
		Name:        ConfigBurndownSampling,
		Description: "How frequently to record the state in days.",
		Flag:        "sampling",
		Type:        IntConfigurationOption,
		Default:     DefaultBurndownGranularity}, {
		Name:        ConfigBurndownTrackFiles,
		Description: "Record detailed statistics per each file.",
		Flag:        "burndown-files",
		Type:        BoolConfigurationOption,
		Default:     false}, {
		Name:        ConfigBurndownTrackPeople,
		Description: "Record detailed statistics per each developer.",
		Flag:        "burndown-people",
		Type:        BoolConfigurationOption,
		Default:     false}, {
		Name:        ConfigBurndownDebug,
		Description: "Validate the trees on each step.",
		Flag:        "burndown-debug",
		Type:        BoolConfigurationOption,
		Default:     false},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (analyser *BurndownAnalysis) Configure(facts map[string]interface{}) {
	if val, exists := facts[ConfigBurndownGranularity].(int); exists {
		analyser.Granularity = val
	}
	if val, exists := facts[ConfigBurndownSampling].(int); exists {
		analyser.Sampling = val
	}
	if val, exists := facts[ConfigBurndownTrackFiles].(bool); exists {
		analyser.TrackFiles = val
	}
	if people, exists := facts[ConfigBurndownTrackPeople].(bool); people {
		if val, exists := facts[FactIdentityDetectorPeopleCount].(int); exists {
			analyser.PeopleNumber = val
			analyser.reversedPeopleDict = facts[FactIdentityDetectorReversedPeopleDict].([]string)
		}
	} else if exists {
		analyser.PeopleNumber = 0
	}
	if val, exists := facts[ConfigBurndownDebug].(bool); exists {
		analyser.Debug = val
	}
}

// Flag for the command line switch which enables this analysis.
func (analyser *BurndownAnalysis) Flag() string {
	return "burndown"
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (analyser *BurndownAnalysis) Initialize(repository *git.Repository) {
	if analyser.Granularity <= 0 {
		log.Printf("Warning: adjusted the granularity to %d days\n",
			DefaultBurndownGranularity)
		analyser.Granularity = DefaultBurndownGranularity
	}
	if analyser.Sampling <= 0 {
		log.Printf("Warning: adjusted the sampling to %d days\n",
			DefaultBurndownGranularity)
		analyser.Sampling = DefaultBurndownGranularity
	}
	if analyser.Sampling > analyser.Granularity {
		log.Printf("Warning: granularity may not be less than sampling, adjusted to %d\n",
			analyser.Granularity)
		analyser.Sampling = analyser.Granularity
	}
	analyser.repository = repository
	analyser.globalStatus = map[int]int64{}
	analyser.globalHistory = [][]int64{}
	analyser.fileHistories = map[string][][]int64{}
	analyser.peopleHistories = make([][][]int64, analyser.PeopleNumber)
	analyser.files = map[string]*File{}
	analyser.matrix = make([]map[int]int64, analyser.PeopleNumber)
	analyser.people = make([]map[int]int64, analyser.PeopleNumber)
	analyser.day = 0
	analyser.previousDay = 0
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, "commit" is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (analyser *BurndownAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	sampling := analyser.Sampling
	if sampling == 0 {
		sampling = 1
	}
	author := deps[DependencyAuthor].(int)
	analyser.day = deps[DependencyDay].(int)
	delta := (analyser.day / sampling) - (analyser.previousDay / sampling)
	if delta > 0 {
		analyser.previousDay = analyser.day
		gs, fss, pss := analyser.groupStatus()
		analyser.updateHistories(gs, fss, pss, delta)
	}
	cache := deps[DependencyBlobCache].(map[plumbing.Hash]*object.Blob)
	treeDiffs := deps[DependencyTreeChanges].(object.Changes)
	fileDiffs := deps[DependencyFileDiff].(map[string]FileDiffData)
	for _, change := range treeDiffs {
		action, _ := change.Action()
		var err error
		switch action {
		case merkletrie.Insert:
			err = analyser.handleInsertion(change, author, cache)
		case merkletrie.Delete:
			err = analyser.handleDeletion(change, author, cache)
		case merkletrie.Modify:
			err = analyser.handleModification(change, author, cache, fileDiffs)
		}
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (analyser *BurndownAnalysis) Finalize() interface{} {
	gs, fss, pss := analyser.groupStatus()
	analyser.updateHistories(gs, fss, pss, 1)
	for key, statuses := range analyser.fileHistories {
		if len(statuses) == len(analyser.globalHistory) {
			continue
		}
		padding := make([][]int64, len(analyser.globalHistory)-len(statuses))
		for i := range padding {
			padding[i] = make([]int64, len(analyser.globalStatus))
		}
		analyser.fileHistories[key] = append(padding, statuses...)
	}
	peopleMatrix := make([][]int64, analyser.PeopleNumber)
	for i, row := range analyser.matrix {
		mrow := make([]int64, analyser.PeopleNumber+2)
		peopleMatrix[i] = mrow
		for key, val := range row {
			if key == AuthorMissing {
				key = -1
			} else if key == authorSelf {
				key = -2
			}
			mrow[key+2] = val
		}
	}
	return BurndownResult{
		GlobalHistory:      analyser.globalHistory,
		FileHistories:      analyser.fileHistories,
		PeopleHistories:    analyser.peopleHistories,
		PeopleMatrix:       peopleMatrix,
		reversedPeopleDict: analyser.reversedPeopleDict,
		sampling:           analyser.Sampling,
		granularity:        analyser.Granularity,
	}
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (analyser *BurndownAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	burndownResult := result.(BurndownResult)
	if binary {
		return analyser.serializeBinary(&burndownResult, writer)
	}
	analyser.serializeText(&burndownResult, writer)
	return nil
}

// Deserialize converts the specified protobuf bytes to BurndownResult.
func (analyser *BurndownAnalysis) Deserialize(pbmessage []byte) (interface{}, error) {
	msg := pb.BurndownAnalysisResults{}
	err := proto.Unmarshal(pbmessage, &msg)
	if err != nil {
		return nil, err
	}
	result := BurndownResult{}
	convertCSR := func(mat *pb.BurndownSparseMatrix) [][]int64 {
		res := make([][]int64, mat.NumberOfRows)
		for i := 0; i < int(mat.NumberOfRows); i++ {
			res[i] = make([]int64, mat.NumberOfColumns)
			for j := 0; j < len(mat.Rows[i].Columns); j++ {
				res[i][j] = int64(mat.Rows[i].Columns[j])
			}
		}
		return res
	}
	result.GlobalHistory = convertCSR(msg.Project)
	result.FileHistories = map[string][][]int64{}
	for _, mat := range msg.Files {
		result.FileHistories[mat.Name] = convertCSR(mat)
	}
	result.reversedPeopleDict = make([]string, len(msg.People))
	result.PeopleHistories = make([][][]int64, len(msg.People))
	for i, mat := range msg.People {
		result.PeopleHistories[i] = convertCSR(mat)
		result.reversedPeopleDict[i] = mat.Name
	}
	if msg.PeopleInteraction != nil {
		result.PeopleMatrix = make([][]int64, msg.PeopleInteraction.NumberOfRows)
	}
	for i := 0; i < len(result.PeopleMatrix); i++ {
		result.PeopleMatrix[i] = make([]int64, msg.PeopleInteraction.NumberOfColumns)
		for j := int(msg.PeopleInteraction.Indptr[i]); j < int(msg.PeopleInteraction.Indptr[i+1]); j++ {
			result.PeopleMatrix[i][msg.PeopleInteraction.Indices[j]] = msg.PeopleInteraction.Data[j]
		}
	}
	result.sampling = int(msg.Sampling)
	result.granularity = int(msg.Granularity)
	return result, nil
}

// MergeResults combines two BurndownResult-s together.
func (analyser *BurndownAnalysis) MergeResults(
	r1, r2 interface{}, c1, c2 *CommonAnalysisResult) interface{} {
	bar1 := r1.(BurndownResult)
	bar2 := r2.(BurndownResult)
	merged := BurndownResult{}
	if bar1.sampling < bar2.sampling {
		merged.sampling = bar1.sampling
	} else {
		merged.sampling = bar2.sampling
	}
	if bar1.granularity < bar2.granularity {
		merged.granularity = bar1.granularity
	} else {
		merged.granularity = bar2.granularity
	}
	var people map[string][3]int
	people, merged.reversedPeopleDict = IdentityDetector{}.MergeReversedDicts(
		bar1.reversedPeopleDict, bar2.reversedPeopleDict)
	var wg sync.WaitGroup
	if len(bar1.GlobalHistory) > 0 || len(bar2.GlobalHistory) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			merged.GlobalHistory = mergeMatrices(
				bar1.GlobalHistory, bar2.GlobalHistory,
				bar1.granularity, bar1.sampling,
				bar2.granularity, bar2.sampling,
				c1, c2)
		}()
	}
	if len(bar1.FileHistories) > 0 || len(bar2.FileHistories) > 0 {
		merged.FileHistories = map[string][][]int64{}
		historyMutex := sync.Mutex{}
		for key, fh1 := range bar1.FileHistories {
			if fh2, exists := bar2.FileHistories[key]; exists {
				wg.Add(1)
				go func(fh1, fh2 [][]int64, key string) {
					defer wg.Done()
					historyMutex.Lock()
					defer historyMutex.Unlock()
					merged.FileHistories[key] = mergeMatrices(
						fh1, fh2, bar1.granularity, bar1.sampling, bar2.granularity, bar2.sampling, c1, c2)
				}(fh1, fh2, key)
			} else {
				historyMutex.Lock()
				merged.FileHistories[key] = fh1
				historyMutex.Unlock()
			}
		}
		for key, fh2 := range bar2.FileHistories {
			if _, exists := bar1.FileHistories[key]; !exists {
				historyMutex.Lock()
				merged.FileHistories[key] = fh2
				historyMutex.Unlock()
			}
		}
	}
	if len(merged.reversedPeopleDict) > 0 {
		merged.PeopleHistories = make([][][]int64, len(merged.reversedPeopleDict))
		for i, key := range merged.reversedPeopleDict {
			ptrs := people[key]
			if ptrs[1] < 0 {
				if len(bar2.PeopleHistories) > 0 {
					merged.PeopleHistories[i] = bar2.PeopleHistories[ptrs[2]]
				}
			} else if ptrs[2] < 0 {
				if len(bar1.PeopleHistories) > 0 {
					merged.PeopleHistories[i] = bar1.PeopleHistories[ptrs[1]]
				}
			} else {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					var m1, m2 [][]int64
					if len(bar1.PeopleHistories) > 0 {
						m1 = bar1.PeopleHistories[ptrs[1]]
					}
					if len(bar2.PeopleHistories) > 0 {
						m2 = bar2.PeopleHistories[ptrs[2]]
					}
					merged.PeopleHistories[i] = mergeMatrices(
						m1, m2,
						bar1.granularity, bar1.sampling,
						bar2.granularity, bar2.sampling,
						c1, c2,
					)
				}(i)
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if len(bar2.PeopleMatrix) == 0 {
				merged.PeopleMatrix = bar1.PeopleMatrix
				// extend the matrix in both directions
				for i := 0; i < len(merged.PeopleMatrix); i++ {
					for j := len(bar1.reversedPeopleDict); j < len(merged.reversedPeopleDict); j++ {
						merged.PeopleMatrix[i] = append(merged.PeopleMatrix[i], 0)
					}
				}
				for i := len(bar1.reversedPeopleDict); i < len(merged.reversedPeopleDict); i++ {
					merged.PeopleMatrix = append(
						merged.PeopleMatrix, make([]int64, len(merged.reversedPeopleDict)+2))
				}
			} else {
				merged.PeopleMatrix = make([][]int64, len(merged.reversedPeopleDict))
				for i := range merged.PeopleMatrix {
					merged.PeopleMatrix[i] = make([]int64, len(merged.reversedPeopleDict)+2)
				}
				for i, key := range bar1.reversedPeopleDict {
					mi := people[key][0] // index in merged.reversedPeopleDict
					copy(merged.PeopleMatrix[mi][:2], bar1.PeopleMatrix[i][:2])
					for j, val := range bar1.PeopleMatrix[i][2:] {
						merged.PeopleMatrix[mi][2+people[bar1.reversedPeopleDict[j]][0]] = val
					}
				}
				for i, key := range bar2.reversedPeopleDict {
					mi := people[key][0] // index in merged.reversedPeopleDict
					merged.PeopleMatrix[mi][0] += bar2.PeopleMatrix[i][0]
					merged.PeopleMatrix[mi][1] += bar2.PeopleMatrix[i][1]
					for j, val := range bar2.PeopleMatrix[i][2:] {
						merged.PeopleMatrix[mi][2+people[bar2.reversedPeopleDict[j]][0]] += val
					}
				}
			}
		}()
	}
	wg.Wait()
	return merged
}

// mergeMatrices takes two [number of samples][number of bands] matrices,
// resamples them to days so that they become square, sums and resamples back to the
// least of (sampling1, sampling2) and (granularity1, granularity2).
func mergeMatrices(m1, m2 [][]int64, granularity1, sampling1, granularity2, sampling2 int,
	c1, c2 *CommonAnalysisResult) [][]int64 {
	commonMerged := *c1
	commonMerged.Merge(c2)

	var granularity, sampling int
	if sampling1 < sampling2 {
		sampling = sampling1
	} else {
		sampling = sampling2
	}
	if granularity1 < granularity2 {
		granularity = granularity1
	} else {
		granularity = granularity2
	}

	size := int((commonMerged.EndTime - commonMerged.BeginTime) / (3600 * 24))
	daily := make([][]float32, size+granularity)
	for i := range daily {
		daily[i] = make([]float32, size+sampling)
	}
	if len(m1) > 0 {
		addBurndownMatrix(m1, granularity1, sampling1, daily,
			int(c1.BeginTime-commonMerged.BeginTime)/(3600*24))
	}
	if len(m2) > 0 {
		addBurndownMatrix(m2, granularity2, sampling2, daily,
			int(c2.BeginTime-commonMerged.BeginTime)/(3600*24))
	}

	// convert daily to [][]in(t64
	result := make([][]int64, (size+sampling-1)/sampling)
	for i := range result {
		result[i] = make([]int64, (size+granularity-1)/granularity)
		sampledIndex := i * sampling
		if i == len(result)-1 {
			sampledIndex = size - 1
		}
		for j := 0; j < len(result[i]); j++ {
			accum := float32(0)
			for k := j * granularity; k < (j+1)*granularity && k < size; k++ {
				accum += daily[sampledIndex][k]
			}
			result[i][j] = int64(accum)
		}
	}
	return result
}

// Explode `matrix` so that it is daily sampled and has daily bands, shift by `offset` days
// and add to the accumulator. `daily` size is square and is guaranteed to fit `matrix` by
// the caller.
// Rows: *at least* len(matrix) * sampling + offset
// Columns: *at least* len(matrix[...]) * granularity + offset
// `matrix` can be sparse, so that the last columns which are equal to 0 are truncated.
func addBurndownMatrix(matrix [][]int64, granularity, sampling int, daily [][]float32, offset int) {
	// Determine the maximum number of bands; the actual one may be larger but we do not care
	maxCols := 0
	for _, row := range matrix {
		if maxCols < len(row) {
			maxCols = len(row)
		}
	}
	neededRows := len(matrix)*sampling + offset
	if len(daily) < neededRows {
		panic(fmt.Sprintf("merge bug: too few daily rows: required %d, have %d",
			neededRows, len(daily)))
	}
	if len(daily[0]) < maxCols {
		panic(fmt.Sprintf("merge bug: too few daily cols: required %d, have %d",
			maxCols, len(daily[0])))
	}
	for x := 0; x < maxCols; x++ {
		for y := 0; y < len(matrix); y++ {
			if x*granularity > (y+1)*sampling {
				// the future is zeros
				continue
			}
			decay := func(startIndex int, startVal float32) {
				if startVal == 0 {
					return
				}
				k := float32(matrix[y][x]) / startVal // <= 1
				scale := float32((y+1)*sampling - startIndex)
				for i := x * granularity; i < (x+1)*granularity; i++ {
					initial := daily[startIndex-1+offset][i+offset]
					for j := startIndex; j < (y+1)*sampling; j++ {
						daily[j+offset][i+offset] = initial * (1 + (k-1)*float32(j-startIndex+1)/scale)
					}
				}
			}
			raise := func(finishIndex int, finishVal float32) {
				var initial float32
				if y > 0 {
					initial = float32(matrix[y-1][x])
				}
				startIndex := y * sampling
				if startIndex < x*granularity {
					startIndex = x * granularity
				}
				if startIndex == finishIndex {
					return
				}
				avg := (finishVal - initial) / float32(finishIndex-startIndex)
				for j := y * sampling; j < finishIndex; j++ {
					for i := startIndex; i <= j; i++ {
						daily[j+offset][i+offset] = avg
					}
				}
				// copy [x*g..y*s)
				for j := y * sampling; j < finishIndex; j++ {
					for i := x * granularity; i < y*sampling; i++ {
						daily[j+offset][i+offset] = daily[j-1+offset][i+offset]
					}
				}
			}
			if (x+1)*granularity >= (y+1)*sampling {
				// x*granularity <= (y+1)*sampling
				// 1. x*granularity <= y*sampling
				//    y*sampling..(y+1)sampling
				//
				//       x+1
				//        /
				//       /
				//      / y+1  -|
				//     /        |
				//    / y      -|
				//   /
				//  / x
				//
				// 2. x*granularity > y*sampling
				//    x*granularity..(y+1)sampling
				//
				//       x+1
				//        /
				//       /
				//      / y+1  -|
				//     /        |
				//    / x      -|
				//   /
				//  / y
				if x*granularity <= y*sampling {
					raise((y+1)*sampling, float32(matrix[y][x]))
				} else if (y+1)*sampling > x*granularity {
					raise((y+1)*sampling, float32(matrix[y][x]))
					avg := float32(matrix[y][x]) / float32((y+1)*sampling-x*granularity)
					for j := x * granularity; j < (y+1)*sampling; j++ {
						for i := x * granularity; i <= j; i++ {
							daily[j+offset][i+offset] = avg
						}
					}
				}
			} else if (x+1)*granularity >= y*sampling {
				// y*sampling <= (x+1)*granularity < (y+1)sampling
				// y*sampling..(x+1)*granularity
				// (x+1)*granularity..(y+1)sampling
				//        x+1
				//         /\
				//        /  \
				//       /    \
				//      /    y+1
				//     /
				//    y
				v1 := float32(matrix[y-1][x])
				v2 := float32(matrix[y][x])
				var peak float32
				delta := float32((x+1)*granularity - y*sampling)
				var scale float32
				var previous float32
				if y > 0 && (y-1)*sampling >= x*granularity {
					// x*g <= (y-1)*s <= y*s <= (x+1)*g <= (y+1)*s
					//           |________|.......^
					if y > 1 {
						previous = float32(matrix[y-2][x])
					}
					scale = float32(sampling)
				} else {
					// (y-1)*s < x*g <= y*s <= (x+1)*g <= (y+1)*s
					//            |______|.......^
					if y == 0 {
						scale = float32(sampling)
					} else {
						scale = float32(y*sampling - x*granularity)
					}
				}
				peak = v1 + (v1-previous)/scale*delta
				if v2 > peak {
					// we need to adjust the peak, it may not be less than the decayed value
					if y < len(matrix)-1 {
						// y*s <= (x+1)*g <= (y+1)*s < (y+2)*s
						//           ^.........|_________|
						k := (v2 - float32(matrix[y+1][x])) / float32(sampling) // > 0
						peak = float32(matrix[y][x]) + k*float32((y+1)*sampling-(x+1)*granularity)
						// peak > v2 > v1
					} else {
						peak = v2
						// not enough data to interpolate; this is at least not restricted
					}
				}
				raise((x+1)*granularity, peak)
				decay((x+1)*granularity, peak)
			} else {
				// (x+1)*granularity < y*sampling
				// y*sampling..(y+1)sampling
				decay(y*sampling, float32(matrix[y-1][x]))
			}
		}
	}
}

func (analyser *BurndownAnalysis) serializeText(result *BurndownResult, writer io.Writer) {
	fmt.Fprintln(writer, "  granularity:", result.granularity)
	fmt.Fprintln(writer, "  sampling:", result.sampling)
	yaml.PrintMatrix(writer, result.GlobalHistory, 2, "project", true)
	if len(result.FileHistories) > 0 {
		fmt.Fprintln(writer, "  files:")
		keys := sortedKeys(result.FileHistories)
		for _, key := range keys {
			yaml.PrintMatrix(writer, result.FileHistories[key], 4, key, true)
		}
	}

	if len(result.PeopleHistories) > 0 {
		fmt.Fprintln(writer, "  people_sequence:")
		for key := range result.PeopleHistories {
			fmt.Fprintln(writer, "    - "+yaml.SafeString(result.reversedPeopleDict[key]))
		}
		fmt.Fprintln(writer, "  people:")
		for key, val := range result.PeopleHistories {
			yaml.PrintMatrix(writer, val, 4, result.reversedPeopleDict[key], true)
		}
		fmt.Fprintln(writer, "  people_interaction: |-")
		yaml.PrintMatrix(writer, result.PeopleMatrix, 4, "", false)
	}
}

func (analyser *BurndownAnalysis) serializeBinary(result *BurndownResult, writer io.Writer) error {
	message := pb.BurndownAnalysisResults{
		Granularity: int32(result.granularity),
		Sampling:    int32(result.sampling),
	}
	if len(result.GlobalHistory) > 0 {
		message.Project = pb.ToBurndownSparseMatrix(result.GlobalHistory, "project")
	}
	if len(result.FileHistories) > 0 {
		message.Files = make([]*pb.BurndownSparseMatrix, len(result.FileHistories))
		keys := sortedKeys(result.FileHistories)
		i := 0
		for _, key := range keys {
			message.Files[i] = pb.ToBurndownSparseMatrix(
				result.FileHistories[key], key)
			i++
		}
	}

	if len(result.PeopleHistories) > 0 {
		message.People = make(
			[]*pb.BurndownSparseMatrix, len(result.PeopleHistories))
		for key, val := range result.PeopleHistories {
			if len(val) > 0 {
				message.People[key] = pb.ToBurndownSparseMatrix(val, result.reversedPeopleDict[key])
			}
		}
		message.PeopleInteraction = pb.DenseToCompressedSparseRowMatrix(result.PeopleMatrix)
	}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	writer.Write(serialized)
	return nil
}

func sortedKeys(m map[string][][]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func checkClose(c io.Closer) {
	if err := c.Close(); err != nil {
		panic(err)
	}
}

// We do a hack and store the day in the first 14 bits and the author index in the last 18.
// Strictly speaking, int can be 64-bit and then the author index occupies 32+18 bits.
// This hack is needed to simplify the values storage inside File-s. We can compare
// different values together and they are compared as days for the same author.
func (analyser *BurndownAnalysis) packPersonWithDay(person int, day int) int {
	if analyser.PeopleNumber == 0 {
		return day
	}
	result := day
	result |= person << 14
	// This effectively means max 16384 days (>44 years) and (131072 - 2) devs
	return result
}

func (analyser *BurndownAnalysis) unpackPersonWithDay(value int) (int, int) {
	if analyser.PeopleNumber == 0 {
		return AuthorMissing, value
	}
	return value >> 14, value & 0x3FFF
}

func (analyser *BurndownAnalysis) updateStatus(
	status interface{}, _ int, previousValue int, delta int) {

	_, previousTime := analyser.unpackPersonWithDay(previousValue)
	status.(map[int]int64)[previousTime] += int64(delta)
}

func (analyser *BurndownAnalysis) updatePeople(
	peopleUncasted interface{}, _ int, previousValue int, delta int) {
	previousAuthor, previousTime := analyser.unpackPersonWithDay(previousValue)
	if previousAuthor == AuthorMissing {
		return
	}
	people := peopleUncasted.([]map[int]int64)
	stats := people[previousAuthor]
	if stats == nil {
		stats = map[int]int64{}
		people[previousAuthor] = stats
	}
	stats[previousTime] += int64(delta)
}

func (analyser *BurndownAnalysis) updateMatrix(
	matrixUncasted interface{}, currentTime int, previousTime int, delta int) {

	matrix := matrixUncasted.([]map[int]int64)
	newAuthor, _ := analyser.unpackPersonWithDay(currentTime)
	oldAuthor, _ := analyser.unpackPersonWithDay(previousTime)
	if oldAuthor == AuthorMissing {
		return
	}
	if newAuthor == oldAuthor && delta > 0 {
		newAuthor = authorSelf
	}
	row := matrix[oldAuthor]
	if row == nil {
		row = map[int]int64{}
		matrix[oldAuthor] = row
	}
	cell, exists := row[newAuthor]
	if !exists {
		row[newAuthor] = 0
		cell = 0
	}
	row[newAuthor] = cell + int64(delta)
}

func (analyser *BurndownAnalysis) newFile(
	author int, day int, size int, global map[int]int64, people []map[int]int64,
	matrix []map[int]int64) *File {
	statuses := make([]Status, 1)
	statuses[0] = NewStatus(global, analyser.updateStatus)
	if analyser.TrackFiles {
		statuses = append(statuses, NewStatus(map[int]int64{}, analyser.updateStatus))
	}
	if analyser.PeopleNumber > 0 {
		statuses = append(statuses, NewStatus(people, analyser.updatePeople))
		statuses = append(statuses, NewStatus(matrix, analyser.updateMatrix))
		day = analyser.packPersonWithDay(author, day)
	}
	return NewFile(day, size, statuses...)
}

func (analyser *BurndownAnalysis) handleInsertion(
	change *object.Change, author int, cache map[plumbing.Hash]*object.Blob) error {
	blob := cache[change.To.TreeEntry.Hash]
	lines, err := CountLines(blob)
	if err != nil {
		if err.Error() == "binary" {
			return nil
		}
		return err
	}
	name := change.To.Name
	file, exists := analyser.files[name]
	if exists {
		return fmt.Errorf("file %s already exists", name)
	}
	file = analyser.newFile(
		author, analyser.day, lines, analyser.globalStatus, analyser.people, analyser.matrix)
	analyser.files[name] = file
	return nil
}

func (analyser *BurndownAnalysis) handleDeletion(
	change *object.Change, author int, cache map[plumbing.Hash]*object.Blob) error {

	blob := cache[change.From.TreeEntry.Hash]
	lines, err := CountLines(blob)
	if err != nil {
		if err.Error() == "binary" {
			return nil
		}
		return err
	}
	name := change.From.Name
	file := analyser.files[name]
	file.Update(analyser.packPersonWithDay(author, analyser.day), 0, 0, lines)
	delete(analyser.files, name)
	return nil
}

func (analyser *BurndownAnalysis) handleModification(
	change *object.Change, author int, cache map[plumbing.Hash]*object.Blob,
	diffs map[string]FileDiffData) error {

	file, exists := analyser.files[change.From.Name]
	if !exists {
		// this indeed may happen
		return analyser.handleInsertion(change, author, cache)
	}

	// possible rename
	if change.To.Name != change.From.Name {
		err := analyser.handleRename(change.From.Name, change.To.Name)
		if err != nil {
			return err
		}
	}

	thisDiffs := diffs[change.To.Name]
	if file.Len() != thisDiffs.OldLinesOfCode {
		log.Printf("====TREE====\n%s", file.Dump())
		return fmt.Errorf("%s: internal integrity error src %d != %d %s -> %s",
			change.To.Name, thisDiffs.OldLinesOfCode, file.Len(),
			change.From.TreeEntry.Hash.String(), change.To.TreeEntry.Hash.String())
	}

	// we do not call RunesToDiffLines so the number of lines equals
	// to the rune count
	position := 0
	pending := diffmatchpatch.Diff{Text: ""}

	apply := func(edit diffmatchpatch.Diff) {
		length := utf8.RuneCountInString(edit.Text)
		if edit.Type == diffmatchpatch.DiffInsert {
			file.Update(analyser.packPersonWithDay(author, analyser.day), position, length, 0)
			position += length
		} else {
			file.Update(analyser.packPersonWithDay(author, analyser.day), position, 0, length)
		}
		if analyser.Debug {
			file.Validate()
		}
	}

	for _, edit := range thisDiffs.Diffs {
		dumpBefore := ""
		if analyser.Debug {
			dumpBefore = file.Dump()
		}
		length := utf8.RuneCountInString(edit.Text)
		debugError := func() {
			log.Printf("%s: internal diff error\n", change.To.Name)
			log.Printf("Update(%d, %d, %d (0), %d (0))\n", analyser.day, position,
				length, utf8.RuneCountInString(pending.Text))
			if dumpBefore != "" {
				log.Printf("====TREE BEFORE====\n%s====END====\n", dumpBefore)
			}
			log.Printf("====TREE AFTER====\n%s====END====\n", file.Dump())
		}
		switch edit.Type {
		case diffmatchpatch.DiffEqual:
			if pending.Text != "" {
				apply(pending)
				pending.Text = ""
			}
			position += length
		case diffmatchpatch.DiffInsert:
			if pending.Text != "" {
				if pending.Type == diffmatchpatch.DiffInsert {
					debugError()
					return errors.New("DiffInsert may not appear after DiffInsert")
				}
				file.Update(analyser.packPersonWithDay(author, analyser.day), position, length,
					utf8.RuneCountInString(pending.Text))
				if analyser.Debug {
					file.Validate()
				}
				position += length
				pending.Text = ""
			} else {
				pending = edit
			}
		case diffmatchpatch.DiffDelete:
			if pending.Text != "" {
				debugError()
				return errors.New("DiffDelete may not appear after DiffInsert/DiffDelete")
			}
			pending = edit
		default:
			debugError()
			return fmt.Errorf("diff operation is not supported: %d", edit.Type)
		}
	}
	if pending.Text != "" {
		apply(pending)
		pending.Text = ""
	}
	if file.Len() != thisDiffs.NewLinesOfCode {
		return fmt.Errorf("%s: internal integrity error dst %d != %d",
			change.To.Name, thisDiffs.NewLinesOfCode, file.Len())
	}
	return nil
}

func (analyser *BurndownAnalysis) handleRename(from, to string) error {
	file, exists := analyser.files[from]
	if !exists {
		return fmt.Errorf("file %s does not exist", from)
	}
	analyser.files[to] = file
	delete(analyser.files, from)
	return nil
}

func (analyser *BurndownAnalysis) groupStatus() ([]int64, map[string][]int64, [][]int64) {
	granularity := analyser.Granularity
	if granularity == 0 {
		granularity = 1
	}
	day := analyser.day
	day++
	adjust := 0
	if day%granularity != 0 {
		adjust = 1
	}
	global := make([]int64, day/granularity+adjust)
	var group int64
	for i := 0; i < day; i++ {
		group += analyser.globalStatus[i]
		if (i % granularity) == (granularity - 1) {
			global[i/granularity] = group
			group = 0
		}
	}
	if day%granularity != 0 {
		global[len(global)-1] = group
	}
	locals := make(map[string][]int64)
	if analyser.TrackFiles {
		for key, file := range analyser.files {
			status := make([]int64, day/granularity+adjust)
			var group int64
			for i := 0; i < day; i++ {
				group += file.Status(1).(map[int]int64)[i]
				if (i % granularity) == (granularity - 1) {
					status[i/granularity] = group
					group = 0
				}
			}
			if day%granularity != 0 {
				status[len(status)-1] = group
			}
			locals[key] = status
		}
	}
	peoples := make([][]int64, len(analyser.people))
	for key, person := range analyser.people {
		status := make([]int64, day/granularity+adjust)
		var group int64
		for i := 0; i < day; i++ {
			group += person[i]
			if (i % granularity) == (granularity - 1) {
				status[i/granularity] = group
				group = 0
			}
		}
		if day%granularity != 0 {
			status[len(status)-1] = group
		}
		peoples[key] = status
	}
	return global, locals, peoples
}

func (analyser *BurndownAnalysis) updateHistories(
	globalStatus []int64, fileStatuses map[string][]int64, peopleStatuses [][]int64, delta int) {
	for i := 0; i < delta; i++ {
		analyser.globalHistory = append(analyser.globalHistory, globalStatus)
	}
	toDelete := make([]string, 0)
	for key, fh := range analyser.fileHistories {
		ls, exists := fileStatuses[key]
		if !exists {
			toDelete = append(toDelete, key)
		} else {
			for i := 0; i < delta; i++ {
				fh = append(fh, ls)
			}
			analyser.fileHistories[key] = fh
		}
	}
	for _, key := range toDelete {
		delete(analyser.fileHistories, key)
	}
	for key, ls := range fileStatuses {
		fh, exists := analyser.fileHistories[key]
		if exists {
			continue
		}
		for i := 0; i < delta; i++ {
			fh = append(fh, ls)
		}
		analyser.fileHistories[key] = fh
	}

	for key, ph := range analyser.peopleHistories {
		ls := peopleStatuses[key]
		for i := 0; i < delta; i++ {
			ph = append(ph, ls)
		}
		analyser.peopleHistories[key] = ph
	}
}

func init() {
	Registry.Register(&BurndownAnalysis{})
}
