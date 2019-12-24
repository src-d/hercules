package leaves

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v10/internal/burndown"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	items "gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v10/internal/rbtree"
	"gopkg.in/src-d/hercules.v10/internal/yaml"
)

// BurndownAnalysis allows to gather the line burndown statistics for a Git repository.
// It is a LeafPipelineItem.
// Reference: https://erikbern.com/2016/12/05/the-half-life-of-code.html
type BurndownAnalysis struct {
	// Granularity sets the size of each band - the number of ticks it spans.
	// Smaller values provide better resolution but require more work and eat more
	// memory. 30 ticks is usually enough.
	Granularity int
	// Sampling sets how detailed is the statistic - the size of the interval in
	// ticks between consecutive measurements. It may not be greater than Granularity. Try 15 or 30.
	Sampling int

	// TrackFiles enables or disables the fine-grained per-file burndown analysis.
	// It does not change the project level burndown results.
	TrackFiles bool

	// PeopleNumber is the number of developers for which to collect the burndown stats. 0 disables it.
	PeopleNumber int

	// TickSize indicates the size of each time granule: day, hour, week, etc.
	TickSize time.Duration

	// HibernationThreshold sets the hibernation threshold for the underlying
	// RBTree allocator. It is useful to trade CPU time for reduced peak memory consumption
	// if there are many branches.
	HibernationThreshold int

	// HibernationToDisk specifies whether the hibernated RBTree allocator must be saved on disk
	// rather than kept in memory.
	HibernationToDisk bool

	// HibernationDirectory is the name of the temporary directory to use for saving hibernated
	// RBTree allocators.
	HibernationDirectory string

	// Debug activates the debugging mode. Analyse() runs slower in this mode
	// but it accurately checks all the intermediate states for invariant
	// violations.
	Debug bool

	// Repository points to the analysed Git repository struct from go-git.
	repository *git.Repository
	// globalHistory is the daily deltas of daily line counts.
	// E.g. tick 0: tick 0 +50 lines
	//      tick 10: tick 0 -10 lines; tick 10 +20 lines
	//      tick 12: tick 0 -5 lines; tick 10 -3 lines; tick 12 +10 lines
	// map [0] [0] = 50
	// map[10] [0] = -10
	// map[10][10] = 20
	// map[12] [0] = -5
	// map[12][10] = -3
	// map[12][12] = 10
	globalHistory sparseHistory
	// fileHistories is the daily deltas of each file's daily line counts.
	fileHistories map[string]sparseHistory
	// peopleHistories is the daily deltas of each person's daily line counts.
	peopleHistories []sparseHistory
	// files is the mapping <file path> -> *File.
	files map[string]*burndown.File
	// fileAllocator is the allocator for RBTree-s in `files`.
	fileAllocator *rbtree.Allocator
	// hibernatedFileName is the path to the serialized `fileAllocator`.
	hibernatedFileName string
	// mergedFiles is used during merges to record the real file hashes
	mergedFiles map[string]bool
	// mergedAuthor of the processed merge commit
	mergedAuthor int
	// renames is a quick and dirty solution for the "future branch renames" problem.
	renames map[string]string
	// deletions is a quick and dirty solution for the "real merge removals" problem.
	deletions map[string]bool
	// matrix is the mutual deletions and self insertions.
	matrix []map[int]int64
	// tick is the most recent tick index processed.
	tick int
	// previousTick is the tick from the previous sample period -
	// different from TicksSinceStart.previousTick.
	previousTick int
	// references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string

	l core.Logger
}

// BurndownResult carries the result of running BurndownAnalysis - it is returned by
// BurndownAnalysis.Finalize().
type BurndownResult struct {
	// [number of samples][number of bands]
	// The number of samples depends on Sampling: the less Sampling, the bigger the number.
	// The number of bands depends on Granularity: the less Granularity, the bigger the number.
	GlobalHistory DenseHistory
	// The key is a path inside the Git repository. The value's dimensions are the same as
	// in GlobalHistory.
	FileHistories map[string]DenseHistory
	// The key is a path inside the Git repository. The value is a mapping from developer indexes
	// (see reversedPeopleDict) and the owned line numbers. Their sum equals to the total number of
	// lines in the file.
	FileOwnership map[string]map[int]int
	// [number of people][number of samples][number of bands]
	PeopleHistories []DenseHistory
	// [number of people][number of people + 2]
	// The first element is the total number of lines added by the author.
	// The second element is the number of removals by unidentified authors (outside reversedPeopleDict).
	// The rest of the elements are equal the number of line removals by the corresponding
	// authors in reversedPeopleDict: 2 -> 0, 3 -> 1, etc.
	PeopleMatrix DenseHistory

	// The following members are private.

	// reversedPeopleDict is borrowed from IdentityDetector and becomes available after
	// Pipeline.Initialize(facts map[string]interface{}). Thus it can be obtained via
	// facts[FactIdentityDetectorReversedPeopleDict].
	reversedPeopleDict []string
	// TickSize references TicksSinceStart.TickSize
	tickSize time.Duration
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
	// ConfigBurndownHibernationThreshold sets the hibernation threshold for the underlying
	// RBTree allocator. It is useful to trade CPU time for reduced peak memory consumption
	// if there are many branches.
	ConfigBurndownHibernationThreshold = "Burndown.HibernationThreshold"
	// ConfigBurndownHibernationToDisk sets whether the hibernated RBTree allocator must be saved
	// on disk rather than kept in memory.
	ConfigBurndownHibernationToDisk = "Burndown.HibernationOnDisk"
	// ConfigBurndownHibernationDirectory sets the name of the temporary directory to use for
	// saving hibernated RBTree allocators.
	ConfigBurndownHibernationDirectory = "Burndown.HibernationDirectory"
	// ConfigBurndownDebug enables some extra debug assertions.
	ConfigBurndownDebug = "Burndown.Debug"
	// DefaultBurndownGranularity is the default number of ticks for BurndownAnalysis.Granularity
	// and BurndownAnalysis.Sampling.
	DefaultBurndownGranularity = 30
	// authorSelf is the internal author index which is used in BurndownAnalysis.Finalize() to
	// format the author overwrites matrix.
	authorSelf = identity.AuthorMissing - 1
)

type sparseHistory = map[int]map[int]int64

// DenseHistory is the matrix [number of samples][number of bands] -> number of lines.
//                                    y                  x
type DenseHistory = [][]int64

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (analyser *BurndownAnalysis) Name() string {
	return "Burndown"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (analyser *BurndownAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (analyser *BurndownAnalysis) Requires() []string {
	return []string{
		items.DependencyFileDiff, items.DependencyTreeChanges, items.DependencyBlobCache,
		items.DependencyTick, identity.DependencyAuthor}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (analyser *BurndownAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigBurndownGranularity,
		Description: "How many time ticks there are in a single band.",
		Flag:        "granularity",
		Type:        core.IntConfigurationOption,
		Default:     DefaultBurndownGranularity}, {
		Name:        ConfigBurndownSampling,
		Description: "How frequently to record the state in time ticks.",
		Flag:        "sampling",
		Type:        core.IntConfigurationOption,
		Default:     DefaultBurndownGranularity}, {
		Name:        ConfigBurndownTrackFiles,
		Description: "Record detailed statistics per each file.",
		Flag:        "burndown-files",
		Type:        core.BoolConfigurationOption,
		Default:     false}, {
		Name:        ConfigBurndownTrackPeople,
		Description: "Record detailed statistics per each developer.",
		Flag:        "burndown-people",
		Type:        core.BoolConfigurationOption,
		Default:     false}, {
		Name: ConfigBurndownHibernationThreshold,
		Description: "The minimum size for the allocated memory in each branch to be compressed." +
			"0 disables this optimization. Lower values trade CPU time more. Sane examples: Nx1000.",
		Flag:    "burndown-hibernation-threshold",
		Type:    core.IntConfigurationOption,
		Default: 0}, {
		Name: ConfigBurndownHibernationToDisk,
		Description: "Save hibernated RBTree allocators to disk rather than keep it in memory; " +
			"requires --burndown-hibernation-threshold to be greater than zero.",
		Flag:    "burndown-hibernation-disk",
		Type:    core.BoolConfigurationOption,
		Default: false}, {
		Name: ConfigBurndownHibernationDirectory,
		Description: "Temporary directory where to save the hibernated RBTree allocators; " +
			"requires --burndown-hibernation-disk.",
		Flag:    "burndown-hibernation-dir",
		Type:    core.PathConfigurationOption,
		Default: ""}, {
		Name:        ConfigBurndownDebug,
		Description: "Validate the trees at each step.",
		Flag:        "burndown-debug",
		Type:        core.BoolConfigurationOption,
		Default:     false},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (analyser *BurndownAnalysis) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		analyser.l = l
	} else {
		analyser.l = core.NewLogger()
	}
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
		if val, exists := facts[identity.FactIdentityDetectorPeopleCount].(int); exists {
			if val < 0 {
				return fmt.Errorf("PeopleNumber is negative: %d", val)
			}
			analyser.PeopleNumber = val
			analyser.reversedPeopleDict = facts[identity.FactIdentityDetectorReversedPeopleDict].([]string)
		}
	} else if exists {
		analyser.PeopleNumber = 0
	}
	if val, exists := facts[ConfigBurndownHibernationThreshold].(int); exists {
		analyser.HibernationThreshold = val
	}
	if val, exists := facts[ConfigBurndownHibernationToDisk].(bool); exists {
		analyser.HibernationToDisk = val
	}
	if val, exists := facts[ConfigBurndownHibernationDirectory].(string); exists {
		analyser.HibernationDirectory = val
	}
	if val, exists := facts[ConfigBurndownDebug].(bool); exists {
		analyser.Debug = val
	}
	if val, exists := facts[items.FactTickSize].(time.Duration); exists {
		analyser.TickSize = val
	}
	return nil
}

// Flag for the command line switch which enables this analysis.
func (analyser *BurndownAnalysis) Flag() string {
	return "burndown"
}

// Description returns the text which explains what the analysis is doing.
func (analyser *BurndownAnalysis) Description() string {
	return "Line burndown stats indicate the numbers of lines which were last edited within " +
		"specific time intervals through time. Search for \"git-of-theseus\" in the internet."
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (analyser *BurndownAnalysis) Initialize(repository *git.Repository) error {
	analyser.l = core.NewLogger()
	if analyser.Granularity <= 0 {
		analyser.l.Warnf("adjusted the granularity to %d ticks\n",
			DefaultBurndownGranularity)
		analyser.Granularity = DefaultBurndownGranularity
	}
	if analyser.Sampling <= 0 {
		analyser.l.Warnf("adjusted the sampling to %d ticks\n",
			DefaultBurndownGranularity)
		analyser.Sampling = DefaultBurndownGranularity
	}
	if analyser.Sampling > analyser.Granularity {
		analyser.l.Warnf("granularity may not be less than sampling, adjusted to %d\n",
			analyser.Granularity)
		analyser.Sampling = analyser.Granularity
	}
	if analyser.TickSize == 0 {
		def := items.DefaultTicksSinceStartTickSize * time.Hour
		analyser.l.Warnf("tick size was not set, adjusted to %v\n", def)
		analyser.TickSize = items.DefaultTicksSinceStartTickSize * time.Hour
	}
	analyser.repository = repository
	analyser.globalHistory = sparseHistory{}
	analyser.fileHistories = map[string]sparseHistory{}
	if analyser.PeopleNumber < 0 {
		return fmt.Errorf("PeopleNumber is negative: %d", analyser.PeopleNumber)
	}
	analyser.peopleHistories = make([]sparseHistory, analyser.PeopleNumber)
	analyser.files = map[string]*burndown.File{}
	analyser.fileAllocator = rbtree.NewAllocator()
	analyser.fileAllocator.HibernationThreshold = analyser.HibernationThreshold
	analyser.mergedFiles = map[string]bool{}
	analyser.mergedAuthor = identity.AuthorMissing
	analyser.renames = map[string]string{}
	analyser.deletions = map[string]bool{}
	analyser.matrix = make([]map[int]int64, analyser.PeopleNumber)
	analyser.tick = 0
	analyser.previousTick = 0
	return nil
}

// Consume runs this PipelineItem on the next commit's data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (analyser *BurndownAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if analyser.fileAllocator.Size() == 0 && len(analyser.files) > 0 {
		panic("BurndownAnalysis.Consume() was called on a hibernated instance")
	}
	author := deps[identity.DependencyAuthor].(int)
	tick := deps[items.DependencyTick].(int)
	if !deps[core.DependencyIsMerge].(bool) {
		analyser.tick = tick
		analyser.onNewTick()
	} else {
		// effectively disables the status updates if the commit is a merge
		// we will analyse the conflicts resolution in Merge()
		analyser.tick = burndown.TreeMergeMark
		analyser.mergedFiles = map[string]bool{}
		analyser.mergedAuthor = author
	}
	cache := deps[items.DependencyBlobCache].(map[plumbing.Hash]*items.CachedBlob)
	treeDiffs := deps[items.DependencyTreeChanges].(object.Changes)
	fileDiffs := deps[items.DependencyFileDiff].(map[string]items.FileDiffData)
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
	// in case there is a merge analyser.tick equals to TreeMergeMark
	analyser.tick = tick
	return nil, nil
}

// Fork clones this item. Everything is copied by reference except the files
// which are copied by value.
func (analyser *BurndownAnalysis) Fork(n int) []core.PipelineItem {
	result := make([]core.PipelineItem, n)
	for i := range result {
		clone := *analyser
		clone.files = map[string]*burndown.File{}
		clone.fileAllocator = clone.fileAllocator.Clone()
		for key, file := range analyser.files {
			clone.files[key] = file.CloneShallow(clone.fileAllocator)
		}
		result[i] = &clone
	}
	return result
}

// Merge combines several items together. We apply the special file merging logic here.
func (analyser *BurndownAnalysis) Merge(branches []core.PipelineItem) {
	all := make([]*BurndownAnalysis, len(branches)+1)
	all[0] = analyser
	for i, branch := range branches {
		all[i+1] = branch.(*BurndownAnalysis)
	}
	keys := map[string]bool{}
	for _, burn := range all {
		for key, val := range burn.mergedFiles {
			// (*)
			// there can be contradicting flags,
			// e.g. item was renamed and a new item written on its place
			// this may be not exactly accurate
			keys[key] = keys[key] || val
		}
	}
	for key, val := range keys {
		if !val {
			for _, burn := range all {
				if f, exists := burn.files[key]; exists {
					f.Delete()
				}
				delete(burn.files, key)
			}
			continue
		}
		files := make([]*burndown.File, 0, len(all))
		for _, burn := range all {
			file := burn.files[key]
			if file != nil {
				// file can be nil if it is considered binary in this branch
				files = append(files, file)
			}
		}
		if len(files) == 0 {
			// so we could be wrong in (*) and there is no such file eventually
			// it could be also removed in the merge commit itself
			continue
		}
		files[0].Merge(
			analyser.packPersonWithTick(analyser.mergedAuthor, analyser.tick),
			files[1:]...)
		for _, burn := range all {
			if burn.files[key] != files[0] {
				if burn.files[key] != nil {
					burn.files[key].Delete()
				}
				burn.files[key] = files[0].CloneDeep(burn.fileAllocator)
			}
		}
	}
	analyser.onNewTick()
}

// Hibernate compresses the bound RBTree memory with the files.
func (analyser *BurndownAnalysis) Hibernate() error {
	analyser.fileAllocator.Hibernate()
	if analyser.HibernationToDisk {
		file, err := ioutil.TempFile(analyser.HibernationDirectory, "*-hercules.bin")
		if err != nil {
			return err
		}
		analyser.hibernatedFileName = file.Name()
		err = file.Close()
		if err != nil {
			analyser.hibernatedFileName = ""
			return err
		}
		err = analyser.fileAllocator.Serialize(analyser.hibernatedFileName)
		if err != nil {
			analyser.hibernatedFileName = ""
			return err
		}
	}
	return nil
}

// Boot decompresses the bound RBTree memory with the files.
func (analyser *BurndownAnalysis) Boot() error {
	if analyser.hibernatedFileName != "" {
		err := analyser.fileAllocator.Deserialize(analyser.hibernatedFileName)
		if err != nil {
			return err
		}
		err = os.Remove(analyser.hibernatedFileName)
		if err != nil {
			return err
		}
		analyser.hibernatedFileName = ""
	}
	analyser.fileAllocator.Boot()
	return nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (analyser *BurndownAnalysis) Finalize() interface{} {
	globalHistory, lastTick := analyser.groupSparseHistory(analyser.globalHistory, -1)
	fileHistories := map[string]DenseHistory{}
	fileOwnership := map[string]map[int]int{}
	for key, history := range analyser.fileHistories {
		if len(history) == 0 {
			continue
		}
		fileHistories[key], _ = analyser.groupSparseHistory(history, lastTick)
		file := analyser.files[key]
		previousLine := 0
		previousAuthor := identity.AuthorMissing
		ownership := map[int]int{}
		fileOwnership[key] = ownership
		file.ForEach(func(line, value int) {
			length := line - previousLine
			if length > 0 {
				ownership[previousAuthor] += length
			}
			previousLine = line
			previousAuthor, _ = analyser.unpackPersonWithTick(int(value))
			if previousAuthor == identity.AuthorMissing {
				previousAuthor = -1
			}
		})
	}
	peopleHistories := make([]DenseHistory, analyser.PeopleNumber)
	for i, history := range analyser.peopleHistories {
		if len(history) > 0 {
			// there can be people with only trivial merge commits and without own lines
			peopleHistories[i], _ = analyser.groupSparseHistory(history, lastTick)
		} else {
			peopleHistories[i] = make(DenseHistory, len(globalHistory))
			for j, gh := range globalHistory {
				peopleHistories[i][j] = make([]int64, len(gh))
			}
		}
	}
	var peopleMatrix DenseHistory
	if len(analyser.matrix) > 0 {
		peopleMatrix = make(DenseHistory, analyser.PeopleNumber)
		for i, row := range analyser.matrix {
			mrow := make([]int64, analyser.PeopleNumber+2)
			peopleMatrix[i] = mrow
			for key, val := range row {
				if key == identity.AuthorMissing {
					key = -1
				} else if key == authorSelf {
					key = -2
				}
				mrow[key+2] = val
			}
		}
	}
	return BurndownResult{
		GlobalHistory:      globalHistory,
		FileHistories:      fileHistories,
		FileOwnership:      fileOwnership,
		PeopleHistories:    peopleHistories,
		PeopleMatrix:       peopleMatrix,
		tickSize:           analyser.TickSize,
		reversedPeopleDict: analyser.reversedPeopleDict,
		sampling:           analyser.Sampling,
		granularity:        analyser.Granularity,
	}
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (analyser *BurndownAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	burndownResult, ok := result.(BurndownResult)
	if !ok {
		return fmt.Errorf("result is not a burndown result: '%v'", result)
	}
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
	convertCSR := func(mat *pb.BurndownSparseMatrix) DenseHistory {
		res := make(DenseHistory, mat.NumberOfRows)
		for i := 0; i < int(mat.NumberOfRows); i++ {
			res[i] = make([]int64, mat.NumberOfColumns)
			for j := 0; j < len(mat.Rows[i].Columns); j++ {
				res[i][j] = int64(mat.Rows[i].Columns[j])
			}
		}
		return res
	}
	result := BurndownResult{
		GlobalHistory: convertCSR(msg.Project),
		FileHistories: map[string]DenseHistory{},
		FileOwnership: map[string]map[int]int{},
		tickSize:      time.Duration(msg.TickSize),

		granularity: int(msg.Granularity),
		sampling:    int(msg.Sampling),
	}
	for i, mat := range msg.Files {
		result.FileHistories[mat.Name] = convertCSR(mat)
		ownership := map[int]int{}
		result.FileOwnership[mat.Name] = ownership
		for key, val := range msg.FilesOwnership[i].Value {
			ownership[int(key)] = int(val)
		}
	}
	result.reversedPeopleDict = make([]string, len(msg.People))
	result.PeopleHistories = make([]DenseHistory, len(msg.People))
	for i, mat := range msg.People {
		result.PeopleHistories[i] = convertCSR(mat)
		result.reversedPeopleDict[i] = mat.Name
	}
	if msg.PeopleInteraction != nil {
		result.PeopleMatrix = make(DenseHistory, msg.PeopleInteraction.NumberOfRows)
	}
	for i := 0; i < len(result.PeopleMatrix); i++ {
		result.PeopleMatrix[i] = make([]int64, msg.PeopleInteraction.NumberOfColumns)
		for j := int(msg.PeopleInteraction.Indptr[i]); j < int(msg.PeopleInteraction.Indptr[i+1]); j++ {
			result.PeopleMatrix[i][msg.PeopleInteraction.Indices[j]] = msg.PeopleInteraction.Data[j]
		}
	}
	return result, nil
}

// MergeResults combines two BurndownResult-s together.
func (analyser *BurndownAnalysis) MergeResults(
	r1, r2 interface{}, c1, c2 *core.CommonAnalysisResult) interface{} {
	bar1 := r1.(BurndownResult)
	bar2 := r2.(BurndownResult)
	if bar1.tickSize != bar2.tickSize {
		return fmt.Errorf("mismatching tick sizes (r1: %d, r2: %d) received",
			bar1.tickSize, bar2.tickSize)
	}
	// for backwards-compatibility, if no tick size is present set to default
	analyser.TickSize = bar1.tickSize
	if analyser.TickSize == 0 {
		analyser.TickSize = items.DefaultTicksSinceStartTickSize * time.Hour
	}
	merged := BurndownResult{
		tickSize: analyser.TickSize,
	}
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
	var people map[string]identity.MergedIndex
	people, merged.reversedPeopleDict = identity.MergeReversedDictsIdentities(
		bar1.reversedPeopleDict, bar2.reversedPeopleDict)
	var wg sync.WaitGroup
	if len(bar1.GlobalHistory) > 0 || len(bar2.GlobalHistory) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			merged.GlobalHistory = analyser.mergeMatrices(
				bar1.GlobalHistory, bar2.GlobalHistory,
				bar1.granularity, bar1.sampling,
				bar2.granularity, bar2.sampling,
				bar1.tickSize,
				c1, c2)
		}()
	}
	// we don't merge files
	if len(merged.reversedPeopleDict) > 0 {
		if len(bar1.PeopleHistories) > 0 || len(bar2.PeopleHistories) > 0 {
			merged.PeopleHistories = make([]DenseHistory, len(merged.reversedPeopleDict))
			for i, key := range merged.reversedPeopleDict {
				ptrs := people[key]
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					var m1, m2 DenseHistory
					if ptrs.First >= 0 {
						m1 = bar1.PeopleHistories[ptrs.First]
					}
					if ptrs.Second >= 0 {
						m2 = bar2.PeopleHistories[ptrs.Second]
					}
					merged.PeopleHistories[i] = analyser.mergeMatrices(
						m1, m2,
						bar1.granularity, bar1.sampling,
						bar2.granularity, bar2.sampling,
						bar1.tickSize,
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
				if len(bar1.PeopleMatrix) > 0 {
					for i := len(bar1.reversedPeopleDict); i < len(merged.reversedPeopleDict); i++ {
						merged.PeopleMatrix = append(
							merged.PeopleMatrix, make([]int64, len(merged.reversedPeopleDict)+2))
					}
				}
			} else {
				merged.PeopleMatrix = make(DenseHistory, len(merged.reversedPeopleDict))
				for i := range merged.PeopleMatrix {
					merged.PeopleMatrix[i] = make([]int64, len(merged.reversedPeopleDict)+2)
				}
				for i, key := range bar1.reversedPeopleDict {
					mi := people[key].Final // index in merged.reversedPeopleDict
					copy(merged.PeopleMatrix[mi][:2], bar1.PeopleMatrix[i][:2])
					for j, val := range bar1.PeopleMatrix[i][2:] {
						merged.PeopleMatrix[mi][2+people[bar1.reversedPeopleDict[j]].Final] = val
					}
				}
				for i, key := range bar2.reversedPeopleDict {
					mi := people[key].Final // index in merged.reversedPeopleDict
					merged.PeopleMatrix[mi][0] += bar2.PeopleMatrix[i][0]
					merged.PeopleMatrix[mi][1] += bar2.PeopleMatrix[i][1]
					for j, val := range bar2.PeopleMatrix[i][2:] {
						merged.PeopleMatrix[mi][2+people[bar2.reversedPeopleDict[j]].Final] += val
					}
				}
			}
		}()
	}
	wg.Wait()
	return merged
}

func roundTime(t time.Time, d time.Duration, dir bool) int {
	if !dir {
		t = items.FloorTime(t, d)
	}
	ticks := float64(t.Unix()) / d.Seconds()
	if dir {
		return int(math.Ceil(ticks))
	}
	return int(math.Floor(ticks))
}

// mergeMatrices takes two [number of samples][number of bands] matrices,
// resamples them to ticks so that they become square, sums and resamples back to the
// least of (sampling1, sampling2) and (granularity1, granularity2).
func (analyser *BurndownAnalysis) mergeMatrices(
	m1, m2 DenseHistory, granularity1, sampling1, granularity2, sampling2 int, tickSize time.Duration,
	c1, c2 *core.CommonAnalysisResult) DenseHistory {
	commonMerged := c1.Copy()
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

	size := roundTime(commonMerged.EndTimeAsTime(), tickSize, true) -
		roundTime(commonMerged.BeginTimeAsTime(), tickSize, false)
	perTick := make([][]float32, size+granularity)
	for i := range perTick {
		perTick[i] = make([]float32, size+sampling)
	}
	if len(m1) > 0 {
		addBurndownMatrix(m1, granularity1, sampling1, perTick,
			roundTime(c1.BeginTimeAsTime(), tickSize, false)-roundTime(commonMerged.BeginTimeAsTime(), tickSize, false))
	}
	if len(m2) > 0 {
		addBurndownMatrix(m2, granularity2, sampling2, perTick,
			roundTime(c2.BeginTimeAsTime(), tickSize, false)-roundTime(commonMerged.BeginTimeAsTime(), tickSize, false))
	}

	// convert daily to [][]int64
	result := make(DenseHistory, (size+sampling-1)/sampling)
	for i := range result {
		result[i] = make([]int64, (size+granularity-1)/granularity)
		sampledIndex := (i+1)*sampling - 1
		for j := 0; j < len(result[i]); j++ {
			accum := float32(0)
			for k := j * granularity; k < (j+1)*granularity; k++ {
				accum += perTick[sampledIndex][k]
			}
			result[i][j] = int64(accum)
		}
	}
	return result
}

// Explode `matrix` so that it is daily sampled and has daily bands, shift by `offset` ticks
// and add to the accumulator. `daily` size is square and is guaranteed to fit `matrix` by
// the caller.
// Rows: *at least* len(matrix) * sampling + offset
// Columns: *at least* len(matrix[...]) * granularity + offset
// `matrix` can be sparse, so that the last columns which are equal to 0 are truncated.
func addBurndownMatrix(matrix DenseHistory, granularity, sampling int, accPerTick [][]float32, offset int) {
	// Determine the maximum number of bands; the actual one may be larger but we do not care
	maxCols := 0
	for _, row := range matrix {
		if maxCols < len(row) {
			maxCols = len(row)
		}
	}
	neededRows := len(matrix)*sampling + offset
	if len(accPerTick) < neededRows {
		log.Panicf("merge bug: too few per-tick rows: required %d, have %d",
			neededRows, len(accPerTick))
	}
	if len(accPerTick[0]) < maxCols {
		log.Panicf("merge bug: too few per-tick cols: required %d, have %d",
			maxCols, len(accPerTick[0]))
	}
	perTick := make([][]float32, len(accPerTick))
	for i, row := range accPerTick {
		perTick[i] = make([]float32, len(row))
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
					initial := perTick[startIndex-1+offset][i+offset]
					for j := startIndex; j < (y+1)*sampling; j++ {
						perTick[j+offset][i+offset] = initial * (1 + (k-1)*float32(j-startIndex+1)/scale)
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
						perTick[j+offset][i+offset] = avg
					}
				}
				// copy [x*g..y*s)
				for j := y * sampling; j < finishIndex; j++ {
					for i := x * granularity; i < y*sampling; i++ {
						perTick[j+offset][i+offset] = perTick[j-1+offset][i+offset]
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
							perTick[j+offset][i+offset] = avg
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
	for y := len(matrix) * sampling; y+offset < len(perTick); y++ {
		copy(perTick[y+offset], perTick[len(matrix)*sampling-1+offset])
	}
	// the original matrix has been resampled by tick
	// add it to the accumulator
	for y, row := range perTick {
		for x, val := range row {
			accPerTick[y][x] += val
		}
	}
}

func (analyser *BurndownAnalysis) serializeText(result *BurndownResult, writer io.Writer) {
	fmt.Fprintln(writer, "  granularity:", result.granularity)
	fmt.Fprintln(writer, "  sampling:", result.sampling)
	fmt.Fprintln(writer, "  tick_size:", int(result.tickSize.Seconds()))
	yaml.PrintMatrix(writer, result.GlobalHistory, 2, "project", true)
	if len(result.FileHistories) > 0 {
		fmt.Fprintln(writer, "  files:")
		keys := sortedKeys(result.FileHistories)
		for _, key := range keys {
			yaml.PrintMatrix(writer, result.FileHistories[key], 4, key, true)
		}
		fmt.Fprintln(writer, "  files_ownership:")
		okeys := make([]string, 0, len(result.FileOwnership))
		for key := range result.FileOwnership {
			okeys = append(okeys, key)
		}
		sort.Strings(okeys)
		for _, key := range okeys {
			owned := result.FileOwnership[key]
			devs := make([]int, 0, len(owned))
			for devi := range owned {
				devs = append(devs, devi)
			}
			sort.Slice(devs, func(i, j int) bool {
				return owned[devs[i]] > owned[devs[j]] // descending order
			})
			for x, devi := range devs {
				var indent string
				if x == 0 {
					indent = "- "
				} else {
					indent = "  "
				}
				fmt.Fprintf(writer, "    %s%d: %d\n", indent, devi, owned[devi])
			}
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
		TickSize:    int64(result.tickSize),
	}
	if len(result.GlobalHistory) > 0 {
		message.Project = pb.ToBurndownSparseMatrix(result.GlobalHistory, "project")
	}
	if len(result.FileHistories) > 0 {
		message.Files = make([]*pb.BurndownSparseMatrix, len(result.FileHistories))
		message.FilesOwnership = make([]*pb.FilesOwnership, len(result.FileHistories))
		keys := sortedKeys(result.FileHistories)
		i := 0
		for _, key := range keys {
			message.Files[i] = pb.ToBurndownSparseMatrix(result.FileHistories[key], key)
			ownership := map[int32]int32{}
			message.FilesOwnership[i] = &pb.FilesOwnership{Value: ownership}
			for key, val := range result.FileOwnership[key] {
				ownership[int32(key)] = int32(val)
			}
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
	}
	if result.PeopleMatrix != nil {
		message.PeopleInteraction = pb.DenseToCompressedSparseRowMatrix(result.PeopleMatrix)
	}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	_, err = writer.Write(serialized)
	return err
}

func sortedKeys(m map[string]DenseHistory) []string {
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

// We do a hack and store the tick in the first 14 bits and the author index in the last 18.
// Strictly speaking, int can be 64-bit and then the author index occupies 32+18 bits.
// This hack is needed to simplify the values storage inside File-s. We can compare
// different values together and they are compared as ticks for the same author.
func (analyser *BurndownAnalysis) packPersonWithTick(person int, tick int) int {
	if analyser.PeopleNumber == 0 {
		return tick
	}
	result := tick & burndown.TreeMergeMark
	result |= person << burndown.TreeMaxBinPower
	// This effectively means max (16383 - 1) ticks (>44 years) and (262143 - 3) devs.
	// One tick less because burndown.TreeMergeMark = ((1 << 14) - 1) is a special tick.
	// Three devs less because:
	// - math.MaxUint32 is the special rbtree value with tick == TreeMergeMark (-1)
	// - identity.AuthorMissing (-2)
	// - authorSelf (-3)
	return result
}

func (analyser *BurndownAnalysis) unpackPersonWithTick(value int) (int, int) {
	if analyser.PeopleNumber == 0 {
		return identity.AuthorMissing, value
	}
	return value >> burndown.TreeMaxBinPower, value & burndown.TreeMergeMark
}

func (analyser *BurndownAnalysis) onNewTick() {
	if analyser.tick > analyser.previousTick {
		analyser.previousTick = analyser.tick
	}
	analyser.mergedAuthor = identity.AuthorMissing
}

func (analyser *BurndownAnalysis) updateGlobal(currentTime, previousTime, delta int) {
	_, curTick := analyser.unpackPersonWithTick(currentTime)
	_, prevTick := analyser.unpackPersonWithTick(previousTime)

	currentHistory := analyser.globalHistory[curTick]
	if currentHistory == nil {
		currentHistory = map[int]int64{}
		analyser.globalHistory[curTick] = currentHistory
	}
	currentHistory[prevTick] += int64(delta)
}

// updateFile is bound to the specific `history` in the closure.
func (analyser *BurndownAnalysis) updateFile(
	history sparseHistory, currentTime, previousTime, delta int) {

	_, curTick := analyser.unpackPersonWithTick(currentTime)
	_, prevTick := analyser.unpackPersonWithTick(previousTime)

	currentHistory := history[curTick]
	if currentHistory == nil {
		currentHistory = map[int]int64{}
		history[curTick] = currentHistory
	}
	currentHistory[prevTick] += int64(delta)
}

func (analyser *BurndownAnalysis) updateAuthor(currentTime, previousTime, delta int) {
	previousAuthor, prevTick := analyser.unpackPersonWithTick(previousTime)
	if previousAuthor == identity.AuthorMissing {
		return
	}
	_, curTick := analyser.unpackPersonWithTick(currentTime)
	history := analyser.peopleHistories[previousAuthor]
	if history == nil {
		history = sparseHistory{}
		analyser.peopleHistories[previousAuthor] = history
	}
	currentHistory := history[curTick]
	if currentHistory == nil {
		currentHistory = map[int]int64{}
		history[curTick] = currentHistory
	}
	currentHistory[prevTick] += int64(delta)
}

func (analyser *BurndownAnalysis) updateMatrix(currentTime, previousTime, delta int) {
	newAuthor, _ := analyser.unpackPersonWithTick(currentTime)
	oldAuthor, _ := analyser.unpackPersonWithTick(previousTime)

	if oldAuthor == identity.AuthorMissing {
		return
	}
	if newAuthor == oldAuthor && delta > 0 {
		newAuthor = authorSelf
	}
	row := analyser.matrix[oldAuthor]
	if row == nil {
		row = map[int]int64{}
		analyser.matrix[oldAuthor] = row
	}
	cell, exists := row[newAuthor]
	if !exists {
		row[newAuthor] = 0
		cell = 0
	}
	row[newAuthor] = cell + int64(delta)
}

func (analyser *BurndownAnalysis) newFile(
	hash plumbing.Hash, name string, author int, tick int, size int) (*burndown.File, error) {

	updaters := make([]burndown.Updater, 1)
	updaters[0] = analyser.updateGlobal
	if analyser.TrackFiles {
		history := analyser.fileHistories[name]
		if history == nil {
			// can be not nil if the file was created in a future branch
			history = sparseHistory{}
		}
		analyser.fileHistories[name] = history
		updaters = append(updaters, func(currentTime, previousTime, delta int) {
			analyser.updateFile(history, currentTime, previousTime, delta)
		})
	}
	if analyser.PeopleNumber > 0 {
		updaters = append(updaters, analyser.updateAuthor)
		updaters = append(updaters, analyser.updateMatrix)
		tick = analyser.packPersonWithTick(author, tick)
	}
	return burndown.NewFile(tick, size, analyser.fileAllocator, updaters...), nil
}

func (analyser *BurndownAnalysis) handleInsertion(
	change *object.Change, author int, cache map[plumbing.Hash]*items.CachedBlob) error {
	blob := cache[change.To.TreeEntry.Hash]
	lines, err := blob.CountLines()
	if err != nil {
		// binary
		return nil
	}
	name := change.To.Name
	file, exists := analyser.files[name]
	if exists {
		return fmt.Errorf("file %s already exists", name)
	}
	var hash plumbing.Hash
	if analyser.tick != burndown.TreeMergeMark {
		hash = blob.Hash
	}
	file, err = analyser.newFile(hash, name, author, analyser.tick, lines)
	analyser.files[name] = file
	delete(analyser.deletions, name)
	if analyser.tick == burndown.TreeMergeMark {
		analyser.mergedFiles[name] = true
	}
	return err
}

func (analyser *BurndownAnalysis) handleDeletion(
	change *object.Change, author int, cache map[plumbing.Hash]*items.CachedBlob) error {

	var name string
	if change.To.TreeEntry.Hash != plumbing.ZeroHash {
		// became binary
		name = change.To.Name
	} else {
		name = change.From.Name
	}
	file, exists := analyser.files[name]
	blob := cache[change.From.TreeEntry.Hash]
	lines, err := blob.CountLines()
	if exists && err != nil {
		return fmt.Errorf("previous version of %s unexpectedly became binary", name)
	}
	if !exists {
		return nil
	}
	// Parallel independent file removals are incorrectly handled. The solution seems to be quite
	// complex, but feel free to suggest your ideas.
	// These edge cases happen *very* rarely, so we don't bother for now.
	tick := analyser.tick
	// Are we merging and this file has never been actually deleted in any branch?
	if analyser.tick == burndown.TreeMergeMark && !analyser.deletions[name] {
		tick = 0
		// Early removal in one branch with pre-merge changes in another is not handled correctly.
	}
	analyser.deletions[name] = true
	file.Update(analyser.packPersonWithTick(author, tick), 0, 0, lines)
	file.Delete()
	delete(analyser.files, name)
	delete(analyser.fileHistories, name)
	stack := []string{name}
	for len(stack) > 0 {
		head := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		analyser.renames[head] = ""
		for key, val := range analyser.renames {
			if val == head {
				stack = append(stack, key)
			}
		}
	}
	if analyser.tick == burndown.TreeMergeMark {
		analyser.mergedFiles[name] = false
	}
	return nil
}

func (analyser *BurndownAnalysis) handleModification(
	change *object.Change, author int, cache map[plumbing.Hash]*items.CachedBlob,
	diffs map[string]items.FileDiffData) error {

	if analyser.tick == burndown.TreeMergeMark {
		analyser.mergedFiles[change.To.Name] = true
	}
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

	// Check for binary changes
	blobFrom := cache[change.From.TreeEntry.Hash]
	_, errFrom := blobFrom.CountLines()
	blobTo := cache[change.To.TreeEntry.Hash]
	_, errTo := blobTo.CountLines()
	if errFrom != errTo {
		if errFrom != nil {
			// the file is no longer binary
			return analyser.handleInsertion(change, author, cache)
		}
		// the file became binary
		return analyser.handleDeletion(change, author, cache)
	} else if errFrom != nil {
		// what are we doing here?!
		return nil
	}

	thisDiffs := diffs[change.To.Name]
	if file.Len() != thisDiffs.OldLinesOfCode {
		analyser.l.Infof("====TREE====\n%s", file.Dump())
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
			file.Update(analyser.packPersonWithTick(author, analyser.tick), position, length, 0)
			position += length
		} else {
			file.Update(analyser.packPersonWithTick(author, analyser.tick), position, 0, length)
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
			analyser.l.Errorf("%s: internal diff error\n", change.To.Name)
			analyser.l.Errorf("Update(%d, %d, %d (0), %d (0))\n", analyser.tick, position,
				length, utf8.RuneCountInString(pending.Text))
			if dumpBefore != "" {
				analyser.l.Errorf("====TREE BEFORE====\n%s====END====\n", dumpBefore)
			}
			analyser.l.Errorf("====TREE AFTER====\n%s====END====\n", file.Dump())
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
				file.Update(analyser.packPersonWithTick(author, analyser.tick), position, length,
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
		return fmt.Errorf("%s: internal integrity error dst %d != %d %s -> %s",
			change.To.Name, thisDiffs.NewLinesOfCode, file.Len(),
			change.From.TreeEntry.Hash.String(), change.To.TreeEntry.Hash.String())
	}
	return nil
}

func (analyser *BurndownAnalysis) handleRename(from, to string) error {
	if from == to {
		return nil
	}
	file, exists := analyser.files[from]
	if !exists {
		return fmt.Errorf("file %s > %s does not exist (files)", from, to)
	}
	delete(analyser.files, from)
	analyser.files[to] = file
	delete(analyser.deletions, to)
	if analyser.tick == burndown.TreeMergeMark {
		analyser.mergedFiles[from] = false
	}

	if analyser.TrackFiles {
		history := analyser.fileHistories[from]
		if history == nil {
			var futureRename string
			if _, exists := analyser.renames[""]; exists {
				panic("burndown renames tracking corruption")
			}
			known := map[string]bool{}
			newRename, exists := analyser.renames[from]
			known[from] = true
			for exists {
				futureRename = newRename
				newRename, exists = analyser.renames[futureRename]
				if known[newRename] {
					// infinite cycle
					futureRename = ""
					for key := range known {
						if analyser.fileHistories[key] != nil {
							futureRename = key
							break
						}
					}
					break
				}
				known[futureRename] = true
			}
			// a future branch could have already renamed it and we are retarded
			if futureRename == "" {
				// the file will be deleted in the future, whatever
				history = sparseHistory{}
			} else {
				history = analyser.fileHistories[futureRename]
				if history == nil {
					return fmt.Errorf("file %s > %s (%s) does not exist (histories)",
						from, to, futureRename)
				}
			}
		}
		delete(analyser.fileHistories, from)
		analyser.fileHistories[to] = history
	}
	analyser.renames[from] = to
	return nil
}

func (analyser *BurndownAnalysis) groupSparseHistory(
	history sparseHistory, lastTick int) (DenseHistory, int) {

	if len(history) == 0 {
		panic("empty history")
	}
	var ticks []int
	for tick := range history {
		ticks = append(ticks, tick)
	}
	sort.Ints(ticks)
	if lastTick >= 0 {
		if ticks[len(ticks)-1] < lastTick {
			ticks = append(ticks, lastTick)
		} else if ticks[len(ticks)-1] > lastTick {
			panic("ticks corruption")
		}
	} else {
		lastTick = ticks[len(ticks)-1]
	}
	// [y][x]
	// y - sampling
	// x - granularity
	samples := lastTick/analyser.Sampling + 1
	bands := lastTick/analyser.Granularity + 1
	result := make(DenseHistory, samples)
	for i := 0; i < bands; i++ {
		result[i] = make([]int64, bands)
	}
	prevsi := 0
	for _, tick := range ticks {
		si := tick / analyser.Sampling
		if si > prevsi {
			state := result[prevsi]
			for i := prevsi + 1; i <= si; i++ {
				copy(result[i], state)
			}
			prevsi = si
		}
		sample := result[si]
		for t, value := range history[tick] {
			sample[t/analyser.Granularity] += value
		}
	}
	return result, lastTick
}

// GetTickSize returns the tick size used to generate this burndown analysis result.
func (br BurndownResult) GetTickSize() time.Duration {
	return br.tickSize
}

// GetIdentities returns the list of developer identities used to generate this burndown analysis result.
// The format is |-joined keys, see internals/plumbing/identity for details.
func (br BurndownResult) GetIdentities() []string {
	return br.reversedPeopleDict
}

func init() {
	core.Registry.Register(&BurndownAnalysis{})
}
