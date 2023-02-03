package leaves

import (
	"fmt"
	"github.com/cyraxred/hercules/internal/linehistory"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/cyraxred/hercules/internal/burndown"
	"github.com/cyraxred/hercules/internal/core"
	"github.com/cyraxred/hercules/internal/pb"
	items "github.com/cyraxred/hercules/internal/plumbing"
	"github.com/cyraxred/hercules/internal/plumbing/identity"
	"github.com/cyraxred/hercules/internal/yaml"
	"github.com/go-git/go-git/v5"
	"github.com/gogo/protobuf/proto"
)

// BurndownAnalysis allows to gather the line burndown statistics for a Git repository.
// It is a LeafPipelineItem.
// Reference: https://erikbern.com/2016/12/05/the-half-life-of-code.html
type BurndownAnalysis struct {
	core.NoopMerger
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
	fileHistories map[linehistory.FileId]sparseHistory
	// peopleHistories is the daily deltas of each person's daily line counts.
	peopleHistories []sparseHistory
	// matrix is the mutual deletions and self insertions.
	matrix []map[linehistory.AuthorId]int64

	// TickSize indicates the size of each time granule: day, hour, week, etc.
	tickSize time.Duration
	// references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string

	fileResolver linehistory.FileIdResolver

	l core.Logger
}

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
	return []string{linehistory.DependencyLineHistory, identity.DependencyAuthor}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (analyser *BurndownAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	return BurndownSharedOptions[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (analyser *BurndownAnalysis) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		analyser.l = l
	} else {
		analyser.l = core.NewLogger()
	}

	if val, exists := facts[items.FactTickSize].(time.Duration); exists {
		analyser.tickSize = val
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

	return nil
}

func (analyser *BurndownAnalysis) ConfigureUpstream(_ map[string]interface{}) error {
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
	analyser.repository = repository
	analyser.globalHistory = sparseHistory{}
	analyser.fileHistories = map[linehistory.FileId]sparseHistory{}
	if analyser.PeopleNumber < 0 {
		return fmt.Errorf("PeopleNumber is negative: %d", analyser.PeopleNumber)
	}
	analyser.peopleHistories = make([]sparseHistory, analyser.PeopleNumber)
	analyser.matrix = make([]map[linehistory.AuthorId]int64, analyser.PeopleNumber)

	return nil
}

func (analyser *BurndownAnalysis) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(analyser, n)
}

func (analyser *BurndownAnalysis) Merge(branches []core.PipelineItem) {
	//for _, branch := range branches {
	//	clone := branch.(*BurndownAnalysis)
	//	for id, fileHistory := range clone.fileHistories
	//}
}

type LineHistoryChange = linehistory.LineHistoryChange

// Consume runs this PipelineItem on the next commits data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (analyser *BurndownAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {

	changes := deps[linehistory.DependencyLineHistory].(linehistory.LineHistoryChanges)
	analyser.fileResolver = changes.Resolver

	for _, change := range changes.Changes {
		if change.IsDelete() {
			if analyser.TrackFiles {
				analyser.updateFileDelete(change)
			}
			continue
		}
		if int(change.PrevAuthor) >= analyser.PeopleNumber && change.PrevAuthor != identity.AuthorMissing {
			change.PrevAuthor = identity.AuthorMissing
		}
		if int(change.CurrAuthor) >= analyser.PeopleNumber && change.CurrAuthor != identity.AuthorMissing {
			change.CurrAuthor = identity.AuthorMissing
		}
		analyser.updateGlobal(change)

		if analyser.TrackFiles {
			analyser.updateFile(change)
		}

		analyser.updateAuthor(change)
		analyser.updateChurnMatrix(change)
	}

	return nil, nil
}

func (analyser *BurndownAnalysis) updateGlobal(change LineHistoryChange) {
	analyser.globalHistory.updateDelta(int(change.PrevTick), int(change.CurrTick), change.Delta)
}

// updateFile is bound to the specific `history` in the closure.
func (analyser *BurndownAnalysis) updateFile(change LineHistoryChange) {

	history := analyser.fileHistories[change.FileId]
	if history == nil {
		// can be not nil if the file was created in a future branch
		history = sparseHistory{}
		analyser.fileHistories[change.FileId] = history
	}

	history.updateDelta(int(change.PrevTick), int(change.CurrTick), change.Delta)
}

func (analyser *BurndownAnalysis) updateFileDelete(change LineHistoryChange) {
	delete(analyser.fileHistories, change.FileId)
}

func (analyser *BurndownAnalysis) updateAuthor(change LineHistoryChange) {
	if change.PrevAuthor == identity.AuthorMissing {
		return
	}

	history := analyser.peopleHistories[change.PrevAuthor]
	if history == nil {
		history = sparseHistory{}
		analyser.peopleHistories[change.PrevAuthor] = history
	}

	history.updateDelta(int(change.PrevTick), int(change.CurrTick), change.Delta)
}

func (analyser *BurndownAnalysis) updateChurnMatrix(change LineHistoryChange) {
	if change.PrevAuthor == identity.AuthorMissing {
		return
	}

	newAuthor := change.CurrAuthor
	if change.Delta > 0 {
		newAuthor = authorSelf
	}
	row := analyser.matrix[change.PrevAuthor]
	if row == nil {
		row = map[linehistory.AuthorId]int64{}
		analyser.matrix[change.PrevAuthor] = row
	}
	row[newAuthor] += int64(change.Delta)
}

// Finalize returns the result of the analysis. Further calls to Consume() are not expected.
func (analyser *BurndownAnalysis) Finalize() interface{} {
	globalHistory, lastTick := analyser.groupSparseHistory(analyser.globalHistory, -1)

	fileHistories := map[string]burndown.DenseHistory{}
	fileOwnership := map[string]map[int]int{}
	for fileId, history := range analyser.fileHistories {
		if len(history) == 0 {
			continue
		}
		if fileName := analyser.fileResolver.NameOf(fileId); fileName != "" {
			fileHistories[fileName], _ = analyser.groupSparseHistory(history, lastTick)
		}
	}

	peopleHistories := make([]burndown.DenseHistory, analyser.PeopleNumber)

	if analyser.PeopleNumber > 0 {
		analyser.collectFileOwnership(fileOwnership)

		for i, history := range analyser.peopleHistories {
			if len(history) > 0 {
				// there can be people with only trivial merge commits and without own lines
				peopleHistories[i], _ = analyser.groupSparseHistory(history, lastTick)
			} else {
				peopleHistories[i] = make(burndown.DenseHistory, len(globalHistory))
				for j, gh := range globalHistory {
					peopleHistories[i][j] = make([]int64, len(gh))
				}
			}
		}
	}

	var peopleMatrix burndown.DenseHistory
	if len(analyser.matrix) > 0 {
		peopleMatrix = make(burndown.DenseHistory, analyser.PeopleNumber)
		for i, row := range analyser.matrix {
			pRow := make([]int64, analyser.PeopleNumber+2)
			peopleMatrix[i] = pRow
			for key, val := range row {
				if key == identity.AuthorMissing {
					key = -1
				} else if key == authorSelf {
					key = -2
				}
				pRow[key+2] = val
			}
		}
	}

	return BurndownResult{
		GlobalHistory:      globalHistory,
		FileHistories:      fileHistories,
		FileOwnership:      fileOwnership,
		PeopleHistories:    peopleHistories,
		PeopleMatrix:       peopleMatrix,
		tickSize:           analyser.tickSize,
		reversedPeopleDict: analyser.reversedPeopleDict,
		sampling:           analyser.Sampling,
		granularity:        analyser.Granularity,
	}
}

func (analyser *BurndownAnalysis) collectFileOwnership(fileOwnership map[string]map[int]int) {
	analyser.fileResolver.ForEachFile(func(fileId linehistory.FileId, fileName string) {
		previousLine := 0
		previousAuthor := identity.AuthorMissing
		ownership := map[int]int{}

		if analyser.fileResolver.ScanFile(fileId,
			func(line int, tick linehistory.TickNumber, author linehistory.AuthorId) {
				length := line - previousLine
				if length > 0 {
					ownership[previousAuthor] += length
				}
				previousLine = line
				if author == identity.AuthorMissing || int(author) >= analyser.PeopleNumber {
					previousAuthor = -1
				} else {
					previousAuthor = int(author)
				}
			}) {
			fileOwnership[fileName] = ownership
		}
	})
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
func (analyser *BurndownAnalysis) Deserialize(message []byte) (interface{}, error) {
	msg := pb.BurndownAnalysisResults{}
	err := proto.Unmarshal(message, &msg)
	if err != nil {
		return nil, err
	}
	convertCSR := func(mat *pb.BurndownSparseMatrix) burndown.DenseHistory {
		res := make(burndown.DenseHistory, mat.NumberOfRows)
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
		FileHistories: map[string]burndown.DenseHistory{},
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
	result.PeopleHistories = make([]burndown.DenseHistory, len(msg.People))
	for i, mat := range msg.People {
		result.PeopleHistories[i] = convertCSR(mat)
		result.reversedPeopleDict[i] = mat.Name
	}
	if msg.PeopleInteraction != nil {
		result.PeopleMatrix = make(burndown.DenseHistory, msg.PeopleInteraction.NumberOfRows)
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
	merged := BurndownResult{
		tickSize: bar1.tickSize,
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
	var sem = make(chan int, 5) // with large files not limiting number of GoRoutines eats 200G of RAM on large merges
	if len(bar1.GlobalHistory) > 0 || len(bar2.GlobalHistory) > 0 {
		wg.Add(1)
		sem <- 1
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			merged.GlobalHistory = burndown.MergeBurndownMatrices(
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
			merged.PeopleHistories = make([]burndown.DenseHistory, len(merged.reversedPeopleDict))
			for i, key := range merged.reversedPeopleDict {
				ptrs := people[key]
				wg.Add(1)
				sem <- 1
				go func(i int) {
					defer wg.Done()
					defer func() { <-sem }()
					var m1, m2 burndown.DenseHistory
					if ptrs.First >= 0 {
						m1 = bar1.PeopleHistories[ptrs.First]
					}
					if ptrs.Second >= 0 {
						m2 = bar2.PeopleHistories[ptrs.Second]
					}
					merged.PeopleHistories[i] = burndown.MergeBurndownMatrices(
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
		sem <- 1
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
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
				merged.PeopleMatrix = make(burndown.DenseHistory, len(merged.reversedPeopleDict))
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

func (analyser *BurndownAnalysis) serializeText(result *BurndownResult, writer io.Writer) {
	_, _ = fmt.Fprintln(writer, "  granularity:", result.granularity)
	_, _ = fmt.Fprintln(writer, "  sampling:", result.sampling)
	_, _ = fmt.Fprintln(writer, "  tick_size:", int(result.tickSize.Seconds()))
	yaml.PrintMatrix(writer, result.GlobalHistory, 2, "project", true)
	if len(result.FileHistories) > 0 {
		_, _ = fmt.Fprintln(writer, "  files:")
		keys := sortedKeys(result.FileHistories)
		for _, key := range keys {
			yaml.PrintMatrix(writer, result.FileHistories[key], 4, key, true)
		}
		_, _ = fmt.Fprintln(writer, "  files_ownership:")
		oKeys := make([]string, 0, len(result.FileOwnership))
		for key := range result.FileOwnership {
			oKeys = append(oKeys, key)
		}
		sort.Strings(oKeys)
		for _, key := range oKeys {
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
				_, _ = fmt.Fprintf(writer, "    %s%d: %d\n", indent, devi, owned[devi])
			}
		}
	}

	if len(result.PeopleHistories) > 0 {
		_, _ = fmt.Fprintln(writer, "  people_sequence:")
		for key := range result.PeopleHistories {
			_, _ = fmt.Fprintln(writer, "    - "+yaml.SafeString(result.reversedPeopleDict[key]))
		}
		_, _ = fmt.Fprintln(writer, "  people:")
		for key, val := range result.PeopleHistories {
			yaml.PrintMatrix(writer, val, 4, result.reversedPeopleDict[key], true)
		}
		_, _ = fmt.Fprintln(writer, "  people_interaction: |-")
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

func (analyser *BurndownAnalysis) groupSparseHistory(
	history sparseHistory, lastTick int) (burndown.DenseHistory, int) {

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
	result := make(burndown.DenseHistory, samples)
	for i := 0; i < bands; i++ {
		result[i] = make([]int64, bands)
	}
	prevSi := 0
	for _, tick := range ticks {
		si := tick / analyser.Sampling
		if si > prevSi {
			state := result[prevSi]
			for i := prevSi + 1; i <= si; i++ {
				copy(result[i], state)
			}
			prevSi = si
		}
		sample := result[si]
		for t, value := range history[tick].deltas {
			sample[t/analyser.Granularity] += value
		}
	}
	return result, lastTick
}

func init() {
	core.Registry.Register(&BurndownAnalysis{})
}
