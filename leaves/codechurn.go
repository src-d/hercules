package leaves

import (
	"fmt"
	"github.com/cyraxred/hercules/internal/linehistory"
	"io"
	"math"
	"time"

	"github.com/cyraxred/hercules/internal/core"
	items "github.com/cyraxred/hercules/internal/plumbing"
	"github.com/cyraxred/hercules/internal/plumbing/identity"
	"github.com/go-git/go-git/v5"
)

// CodeChurnAnalysis allows to gather the code churn statistics for a Git repository.
// It is a LeafPipelineItem.
type CodeChurnAnalysis struct {
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

	// TickSize indicates the size of each time granule: day, hour, week, etc.
	tickSize time.Duration
	// references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string

	// code churns indexed by people
	codeChurns  []personChurnStats
	churnDeltas map[churnDeltaKey]churnDelta

	fileResolver linehistory.FileIdResolver

	l core.Logger
}

type churnDeletedFileEntry struct {
	fileId    linehistory.FileId
	deletedAt int
	entry     churnFileEntry
}

type personChurnStats struct {
	files map[linehistory.FileId]churnFileEntry
}

func (p *personChurnStats) getFileEntry(id linehistory.FileId) (entry churnFileEntry) {
	if p.files != nil {
		entry = p.files[id]
		if entry.deleteHistory != nil {
			return
		}
	} else {
		p.files = map[linehistory.FileId]churnFileEntry{}
	}
	entry.deleteHistory = map[linehistory.AuthorId]sparseHistory{}
	return
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (analyser *CodeChurnAnalysis) Name() string {
	return "CodeChurn"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (analyser *CodeChurnAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (analyser *CodeChurnAnalysis) Requires() []string {
	return []string{linehistory.DependencyLineHistory, identity.DependencyAuthor}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (analyser *CodeChurnAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	return BurndownSharedOptions[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (analyser *CodeChurnAnalysis) Configure(facts map[string]interface{}) error {
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
	if val, exists := facts[identity.FactIdentityDetectorPeopleCount].(int); exists {
		if val < 0 {
			return fmt.Errorf("PeopleNumber is negative: %d", val)
		}
		analyser.PeopleNumber = val
		analyser.reversedPeopleDict = facts[identity.FactIdentityDetectorReversedPeopleDict].([]string)
	}

	return nil
}

func (analyser *CodeChurnAnalysis) ConfigureUpstream(_ map[string]interface{}) error {
	return nil
}

// Flag for the command line switch which enables this analysis.
func (analyser *CodeChurnAnalysis) Flag() string {
	return "codechurn"
}

// Description returns the text which explains what the analysis is doing.
func (analyser *CodeChurnAnalysis) Description() string {
	// TODO description
	return "Line burndown stats indicate the numbers of lines which were last edited within " +
		"specific time intervals through time. Search for \"git-of-theseus\" in the internet."
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (analyser *CodeChurnAnalysis) Initialize(repository *git.Repository) error {
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

	if analyser.PeopleNumber < 0 {
		return fmt.Errorf("PeopleNumber is negative: %d", analyser.PeopleNumber)
	}
	analyser.codeChurns = make([]personChurnStats, analyser.PeopleNumber)
	analyser.churnDeltas = map[churnDeltaKey]churnDelta{}

	return nil
}

func (analyser *CodeChurnAnalysis) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(analyser, n)
}

// Consume runs this PipelineItem on the next commits data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (analyser *CodeChurnAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {

	changes := deps[linehistory.DependencyLineHistory].(linehistory.LineHistoryChanges)
	analyser.fileResolver = changes.Resolver

	for _, change := range changes.Changes {
		if change.IsDelete() {
			continue
		}
		if int(change.PrevAuthor) >= analyser.PeopleNumber && change.PrevAuthor != identity.AuthorMissing {
			change.PrevAuthor = identity.AuthorMissing
		}
		if int(change.CurrAuthor) >= analyser.PeopleNumber && change.CurrAuthor != identity.AuthorMissing {
			change.CurrAuthor = identity.AuthorMissing
		}

		analyser.updateAuthor(change)
	}

	return nil, nil
}

type churnDeltaKey struct {
	linehistory.AuthorId
	linehistory.FileId
}

type churnDelta struct {
	lastTouch linehistory.TickNumber
	churnLines
}

type churnLines struct {
	inserted        int32
	deletedBySelf   int32
	deletedByOthers int32
	//	deletedAtOthers int32
}

type churnFileEntry struct {
	insertedLines int32
	ownedLines    int32
	memorability  float32
	awareness     float32

	deleteHistory map[linehistory.AuthorId]sparseHistory
}

func (analyser *CodeChurnAnalysis) updateAwareness(change LineHistoryChange, fileEntry *churnFileEntry) {
	lineDelta := int32(change.Delta)

	deltaKey := churnDeltaKey{change.PrevAuthor, change.FileId}
	delta, hasDelta := analyser.churnDeltas[deltaKey]

	if delta.lastTouch != change.CurrTick {
		if hasDelta {
			if change.PrevAuthor != change.CurrAuthor {
				delta.deletedByOthers -= lineDelta
				lineDelta = 0
			}
			awareness, memorability := analyser.calculateAwareness(*fileEntry, change, delta.lastTouch, delta.churnLines)
			fileEntry.awareness, fileEntry.memorability = float32(awareness), float32(memorability)
		}
		if lineDelta == 0 {
			delete(analyser.churnDeltas, deltaKey)
			return
		}

		delta = churnDelta{lastTouch: change.CurrTick}
	}

	if change.PrevAuthor != change.CurrAuthor {
		if lineDelta < 0 {
			delta.deletedByOthers -= lineDelta
		}
	} else {
		if lineDelta > 0 {
			delta.inserted += lineDelta
		} else {
			delta.deletedBySelf -= lineDelta
		}
	}
	analyser.churnDeltas[deltaKey] = delta
}

func (analyser *CodeChurnAnalysis) updateAuthor(change LineHistoryChange) {
	if change.PrevAuthor == identity.AuthorMissing || change.Delta == 0 {
		return
	}

	fileEntry := analyser.codeChurns[change.PrevAuthor].getFileEntry(change.FileId)

	analyser.updateAwareness(change, &fileEntry)

	lineDelta := int32(change.Delta)
	fileEntry.ownedLines += lineDelta
	if change.Delta > 0 {
		// PrevAuthor == CurrAuthor
		fileEntry.insertedLines += lineDelta
	} else {
		history := fileEntry.deleteHistory[change.CurrAuthor]
		if history == nil {
			history = sparseHistory{}
			fileEntry.deleteHistory[change.CurrAuthor] = history
		}
		history.updateDelta(int(change.PrevTick), int(change.CurrTick), change.Delta)
	}

	analyser.codeChurns[change.PrevAuthor].files[change.FileId] = fileEntry
}

// Finalize returns the result of the analysis. Further calls to Consume() are not expected.
func (analyser *CodeChurnAnalysis) Finalize() interface{} {

	println()
	for pId, person := range analyser.codeChurns {
		inserted := int32(0)
		deletedBySelf := int32(0)
		deletedByOthers := int32(0)

		for _, entry := range person.files {
			inserted += entry.insertedLines
			//deletedBySelf += entry.deletedBySelf
			//deletedByOthers += entry.deletedByOthers
		}

		name := ""
		if pId >= 0 {
			name = analyser.reversedPeopleDict[pId]
		}
		fmt.Printf("%s (%d):\t\t%d\t%d\t%d = %d\n", name, pId, inserted, deletedBySelf, deletedByOthers,
			inserted+deletedBySelf+deletedByOthers)
	}
	println()

	return nil
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (analyser *CodeChurnAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	return nil
}

// Deserialize converts the specified protobuf bytes to BurndownResult.
func (analyser *CodeChurnAnalysis) Deserialize(message []byte) (interface{}, error) {
	return nil, nil
}

// MergeResults combines two BurndownResult-s together.
func (analyser *CodeChurnAnalysis) MergeResults(
	r1, r2 interface{}, c1, c2 *core.CommonAnalysisResult) interface{} {
	return nil
}

func (analyser *CodeChurnAnalysis) memoryLoss(x float64) float64 {
	const halfLossPeriod = 30
	return 1.0 / (1.0 + math.Exp(x-halfLossPeriod))
}

func (analyser *CodeChurnAnalysis) calculateAwareness(entry churnFileEntry, change LineHistoryChange,
	lastTouch linehistory.TickNumber, delta churnLines) (awareness, memorability float64) {

	const awarenessLowCut = 0.5
	const memorabilityMin = 0.5

	if entry.insertedLines == 0 {
		// initial
		return 0, memorabilityMin
	}
	awareness, memorability = float64(entry.awareness), float64(entry.memorability)
	if lastTouch >= change.CurrTick {
		return
	}

	ownedLines := 0.0
	if entry.ownedLines > 0 {
		ownedLines = float64(entry.ownedLines)
		awareness = math.Max(0, awareness*
			float64(entry.ownedLines-delta.deletedByOthers-delta.deletedBySelf)/ownedLines)
	}
	awareness += float64(delta.inserted)

	timeDelta := float64(int(change.CurrTick - lastTouch))
	reinforcementFactor := 1.0 // TODO reinforcementFactor

	memorability = math.Min(1, memorability*reinforcementFactor*(ownedLines+float64(delta.inserted))/
		(ownedLines+float64(delta.deletedByOthers)))
	// memorability is increased by delta.inserted + delta.deletedByOthers
	// memorability is reduced by delta.deletedByOthers

	if awareness > awarenessLowCut {
		memorability = math.Min(memorability, memorabilityMin)

		awareness = awareness * analyser.memoryLoss(timeDelta*(1+memorabilityMin-memorability))
		if awareness >= awarenessLowCut {
			return
		}
	}
	return 0, 0

	// memory halflife = min 30d max 180d
	// reinforcement period = memorability * 3 months

	// memory loss = 0.5 ^ (period / (reinforcement_period * memorability))
	// memory gain

	// negative Delta is better for memorability
	// frequent access - better
	// more owned lines - better
	// more deleted lines of others - better
	// more deleted lines of own - keepup
	// larger file - less awareness

	//	awareness = entry.awareness + float32(entry.ownedLines
}

func init() {
	core.Registry.Register(&CodeChurnAnalysis{})
}
