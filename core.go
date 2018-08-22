package hercules

import (
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v4/internal/core"
	"gopkg.in/src-d/hercules.v4/internal/plumbing"
	"gopkg.in/src-d/hercules.v4/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v4/internal/plumbing/uast"
	"gopkg.in/src-d/hercules.v4/leaves"
	"gopkg.in/src-d/hercules.v4/internal/yaml"
)

// ConfigurationOptionType represents the possible types of a ConfigurationOption's value.
type ConfigurationOptionType = core.ConfigurationOptionType

const (
	// BoolConfigurationOption reflects the boolean value type.
	BoolConfigurationOption = core.BoolConfigurationOption
	// IntConfigurationOption reflects the integer value type.
	IntConfigurationOption = core.IntConfigurationOption
	// StringConfigurationOption reflects the string value type.
	StringConfigurationOption = core.StringConfigurationOption
	// FloatConfigurationOption reflects a floating point value type.
	FloatConfigurationOption = core.FloatConfigurationOption
	// StringsConfigurationOption reflects the array of strings value type.
	StringsConfigurationOption = core.StringsConfigurationOption
)

// ConfigurationOption allows for the unified, retrospective way to setup PipelineItem-s.
type ConfigurationOption = core.ConfigurationOption

// PipelineItem is the interface for all the units in the Git commits analysis pipeline.
type PipelineItem = core.PipelineItem

// FeaturedPipelineItem enables switching the automatic insertion of pipeline items on or off.
type FeaturedPipelineItem = core.FeaturedPipelineItem

// LeafPipelineItem corresponds to the top level pipeline items which produce the end results.
type LeafPipelineItem = core.LeafPipelineItem

// ResultMergeablePipelineItem specifies the methods to combine several analysis results together.
type ResultMergeablePipelineItem = core.ResultMergeablePipelineItem

// CommonAnalysisResult holds the information which is always extracted at Pipeline.Run().
type CommonAnalysisResult = core.CommonAnalysisResult

// NoopMerger provides an empty Merge() method suitable for PipelineItem.
type NoopMerger = core.NoopMerger

// OneShotMergeProcessor provides the convenience method to consume merges only once.
type OneShotMergeProcessor = core.OneShotMergeProcessor

// MetadataToCommonAnalysisResult copies the data from a Protobuf message.
func MetadataToCommonAnalysisResult(meta *core.Metadata) *CommonAnalysisResult {
	return core.MetadataToCommonAnalysisResult(meta)
}

// Pipeline is the core Hercules entity which carries several PipelineItems and executes them.
// See the extended example of how a Pipeline works in doc.go
type Pipeline = core.Pipeline

const (
	// ConfigPipelineDumpPath is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which enables saving the items DAG to the specified file.
	ConfigPipelineDumpPath = core.ConfigPipelineDumpPath
	// ConfigPipelineDryRun is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which disables Configure() and Initialize() invocation on each PipelineItem during the
	// Pipeline initialization.
	// Subsequent Run() calls are going to fail. Useful with ConfigPipelineDumpPath=true.
	ConfigPipelineDryRun = core.ConfigPipelineDryRun
	// ConfigPipelineCommits is the name of the Pipeline configuration option (Pipeline.Initialize())
	// which allows to specify the custom commit sequence. By default, Pipeline.Commits() is used.
	ConfigPipelineCommits = core.ConfigPipelineCommits
)

// NewPipeline initializes a new instance of Pipeline struct.
func NewPipeline(repository *git.Repository) *Pipeline {
	return core.NewPipeline(repository)
}

// LoadCommitsFromFile reads the file by the specified FS path and generates the sequence of commits
// by interpreting each line as a Git commit hash.
func LoadCommitsFromFile(path string, repository *git.Repository) ([]*object.Commit, error) {
	return core.LoadCommitsFromFile(path, repository)
}

// ForkSamePipelineItem clones items by referencing the same origin.
func ForkSamePipelineItem(origin PipelineItem, n int) []PipelineItem {
	return core.ForkSamePipelineItem(origin ,n)
}

// ForkCopyPipelineItem clones items by copying them by value from the origin.
func ForkCopyPipelineItem(origin PipelineItem, n int) []PipelineItem {
	return core.ForkCopyPipelineItem(origin ,n)
}

// PipelineItemRegistry contains all the known PipelineItem-s.
type PipelineItemRegistry = core.PipelineItemRegistry

// Registry contains all known pipeline item types.
var Registry = core.Registry

const (
	// DependencyCommit is the name of one of the three items in `deps` supplied to PipelineItem.Consume()
	// which always exists. It corresponds to the currently analyzed commit.
	DependencyCommit = core.DependencyCommit
	// DependencyIndex is the name of one of the three items in `deps` supplied to PipelineItem.Consume()
	// which always exists. It corresponds to the currently analyzed commit's index.
	DependencyIndex = core.DependencyIndex
	// DependencyIsMerge is the name of one of the three items in `deps` supplied to PipelineItem.Consume()
	// which always exists. It indicates whether the analyzed commit is a merge commit.
	// Checking the number of parents is not correct - we remove the back edges during the DAG simplification.
	DependencyIsMerge = core.DependencyIsMerge
	// DependencyAuthor is the name of the dependency provided by identity.Detector.
	DependencyAuthor = identity.DependencyAuthor
	// DependencyBlobCache identifies the dependency provided by BlobCache.
	DependencyBlobCache = plumbing.DependencyBlobCache
	// DependencyDay is the name of the dependency which DaysSinceStart provides - the number
	// of days since the first commit in the analysed sequence.
	DependencyDay = plumbing.DependencyDay
	// DependencyFileDiff is the name of the dependency provided by FileDiff.
	DependencyFileDiff = plumbing.DependencyFileDiff
	// DependencyTreeChanges is the name of the dependency provided by TreeDiff.
	DependencyTreeChanges = plumbing.DependencyTreeChanges
	// DependencyUastChanges is the name of the dependency provided by Changes.
	DependencyUastChanges = uast.DependencyUastChanges
	// DependencyUasts is the name of the dependency provided by Extractor.
	DependencyUasts = uast.DependencyUasts
	// FactCommitsByDay contains the mapping between day indices and the corresponding commits.
	FactCommitsByDay = plumbing.FactCommitsByDay
	// FactIdentityDetectorPeopleCount is the name of the fact which is inserted in
	// identity.Detector.Configure(). It is equal to the overall number of unique authors
	// (the length of ReversedPeopleDict).
	FactIdentityDetectorPeopleCount = identity.FactIdentityDetectorPeopleCount
	// FactIdentityDetectorPeopleDict is the name of the fact which is inserted in
	// identity.Detector.Configure(). It corresponds to identity.Detector.PeopleDict - the mapping
	// from the signatures to the author indices.
	FactIdentityDetectorPeopleDict = identity.FactIdentityDetectorPeopleDict
	// FactIdentityDetectorReversedPeopleDict is the name of the fact which is inserted in
	// identity.Detector.Configure(). It corresponds to identity.Detector.ReversedPeopleDict -
	// the mapping from the author indices to the main signature.
	FactIdentityDetectorReversedPeopleDict = identity.FactIdentityDetectorReversedPeopleDict
)

// FileDiffData is the type of the dependency provided by plumbing.FileDiff.
type FileDiffData = plumbing.FileDiffData

// CountLines returns the number of lines in a *object.Blob.
func CountLines(file *object.Blob) (int, error) {
	return plumbing.CountLines(file)
}

// SafeYamlString escapes the string so that it can be reliably used in YAML.
func SafeYamlString(str string) string {
	return yaml.SafeString(str)
}

func init() {
	// hack to link with .leaves
	_ = leaves.BurndownAnalysis{}
}
