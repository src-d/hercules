package hercules

import (
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v4/internal/core"
	"gopkg.in/src-d/hercules.v4/leaves"
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

// MergeablePipelineItem specifies the methods to combine several analysis results together.
type MergeablePipelineItem = core.MergeablePipelineItem

// CommonAnalysisResult holds the information which is always extracted at Pipeline.Run().
type CommonAnalysisResult = core.CommonAnalysisResult

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

// PipelineItemRegistry contains all the known PipelineItem-s.
type PipelineItemRegistry = core.PipelineItemRegistry

// Registry contains all known pipeline item types.
var Registry = core.Registry

func init() {
	// hack to link with .leaves
	_ = leaves.BurndownAnalysis{}
}
