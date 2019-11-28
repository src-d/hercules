package leaves

import (
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/gogo/protobuf/proto"
	imports2 "github.com/src-d/imports"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/imports"
	"gopkg.in/src-d/hercules.v10/internal/yaml"
)

// ImportsPerDeveloper collects imports per developer.
type ImportsPerDeveloper struct {
	core.NoopMerger
	core.OneShotMergeProcessor
	// imports is the mapping from dev indexes to languages to import names to usage counts.
	imports map[int]map[string]map[string]int
	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string

	l core.Logger
}

// ImportsPerDeveloperResult is returned by Finalize() and represents the analysis result.
type ImportsPerDeveloperResult struct {
	// Imports is the mapping from dev indexes to languages to import names to usage counts.
	Imports map[int]map[string]map[string]int
	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (ipd *ImportsPerDeveloper) Name() string {
	return "ImportsPerDeveloper"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (ipd *ImportsPerDeveloper) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (ipd *ImportsPerDeveloper) Requires() []string {
	return []string{imports.DependencyImports, identity.DependencyAuthor}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (ipd *ImportsPerDeveloper) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{}
}

// Flag for the command line switch which enables this analysis.
func (ipd *ImportsPerDeveloper) Flag() string {
	return "imports-per-dev"
}

// Description returns the text which explains what the analysis is doing.
func (ipd *ImportsPerDeveloper) Description() string {
	return "Whenever a file is changed or added, we extract the imports from it and increment " +
		"their usage for the commit author."
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (ipd *ImportsPerDeveloper) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		ipd.l = l
	}
	ipd.reversedPeopleDict = facts[identity.FactIdentityDetectorReversedPeopleDict].([]string)
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (ipd *ImportsPerDeveloper) Initialize(repository *git.Repository) error {
	ipd.l = core.NewLogger()
	ipd.imports = map[int]map[string]map[string]int{}
	ipd.OneShotMergeProcessor.Initialize()
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (ipd *ImportsPerDeveloper) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if deps[core.DependencyIsMerge].(bool) {
		// we ignore merge commits
		// TODO(vmarkovtsev): handle them better
		return nil, nil
	}
	author := deps[identity.DependencyAuthor].(int)
	imps := deps[imports.DependencyImports].(map[plumbing.Hash]imports2.File)
	aimps := ipd.imports[author]
	if aimps == nil {
		aimps = map[string]map[string]int{}
		ipd.imports[author] = aimps
	}
	for _, file := range imps {
		limps := aimps[file.Lang]
		if limps == nil {
			limps = map[string]int{}
			aimps[file.Lang] = limps
		}
		for _, imp := range file.Imports {
			limps[imp]++
		}
	}
	return nil, nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (ipd *ImportsPerDeveloper) Finalize() interface{} {
	return ImportsPerDeveloperResult{Imports: ipd.imports, reversedPeopleDict: ipd.reversedPeopleDict}
}

// Fork clones this PipelineItem.
func (ipd *ImportsPerDeveloper) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(ipd, n)
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (ipd *ImportsPerDeveloper) Serialize(result interface{}, binary bool, writer io.Writer) error {
	importsResult := result.(ImportsPerDeveloperResult)
	if binary {
		return ipd.serializeBinary(&importsResult, writer)
	}
	ipd.serializeText(&importsResult, writer)
	return nil
}

func (ipd *ImportsPerDeveloper) serializeText(result *ImportsPerDeveloperResult, writer io.Writer) {
	for dev, imps := range result.Imports {
		obj, err := json.Marshal(imps)
		if err != nil {
			log.Panicf("Could not serialize %v: %v", imps, err)
		}
		fmt.Fprintf(writer, "  - %s: %s\n", yaml.SafeString(result.reversedPeopleDict[dev]), string(obj))
	}
}

func (ipd *ImportsPerDeveloper) serializeBinary(result *ImportsPerDeveloperResult, writer io.Writer) error {
	message := pb.ImportsPerDeveloperResults{
		Imports:     make([]*pb.ImportsPerDeveloper, len(result.Imports)),
		AuthorIndex: result.reversedPeopleDict,
	}
	for key, vals := range result.Imports {
		dev := &pb.ImportsPerDeveloper{Languages: map[string]*pb.ImportsPerLanguage{}}
		message.Imports[key] = dev
		for lang, counts := range vals {
			counts64 := map[string]int64{}
			dev.Languages[lang] = &pb.ImportsPerLanguage{Counts: counts64}
			for imp, n := range counts {
				counts64[imp] = int64(n)
			}
		}
	}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	_, err = writer.Write(serialized)
	return err
}

func init() {
	core.Registry.Register(&ImportsPerDeveloper{})
}
