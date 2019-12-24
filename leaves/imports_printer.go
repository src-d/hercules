package leaves

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	imports2 "github.com/src-d/imports"
	"gopkg.in/src-d/go-git.v4"
	gitplumbing "gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	"gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/imports"
	"gopkg.in/src-d/hercules.v10/internal/yaml"
)

// ImportsMap is the type of the mapping from dev indexes to languages to import names to ticks to
// usage counts. Ticks start counting from 0 and correspond to the earliest commit timestamp
// (included in the YAML/protobuf header).
type ImportsMap = map[int]map[string]map[string]map[int]int64

// ImportsPerDeveloper collects imports per developer.
type ImportsPerDeveloper struct {
	core.NoopMerger
	core.OneShotMergeProcessor
	// TickSize defines the time mapping granularity (the last ImportsMap's key).
	TickSize time.Duration
	// imports mapping, see the referenced type for details.
	imports ImportsMap
	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
	l                  core.Logger
}

// ImportsPerDeveloperResult is returned by Finalize() and represents the analysis result.
type ImportsPerDeveloperResult struct {
	// Imports is the imports mapping, see the referenced type for details.
	Imports ImportsMap
	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
	// tickSize references TicksSinceStart.TickSize
	tickSize time.Duration
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
	return []string{imports.DependencyImports, identity.DependencyAuthor, plumbing.DependencyTick}
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
	if val, exists := facts[plumbing.FactTickSize].(time.Duration); exists {
		ipd.TickSize = val
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (ipd *ImportsPerDeveloper) Initialize(repository *git.Repository) error {
	ipd.l = core.NewLogger()
	ipd.imports = ImportsMap{}
	ipd.OneShotMergeProcessor.Initialize()
	if ipd.TickSize == 0 {
		ipd.TickSize = time.Hour * 24
		ipd.l.Warnf("tick size was not set, adjusted to %v\n", ipd.TickSize)
	}
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
	imps := deps[imports.DependencyImports].(map[gitplumbing.Hash]imports2.File)
	aimps := ipd.imports[author]
	tick := deps[plumbing.DependencyTick].(int)
	if aimps == nil {
		aimps = map[string]map[string]map[int]int64{}
		ipd.imports[author] = aimps
	}
	for _, file := range imps {
		limps := aimps[file.Lang]
		if limps == nil {
			limps = map[string]map[int]int64{}
			aimps[file.Lang] = limps
		}
		for _, imp := range file.Imports {
			timps, exists := limps[imp]
			if !exists {
				timps = map[int]int64{}
				limps[imp] = timps
			}
			timps[tick]++
		}
	}
	return nil, nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (ipd *ImportsPerDeveloper) Finalize() interface{} {
	return ImportsPerDeveloperResult{
		Imports:            ipd.imports,
		reversedPeopleDict: ipd.reversedPeopleDict,
		tickSize:           ipd.TickSize,
	}
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
	devs := make([]int, 0, len(result.Imports))
	for dev := range result.Imports {
		devs = append(devs, dev)
	}
	sort.Ints(devs)
	fmt.Fprintln(writer, "  tick_size:", int(result.tickSize.Seconds()))
	fmt.Fprintln(writer, "  imports:")
	for _, dev := range devs {
		imps := result.Imports[dev]
		obj, err := json.Marshal(imps)
		if err != nil {
			log.Panicf("Could not serialize %v: %v", imps, err)
		}
		fmt.Fprintf(writer, "    %s: %s\n", yaml.SafeString(result.reversedPeopleDict[dev]), string(obj))
	}
}

func (ipd *ImportsPerDeveloper) serializeBinary(result *ImportsPerDeveloperResult, writer io.Writer) error {
	message := pb.ImportsPerDeveloperResults{
		Imports:     make([]*pb.ImportsPerDeveloper, len(result.Imports)),
		AuthorIndex: result.reversedPeopleDict,
		TickSize:    int64(result.tickSize),
	}
	for key, dev := range result.Imports {
		pbdev := &pb.ImportsPerDeveloper{Languages: map[string]*pb.ImportsPerLanguage{}}
		message.Imports[key] = pbdev
		for lang, ticks := range dev {
			pbticks := map[string]*pb.ImportsPerTick{}
			pbdev.Languages[lang] = &pb.ImportsPerLanguage{Ticks: pbticks}
			for imp, tick := range ticks {
				counts := map[int32]int64{}
				pbticks[imp] = &pb.ImportsPerTick{Counts: counts}
				for ti, val := range tick {
					counts[int32(ti)] = val
				}
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

// Deserialize converts the specified protobuf bytes to ImportsPerDeveloperResult.
func (ipd *ImportsPerDeveloper) Deserialize(pbmessage []byte) (interface{}, error) {
	msg := pb.ImportsPerDeveloperResults{}
	err := proto.Unmarshal(pbmessage, &msg)
	if err != nil {
		return nil, err
	}
	r := ImportsPerDeveloperResult{
		Imports:            ImportsMap{},
		reversedPeopleDict: msg.AuthorIndex,
		tickSize:           time.Duration(msg.TickSize),
	}
	for devi, dev := range msg.Imports {
		rdev := map[string]map[string]map[int]int64{}
		r.Imports[devi] = rdev
		for lang, names := range dev.Languages {
			rlang := map[string]map[int]int64{}
			rdev[lang] = rlang
			for name, ticks := range names.Ticks {
				rticks := map[int]int64{}
				rlang[name] = rticks
				for tick, val := range ticks.Counts {
					rticks[int(tick)] = val
				}
			}
		}
	}
	return r, nil
}

func init() {
	core.Registry.Register(&ImportsPerDeveloper{})
}
