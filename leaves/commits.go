package leaves

import (
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	items "gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v10/internal/yaml"
)

// CommitsAnalysis extracts statistics for each commit
type CommitsAnalysis struct {
	core.NoopMerger

	// commits stores statistics for each commit
	commits []*CommitStat
	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string

	l core.Logger
}

// CommitsResult is returned by CommitsAnalysis.Finalize() and carries the statistics
// per commit.
type CommitsResult struct {
	Commits []*CommitStat

	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
}

// FileStat is the statistics for a file in a commit
type FileStat struct {
	Name     string
	Language string
	items.LineStats
}

// CommitStat is the statistics for a commit
type CommitStat struct {
	Hash   string
	When   int64
	Author int
	Files  []FileStat
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (ca *CommitsAnalysis) Name() string {
	return "CommitsStat"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (ca *CommitsAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (ca *CommitsAnalysis) Requires() []string {
	return []string{
		identity.DependencyAuthor, items.DependencyLanguages, items.DependencyLineStats}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (ca *CommitsAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	return nil
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (ca *CommitsAnalysis) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		ca.l = l
	}
	if val, exists := facts[identity.FactIdentityDetectorReversedPeopleDict].([]string); exists {
		ca.reversedPeopleDict = val
	}
	return nil
}

// Flag for the command line switch which enables this analysis.
func (ca *CommitsAnalysis) Flag() string {
	return "commits-stat"
}

// Description returns the text which explains what the analysis is doing.
func (ca *CommitsAnalysis) Description() string {
	return "Extracts statistics for each commit. Identical to `git log --stat`"
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (ca *CommitsAnalysis) Initialize(repository *git.Repository) error {
	ca.l = core.NewLogger()
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (ca *CommitsAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if deps[core.DependencyIsMerge].(bool) {
		return nil, nil
	}
	commit := deps[core.DependencyCommit].(*object.Commit)
	author := deps[identity.DependencyAuthor].(int)
	lineStats := deps[items.DependencyLineStats].(map[object.ChangeEntry]items.LineStats)
	langs := deps[items.DependencyLanguages].(map[plumbing.Hash]string)
	cs := CommitStat{
		Hash:   commit.Hash.String(),
		When:   commit.Author.When.Unix(),
		Author: author,
	}
	for entry, stats := range lineStats {
		cs.Files = append(cs.Files, FileStat{
			Name:      entry.Name,
			Language:  langs[entry.TreeEntry.Hash],
			LineStats: stats,
		})
	}
	ca.commits = append(ca.commits, &cs)
	return nil, nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (ca *CommitsAnalysis) Finalize() interface{} {
	return CommitsResult{
		Commits:            ca.commits,
		reversedPeopleDict: ca.reversedPeopleDict,
	}
}

// Fork clones this pipeline item.
func (ca *CommitsAnalysis) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(ca, n)
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (ca *CommitsAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	commitsResult := result.(CommitsResult)
	if binary {
		return ca.serializeBinary(&commitsResult, writer)
	}
	ca.serializeText(&commitsResult, writer)
	return nil
}

func (ca *CommitsAnalysis) serializeText(result *CommitsResult, writer io.Writer) {
	fmt.Fprintln(writer, "  commits:")
	for _, c := range result.Commits {
		fmt.Fprintf(writer, "    - hash: %s\n", c.Hash)
		fmt.Fprintf(writer, "      when: %d\n", c.When)
		fmt.Fprintf(writer, "      author: %d\n", c.Author)
		fmt.Fprintf(writer, "      files:\n")
		for _, f := range c.Files {
			fmt.Fprintf(writer, "       - name: %s\n", f.Name)
			fmt.Fprintf(writer, "         language: %s\n", f.Language)
			fmt.Fprintf(writer, "         stat: [%d, %d, %d]\n", f.Added, f.Changed, f.Removed)
		}
	}
	fmt.Fprintln(writer, "  people:")
	for _, person := range result.reversedPeopleDict {
		fmt.Fprintf(writer, "  - %s\n", yaml.SafeString(person))
	}
}

func (ca *CommitsAnalysis) serializeBinary(result *CommitsResult, writer io.Writer) error {
	message := pb.CommitsAnalysisResults{}
	message.AuthorIndex = result.reversedPeopleDict
	message.Commits = make([]*pb.Commit, len(result.Commits))
	for i, c := range result.Commits {
		files := make([]*pb.CommitFile, len(c.Files))
		for i, f := range c.Files {
			files[i] = &pb.CommitFile{
				Name:     f.Name,
				Language: f.Language,
				Stats: &pb.LineStats{
					Added:   int32(f.LineStats.Added),
					Changed: int32(f.LineStats.Changed),
					Removed: int32(f.LineStats.Removed),
				},
			}
		}

		message.Commits[i] = &pb.Commit{
			Hash:         c.Hash,
			WhenUnixTime: c.When,
			Author:       int32(c.Author),
			Files:        files,
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
	core.Registry.Register(&CommitsAnalysis{})
}
