package identity

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"os"
	"strings"
	"time"

	"github.com/cyraxred/hercules/internal/core"
	"github.com/go-git/go-git/v5"
	"github.com/pkg/errors"
)

// StoryDetector determines the author of a commit. Same person can commit under different
// signatures, and we apply some heuristics to merge those together.
// It is a PipelineItem.
type StoryDetector struct {
	core.NoopMerger
	// PeopleDict maps email || name  -> developer id
	MergeHashDict map[plumbing.Hash]int
	// ReversedPeopleDict maps developer id -> description
	MergeNames []string

	mergeNameCount  int
	expandMergeDict bool

	l core.Logger
}

const (
	// FactStoryDetectorMergeDict is the name of the fact which is inserted in
	// StoryDetector.Configure(). It corresponds to StoryDetector.ReversedPeopleDict -
	// the mapping from the author indices to the main signature.
	FactStoryDetectorMergeDict = "StoryDetector.MergeDict"
	// ConfigStoryDetectorMergeDictPath is the name of the configuration option
	// (StoryDetector.Configure()) which allows to set the external PeopleDict mapping from a file.
	ConfigStoryDetectorMergeDictPath = "StoryDetector.MergeDictPath"
)

var _ core.IdentityResolver = storyResolver{}

type storyResolver struct {
	identities *StoryDetector
}

func (v storyResolver) Count() int {
	if v.identities == nil {
		return 0
	}
	return v.identities.mergeNameCount
}

func (v storyResolver) FriendlyNameOf(id core.AuthorId) string {
	if id == core.AuthorMissing || id < 0 || v.identities == nil || int(id) >= len(v.identities.MergeNames) {
		return core.AuthorMissingName
	}
	return v.identities.MergeNames[id]
}

func (v storyResolver) ForEachIdentity(callback func(core.AuthorId, string)) bool {
	if v.identities == nil {
		return false
	}
	for id, name := range v.identities.MergeNames {
		callback(core.AuthorId(id), name)
	}
	return true
}

func (v storyResolver) CopyFriendlyNames() []string {
	return append([]string(nil), v.identities.MergeNames...)
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (detector *StoryDetector) Name() string {
	return "StoryDetector"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (detector *StoryDetector) Provides() []string {
	return []string{DependencyAuthor}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (detector *StoryDetector) Requires() []string {
	return []string{}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (detector *StoryDetector) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigStoryDetectorMergeDictPath,
		Description: "Path to the file with developer -> name|email associations.",
		Flag:        "story-dict",
		Type:        core.PathConfigurationOption,
		Default:     ""},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (detector *StoryDetector) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		detector.l = l
	} else {
		detector.l = core.NewLogger()
	}

	detector.expandMergeDict = false
	if val, exists := facts[FactStoryDetectorMergeDict].(map[plumbing.Hash]string); exists {
		detector.MergeHashDict, detector.MergeNames = splitMergeDict(val)
		detector.mergeNameCount = len(detector.MergeNames)
	} else if dictPath, ok := facts[ConfigStoryDetectorMergeDictPath].(string); ok && dictPath != "" {
		err := detector.LoadMergeDict(dictPath)
		if err != nil {
			return errors.Errorf("failed to load %s: %v", dictPath, err)
		}
		detector.mergeNameCount = len(detector.MergeNames)
	} else if mergeCount, ok := facts[core.FactMergeHashCount].(int); ok {
		detector.MergeHashDict = make(map[plumbing.Hash]int, mergeCount)
		detector.mergeNameCount = mergeCount
		detector.expandMergeDict = true
	} else {
		return errors.Errorf("merge tracks are not available")
	}

	var resolver core.IdentityResolver = storyResolver{detector}
	facts[core.FactIdentityResolver] = resolver
	return nil
}

func splitMergeDict(dict map[plumbing.Hash]string) (hashDict map[plumbing.Hash]int, names []string) {
	uniqueNames := map[string]int{}
	hashDict = make(map[plumbing.Hash]int, len(dict))
	for k, v := range dict {
		id, ok := uniqueNames[v]
		if !ok {
			id = len(uniqueNames)
			uniqueNames[v] = id
		}
		hashDict[k] = id
	}
	names = make([]string, len(uniqueNames))
	for k, v := range uniqueNames {
		names[v] = k
	}
	return
}

func (*StoryDetector) ConfigureUpstream(facts map[string]interface{}) error {
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (detector *StoryDetector) Initialize(repository *git.Repository) error {
	detector.l = core.NewLogger()
	return nil
}

func (detector *StoryDetector) Features() []string {
	return []string{core.FeatureMergeTracks}
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (detector *StoryDetector) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	author, err := detector.putAuthorId(deps[core.DependencyNextMerge].(*object.Commit))
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{DependencyAuthor: author}, nil
}

func (detector *StoryDetector) putAuthorId(nextMerge *object.Commit) (core.AuthorId, error) {
	if nextMerge == nil {
		return core.AuthorMissing, nil
	}
	if author, ok := detector.MergeHashDict[nextMerge.Hash]; ok {
		return core.AuthorId(author), nil
	}
	if !detector.expandMergeDict {
		return core.AuthorMissing, nil
	}
	if len(detector.MergeNames) >= detector.mergeNameCount {
		return core.AuthorMissing, errors.New("number of merge hashes exceeded")
	}

	n := len(detector.MergeNames)
	name := detector.makeMergeName(n, nextMerge)
	detector.MergeHashDict[nextMerge.Hash] = n
	detector.MergeNames = append(detector.MergeNames, name)
	return core.AuthorId(n), nil
}

// Fork clones this PipelineItem.
func (detector *StoryDetector) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(detector, n)
}

// LoadMergeDict loads author signatures from a text file.
// The format is one signature per line, and the signature consists of several
// keys separated by "|". The first key is the main one and used to reference all the rest.
func (detector *StoryDetector) LoadMergeDict(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	dict := make(map[plumbing.Hash]int)
	var reverseDict []string
	for scanner.Scan() {
		textLine := scanner.Text()
		values := strings.Split(textLine, "|")
		id := len(reverseDict)
		i := 0

		for ; i < len(values); i++ {
			value := values[i]
			var key plumbing.Hash
			n := 0
			n, err = hex.Decode(key[:], []byte(value))
			if err == nil && n != len(key) {
				err = errors.Errorf("hash must be of %d bytes: %s", len(key), value)
			}
			if err != nil {
				if i == len(values)-1 {
					break
				}
				return err
			}
			if id2, found := dict[key]; found {
				return errors.Errorf("ambigous hash: %s = (%d) %s", value, id2, reverseDict[id2])
			}
			dict[key] = id
		}
		name := ""
		if i == len(values) {
			name = fmt.Sprintf("Merge #%d", id)
		} else {
			name = values[i]
		}
		reverseDict = append(reverseDict, name)
	}
	detector.MergeHashDict = dict
	detector.MergeNames = reverseDict
	return nil
}

func (detector *StoryDetector) makeMergeName(index int, merge *object.Commit) string {
	return fmt.Sprintf("Merge #%d (%s) at %s",
		index, merge.Hash.String()[:7], merge.Author.When.Format(time.RFC822Z))
}

func init() {
	core.Registry.Register(&StoryDetector{})
}
