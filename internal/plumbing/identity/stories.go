package identity

import (
	"bufio"
	"os"
	"sort"
	"strings"

	"github.com/cyraxred/hercules/internal/core"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/pkg/errors"
)

// StoryDetector determines the author of a commit. Same person can commit under different
// signatures, and we apply some heuristics to merge those together.
// It is a PipelineItem.
type StoryDetector struct {
	core.NoopMerger
	// PeopleDict maps email || name  -> developer id
	PeopleDict map[string]int
	// ReversedPeopleDict maps developer id -> description
	ReversedPeopleDict []string

	ExactSignatures bool

	l core.Logger
}

const (
	// FactStoryDetectorStoryList is the name of the fact which is inserted in
	// StoryDetector.Configure(). It corresponds to StoryDetector.ReversedPeopleDict -
	// the mapping from the author indices to the main signature.
	FactStoryDetectorStoryList = "StoryDetector.StoryList"
	// ConfigStoryDetectorStoryListPath is the name of the configuration option
	// (StoryDetector.Configure()) which allows to set the external PeopleDict mapping from a file.
	ConfigStoryDetectorStoryListPath = "StoryDetector.StoryListPath"
)

var _ core.IdentityResolver = storyResolver{}

type storyResolver struct {
	identities *StoryDetector
}

func (v storyResolver) Count() int {
	if v.identities == nil {
		return 0
	}
	return len(v.identities.ReversedPeopleDict)
}

func (v storyResolver) FriendlyNameOf(id core.AuthorId) string {
	if id == core.AuthorMissing || id < 0 || v.identities == nil || int(id) >= len(v.identities.ReversedPeopleDict) {
		return core.AuthorMissingName
	}
	return v.identities.ReversedPeopleDict[id]
}

func (v storyResolver) FindIdOf(name string) core.AuthorId {
	if v.identities != nil {
		if id, ok := v.identities.PeopleDict[name]; ok {
			return core.AuthorId(id)
		}
	}
	return core.AuthorId(-1)
}

func (v storyResolver) ForEachIdentity(callback func(core.AuthorId, string)) bool {
	if v.identities == nil {
		return false
	}
	for id, name := range v.identities.ReversedPeopleDict {
		callback(core.AuthorId(id), name)
	}
	return true
}

func (v storyResolver) CopyFriendlyNames() []string {
	return append([]string(nil), v.identities.ReversedPeopleDict...)
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
		Name:        ConfigStoryDetectorStoryListPath,
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

	detector.PeopleDict = nil
	if val, exists := facts[FactStoryDetectorStoryList].([]string); exists {
		detector.ReversedPeopleDict = val
	}

	if peopleDictPath, ok := facts[ConfigStoryDetectorStoryListPath].(string); ok && peopleDictPath != "" {
		err := detector.LoadPeopleDict(peopleDictPath)
		if err != nil {
			return errors.Errorf("failed to load %s: %v", peopleDictPath, err)
		}
	}

	if detector.ReversedPeopleDict == nil {
		if _, exists := facts[core.ConfigPipelineCommits]; !exists {
			panic("StoryDetector needs a list of commits to initialize.")
		}
		detector.GeneratePeopleDict(facts[core.ConfigPipelineCommits].([]*object.Commit))
	}

	if detector.PeopleDict == nil {
		detector.PeopleDict = make(map[string]int, len(detector.ReversedPeopleDict))
		for k, v := range detector.ReversedPeopleDict {
			detector.PeopleDict[v] = k
		}
	}

	var resolver core.IdentityResolver = storyResolver{detector}
	facts[core.FactIdentityResolver] = resolver
	return nil
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

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (detector *StoryDetector) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps[core.DependencyCommit].(*object.Commit)
	var authorID int
	var exists bool
	signature := commit.Author
	if !detector.ExactSignatures {
		authorID, exists = detector.PeopleDict[strings.ToLower(signature.Email)]
		if !exists {
			authorID, exists = detector.PeopleDict[strings.ToLower(signature.Name)]
		}
	} else {
		authorID, exists = detector.PeopleDict[strings.ToLower(signature.String())]
	}
	if !exists {
		authorID = core.AuthorMissing
	}
	return map[string]interface{}{DependencyAuthor: authorID}, nil
}

// Fork clones this PipelineItem.
func (detector *StoryDetector) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(detector, n)
}

// LoadPeopleDict loads author signatures from a text file.
// The format is one signature per line, and the signature consists of several
// keys separated by "|". The first key is the main one and used to reference all the rest.
func (detector *StoryDetector) LoadPeopleDict(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	dict := make(map[string]int)
	var reverseDict []string
	size := 0
	for scanner.Scan() {
		ids := strings.Split(scanner.Text(), "|")
		canon := ids[0]
		var exists bool
		var canonIndex int
		// lookup or create a new canonical value
		if canonIndex, exists = dict[strings.ToLower(canon)]; !exists {
			reverseDict = append(reverseDict, canon)
			canonIndex = size
			size++
		}
		for _, id := range ids {
			dict[strings.ToLower(id)] = canonIndex
		}
	}
	detector.PeopleDict = dict
	detector.ReversedPeopleDict = reverseDict
	return nil
}

// GeneratePeopleDict loads author signatures from the specified list of Git commits.
func (detector *StoryDetector) GeneratePeopleDict(commits []*object.Commit) {
	dict := map[string]int{}
	emails := map[int][]string{}
	names := map[int][]string{}
	size := 0

	mailmapFile, err := commits[len(commits)-1].File(".mailmap")
	// TODO(vmarkovtsev): properly handle .mailmap if ExactSignatures
	if !detector.ExactSignatures && err == nil {
		mailMapContents, err := mailmapFile.Contents()
		if err == nil {
			mailmap := ParseMailmap(mailMapContents)
			for key, val := range mailmap {
				key = strings.ToLower(key)
				toEmail := strings.ToLower(val.Email)
				toName := strings.ToLower(val.Name)
				id, exists := dict[toEmail]
				if !exists {
					id, exists = dict[toName]
				}
				if exists {
					dict[key] = id
				} else {
					id = size
					size++
					if toEmail != "" {
						dict[toEmail] = id
						emails[id] = append(emails[id], toEmail)
					}
					if toName != "" {
						dict[toName] = id
						names[id] = append(names[id], toName)
					}
					dict[key] = id
				}
				if strings.Contains(key, "@") {
					exists := false
					for _, val := range emails[id] {
						if key == val {
							exists = true
							break
						}
					}
					if !exists {
						emails[id] = append(emails[id], key)
					}
				} else {
					exists := false
					for _, val := range names[id] {
						if key == val {
							exists = true
							break
						}
					}
					if !exists {
						names[id] = append(names[id], key)
					}
				}
			}
		}
	}

	for _, commit := range commits {
		if !detector.ExactSignatures {
			email := strings.ToLower(commit.Author.Email)
			name := strings.ToLower(commit.Author.Name)
			id, exists := dict[email]
			if exists {
				_, exists := dict[name]
				if !exists {
					dict[name] = id
					names[id] = append(names[id], name)
				}
				continue
			}
			id, exists = dict[name]
			if exists {
				dict[email] = id
				emails[id] = append(emails[id], email)
				continue
			}
			dict[email] = size
			dict[name] = size
			emails[size] = append(emails[size], email)
			names[size] = append(names[size], name)
			size++
		} else { // !detector.ExactSignatures
			sig := strings.ToLower(commit.Author.String())
			if _, exists := dict[sig]; !exists {
				dict[sig] = size
				size++
			}
		}
	}
	reverseDict := make([]string, size)
	if !detector.ExactSignatures {
		for _, val := range dict {
			sort.Strings(names[val])
			sort.Strings(emails[val])
			reverseDict[val] = strings.Join(names[val], "|") + "|" + strings.Join(emails[val], "|")
		}
	} else {
		for key, val := range dict {
			reverseDict[val] = key
		}
	}
	detector.PeopleDict = dict
	detector.ReversedPeopleDict = reverseDict
}

func init() {
	//	core.Registry.Register(&StoryDetector{})
}
