package identity

import (
	"bufio"
	"os"
	"sort"
	"strings"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v4/internal/core"
)

// Detector determines the author of a commit. Same person can commit under different
// signatures, and we apply some heuristics to merge those together.
// It is a PipelineItem.
type Detector struct {
	// PeopleDict maps email || name  -> developer id
	PeopleDict map[string]int
	// ReversedPeopleDict maps developer id -> description
	ReversedPeopleDict []string
}

const (
	// AuthorMissing is the internal author index which denotes any unmatched identities
	// (Detector.Consume()).
	AuthorMissing = (1 << 18) - 1
	// AuthorMissingName is the string name which corresponds to AuthorMissing.
	AuthorMissingName = "<unmatched>"

	// FactIdentityDetectorPeopleDict is the name of the fact which is inserted in
	// Detector.Configure(). It corresponds to Detector.PeopleDict - the mapping
	// from the signatures to the author indices.
	FactIdentityDetectorPeopleDict = "IdentityDetector.PeopleDict"
	// FactIdentityDetectorReversedPeopleDict is the name of the fact which is inserted in
	// Detector.Configure(). It corresponds to Detector.ReversedPeopleDict -
	// the mapping from the author indices to the main signature.
	FactIdentityDetectorReversedPeopleDict = "IdentityDetector.ReversedPeopleDict"
	// ConfigIdentityDetectorPeopleDictPath is the name of the configuration option
	// (Detector.Configure()) which allows to set the external PeopleDict mapping from a file.
	ConfigIdentityDetectorPeopleDictPath = "IdentityDetector.PeopleDictPath"
	// FactIdentityDetectorPeopleCount is the name of the fact which is inserted in
	// Detector.Configure(). It is equal to the overall number of unique authors
	// (the length of ReversedPeopleDict).
	FactIdentityDetectorPeopleCount = "IdentityDetector.PeopleCount"

	// DependencyAuthor is the name of the dependency provided by Detector.
	DependencyAuthor = "author"
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (detector *Detector) Name() string {
	return "IdentityDetector"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (detector *Detector) Provides() []string {
	arr := [...]string{DependencyAuthor}
	return arr[:]
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (detector *Detector) Requires() []string {
	return []string{}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (detector *Detector) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigIdentityDetectorPeopleDictPath,
		Description: "Path to the developers' email associations.",
		Flag:        "people-dict",
		Type:        core.StringConfigurationOption,
		Default:     ""},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (detector *Detector) Configure(facts map[string]interface{}) {
	if val, exists := facts[FactIdentityDetectorPeopleDict].(map[string]int); exists {
		detector.PeopleDict = val
	}
	if val, exists := facts[FactIdentityDetectorReversedPeopleDict].([]string); exists {
		detector.ReversedPeopleDict = val
	}
	if detector.PeopleDict == nil || detector.ReversedPeopleDict == nil {
		peopleDictPath, _ := facts[ConfigIdentityDetectorPeopleDictPath].(string)
		if peopleDictPath != "" {
			detector.LoadPeopleDict(peopleDictPath)
			facts[FactIdentityDetectorPeopleCount] = len(detector.ReversedPeopleDict) - 1
		} else {
			if _, exists := facts[core.ConfigPipelineCommits]; !exists {
				panic("IdentityDetector needs a list of commits to initialize.")
			}
			detector.GeneratePeopleDict(facts[core.ConfigPipelineCommits].([]*object.Commit))
			facts[FactIdentityDetectorPeopleCount] = len(detector.ReversedPeopleDict)
		}
	} else {
		facts[FactIdentityDetectorPeopleCount] = len(detector.ReversedPeopleDict)
	}
	facts[FactIdentityDetectorPeopleDict] = detector.PeopleDict
	facts[FactIdentityDetectorReversedPeopleDict] = detector.ReversedPeopleDict
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (detector *Detector) Initialize(repository *git.Repository) {
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (detector *Detector) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps[core.DependencyCommit].(*object.Commit)
	signature := commit.Author
	authorID, exists := detector.PeopleDict[strings.ToLower(signature.Email)]
	if !exists {
		authorID, exists = detector.PeopleDict[strings.ToLower(signature.Name)]
		if !exists {
			authorID = AuthorMissing
		}
	}
	return map[string]interface{}{DependencyAuthor: authorID}, nil
}

func (detector *Detector) Fork(n int) []core.PipelineItem {
	detectors := make([]core.PipelineItem, n)
	for i := 0; i < n; i++ {
		// we are safe to share the same dictionaries across branches
		detectors[i] = detector
	}
	return detectors
}

func (detector *Detector) Merge(branches []core.PipelineItem) {
}

// LoadPeopleDict loads author signatures from a text file.
// The format is one signature per line, and the signature consists of several
// keys separated by "|". The first key is the main one and used to reference all the rest.
func (detector *Detector) LoadPeopleDict(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	dict := make(map[string]int)
	reverseDict := []string{}
	size := 0
	for scanner.Scan() {
		ids := strings.Split(scanner.Text(), "|")
		for _, id := range ids {
			dict[strings.ToLower(id)] = size
		}
		reverseDict = append(reverseDict, ids[0])
		size++
	}
	reverseDict = append(reverseDict, AuthorMissingName)
	detector.PeopleDict = dict
	detector.ReversedPeopleDict = reverseDict
	return nil
}

// GeneratePeopleDict loads author signatures from the specified list of Git commits.
func (detector *Detector) GeneratePeopleDict(commits []*object.Commit) {
	dict := map[string]int{}
	emails := map[int][]string{}
	names := map[int][]string{}
	size := 0

	mailmapFile, err := commits[len(commits)-1].File(".mailmap")
	if err == nil {
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
	}
	reverseDict := make([]string, size)
	for _, val := range dict {
		sort.Strings(names[val])
		sort.Strings(emails[val])
		reverseDict[val] = strings.Join(names[val], "|") + "|" + strings.Join(emails[val], "|")
	}
	detector.PeopleDict = dict
	detector.ReversedPeopleDict = reverseDict
}

// MergeReversedDicts joins two identity lists together, excluding duplicates, in-order.
func (detector Detector) MergeReversedDicts(rd1, rd2 []string) (map[string][3]int, []string) {
	people := map[string][3]int{}
	for i, pid := range rd1 {
		ptrs := people[pid]
		ptrs[0] = len(people)
		ptrs[1] = i
		ptrs[2] = -1
		people[pid] = ptrs
	}
	for i, pid := range rd2 {
		ptrs, exists := people[pid]
		if !exists {
			ptrs[0] = len(people)
			ptrs[1] = -1
		}
		ptrs[2] = i
		people[pid] = ptrs
	}
	mrd := make([]string, len(people))
	for name, ptrs := range people {
		mrd[ptrs[0]] = name
	}
	return people, mrd
}

func init() {
	core.Registry.Register(&Detector{})
}
