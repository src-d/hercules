package hercules

import (
	"bufio"
	"os"
	"sort"
	"strings"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

// IdentityDetector determines the author of a commit. Same person can commit under different
// signatures, and we apply some heuristics to merge those together.
// It is a PipelineItem.
type IdentityDetector struct {
	// PeopleDict maps email || name  -> developer id.
	PeopleDict map[string]int
	// ReversedPeopleDict maps developer id -> description
	ReversedPeopleDict []string
}

const (
	// AuthorMissing is the internal author index which denotes any unmatched identities
	// (IdentityDetector.Consume()).
	AuthorMissing   = (1 << 18) - 1
	// AuthorMissingName is the string name which corresponds to AuthorMissing.
	AuthorMissingName = "<unmatched>"

	// FactIdentityDetectorPeopleDict is the name of the fact which is inserted in
	// IdentityDetector.Configure(). It corresponds to IdentityDetector.PeopleDict - the mapping
	// from the signatures to the author indices.
	FactIdentityDetectorPeopleDict         = "IdentityDetector.PeopleDict"
	// FactIdentityDetectorReversedPeopleDict is the name of the fact which is inserted in
	// IdentityDetector.Configure(). It corresponds to IdentityDetector.ReversedPeopleDict -
	// the mapping from the author indices to the main signature.
	FactIdentityDetectorReversedPeopleDict = "IdentityDetector.ReversedPeopleDict"
	// ConfigIdentityDetectorPeopleDictPath is the name of the configuration option
	// (IdentityDetector.Configure()) which allows to set the external PeopleDict mapping from a file.
	ConfigIdentityDetectorPeopleDictPath   = "IdentityDetector.PeopleDictPath"
	// FactIdentityDetectorPeopleCount is the name of the fact which is inserted in
	// IdentityDetector.Configure(). It is equal to the overall number of unique authors
	// (the length of ReversedPeopleDict).
	FactIdentityDetectorPeopleCount        = "IdentityDetector.PeopleCount"

	// DependencyAuthor is the name of the dependency provided by IdentityDetector.
	DependencyAuthor = "author"
)

func (id *IdentityDetector) Name() string {
	return "IdentityDetector"
}

func (id *IdentityDetector) Provides() []string {
	arr := [...]string{DependencyAuthor}
	return arr[:]
}

func (id *IdentityDetector) Requires() []string {
	return []string{}
}

func (id *IdentityDetector) ListConfigurationOptions() []ConfigurationOption {
	options := [...]ConfigurationOption{{
		Name:        ConfigIdentityDetectorPeopleDictPath,
		Description: "Path to the developers' email associations.",
		Flag:        "people-dict",
		Type:        StringConfigurationOption,
		Default:     ""},
	}
	return options[:]
}

func (id *IdentityDetector) Configure(facts map[string]interface{}) {
	if val, exists := facts[FactIdentityDetectorPeopleDict].(map[string]int); exists {
		id.PeopleDict = val
	}
	if val, exists := facts[FactIdentityDetectorReversedPeopleDict].([]string); exists {
		id.ReversedPeopleDict = val
	}
	if id.PeopleDict == nil || id.ReversedPeopleDict == nil {
		peopleDictPath, _ := facts[ConfigIdentityDetectorPeopleDictPath].(string)
		if peopleDictPath != "" {
			id.LoadPeopleDict(peopleDictPath)
			facts[FactIdentityDetectorPeopleCount] = len(id.ReversedPeopleDict) - 1
		} else {
			if _, exists := facts[ConfigPipelineCommits]; !exists {
				panic("IdentityDetector needs a list of commits to initialize.")
			}
			id.GeneratePeopleDict(facts[ConfigPipelineCommits].([]*object.Commit))
			facts[FactIdentityDetectorPeopleCount] = len(id.ReversedPeopleDict)
		}
	} else {
		facts[FactIdentityDetectorPeopleCount] = len(id.ReversedPeopleDict)
	}
	facts[FactIdentityDetectorPeopleDict] = id.PeopleDict
	facts[FactIdentityDetectorReversedPeopleDict] = id.ReversedPeopleDict
}

func (id *IdentityDetector) Initialize(repository *git.Repository) {
}

func (id *IdentityDetector) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps["commit"].(*object.Commit)
	signature := commit.Author
	authorID, exists := id.PeopleDict[strings.ToLower(signature.Email)]
	if !exists {
		authorID, exists = id.PeopleDict[strings.ToLower(signature.Name)]
		if !exists {
			authorID = AuthorMissing
		}
	}
	return map[string]interface{}{DependencyAuthor: authorID}, nil
}

func (id *IdentityDetector) LoadPeopleDict(path string) error {
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
	id.PeopleDict = dict
	id.ReversedPeopleDict = reverseDict
	return nil
}

func (id *IdentityDetector) GeneratePeopleDict(commits []*object.Commit) {
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
	id.PeopleDict = dict
	id.ReversedPeopleDict = reverseDict
}

// MergeReversedDicts joins two identity lists together, excluding duplicates, in-order.
func (id IdentityDetector) MergeReversedDicts(rd1, rd2 []string) (map[string][3]int, []string) {
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
	Registry.Register(&IdentityDetector{})
}
