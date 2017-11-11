package hercules

import (
	"bufio"
	"os"
	"sort"
	"strings"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type IdentityDetector struct {
	// Maps email || name  -> developer id.
	PeopleDict map[string]int
	// Maps developer id -> description
	ReversedPeopleDict []string
}

const (
	MISSING_AUTHOR = (1 << 18) - 1
	SELF_AUTHOR    = (1 << 18) - 2

	FactIdentityDetectorPeopleDict         = "IdentityDetector.PeopleDict"
	FactIdentityDetectorReversedPeopleDict = "IdentityDetector.ReversedPeopleDict"
	ConfigIdentityDetectorPeopleDictPath   = "IdentityDetector.PeopleDictPath"
	FactIdentityDetectorPeopleCount        = "IdentityDetector.PeopleCount"
)

func (id *IdentityDetector) Name() string {
	return "IdentityDetector"
}

func (id *IdentityDetector) Provides() []string {
	arr := [...]string{"author"}
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
	if id.PeopleDict == nil {
		if id.ReversedPeopleDict != nil {
			panic("IdentityDetector: ReversedPeopleDict != nil while PeopleDict == nil")
		}
		peopleDictPath, _ := facts[ConfigIdentityDetectorPeopleDictPath].(string)
		if peopleDictPath != "" {
			id.LoadPeopleDict(peopleDictPath)
			facts[FactIdentityDetectorPeopleCount] = len(id.ReversedPeopleDict) - 1
		} else {
			id.GeneratePeopleDict(facts["commits"].([]*object.Commit))
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

func (self *IdentityDetector) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps["commit"].(*object.Commit)
	signature := commit.Author
	id, exists := self.PeopleDict[strings.ToLower(signature.Email)]
	if !exists {
		id, exists = self.PeopleDict[strings.ToLower(signature.Name)]
		if !exists {
			id = MISSING_AUTHOR
		}
	}
	return map[string]interface{}{"author": id}, nil
}

func (id *IdentityDetector) LoadPeopleDict(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	dict := make(map[string]int)
	reverse_dict := []string{}
	size := 0
	for scanner.Scan() {
		ids := strings.Split(scanner.Text(), "|")
		for _, id := range ids {
			dict[strings.ToLower(id)] = size
		}
		reverse_dict = append(reverse_dict, ids[0])
		size += 1
	}
	reverse_dict = append(reverse_dict, "<unmatched>")
	id.PeopleDict = dict
	id.ReversedPeopleDict = reverse_dict
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
		size += 1
	}
	reverse_dict := make([]string, size)
	for _, val := range dict {
		sort.Strings(names[val])
		sort.Strings(emails[val])
		reverse_dict[val] = strings.Join(names[val], "|") + "|" + strings.Join(emails[val], "|")
	}
	id.PeopleDict = dict
	id.ReversedPeopleDict = reverse_dict
}

func init() {
	Registry.Register(&IdentityDetector{})
}
