package hercules

import (
	"bufio"
	"os"
	"strings"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type IdentityDetector struct {
	// Maps email || name  -> developer id.
	PeopleDict map[string]int
	// Maps developer id -> description
	ReversePeopleDict []string
}

const MISSING_AUTHOR = (1 << 18) - 1
const SELF_AUTHOR = (1 << 18) - 2

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

func (id *IdentityDetector) Finalize() interface{} {
	return nil
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
		ids := strings.Split(strings.ToLower(scanner.Text()), "|")
		for _, id := range ids {
			dict[id] = size
		}
		reverse_dict = append(reverse_dict, ids[0])
		size += 1
	}
	reverse_dict = append(reverse_dict, "<unmatched>")
	id.PeopleDict = dict
	id.ReversePeopleDict = reverse_dict
	return nil
}

func (id *IdentityDetector) GeneratePeopleDict(commits []*object.Commit) {
	dict := make(map[string]int)
	emails := make(map[int][]string)
	names := make(map[int][]string)
	size := 0
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
		reverse_dict[val] = strings.Join(names[val], "|") + "|" + strings.Join(emails[val], "|")
	}
	id.PeopleDict = dict
	id.ReversePeopleDict = reverse_dict
}
