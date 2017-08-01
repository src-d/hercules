package hercules

import (
	"sort"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
)

type Couples struct {
	// The number of developers for which to build the matrix. 0 disables this analysis.
	PeopleNumber int

	// people store how many times every developer committed to every file.
	people []map[string]int
	// people_commits is the number of commits each author made
	people_commits []int
	// files store every file occurred in the same commit with every other file.
	files map[string]map[string]int
}

type CouplesResult struct {
	PeopleMatrix []map[int]int64
	PeopleFiles  [][]string
	FilesMatrix  []map[int]int
	Files        []string
}

func (couples *Couples) Name() string {
	return "Couples"
}

func (couples *Couples) Provides() []string {
	return []string{}
}

func (couples *Couples) Requires() []string {
	arr := [...]string{"author", "renamed_changes"}
	return arr[:]
}

func (couples *Couples) Initialize(repository *git.Repository) {
	couples.people = make([]map[string]int, couples.PeopleNumber)
	for i := range couples.people {
		couples.people[i] = map[string]int{}
	}
	couples.people_commits = make([]int, couples.PeopleNumber)
	couples.files = map[string]map[string]int{}
}

func (couples *Couples) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	author := deps["author"].(int)
	couples.people_commits[author] += 1
	tree_diff := deps["renamed_changes"].(object.Changes)
	context := make([]string, 0)
	deleteFile := func(name string) {
		// we do not remove the file from people - the context does not expire
		delete(couples.files, name)
		for _, otherFiles := range couples.files {
			delete(otherFiles, name)
		}
	}
	for _, change := range tree_diff {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		switch action {
		case merkletrie.Insert:
			context = append(context, change.To.Name)
			couples.people[author][change.To.Name] += 1
		case merkletrie.Delete:
			deleteFile(change.From.Name)
		case merkletrie.Modify:
			toName := change.To.Name
			fromName := change.From.Name
			if fromName != toName {
				// renamed
				couples.files[toName] = couples.files[fromName]
				for _, otherFiles := range couples.files {
					val, exists := otherFiles[fromName]
					if exists {
						otherFiles[toName] = val
					}
				}
				deleteFile(change.From.Name)
			}
			context = append(context, toName)
			couples.people[author][toName] += 1
		}
	}
	for _, file := range context {
		for _, otherFile := range context {
			lane, exists := couples.files[file]
			if !exists {
				lane = map[string]int{}
				couples.files[file] = lane
			}
			lane[otherFile] += 1
		}
	}
	return nil, nil
}

func (couples *Couples) Finalize() interface{} {
	peopleMatrix := make([]map[int]int64, couples.PeopleNumber)
	peopleFiles := make([][]string, couples.PeopleNumber)
	for i := range peopleMatrix {
		peopleMatrix[i] = map[int]int64{}
		for file, commits := range couples.people[i] {
			//could be normalized further, by replacing file with idx in fileSequence
			//but the would trade the space for readabilyt of result
			peopleFiles[i] = append(peopleFiles[i], file)
			for j, otherFiles := range couples.people {
				if i == j {
					continue
				}
				otherCommits := otherFiles[file]
				delta := otherCommits
				if otherCommits > commits {
					delta = commits
				}
				if delta > 0 {
					peopleMatrix[i][j] += int64(delta)
				}
			}
		}
		peopleMatrix[i][i] = int64(couples.people_commits[i])
	}
	filesSequence := make([]string, len(couples.files))
	i := 0
	for file := range couples.files {
		filesSequence[i] = file
		i++
	}
	sort.Strings(filesSequence)
	filesIndex := map[string]int{}
	for i, file := range filesSequence {
		filesIndex[file] = i
	}
	filesMatrix := make([]map[int]int, len(filesIndex))
	for i := range filesMatrix {
		filesMatrix[i] = map[int]int{}
		for otherFile, cooccs := range couples.files[filesSequence[i]] {
			filesMatrix[i][filesIndex[otherFile]] = cooccs
		}
	}
	return CouplesResult{PeopleMatrix: peopleMatrix, PeopleFiles: peopleFiles, Files: filesSequence, FilesMatrix: filesMatrix}
}
