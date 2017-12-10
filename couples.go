package hercules

import (
	"fmt"
	"io"
	"sort"

	"github.com/gogo/protobuf/proto"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v3/pb"
	"gopkg.in/src-d/hercules.v3/yaml"
)

type CouplesAnalysis struct {
	// The number of developers for which to build the matrix. 0 disables this analysis.
	PeopleNumber int

	// people store how many times every developer committed to every file.
	people []map[string]int
	// people_commits is the number of commits each author made
	people_commits []int
	// files store every file occurred in the same commit with every other file.
	files map[string]map[string]int
	// references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
}

type CouplesResult struct {
	PeopleMatrix []map[int]int64
	PeopleFiles  [][]int
	FilesMatrix  []map[int]int64
	Files        []string
}

func (couples *CouplesAnalysis) Name() string {
	return "Couples"
}

func (couples *CouplesAnalysis) Provides() []string {
	return []string{}
}

func (couples *CouplesAnalysis) Requires() []string {
	arr := [...]string{"author", "changes"}
	return arr[:]
}

func (couples *CouplesAnalysis) ListConfigurationOptions() []ConfigurationOption {
	return []ConfigurationOption{}
}

func (couples *CouplesAnalysis) Configure(facts map[string]interface{}) {
	if val, exists := facts[FactIdentityDetectorPeopleCount].(int); exists {
		couples.PeopleNumber = val
		couples.reversedPeopleDict = facts[FactIdentityDetectorReversedPeopleDict].([]string)
	}
}

func (couples *CouplesAnalysis) Flag() string {
	return "couples"
}

func (couples *CouplesAnalysis) Initialize(repository *git.Repository) {
	couples.people = make([]map[string]int, couples.PeopleNumber+1)
	for i := range couples.people {
		couples.people[i] = map[string]int{}
	}
	couples.people_commits = make([]int, couples.PeopleNumber+1)
	couples.files = map[string]map[string]int{}
}

func (couples *CouplesAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	author := deps["author"].(int)
	if author == MISSING_AUTHOR {
		author = couples.PeopleNumber
	}
	couples.people_commits[author] += 1
	tree_diff := deps["changes"].(object.Changes)
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
		toName := change.To.Name
		fromName := change.From.Name
		switch action {
		case merkletrie.Insert:
			context = append(context, toName)
			couples.people[author][toName] += 1
		case merkletrie.Delete:
			deleteFile(fromName)
			couples.people[author][fromName] += 1
		case merkletrie.Modify:
			if fromName != toName {
				// renamed
				couples.files[toName] = couples.files[fromName]
				for _, otherFiles := range couples.files {
					val, exists := otherFiles[fromName]
					if exists {
						otherFiles[toName] = val
					}
				}
				deleteFile(fromName)
				for _, authorFiles := range couples.people {
					val, exists := authorFiles[fromName]
					if exists {
						authorFiles[toName] = val
						delete(authorFiles, fromName)
					}
				}
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

func (couples *CouplesAnalysis) Finalize() interface{} {
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

	peopleMatrix := make([]map[int]int64, couples.PeopleNumber+1)
	peopleFiles := make([][]int, couples.PeopleNumber+1)
	for i := range peopleMatrix {
		peopleMatrix[i] = map[int]int64{}
		for file, commits := range couples.people[i] {
			fi, exists := filesIndex[file]
			if exists {
				peopleFiles[i] = append(peopleFiles[i], fi)
			}
			for j, otherFiles := range couples.people {
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
		sort.Ints(peopleFiles[i])
	}

	filesMatrix := make([]map[int]int64, len(filesIndex))
	for i := range filesMatrix {
		filesMatrix[i] = map[int]int64{}
		for otherFile, cooccs := range couples.files[filesSequence[i]] {
			filesMatrix[i][filesIndex[otherFile]] = int64(cooccs)
		}
	}
	return CouplesResult{
		PeopleMatrix: peopleMatrix, PeopleFiles: peopleFiles,
		Files: filesSequence, FilesMatrix: filesMatrix}
}

func (couples *CouplesAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	couplesResult := result.(CouplesResult)
	if binary {
		return couples.serializeBinary(&couplesResult, writer)
	}
	couples.serializeText(&couplesResult, writer)
	return nil
}

func (couples *CouplesAnalysis) serializeText(result *CouplesResult, writer io.Writer) {
	fmt.Fprintln(writer, "  files_coocc:")
	fmt.Fprintln(writer, "    index:")
	for _, file := range result.Files {
		fmt.Fprintf(writer, "      - %s\n", yaml.SafeString(file))
	}

	fmt.Fprintln(writer, "    matrix:")
	for _, files := range result.FilesMatrix {
		fmt.Fprint(writer, "      - {")
		indices := []int{}
		for file := range files {
			indices = append(indices, file)
		}
		sort.Ints(indices)
		for i, file := range indices {
			fmt.Fprintf(writer, "%d: %d", file, files[file])
			if i < len(indices)-1 {
				fmt.Fprint(writer, ", ")
			}
		}
		fmt.Fprintln(writer, "}")
	}

	fmt.Fprintln(writer, "  people_coocc:")
	fmt.Fprintln(writer, "    index:")
	for _, person := range couples.reversedPeopleDict {
		fmt.Fprintf(writer, "      - %s\n", yaml.SafeString(person))
	}

	fmt.Fprintln(writer, "    matrix:")
	for _, people := range result.PeopleMatrix {
		fmt.Fprint(writer, "      - {")
		indices := []int{}
		for file := range people {
			indices = append(indices, file)
		}
		sort.Ints(indices)
		for i, person := range indices {
			fmt.Fprintf(writer, "%d: %d", person, people[person])
			if i < len(indices)-1 {
				fmt.Fprint(writer, ", ")
			}
		}
		fmt.Fprintln(writer, "}")
	}

	fmt.Fprintln(writer, "    author_files:") // sorted by number of files each author changed
	peopleFiles := sortByNumberOfFiles(result.PeopleFiles, couples.reversedPeopleDict, result.Files)
	for _, authorFiles := range peopleFiles {
		fmt.Fprintf(writer, "      - %s:\n", yaml.SafeString(authorFiles.Author))
		sort.Strings(authorFiles.Files)
		for _, file := range authorFiles.Files {
			fmt.Fprintf(writer, "        - %s\n", yaml.SafeString(file)) // sorted by path
		}
	}
}

func sortByNumberOfFiles(
	peopleFiles [][]int, peopleDict []string, filesDict []string) authorFilesList {
	var pfl authorFilesList
	for peopleIdx, files := range peopleFiles {
		if peopleIdx < len(peopleDict) {
			fileNames := make([]string, len(files))
			for i, fi := range files {
				fileNames[i] = filesDict[fi]
			}
			pfl = append(pfl, authorFiles{peopleDict[peopleIdx], fileNames})
		}
	}
	sort.Sort(pfl)
	return pfl
}

type authorFiles struct {
	Author string
	Files  []string
}

type authorFilesList []authorFiles

func (s authorFilesList) Len() int {
	return len(s)
}
func (s authorFilesList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s authorFilesList) Less(i, j int) bool {
	return len(s[i].Files) < len(s[j].Files)
}

func (couples *CouplesAnalysis) serializeBinary(result *CouplesResult, writer io.Writer) error {
	message := pb.CouplesAnalysisResults{}

	message.FileCouples = &pb.Couples{
		Index:  result.Files,
		Matrix: pb.MapToCompressedSparseRowMatrix(result.FilesMatrix),
	}
	message.DeveloperCouples = &pb.Couples{
		Index:  couples.reversedPeopleDict,
		Matrix: pb.MapToCompressedSparseRowMatrix(result.PeopleMatrix),
	}
	message.TouchedFiles = &pb.DeveloperTouchedFiles{
		Developers: make([]*pb.TouchedFiles, len(couples.reversedPeopleDict)),
	}
	for key := range couples.reversedPeopleDict {
		files := result.PeopleFiles[key]
		int32Files := make([]int32, len(files))
		for i, f := range files {
			int32Files[i] = int32(f)
		}
		message.TouchedFiles.Developers[key] = &pb.TouchedFiles{
			Files: int32Files,
		}
	}

	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	writer.Write(serialized)
	return nil
}

func init() {
	Registry.Register(&CouplesAnalysis{})
}
