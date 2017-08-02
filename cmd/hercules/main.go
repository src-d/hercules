/*
Package main provides the command line tool to gather the line burndown
statistics from Git repositories. Usage:

	hercules <URL or FS path>
*/
package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/src-d/go-billy.v3/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"gopkg.in/src-d/hercules.v1"
)

func safeString(str string) string {
	str = strings.Replace(str, "\"", "'", -1)
	str = strings.Replace(str, "\\", " ", -1)
	str = strings.Replace(str, ":", " ", -1)
	return "\"" + str + "\""
}

func printMatrix(matrix [][]int64, name string, fixNegative bool) {
	// determine the maximum length of each value
	var maxnum int64 = -(1 << 32)
	var minnum int64 = 1 << 32
	for _, status := range matrix {
		for _, val := range status {
			if val > maxnum {
				maxnum = val
			}
			if val < minnum {
				minnum = val
			}
		}
	}
	width := len(strconv.FormatInt(maxnum, 10))
	if !fixNegative && minnum < 0 {
		width = len(strconv.FormatInt(minnum, 10))
	}
	last := len(matrix[len(matrix)-1])
	indent := 2
	if name != "" {
		fmt.Printf("  %s: |-\n", safeString(name))
		indent += 2
	}
	// print the resulting triangular matrix
	for _, status := range matrix {
		fmt.Print(strings.Repeat(" ", indent-1))
		for i := 0; i < last; i++ {
			var val int64
			if i < len(status) {
				val = status[i]
				// not sure why this sometimes happens...
				// TODO(vmarkovtsev): find the root cause of tiny negative balances
				if fixNegative && val < 0 {
					val = 0
				}
			}
			fmt.Printf(" %[1]*[2]d", width, val)
		}
		fmt.Println()
	}
}

func printCouples(result *hercules.CouplesResult, peopleDict []string) {
	fmt.Println("files_coocc:")
	fmt.Println("  index:")
	for _, file := range result.Files {
		fmt.Printf("    - %s\n", safeString(file))
	}

	fmt.Println("  matrix:")
	for _, files := range result.FilesMatrix {
		fmt.Print("    - {")
		indices := []int{}
		for file := range files {
			indices = append(indices, file)
		}
		sort.Ints(indices)
		for i, file := range indices {
			fmt.Printf("%d: %d", file, files[file])
			if i < len(indices)-1 {
				fmt.Print(", ")
			}
		}
		fmt.Println("}")
	}

	fmt.Println("people_coocc:")
	fmt.Println("  index:")
	for _, person := range peopleDict {
		fmt.Printf("    - %s\n", safeString(person))
	}

	fmt.Println("  matrix:")
	for _, people := range result.PeopleMatrix {
		fmt.Print("    - {")
		indices := []int{}
		for file := range people {
			indices = append(indices, file)
		}
		sort.Ints(indices)
		for i, person := range indices {
			fmt.Printf("%d: %d", person, people[person])
			if i < len(indices)-1 {
				fmt.Print(", ")
			}
		}
		fmt.Println("}")
	}

	fmt.Println("  author_files:") // sorted by number of files each author changed
	peopleFiles := sortByNumberOfFiles(result.PeopleFiles, peopleDict)
	for _, authorFiles := range peopleFiles {
		fmt.Printf("    - %s:\n", safeString(authorFiles.Author))
		sort.Strings(authorFiles.Files)
		for _, file := range authorFiles.Files {
			fmt.Printf("      - %s\n", safeString(file)) // sorted by path
		}
	}
}

func sortByNumberOfFiles(peopleFiles [][]string, peopleDict []string) AuthorFilesList {
	var pfl AuthorFilesList
	for peopleIdx, files := range peopleFiles {
		pfl = append(pfl, AuthorFiles{peopleDict[peopleIdx], files})
	}
	sort.Sort(pfl)
	return pfl
}

type AuthorFiles struct {
	Author string
	Files  []string
}

type AuthorFilesList []AuthorFiles

func (s AuthorFilesList) Len() int {
	return len(s)
}
func (s AuthorFilesList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s AuthorFilesList) Less(i, j int) bool {
	return len(s[i].Files) < len(s[j].Files)
}

func sortedKeys(m map[string][][]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func main() {
	var with_files bool
	var with_people bool
	var with_couples bool
	var people_dict_path string
	var profile bool
	var granularity, sampling, similarity_threshold int
	var commitsFile string
	var debug bool
	flag.BoolVar(&with_files, "files", false, "Output detailed statistics per each file.")
	flag.BoolVar(&with_people, "people", false, "Output detailed statistics per each developer.")
	flag.BoolVar(&with_couples, "couples", false, "Gather the co-occurrence matrix "+
		"for files and people.")
	flag.StringVar(&people_dict_path, "people-dict", "", "Path to the developers' email associations.")
	flag.BoolVar(&profile, "profile", false, "Collect the profile to hercules.pprof.")
	flag.IntVar(&granularity, "granularity", 30, "How many days there are in a single band.")
	flag.IntVar(&sampling, "sampling", 30, "How frequently to record the state in days.")
	flag.IntVar(&similarity_threshold, "M", 90,
		"A threshold on the similarity index used to detect renames.")
	flag.BoolVar(&debug, "debug", false, "Validate the trees on each step.")
	flag.StringVar(&commitsFile, "commits", "", "Path to the text file with the "+
		"commit history to follow instead of the default rev-list "+
		"--first-parent. The format is the list of hashes, each hash on a "+
		"separate line. The first hash is the root.")
	flag.Parse()
	if granularity <= 0 {
		fmt.Fprint(os.Stderr, "Warning: adjusted the granularity to 1 day\n")
		granularity = 1
	}
	if profile {
		go http.ListenAndServe("localhost:6060", nil)
		prof, _ := os.Create("hercules.pprof")
		pprof.StartCPUProfile(prof)
		defer pprof.StopCPUProfile()
	}
	if len(flag.Args()) == 0 || len(flag.Args()) > 3 {
		fmt.Fprint(os.Stderr,
			"Usage: hercules <path to repo or URL> [<disk cache path>]\n")
		os.Exit(1)
	}
	uri := flag.Arg(0)
	var repository *git.Repository
	var storage storage.Storer
	var err error
	if strings.Contains(uri, "://") {
		if len(flag.Args()) == 2 {
			storage, err = filesystem.NewStorage(osfs.New(flag.Arg(1)))
			if err != nil {
				panic(err)
			}
		} else {
			storage = memory.NewStorage()
		}
		fmt.Fprint(os.Stderr, "cloning...\r")
		repository, err = git.Clone(storage, nil, &git.CloneOptions{
			URL: uri,
		})
		fmt.Fprint(os.Stderr, "          \r")
	} else {
		if uri[len(uri)-1] == os.PathSeparator {
			uri = uri[:len(uri)-1]
		}
		repository, err = git.PlainOpen(uri)
	}
	if err != nil {
		panic(err)
	}

	// core logic
	pipeline := hercules.NewPipeline(repository)
	pipeline.OnProgress = func(commit, length int) {
		fmt.Fprintf(os.Stderr, "%d / %d\r", commit, length)
	}
	// list of commits belonging to the default branch, from oldest to newest
	// rev-list --first-parent
	var commits []*object.Commit
	if commitsFile == "" {
		commits = pipeline.Commits()
	} else {
		commits = hercules.LoadCommitsFromFile(commitsFile, repository)
	}

	pipeline.AddItem(&hercules.BlobCache{})
	pipeline.AddItem(&hercules.DaysSinceStart{})
	pipeline.AddItem(&hercules.RenameAnalysis{SimilarityThreshold: similarity_threshold})
	pipeline.AddItem(&hercules.TreeDiff{})
	id_matcher := &hercules.IdentityDetector{}
	if with_people || with_couples {
		if people_dict_path != "" {
			id_matcher.LoadPeopleDict(people_dict_path)
		} else {
			id_matcher.GeneratePeopleDict(commits)
		}
	}
	pipeline.AddItem(id_matcher)
	burndowner := &hercules.BurndownAnalysis{
		Granularity:  granularity,
		Sampling:     sampling,
		Debug:        debug,
		PeopleNumber: len(id_matcher.ReversePeopleDict),
	}
	pipeline.AddItem(burndowner)
	var coupler *hercules.Couples
	if with_couples {
		coupler = &hercules.Couples{PeopleNumber: len(id_matcher.ReversePeopleDict)}
		pipeline.AddItem(coupler)
	}

	pipeline.Initialize()
	result, err := pipeline.Run(commits)
	if err != nil {
		panic(err)
	}
	fmt.Fprint(os.Stderr, "writing...    \r")
	burndown_results := result[burndowner].(hercules.BurndownResult)
	var couples_result hercules.CouplesResult
	if with_couples {
		couples_result = result[coupler].(hercules.CouplesResult)
	}
	fmt.Fprint(os.Stderr, "                \r")
	if len(burndown_results.GlobalHistory) == 0 {
		return
	}
	// print the start date, granularity, sampling
	fmt.Println("burndown:")
	fmt.Println("  version: 1")
	fmt.Println("  begin:", commits[0].Author.When.Unix())
	fmt.Println("  end:", commits[len(commits)-1].Author.When.Unix())
	fmt.Println("  granularity:", granularity)
	fmt.Println("  sampling:", sampling)
	fmt.Println("project:")
	printMatrix(burndown_results.GlobalHistory, uri, true)
	if with_files {
		fmt.Println("files:")
		keys := sortedKeys(burndown_results.FileHistories)
		for _, key := range keys {
			printMatrix(burndown_results.FileHistories[key], key, true)
		}
	}
	if with_people {
		fmt.Println("people_sequence:")
		for key := range burndown_results.PeopleHistories {
			fmt.Println("  - " + id_matcher.ReversePeopleDict[key])
		}
		fmt.Println("people:")
		for key, val := range burndown_results.PeopleHistories {
			printMatrix(val, id_matcher.ReversePeopleDict[key], true)
		}
		fmt.Println("people_interaction: |-")
		printMatrix(burndown_results.PeopleMatrix, "", false)
	}
	if with_couples {
		printCouples(&couples_result, id_matcher.ReversePeopleDict)
	}
}
