/*
Package main provides the command line tool to gather the line burndown
statistics from Git repositories. Usage:

	hercules <URL or FS path>
*/
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/src-d/go-billy.v2/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"gopkg.in/src-d/hercules.v1"
)

// Signature stores the author's identification. Only a single field is used to identify the
// commit: first Email is checked, then Name.
type Signature struct {
	Name string
	Email string
}

func loadPeopleDict(path string) (map[string]int, map[int]string, int) {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	dict := make(map[string]int)
	reverse_dict := make(map[int]string)
	size := 0
	for scanner.Scan() {
		for _, id := range strings.Split(scanner.Text(), "|") {
			dict[id] = size
		}
		reverse_dict[size] = scanner.Text()
		size += 1
	}
	return dict, reverse_dict, size
}

func generatePeopleDict(commits []*object.Commit) (map[string]int, map[int]string, int) {
	dict := make(map[string]int)
	emails := make(map[int][]string)
	names := make(map[int][]string)
	size := 0
	for _, commit := range commits {
		id, exists := dict[commit.Author.Email]
		if exists {
			_, exists := dict[commit.Author.Name]
			if !exists {
				dict[commit.Author.Name] = id
			  names[id] = append(names[id], commit.Author.Name)
			}
			continue
		}
		id, exists = dict[commit.Author.Name]
		if exists {
			dict[commit.Author.Email] = id
			emails[id] = append(emails[id], commit.Author.Email)
			continue
		}
		dict[commit.Author.Email] = size
		dict[commit.Author.Name] = size
		emails[size] = append(emails[size], commit.Author.Email)
		names[size] = append(names[size], commit.Author.Name)
		size += 1
	}
	reverse_dict := make(map[int]string)
	for _, val := range dict {
		reverse_dict[val] = strings.Join(names[val], "|") + "|" + strings.Join(emails[val], "|")
	}
	return dict, reverse_dict, size
}

func loadCommitsFromFile(path string, repository *git.Repository) []*object.Commit {
	var file io.Reader
	if path != "-" {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		defer file.Close()
	} else {
		file = os.Stdin
	}
	scanner := bufio.NewScanner(file)
	commits := []*object.Commit{}
	for scanner.Scan() {
		hash := plumbing.NewHash(scanner.Text())
		if len(hash) != 20 {
			panic("invalid commit hash " + scanner.Text())
		}
		commit, err := repository.CommitObject(hash)
		if err != nil {
			panic(err)
		}
		commits = append(commits, commit)
	}
	return commits
}

func printStatuses(statuses [][]int64, name string) {
	// determine the maximum length of each value
	var maxnum int64
	for _, status := range statuses {
		for _, val := range status {
			if val > maxnum {
				maxnum = val
			}
		}
	}
	width := len(strconv.FormatInt(maxnum, 10))
	last := len(statuses[len(statuses)-1])
	if name != "" {
		fmt.Println(name)
	}
	// print the resulting triangle matrix
	for _, status := range statuses {
		for i := 0; i < last; i++ {
			var val int64
			if i < len(status) {
				val = status[i]
				// not sure why this sometimes happens...
				// TODO(vmarkovtsev): find the root cause of tiny negative balances
				if val < 0 {
					val = 0
				}
			}
			fmt.Printf("%[1]*[2]d ", width, val)
		}
		fmt.Println()
	}
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
	var people_dict_path string
	var profile bool
	var granularity, sampling, similarity_threshold int
	var commitsFile string
	var debug bool
	flag.BoolVar(&with_files, "files", false, "Output detailed statistics per each file.")
	flag.BoolVar(&with_people, "people", false, "Output detailed statistics per each developer.")
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
	analyser := hercules.Analyser{
		Repository: repository,
		OnProgress: func(commit, length int) {
			fmt.Fprintf(os.Stderr, "%d / %d\r", commit, length)
		},
		Granularity:         granularity,
		Sampling:            sampling,
		SimilarityThreshold: similarity_threshold,
		Debug:               debug,
	}
	// list of commits belonging to the default branch, from oldest to newest
	// rev-list --first-parent
	var commits []*object.Commit
	if commitsFile == "" {
		commits = analyser.Commits()
	} else {
		commits = loadCommitsFromFile(commitsFile, repository)
	}
	var people_ids map[int]string
	if with_people {
		var people_dict map[string]int
		var people_number int
		if people_dict_path != "" {
			people_dict, people_ids, people_number = loadPeopleDict(people_dict_path)
		} else {
			people_dict, people_ids, people_number = generatePeopleDict(commits)
		}
		analyser.PeopleNumber = people_number
		analyser.PeopleDict = people_dict
	}
	global_statuses, file_statuses, people_statuses, people_matrix := analyser.Analyse(commits)
	fmt.Fprint(os.Stderr, "                \r")
	if len(global_statuses) == 0 {
		return
	}
	// print the start date, granularity, sampling
	fmt.Println(commits[0].Author.When.Unix(),
		commits[len(commits)-1].Author.When.Unix(),
		granularity, sampling)
	printStatuses(global_statuses, "")
	if with_files {
		keys := sortedKeys(file_statuses)
		for _, key := range keys {
			fmt.Println()
			printStatuses(file_statuses[key], key)
		}
	}
	if with_people {
		fmt.Printf("%d\n", len(people_statuses))
		for key, val := range people_statuses {
			fmt.Printf("%d: ", key)
			printStatuses(val, people_ids[key])
			fmt.Println()
		}
		for _, row := range(people_matrix) {
			for _, cell := range(row) {
				fmt.Print(cell, " ")
			}
			fmt.Print("\n")
		}
	}
}
