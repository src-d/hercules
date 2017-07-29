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
	if with_people {
		if people_dict_path != "" {
			id_matcher.LoadPeopleDict(people_dict_path)
		} else {
			id_matcher.GeneratePeopleDict(commits)
		}
	}
	pipeline.AddItem(id_matcher)
	burndowner := &hercules.BurndownAnalysis{
		Granularity:         granularity,
		Sampling:            sampling,
		Debug:               debug,
		PeopleNumber:        len(id_matcher.ReversePeopleDict),
	}
	pipeline.AddItem(burndowner)

	pipeline.Initialize()
	result, err := pipeline.Run(commits)
	if err != nil {
		panic(err)
	}
	burndown_results := result[burndowner].(hercules.BurndownResult)
	fmt.Fprint(os.Stderr, "                \r")
	if len(burndown_results.GlobalHistory) == 0 {
		return
	}
	// print the start date, granularity, sampling
	fmt.Println(commits[0].Author.When.Unix(),
		commits[len(commits)-1].Author.When.Unix(),
		granularity, sampling)
	printStatuses(burndown_results.GlobalHistory, uri)
	if with_files {
		fmt.Print("files\n")
		keys := sortedKeys(burndown_results.FileHistories)
		for _, key := range keys {
			printStatuses(burndown_results.FileHistories[key], key)
		}
	}
	if with_people {
		fmt.Print("people\n")
		for key, val := range burndown_results.PeopleHistories {
			fmt.Printf("%d: ", key)
			printStatuses(val, id_matcher.ReversePeopleDict[key])
		}
		fmt.Println()
		for _, row := range(burndown_results.PeopleMatrix) {
			for _, cell := range(row) {
				fmt.Print(cell, " ")
			}
			fmt.Print("\n")
		}
	}
}
