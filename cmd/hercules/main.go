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

func main() {
	var profile bool
	var granularity, sampling, similarity_threshold int
	var commitsFile string
	var debug bool
	flag.BoolVar(&profile, "profile", false, "Collect the profile to hercules.pprof.")
	flag.IntVar(&granularity, "granularity", 30, "Report granularity in days.")
	flag.IntVar(&sampling, "sampling", 30, "Report sampling in days.")
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
	statuses := analyser.Analyse(commits)
	fmt.Fprint(os.Stderr, "                \r")
	if len(statuses) == 0 {
		return
	}
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
	// print the start date, granularity, sampling
	fmt.Println(commits[0].Author.When.Unix(),
		commits[len(commits)-1].Author.When.Unix(),
		granularity, sampling)
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
