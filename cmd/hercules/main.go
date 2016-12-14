package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/hercules.v1"
)

func loadCommitsFromFile(path string, repositry *git.Repository) []*git.Commit {
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
	commits := []*git.Commit{}
	for scanner.Scan() {
		hash := plumbing.NewHash(scanner.Text())
		if len(hash) != 20 {
			panic("invalid commit hash " + scanner.Text())
		}
		commit, err := repositry.Commit(hash)
		if err != nil {
			panic(err)
		}
		commits = append(commits, commit)
	}
	return commits
}

func main() {
	var profile bool
	var granularity, sampling int
	var commitsFile string
	flag.BoolVar(&profile, "profile", false, "Collect the profile to hercules.pprof.")
	flag.IntVar(&granularity, "granularity", 30, "Report granularity in days.")
	flag.IntVar(&sampling, "sampling", 30, "Report sampling in days.")
	flag.StringVar(&commitsFile, "commits", "", "Path to the text file with the " +
	    "commit history to follow instead of the default rev-list " +
			"--first-parent. The format is the list of hashes, each hash on a " +
			"separate line. The first hash is the root.")
	flag.Parse()
	if (granularity <= 0) {
		fmt.Fprint(os.Stderr, "Warning: adjusted the granularity to 1 day\n")
		granularity = 1
	}
	if profile {
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
	var err error
	if strings.Contains(uri, "://") {
		if len(flag.Args()) == 2 {
			repository, err = git.NewFilesystemRepository(flag.Arg(1))
			if err != nil {
				panic(err)
			}
		} else {
			repository = git.NewMemoryRepository()
		}
		fmt.Fprint(os.Stderr, "cloning...\r")
		err = repository.Clone(&git.CloneOptions{
		  URL: uri,
	  })
		fmt.Fprint(os.Stderr, "          \r")
	} else {
		if uri[len(uri) - 1] == os.PathSeparator {
			uri = uri[:len(uri) - 1]
		}
		if !strings.HasSuffix(uri, ".git") {
			uri += string(os.PathSeparator) + ".git"
		}
		repository, err = git.NewFilesystemRepository(uri)
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
		Granularity: granularity,
		Sampling: sampling,
	}
	// list of commits belonging to the default branch, from oldest to newest
	// rev-list --first-parent
	var commits []*git.Commit
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
	last := len(statuses[len(statuses) - 1])
	// print the start date, granularity, sampling
	fmt.Println(commits[0].Author.When.Unix(), granularity, sampling)
	// print the resulting triangle matrix
	for _, status := range statuses {
		for i := 0; i < last; i++ {
			var val int64
			if i < len(status) {
				val = status[i]
			}
			fmt.Printf("%[1]*[2]d ", width, val)
		}
		fmt.Println()
	}
}
