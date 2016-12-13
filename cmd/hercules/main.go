package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/hercules.v1"
)

func main() {
	var profile bool
	flag.BoolVar(&profile, "profile", false, "Collect the profile to hercules.pprof.")
	flag.Parse()
	if profile {
		prof, _ := os.Create("profile")
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
		fmt.Fprint(os.Stderr, "Cloning...\r")
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
		OnProgress: func(commit int) {
		  fmt.Fprintf(os.Stderr, "%d\r", commit)
	  },
	}
	statuses, last := analyser.Analyse()
	fmt.Fprint(os.Stderr, "        \r")
	if len(statuses) == 0 {
		return
	}
	// determine the maximum length of each value
	var maxnum int64
	for _, status := range statuses {
		var week int64
		for i := 0; i < last; i++ {
			val, _ := status[i]
			week += val
			if i % 7 == 6 {
				if week > maxnum {
					maxnum = week;
				}
				week = 0
			}
		}
	}
	width := len(strconv.FormatInt(maxnum, 10))
	// print the resulting triangle matrix
	for _, status := range statuses {
		var week int64
		for i := 0; i < last; i++ {
			val, _ := status[i]
			week += val
			if i % 7 == 6 {
				fmt.Printf("%[1]*[2]d ", width, week)
				week = 0
			}
		}
		println()
	}
}
