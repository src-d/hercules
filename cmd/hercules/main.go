/*
Package main provides the command line tool to gather the line burndown
statistics from Git repositories. Usage:

	hercules <URL or FS path>

Output is always written to stdout, progress is written to stderr.
Output formats:

- YAML (default)
- Protocol Buffers (-pb)

Extensions:

-files include line burndown stats for each file alive in HEAD
-people include line burndown stats for each developer
-couples include coupling betwwen files and developers

-granularity sets the number of days in each band of the burndown charts.
-sampling set the frequency of measuring the state of burndown in days.

-people-dict allows to specify a hand-crafted identity matching list. The format is text,
each identity on separate line, matching names and emails separated with |

-debug activates the debugging mode - hardly ever needed, used internally during the development.
-profile activates the profile collection and runs the server on localhost:6060
*/
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"sort"
	"strings"

	"gopkg.in/src-d/go-billy.v3/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"gopkg.in/src-d/hercules.v2"
	"gopkg.in/src-d/hercules.v2/stdout"
	"gopkg.in/src-d/hercules.v2/pb"
	"github.com/gogo/protobuf/proto"
	"time"
)

func sortedKeys(m map[string][][]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

type OneLineWriter struct {
	Writer io.Writer
}

func (writer OneLineWriter) Write(p []byte) (n int, err error) {
	if p[len(p) - 1] == '\n' {
		p = p[:len(p) - 1]
		if len(p) > 5 && bytes.Compare(p[len(p) - 5:], []byte("done.")) == 0 {
			p = []byte("cloning...")
		}
		p = append(p, '\r')
		writer.Writer.Write([]byte("\r" + strings.Repeat(" ", 80) + "\r"))
	}
	n, err = writer.Writer.Write(p)
	return
}

func main() {
	var protobuf bool
	var withFiles bool
	var withPeople bool
	var withCouples bool
	var withUasts bool
	var people_dict_path string
	var profile bool
	var granularity, sampling, similarityThreshold int
	var bblfshEndpoint string
	var bblfshTimeout int
	var uastPoolSize int
	var commitsFile string
	var ignoreMissingSubmodules bool
	var debug bool
	flag.BoolVar(&withFiles, "files", false, "Output detailed statistics per each file.")
	flag.BoolVar(&withPeople, "people", false, "Output detailed statistics per each developer.")
	flag.BoolVar(&withCouples, "couples", false, "Gather the co-occurrence matrix "+
		"for files and people.")
	flag.BoolVar(&withUasts, "uasts", false, "Output pairs of Universal Abstract Syntax Trees for " +
			"every changed file in each commit.")
	flag.StringVar(&people_dict_path, "people-dict", "", "Path to the developers' email associations.")
	flag.BoolVar(&profile, "profile", false, "Collect the profile to hercules.pprof.")
	flag.IntVar(&granularity, "granularity", 30, "How many days there are in a single band.")
	flag.IntVar(&sampling, "sampling", 30, "How frequently to record the state in days.")
	flag.IntVar(&similarityThreshold, "M", 90,
		"A threshold on the similarity index used to detect renames.")
	flag.BoolVar(&debug, "debug", false, "Validate the trees on each step.")
	flag.StringVar(&commitsFile, "commits", "", "Path to the text file with the "+
		"commit history to follow instead of the default rev-list "+
		"--first-parent. The format is the list of hashes, each hash on a "+
		"separate line. The first hash is the root.")
	flag.BoolVar(&ignoreMissingSubmodules, "ignore-missing-submodules", false,
		"Do not panic on submodules which are not registered..")
	flag.BoolVar(&protobuf, "pb", false, "The output format will be Protocol Buffers instead of YAML.")
	flag.IntVar(&uastPoolSize, "uast-pool-size", 1, "Number of goroutines to extract UASTs.")
	flag.StringVar(&bblfshEndpoint, "bblfsh", "0.0.0.0:9432", "Babelfish server's endpoint.")
	flag.IntVar(&bblfshTimeout, "bblfsh-timeout", 20, "Babelfish's server timeout.")
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
	var backend storage.Storer
	var err error
	if strings.Contains(uri, "://") {
		if len(flag.Args()) == 2 {
			backend, err = filesystem.NewStorage(osfs.New(flag.Arg(1)))
			if err != nil {
				panic(err)
			}
			_, err = os.Stat(flag.Arg(1))
			if !os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "warning: deleted %s\n", flag.Arg(1))
				os.RemoveAll(flag.Arg(1))
			}
		} else {
			backend = memory.NewStorage()
		}
		fmt.Fprint(os.Stderr, "cloning...\r")
		repository, err = git.Clone(backend, nil, &git.CloneOptions{
			URL: uri,
			Progress: OneLineWriter{Writer: os.Stderr},
		})
		fmt.Fprint(os.Stderr, strings.Repeat(" ", 80) + "\r")
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
		if commit < length {
			fmt.Fprintf(os.Stderr, "%d / %d\r", commit, length)
		} else {
			fmt.Fprint(os.Stderr, "finalizing...    \r")
		}
	}
	// list of commits belonging to the default branch, from oldest to newest
	// rev-list --first-parent
	var commits []*object.Commit
	if commitsFile == "" {
		commits = pipeline.Commits()
	} else {
		commits, err = hercules.LoadCommitsFromFile(commitsFile, repository)
		if err != nil {
			panic(err)
		}
	}

	pipeline.AddItem(&hercules.BlobCache{
		IgnoreMissingSubmodules: ignoreMissingSubmodules,
	})
	var uastSaver *hercules.UASTChangesSaver
	if withUasts {
		pipeline.AddItem(&hercules.UASTExtractor{
			Endpoint: bblfshEndpoint,
			Context: func() context.Context {
				ctx, _ := context.WithTimeout(context.Background(),
					                            time.Duration(bblfshTimeout) * time.Second)
				return ctx
			},
			PoolSize: uastPoolSize,
		  Extensions: map[string]bool {"py": true, "java": true}})
		pipeline.AddItem(&hercules.UASTChanges{})
		uastSaver = &hercules.UASTChangesSaver{}
		pipeline.AddItem(uastSaver)
	}
	pipeline.AddItem(&hercules.DaysSinceStart{})
	pipeline.AddItem(&hercules.RenameAnalysis{SimilarityThreshold: similarityThreshold})
	pipeline.AddItem(&hercules.TreeDiff{})
	pipeline.AddItem(&hercules.FileDiff{})
	idMatcher := &hercules.IdentityDetector{}
	var peopleCount int
	if withPeople || withCouples {
		if people_dict_path != "" {
			idMatcher.LoadPeopleDict(people_dict_path)
			peopleCount = len(idMatcher.ReversePeopleDict) - 1
		} else {
			idMatcher.GeneratePeopleDict(commits)
			peopleCount = len(idMatcher.ReversePeopleDict)
		}
	}
	pipeline.AddItem(idMatcher)
	burndowner := &hercules.BurndownAnalysis{
		Granularity:  granularity,
		Sampling:     sampling,
		Debug:        debug,
		TrackFiles:   withFiles,
		PeopleNumber: peopleCount,
	}
	pipeline.AddItem(burndowner)
	var coupler *hercules.Couples
	if withCouples {
		coupler = &hercules.Couples{PeopleNumber: peopleCount}
		pipeline.AddItem(coupler)
	}

	pipeline.Initialize()
	result, err := pipeline.Run(commits)
	if err != nil {
		panic(err)
	}
	fmt.Fprint(os.Stderr, "writing...    \r")
	burndownResults := result[burndowner].(hercules.BurndownResult)
	if len(burndownResults.GlobalHistory) == 0 {
		return
	}
	var couplesResult hercules.CouplesResult
	if withCouples {
		couplesResult = result[coupler].(hercules.CouplesResult)
	}
	if withUasts {
		changedUasts := result[uastSaver].([][]hercules.UASTChange)
		for i, changes := range changedUasts {
			for j, change := range changes {
				if change.Before == nil || change.After == nil {
					continue
				}
				bs, _ := change.Before.Marshal()
				ioutil.WriteFile(fmt.Sprintf(
					"%d_%d_before_%s.pb", i, j, change.Change.From.TreeEntry.Hash.String()), bs, 0666)
				blob, _ := repository.BlobObject(change.Change.From.TreeEntry.Hash)
				s, _ := (&object.File{Blob: *blob}).Contents()
				ioutil.WriteFile(fmt.Sprintf(
					"%d_%d_before_%s.src", i, j, change.Change.From.TreeEntry.Hash.String()), []byte(s), 0666)
				bs, _ = change.After.Marshal()
				ioutil.WriteFile(fmt.Sprintf(
					"%d_%d_after_%s.pb", i, j, change.Change.To.TreeEntry.Hash.String()), bs, 0666)
				blob, _ = repository.BlobObject(change.Change.To.TreeEntry.Hash)
				s, _ = (&object.File{Blob: *blob}).Contents()
				ioutil.WriteFile(fmt.Sprintf(
					"%d_%d_after_%s.src", i, j, change.Change.To.TreeEntry.Hash.String()), []byte(s), 0666)
			}
		}
	}
	begin := commits[0].Author.When.Unix()
	end := commits[len(commits)-1].Author.When.Unix()
	if !protobuf {
		printResults(uri, begin, end, granularity, sampling,
			withFiles, withPeople, withCouples,
			burndownResults, couplesResult, idMatcher.ReversePeopleDict)
	} else {
		serializeResults(uri, begin, end, granularity, sampling,
			withFiles, withPeople, withCouples,
			burndownResults, couplesResult, idMatcher.ReversePeopleDict)
	}
}

func printResults(
	uri string, begin, end int64, granularity, sampling int,
	withFiles, withPeople, withCouples bool,
	burndownResults hercules.BurndownResult,
	couplesResult hercules.CouplesResult,
	reversePeopleDict []string) {

	fmt.Println("burndown:")
	fmt.Println("  version: 1")
	fmt.Println("  begin:", begin)
	fmt.Println("  end:", end)
	fmt.Println("  granularity:", granularity)
	fmt.Println("  sampling:", sampling)
	fmt.Println("project:")
	stdout.PrintMatrix(burndownResults.GlobalHistory, uri, true)
	if withFiles {
		fmt.Println("files:")
		keys := sortedKeys(burndownResults.FileHistories)
		for _, key := range keys {
			stdout.PrintMatrix(burndownResults.FileHistories[key], key, true)
		}
	}
	if withPeople {
		fmt.Println("people_sequence:")
		for key := range burndownResults.PeopleHistories {
			fmt.Println("  - " + stdout.SafeString(reversePeopleDict[key]))
		}
		fmt.Println("people:")
		for key, val := range burndownResults.PeopleHistories {
			stdout.PrintMatrix(val, reversePeopleDict[key], true)
		}
		fmt.Println("people_interaction: |-")
		stdout.PrintMatrix(burndownResults.PeopleMatrix, "", false)
	}
	if withCouples {
		stdout.PrintCouples(&couplesResult, reversePeopleDict)
	}
}

func serializeResults(
	uri string, begin, end int64, granularity, sampling int,
	withFiles, withPeople, withCouples bool,
	burndownResults hercules.BurndownResult,
	couplesResult hercules.CouplesResult,
	reversePeopleDict []string) {

  header := pb.Metadata{
	  Version: 1,
	  Cmdline: strings.Join(os.Args, " "),
	  Repository: uri,
    BeginUnixTime: begin,
	  EndUnixTime: end,
	  Granularity: int32(granularity),
	  Sampling: int32(sampling),
  }

	message := pb.AnalysisResults{
		Header: &header,
		BurndownProject: pb.ToBurndownSparseMatrix(burndownResults.GlobalHistory, uri),
	}

	if withFiles {
		message.BurndownFiles = make([]*pb.BurndownSparseMatrix, len(burndownResults.FileHistories))
		keys := sortedKeys(burndownResults.FileHistories)
		i := 0
		for _, key := range keys {
			message.BurndownFiles[i] = pb.ToBurndownSparseMatrix(
				burndownResults.FileHistories[key], key)
			i++
		}
	}

	if withPeople {
		message.BurndownDevelopers = make(
		  []*pb.BurndownSparseMatrix, len(burndownResults.PeopleHistories))
		for key, val := range burndownResults.PeopleHistories {
			message.BurndownDevelopers[key] = pb.ToBurndownSparseMatrix(val, reversePeopleDict[key])
		}
		message.DevelopersInteraction = pb.DenseToCompressedSparseRowMatrix(
			burndownResults.PeopleMatrix)
	}

	if withCouples {
		message.FileCouples = &pb.Couples{
			Index: couplesResult.Files,
			Matrix: pb.MapToCompressedSparseRowMatrix(couplesResult.FilesMatrix),
		}
		message.DeveloperCouples = &pb.Couples{
			Index: reversePeopleDict,
			Matrix: pb.MapToCompressedSparseRowMatrix(couplesResult.PeopleMatrix),
		}
		message.TouchedFiles = &pb.DeveloperTouchedFiles{
      Developers: make([]*pb.TouchedFiles, len(reversePeopleDict)),
		}
		for key := range reversePeopleDict {
			files := couplesResult.PeopleFiles[key]
			int32Files := make([]int32, len(files))
			for i, f := range files {
				int32Files[i] = int32(f)
			}
			message.TouchedFiles.Developers[key] = &pb.TouchedFiles{
				Files: int32Files,
			}
		}
	}

	serialized, err := proto.Marshal(&message)
	if err != nil {
		panic(err)
	}
  os.Stdout.Write(serialized)
}
