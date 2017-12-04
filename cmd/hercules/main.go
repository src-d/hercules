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
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"plugin"
	"runtime/pprof"
	"strings"

	"gopkg.in/src-d/go-billy.v3/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"gopkg.in/src-d/hercules.v3"
	"gopkg.in/src-d/hercules.v3/pb"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"github.com/gogo/protobuf/proto"
)

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

func loadRepository(uri string) *git.Repository {
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
		fmt.Fprint(os.Stderr, "connecting...\r")
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
	return repository
}


type arrayPluginFlags map[string]bool

func (apf *arrayPluginFlags) String() string {
	list := []string{}
	for key := range *apf {
		list = append(list, key)
	}
	return strings.Join(list, ", ")
}

func (apf *arrayPluginFlags) Set(value string) error {
	(*apf)[value] = true
	return nil
}

func loadPlugins() {
	pluginFlags := arrayPluginFlags{}
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	pluginFlagName := "plugin"
	pluginDesc := "Load the specified plugin by the full or relative path. " +
			"Can be specified multiple times."
	fs.Var(&pluginFlags, pluginFlagName, pluginDesc)
	flag.Var(&pluginFlags, pluginFlagName, pluginDesc)
	fs.Parse(os.Args[1:])
	for path := range pluginFlags {
		_, err := plugin.Open(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load plugin from %s %s", path, err)
		}
	}
}

func main() {
	loadPlugins()
	var printVersion bool
	var protobuf bool
	var profile bool
	var commitsFile string
	flag.BoolVar(&profile, "profile", false, "Collect the profile to hercules.pprof.")
	flag.StringVar(&commitsFile, "commits", "", "Path to the text file with the "+
		"commit history to follow instead of the default rev-list "+
		"--first-parent. The format is the list of hashes, each hash on a "+
		"separate line. The first hash is the root.")
	flag.BoolVar(&protobuf, "pb", false, "The output format will be Protocol Buffers instead of YAML.")
	flag.BoolVar(&printVersion, "version", false, "Print version information and exit.")
	facts, deployChoices := hercules.Registry.AddFlags()
	flag.Parse()

	if printVersion {
		fmt.Printf("Version: 3\nGit:     %s\n", hercules.GIT_HASH)
		return
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
	repository := loadRepository(uri)

	// core logic
	pipeline := hercules.NewPipeline(repository)
	pipeline.SetFeaturesFromFlags()
	progress := mpb.New(mpb.Output(os.Stderr))
	var bar *mpb.Bar
	pipeline.OnProgress = func(commit, length int) {
		if bar == nil {
			bar = progress.AddBar(int64(length + 1),
				mpb.PrependDecorators(decor.DynamicName(
					func (stats *decor.Statistics) string {
						if stats.Current < stats.Total {
							return fmt.Sprintf("%d / %d", stats.Current, length)
						}
						return "finalizing"
					}, 10, 0)),
				mpb.AppendDecorators(decor.ETA(4, 0)),
			)
		}
		bar.Incr(commit - int(bar.Current()))
	}

	var commits []*object.Commit
	if commitsFile == "" {
		// list of commits belonging to the default branch, from oldest to newest
		// rev-list --first-parent
		commits = pipeline.Commits()
	} else {
		var err error
		commits, err = hercules.LoadCommitsFromFile(commitsFile, repository)
		if err != nil {
			panic(err)
		}
	}
	facts["commits"] = commits
	deployed := []hercules.PipelineItem{}
	for name, valPtr := range deployChoices {
		if *valPtr {
			deployed = append(deployed, pipeline.DeployItem(hercules.Registry.Summon(name)[0]))
		}
	}
	pipeline.Initialize(facts)
	results, err := pipeline.Run(commits)
	if err != nil {
		panic(err)
	}
	progress.Stop()
	fmt.Fprint(os.Stderr, "writing...\r")
	begin := commits[0].Author.When.Unix()
	end := commits[len(commits)-1].Author.When.Unix()
	if !protobuf {
		printResults(uri, begin, end, len(commits), deployed, results)
	} else {
		protobufResults(uri, begin, end, len(commits), deployed, results)
	}
}

func printResults(
	uri string, begin, end int64, commitsCount int, deployed []hercules.PipelineItem,
	results map[hercules.PipelineItem]interface{}) {
	fmt.Println("hercules:")
	fmt.Println("  version: 3")
	fmt.Println("  hash:", hercules.GIT_HASH)
	fmt.Println("  repository:", uri)
	fmt.Println("  begin_unix_time:", begin)
	fmt.Println("  end_unix_time:", end)
	fmt.Println("  commits:", commitsCount)

	for _, item := range deployed {
		result := results[item]
		fmt.Printf("%s:\n", item.Name())
		err := interface{}(item).(hercules.LeafPipelineItem).Serialize(result, false, os.Stdout)
		if err != nil {
			panic(err)
		}
	}
}

func protobufResults(
	uri string, begin, end int64, commitsCount int, deployed []hercules.PipelineItem,
	results map[hercules.PipelineItem]interface{}) {

  header := pb.Metadata{
	  Version: 1,
	  Hash: hercules.GIT_HASH,
	  Repository: uri,
    BeginUnixTime: begin,
	  EndUnixTime: end,
	  Commits: int32(commitsCount),
  }

	message := pb.AnalysisResults{
		Header: &header,
		Contents: map[string][]byte{},
	}

	for _, item := range deployed {
		result := results[item]
		buffer := &bytes.Buffer{}
		err := interface{}(item).(hercules.LeafPipelineItem).Serialize(result, true, buffer)
		if err != nil {
			panic(err)
		}
		message.Contents[item.Name()] = buffer.Bytes()
	}

	serialized, err := proto.Marshal(&message)
	if err != nil {
		panic(err)
	}
  os.Stdout.Write(serialized)
}
