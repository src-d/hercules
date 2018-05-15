package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"plugin"
	"runtime/pprof"
	"strings"
	_ "unsafe" // for go:linkname

	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/crypto/ssh/terminal"
	progress "gopkg.in/cheggaaa/pb.v1"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"gopkg.in/src-d/hercules.v4"
	"gopkg.in/src-d/hercules.v4/internal/pb"
)

// oneLineWriter splits the output data by lines and outputs one on top of another using '\r'.
// It also does some dark magic to handle Git statuses.
type oneLineWriter struct {
	Writer io.Writer
}

func (writer oneLineWriter) Write(p []byte) (n int, err error) {
	if p[len(p)-1] == '\n' {
		p = p[:len(p)-1]
		if len(p) > 5 && bytes.Compare(p[len(p)-5:], []byte("done.")) == 0 {
			p = []byte("cloning...")
		}
		p = append(p, '\r')
		writer.Writer.Write([]byte("\r" + strings.Repeat(" ", 80) + "\r"))
	}
	n, err = writer.Writer.Write(p)
	return
}

func loadRepository(uri string, cachePath string, disableStatus bool) *git.Repository {
	var repository *git.Repository
	var backend storage.Storer
	var err error
	if strings.Contains(uri, "://") {
		if cachePath != "" {
			backend, err = filesystem.NewStorage(osfs.New(cachePath))
			if err != nil {
				panic(err)
			}
			_, err = os.Stat(cachePath)
			if !os.IsNotExist(err) {
				log.Printf("warning: deleted %s\n", cachePath)
				os.RemoveAll(cachePath)
			}
		} else {
			backend = memory.NewStorage()
		}
		cloneOptions := &git.CloneOptions{URL: uri}
		if !disableStatus {
			fmt.Fprint(os.Stderr, "connecting...\r")
			cloneOptions.Progress = oneLineWriter{Writer: os.Stderr}
		}
		repository, err = git.Clone(backend, nil, cloneOptions)
		if !disableStatus {
			fmt.Fprint(os.Stderr, strings.Repeat(" ", 80)+"\r")
		}
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

func (apf *arrayPluginFlags) Type() string {
	return "path"
}

func loadPlugins() {
	pluginFlags := arrayPluginFlags{}
	fs := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	pluginFlagName := "plugin"
	const pluginDesc = "Load the specified plugin by the full or relative path. " +
		"Can be specified multiple times."
	fs.Var(&pluginFlags, pluginFlagName, pluginDesc)
	pflag.Var(&pluginFlags, pluginFlagName, pluginDesc)
	fs.Parse(os.Args[1:])
	for path := range pluginFlags {
		_, err := plugin.Open(path)
		if err != nil {
			log.Printf("Failed to load plugin from %s %s\n", path, err)
		}
	}
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "hercules",
	Short: "Analyse a Git repository.",
	Long: `Hercules is a flexible and fast Git repository analysis engine. The base command executes
the commit processing pipeline which is automatically generated from the dependencies of one
or several analysis targets. The list of the available targets is printed in --help. External
targets can be added using the --plugin system.`,
	Args: cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		flags := cmd.Flags()
		commitsFile, _ := flags.GetString("commits")
		protobuf, _ := flags.GetBool("pb")
		profile, _ := flags.GetBool("profile")
		disableStatus, _ := flags.GetBool("quiet")

		if profile {
			go http.ListenAndServe("localhost:6060", nil)
			prof, _ := os.Create("hercules.pprof")
			pprof.StartCPUProfile(prof)
			defer pprof.StopCPUProfile()
		}
		uri := args[0]
		cachePath := ""
		if len(args) == 2 {
			cachePath = args[1]
		}
		repository := loadRepository(uri, cachePath, disableStatus)

		// core logic
		pipeline := hercules.NewPipeline(repository)
		pipeline.SetFeaturesFromFlags()
		var bar *progress.ProgressBar
		if !disableStatus {
			pipeline.OnProgress = func(commit, length int) {
				if bar == nil {
					bar = progress.New(length)
					bar.Callback = func(msg string) {
						os.Stderr.WriteString("\r" + msg)
					}
					bar.NotPrint = true
					bar.ShowPercent = false
					bar.ShowSpeed = false
					bar.SetMaxWidth(80)
					bar.Start()
				}
				if commit == length {
					bar.Finish()
					fmt.Fprint(os.Stderr, "\r"+strings.Repeat(" ", 80)+"\rfinalizing...")
				} else {
					bar.Set(commit)
				}
			}
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
		cmdlineFacts["commits"] = commits
		deployed := []hercules.LeafPipelineItem{}
		for name, valPtr := range cmdlineDeployed {
			if *valPtr {
				item := pipeline.DeployItem(hercules.Registry.Summon(name)[0])
				deployed = append(deployed, item.(hercules.LeafPipelineItem))
			}
		}
		pipeline.Initialize(cmdlineFacts)
		if dryRun, _ := cmdlineFacts[hercules.ConfigPipelineDryRun].(bool); dryRun {
			return
		}
		results, err := pipeline.Run(commits)
		if err != nil {
			panic(err)
		}
		if !disableStatus {
			fmt.Fprint(os.Stderr, "\r"+strings.Repeat(" ", 80)+"\r")
			// if not a terminal, the user will not see the output, so show the status
			if !terminal.IsTerminal(int(os.Stdout.Fd())) {
				fmt.Fprint(os.Stderr, "writing...\r")
			}
		}
		if !protobuf {
			printResults(uri, deployed, results)
		} else {
			protobufResults(uri, deployed, results)
		}
	},
}

func printResults(
	uri string, deployed []hercules.LeafPipelineItem,
	results map[hercules.LeafPipelineItem]interface{}) {
	commonResult := results[nil].(*hercules.CommonAnalysisResult)

	fmt.Println("hercules:")
	fmt.Println("  version: 3")
	fmt.Println("  hash:", hercules.BinaryGitHash)
	fmt.Println("  repository:", uri)
	fmt.Println("  begin_unix_time:", commonResult.BeginTime)
	fmt.Println("  end_unix_time:", commonResult.EndTime)
	fmt.Println("  commits:", commonResult.CommitsNumber)
	fmt.Println("  run_time:", commonResult.RunTime.Nanoseconds()/1e6)

	for _, item := range deployed {
		result := results[item]
		fmt.Printf("%s:\n", item.Name())
		if err := item.Serialize(result, false, os.Stdout); err != nil {
			panic(err)
		}
	}
}

func protobufResults(
	uri string, deployed []hercules.LeafPipelineItem,
	results map[hercules.LeafPipelineItem]interface{}) {

	header := pb.Metadata{
		Version:    2,
		Hash:       hercules.BinaryGitHash,
		Repository: uri,
	}
	results[nil].(*hercules.CommonAnalysisResult).FillMetadata(&header)

	message := pb.AnalysisResults{
		Header:   &header,
		Contents: map[string][]byte{},
	}

	for _, item := range deployed {
		result := results[item]
		buffer := &bytes.Buffer{}
		if err := item.Serialize(result, true, buffer); err != nil {
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

// animate the private function defined in Cobra
//go:linkname tmpl github.com/spf13/cobra.tmpl
func tmpl(w io.Writer, text string, data interface{}) error

func formatUsage(c *cobra.Command) error {
	// the default UsageFunc() does some private magic c.mergePersistentFlags()
	// this should stay on top
	localFlags := c.LocalFlags()
	leaves := hercules.Registry.GetLeaves()
	plumbing := hercules.Registry.GetPlumbingItems()
	features := hercules.Registry.GetFeaturedItems()
	filter := map[string]bool{}
	for _, l := range leaves {
		filter[l.Flag()] = true
		for _, cfg := range l.ListConfigurationOptions() {
			filter[cfg.Flag] = true
		}
	}
	for _, i := range plumbing {
		for _, cfg := range i.ListConfigurationOptions() {
			filter[cfg.Flag] = true
		}
	}

	for key := range filter {
		localFlags.Lookup(key).Hidden = true
	}
	args := map[string]interface{}{
		"c":        c,
		"leaves":   leaves,
		"plumbing": plumbing,
		"features": features,
	}

	template := `Usage:{{if .c.Runnable}}
  {{.c.UseLine}}{{end}}{{if .c.HasAvailableSubCommands}}
  {{.c.CommandPath}} [command]{{end}}{{if gt (len .c.Aliases) 0}}

Aliases:
  {{.c.NameAndAliases}}{{end}}{{if .c.HasExample}}

Examples:
{{.c.Example}}{{end}}{{if .c.HasAvailableSubCommands}}

Available Commands:{{range .c.Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .c.HasAvailableLocalFlags}}

Flags:
{{.c.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}

Analysis Targets:{{range .leaves}}
      --{{rpad .Flag 40 }}Runs {{.Name}} analysis.{{range .ListConfigurationOptions}}
          --{{if .Type.String}}{{rpad (print .Flag " " .Type.String) 40}}{{else}}{{rpad .Flag 40}}{{end}}{{.Description}}{{if .Default}} The default value is {{.FormatDefault}}.{{end}}{{end}}{{end}}

Plumbing Options:{{range .plumbing}}{{$name := .Name}}{{range .ListConfigurationOptions}}
      --{{if .Type.String}}{{rpad (print .Flag " " .Type.String " [" $name "]") 40}}{{else}}{{rpad (print .Flag " [" $name "]") 40}}{{end}}{{.Description}}{{if .Default}} The default value is {{.FormatDefault}}.{{end}}{{end}}{{end}}

--feature:{{range $key, $value := .features}}
      {{rpad $key 40}}Enables {{range $index, $item := $value}}{{if $index}}, {{end}}{{$item.Name}}{{end}}.{{end}}{{if .c.HasAvailableInheritedFlags}}

Global Flags:
{{.c.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .c.HasHelpSubCommands}}

Additional help topics:{{range .c.Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .c.HasAvailableSubCommands}}

Use "{{.c.CommandPath}} [command] --help" for more information about a command.{{end}}
`
	err := tmpl(c.OutOrStderr(), template, args)
	for key := range filter {
		localFlags.Lookup(key).Hidden = false
	}
	if err != nil {
		c.Println(err)
	}
	return err
}

// versionCmd prints the API version and the Git commit hash
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information and exit.",
	Long:  ``,
	Args:  cobra.MaximumNArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Version: %d\nGit:     %s\n", hercules.BinaryVersion, hercules.BinaryGitHash)
	},
}

var cmdlineFacts map[string]interface{}
var cmdlineDeployed map[string]*bool

func init() {
	loadPlugins()
	rootCmd.MarkFlagFilename("plugin")
	rootFlags := rootCmd.Flags()
	rootFlags.String("commits", "", "Path to the text file with the "+
		"commit history to follow instead of the default rev-list "+
		"--first-parent. The format is the list of hashes, each hash on a "+
		"separate line. The first hash is the root.")
	rootCmd.MarkFlagFilename("commits")
	rootFlags.Bool("pb", false, "The output format will be Protocol Buffers instead of YAML.")
	rootFlags.Bool("quiet", !terminal.IsTerminal(int(os.Stdin.Fd())),
		"Do not print status updates to stderr.")
	rootFlags.Bool("profile", false, "Collect the profile to hercules.pprof.")
	cmdlineFacts, cmdlineDeployed = hercules.Registry.AddFlags(rootFlags)
	rootCmd.SetUsageFunc(formatUsage)
	rootCmd.AddCommand(versionCmd)
	versionCmd.SetUsageFunc(versionCmd.UsageFunc())
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
