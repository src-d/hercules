package uast

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/tunny"
	"github.com/gogo/protobuf/proto"
	bblfsh "gopkg.in/bblfsh/client-go.v3"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes/nodesproto"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	items "gopkg.in/src-d/hercules.v10/internal/plumbing"
)

// Extractor retrieves UASTs from Babelfish server which correspond to changed files in a commit.
// It is a PipelineItem.
type Extractor struct {
	core.NoopMerger
	Endpoint              string
	Context               func() (context.Context, context.CancelFunc)
	PoolSize              int
	FailOnErrors          bool
	ProcessedFiles        map[string]int
	IgnoredMissingDrivers map[string]bool

	clients []*bblfsh.Client
	pool    *tunny.Pool

	l core.Logger
}

const (
	// ConfigUASTEndpoint is the name of the configuration option (Extractor.Configure())
	// which sets the Babelfish server address.
	ConfigUASTEndpoint = "UAST.Endpoint"
	// ConfigUASTTimeout is the name of the configuration option (Extractor.Configure())
	// which sets the maximum amount of time to wait for a Babelfish server response.
	ConfigUASTTimeout = "UAST.Timeout"
	// ConfigUASTPoolSize is the name of the configuration option (Extractor.Configure())
	// which sets the number of goroutines to run for UAST parse queries.
	ConfigUASTPoolSize = "UAST.PoolSize"
	// ConfigUASTFailOnErrors is the name of the configuration option (Extractor.Configure())
	// which enables early exit in case of any Babelfish UAST parsing errors.
	ConfigUASTFailOnErrors = "UAST.FailOnErrors"
	// ConfigUASTIgnoreMissingDrivers is the name of the configuration option (Extractor.Configure())
	// which sets the ignored missing driver names.
	ConfigUASTIgnoreMissingDrivers = "UAST.IgnoreMissingDrivers"
	// DefaultBabelfishEndpoint is the default address of the Babelfish parsing server.
	DefaultBabelfishEndpoint = "0.0.0.0:9432"
	// DefaultBabelfishTimeout is the default value of the RPC timeout in seconds.
	DefaultBabelfishTimeout = 20
	// FeatureUast is the name of the Pipeline feature which activates all the items related to UAST.
	FeatureUast = "uast"
	// DependencyUasts is the name of the dependency provided by Extractor.
	DependencyUasts = "uasts"
)

var (
	// DefaultBabelfishWorkers is the default number of parsing RPC goroutines.
	DefaultBabelfishWorkers = runtime.NumCPU() * 2
	// DefaultIgnoredMissingDrivers is the languages which are ignored if the Babelfish driver is missing.
	DefaultIgnoredMissingDrivers = []string{"markdown", "text", "yaml", "json"}
)

type uastTask struct {
	Lock   *sync.RWMutex
	Dest   map[plumbing.Hash]nodes.Node
	Name   string
	Hash   plumbing.Hash
	Data   []byte
	Errors *[]error
}

type worker struct {
	Client    *bblfsh.Client
	Extractor *Extractor
}

// Process will synchronously perform a job and return the result.
func (w worker) Process(data interface{}) interface{} {
	return w.Extractor.extractTask(w.Client, data)
}
func (w worker) BlockUntilReady() {}
func (w worker) Interrupt()       {}
func (w worker) Terminate()       {}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (exr *Extractor) Name() string {
	return "UAST"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (exr *Extractor) Provides() []string {
	return []string{DependencyUasts}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (exr *Extractor) Requires() []string {
	return []string{items.DependencyTreeChanges, items.DependencyBlobCache}
}

// Features which must be enabled for this PipelineItem to be automatically inserted into the DAG.
func (exr *Extractor) Features() []string {
	return []string{FeatureUast}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (exr *Extractor) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigUASTEndpoint,
		Description: "How many days there are in a single band.",
		Flag:        "bblfsh",
		Type:        core.StringConfigurationOption,
		Default:     DefaultBabelfishEndpoint}, {
		Name:        ConfigUASTTimeout,
		Description: "Babelfish's server timeout in seconds.",
		Flag:        "bblfsh-timeout",
		Type:        core.IntConfigurationOption,
		Default:     DefaultBabelfishTimeout}, {
		Name:        ConfigUASTPoolSize,
		Description: "Number of goroutines to extract UASTs.",
		Flag:        "bblfsh-pool-size",
		Type:        core.IntConfigurationOption,
		Default:     DefaultBabelfishWorkers}, {
		Name:        ConfigUASTFailOnErrors,
		Description: "Panic if there is a UAST extraction error.",
		Flag:        "bblfsh-fail-on-error",
		Type:        core.BoolConfigurationOption,
		Default:     false}, {
		Name:        ConfigUASTIgnoreMissingDrivers,
		Description: "Do not warn about missing drivers for the specified languages.",
		Flag:        "bblfsh-ignored-drivers",
		Type:        core.StringsConfigurationOption,
		Default:     DefaultIgnoredMissingDrivers},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (exr *Extractor) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		exr.l = l
	}
	if val, exists := facts[ConfigUASTEndpoint].(string); exists {
		exr.Endpoint = val
	}
	if val, exists := facts[ConfigUASTTimeout].(int); exists {
		exr.Context = func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(),
				time.Duration(val)*time.Second)
		}
	}
	if val, exists := facts[ConfigUASTPoolSize].(int); exists {
		exr.PoolSize = val
	}
	if val, exists := facts[ConfigUASTFailOnErrors].(bool); exists {
		exr.FailOnErrors = val
	}
	if val, exists := facts[ConfigUASTIgnoreMissingDrivers].([]string); exists {
		exr.IgnoredMissingDrivers = map[string]bool{}
		for _, name := range val {
			exr.IgnoredMissingDrivers[name] = true
		}
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (exr *Extractor) Initialize(repository *git.Repository) error {
	exr.l = core.NewLogger()
	if exr.Context == nil {
		exr.Context = func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(),
				time.Duration(DefaultBabelfishTimeout)*time.Second)
		}
	}
	if exr.Endpoint == "" {
		exr.Endpoint = DefaultBabelfishEndpoint
	}
	if exr.PoolSize == 0 {
		exr.PoolSize = DefaultBabelfishWorkers
	}
	poolSize := exr.PoolSize
	if poolSize == 0 {
		poolSize = runtime.NumCPU()
	}
	exr.clients = make([]*bblfsh.Client, poolSize)
	for i := 0; i < poolSize; i++ {
		client, err := bblfsh.NewClient(exr.Endpoint)
		if err != nil {
			if err.Error() == "context deadline exceeded" {
				exr.l.Error("Looks like the Babelfish server is not running. Please refer " +
					"to https://docs.sourced.tech/babelfish/using-babelfish/getting-started#running-with-docker-recommended")
			}
			return err
		}
		exr.clients[i] = client
	}
	if exr.pool != nil {
		exr.pool.Close()
	}
	{
		i := 0
		exr.pool = tunny.New(poolSize, func() tunny.Worker {
			w := worker{Client: exr.clients[i], Extractor: exr}
			i++
			return w
		})
	}
	if exr.pool == nil {
		panic("UAST goroutine pool was not created")
	}
	exr.ProcessedFiles = map[string]int{}
	if exr.IgnoredMissingDrivers == nil {
		exr.IgnoredMissingDrivers = map[string]bool{}
		for _, name := range DefaultIgnoredMissingDrivers {
			exr.IgnoredMissingDrivers[name] = true
		}
	}
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (exr *Extractor) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	cache := deps[items.DependencyBlobCache].(map[plumbing.Hash]*items.CachedBlob)
	treeDiffs := deps[items.DependencyTreeChanges].(object.Changes)
	uasts := map[plumbing.Hash]nodes.Node{}
	lock := sync.RWMutex{}
	errs := make([]error, 0)
	wg := sync.WaitGroup{}
	submit := func(change *object.Change) {
		exr.ProcessedFiles[change.To.Name]++
		wg.Add(1)
		go func(task interface{}) {
			exr.pool.Process(task)
			wg.Done()
		}(uastTask{
			Lock:   &lock,
			Dest:   uasts,
			Name:   change.To.Name,
			Hash:   change.To.TreeEntry.Hash,
			Data:   cache[change.To.TreeEntry.Hash].Data,
			Errors: &errs,
		})
	}
	for _, change := range treeDiffs {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		switch action {
		case merkletrie.Insert:
			submit(change)
		case merkletrie.Delete:
			continue
		case merkletrie.Modify:
			submit(change)
		}
	}
	wg.Wait()
	if len(errs) > 0 {
		msgs := make([]string, len(errs))
		for i, err := range errs {
			msgs[i] = err.Error()
		}
		joined := strings.Join(msgs, "\n")
		if exr.FailOnErrors {
			return nil, errors.New(joined)
		}
		exr.l.Error(joined)
	}
	return map[string]interface{}{DependencyUasts: uasts}, nil
}

// Dispose closes the open GRPC channels.
func (exr *Extractor) Dispose() {
	for _, client := range exr.clients {
		client.Close()
	}
}

// Fork clones this PipelineItem.
func (exr *Extractor) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(exr, n)
}

func (exr *Extractor) extractUAST(
	client *bblfsh.Client, name string, data []byte) (nodes.Node, error) {
	ctx, cancel := exr.Context()
	if cancel != nil {
		defer cancel()
	}
	request := client.NewParseRequest().
		Content(string(data)).Filename(name).Mode(bblfsh.Semantic).Context(ctx)
	response, _, err := request.UAST()
	if err != nil {
		if strings.Contains("missing driver", err.Error()) {
			return nil, nil
		}
		return nil, err
	}
	return response, nil
}

func (exr *Extractor) extractTask(client *bblfsh.Client, data interface{}) interface{} {
	task := data.(uastTask)
	node, err := exr.extractUAST(client, task.Name, task.Data)
	task.Lock.Lock()
	defer task.Lock.Unlock()
	if err != nil {
		for lang := range exr.IgnoredMissingDrivers {
			if strings.HasSuffix(err.Error(), "\""+lang+"\"") {
				return nil
			}
		}
		*task.Errors = append(*task.Errors,
			fmt.Errorf("\nfile %s, blob %s: %v", task.Name, task.Hash.String(), err))
		return nil
	}
	if node != nil {
		task.Dest[task.Hash] = node
	}
	return nil
}

// Change is the type of the items in the list of changes which is provided by Changes.
type Change struct {
	Before nodes.Node
	After  nodes.Node
	Change *object.Change
}

const (
	// DependencyUastChanges is the name of the dependency provided by Changes.
	DependencyUastChanges = "changed_uasts"
)

// Changes is a structured analog of TreeDiff: it provides UASTs for every logical change
// in a commit. It is a PipelineItem.
type Changes struct {
	core.NoopMerger
	cache map[plumbing.Hash]nodes.Node

	l core.Logger
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (uc *Changes) Name() string {
	return "UASTChanges"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (uc *Changes) Provides() []string {
	return []string{DependencyUastChanges}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (uc *Changes) Requires() []string {
	return []string{DependencyUasts, items.DependencyTreeChanges}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (uc *Changes) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (uc *Changes) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		uc.l = l
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (uc *Changes) Initialize(repository *git.Repository) error {
	uc.l = core.NewLogger()
	uc.cache = map[plumbing.Hash]nodes.Node{}
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (uc *Changes) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	uasts := deps[DependencyUasts].(map[plumbing.Hash]nodes.Node)
	treeDiffs := deps[items.DependencyTreeChanges].(object.Changes)
	commit := make([]Change, 0, len(treeDiffs))
	for _, change := range treeDiffs {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		switch action {
		case merkletrie.Insert:
			hashTo := change.To.TreeEntry.Hash
			uastTo := uasts[hashTo]
			commit = append(commit, Change{Before: nil, After: uastTo, Change: change})
			uc.cache[hashTo] = uastTo
		case merkletrie.Delete:
			hashFrom := change.From.TreeEntry.Hash
			commit = append(commit, Change{Before: uc.cache[hashFrom], After: nil, Change: change})
			delete(uc.cache, hashFrom)
		case merkletrie.Modify:
			hashFrom := change.From.TreeEntry.Hash
			hashTo := change.To.TreeEntry.Hash
			uastTo := uasts[hashTo]
			commit = append(commit, Change{Before: uc.cache[hashFrom], After: uastTo, Change: change})
			delete(uc.cache, hashFrom)
			uc.cache[hashTo] = uastTo
		}
	}
	return map[string]interface{}{DependencyUastChanges: commit}, nil
}

// Fork clones this PipelineItem.
func (uc *Changes) Fork(n int) []core.PipelineItem {
	ucs := make([]core.PipelineItem, n)
	for i := 0; i < n; i++ {
		clone := &Changes{
			cache: map[plumbing.Hash]nodes.Node{},
		}
		for key, val := range uc.cache {
			clone.cache[key] = val
		}
		ucs[i] = clone
	}
	return ucs
}

// ChangesSaver dumps changed files and corresponding UASTs for every commit.
// it is a LeafPipelineItem.
type ChangesSaver struct {
	core.NoopMerger
	core.OneShotMergeProcessor
	// OutputPath points to the target directory with UASTs
	OutputPath string

	repository *git.Repository
	result     [][]Change

	l core.Logger
}

const (
	// ConfigUASTChangesSaverOutputPath is the name of the configuration option
	// (ChangesSaver.Configure()) which sets the target directory where to save the files.
	ConfigUASTChangesSaverOutputPath = "ChangesSaver.OutputPath"
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (saver *ChangesSaver) Name() string {
	return "UASTChangesSaver"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (saver *ChangesSaver) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (saver *ChangesSaver) Requires() []string {
	return []string{DependencyUastChanges}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (saver *ChangesSaver) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigUASTChangesSaverOutputPath,
		Description: "The target directory where to store the changed UAST files.",
		Flag:        "changed-uast-dir",
		Type:        core.PathConfigurationOption,
		Default:     "."},
	}
	return options[:]
}

// Flag for the command line switch which enables this analysis.
func (saver *ChangesSaver) Flag() string {
	return "dump-uast-changes"
}

// Description returns the text which explains what the analysis is doing.
func (saver *ChangesSaver) Description() string {
	return "Saves UASTs and file contents on disk for each commit."
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (saver *ChangesSaver) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		saver.l = l
	}
	if val, exists := facts[ConfigUASTChangesSaverOutputPath]; exists {
		saver.OutputPath = val.(string)
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (saver *ChangesSaver) Initialize(repository *git.Repository) error {
	saver.l = core.NewLogger()
	saver.repository = repository
	saver.result = [][]Change{}
	saver.OneShotMergeProcessor.Initialize()
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (saver *ChangesSaver) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if !saver.ShouldConsumeCommit(deps) {
		return nil, nil
	}
	changes := deps[DependencyUastChanges].([]Change)
	saver.result = append(saver.result, changes)
	return nil, nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (saver *ChangesSaver) Finalize() interface{} {
	return saver.result
}

// Fork clones this PipelineItem.
func (saver *ChangesSaver) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(saver, n)
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (saver *ChangesSaver) Serialize(result interface{}, binary bool, writer io.Writer) error {
	saverResult := result.([][]Change)
	fileNames := saver.dumpFiles(saverResult)
	if binary {
		return saver.serializeBinary(fileNames, writer)
	}
	saver.serializeText(fileNames, writer)
	return nil
}

func (saver *ChangesSaver) dumpFiles(result [][]Change) []*pb.UASTChange {
	var fileNames []*pb.UASTChange
	dumpUast := func(uast nodes.Node, path string) {
		f, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		err = nodesproto.WriteTo(f, uast)
		if err != nil {
			panic(err)
		}
	}
	for i, changes := range result {
		for j, change := range changes {
			if change.Before == nil || change.After == nil {
				continue
			}
			record := &pb.UASTChange{FileName: change.Change.To.Name}
			record.UastBefore = path.Join(saver.OutputPath, fmt.Sprintf(
				"%d_%d_before_%s.pb", i, j, change.Change.From.TreeEntry.Hash.String()))
			dumpUast(change.Before, record.UastBefore)
			blob, _ := saver.repository.BlobObject(change.Change.From.TreeEntry.Hash)
			s, _ := (&object.File{Blob: *blob}).Contents()
			record.SrcBefore = path.Join(saver.OutputPath, fmt.Sprintf(
				"%d_%d_before_%s.src", i, j, change.Change.From.TreeEntry.Hash.String()))
			err := ioutil.WriteFile(record.SrcBefore, []byte(s), 0666)
			if err != nil {
				panic(err)
			}
			record.UastAfter = path.Join(saver.OutputPath, fmt.Sprintf(
				"%d_%d_after_%s.pb", i, j, change.Change.To.TreeEntry.Hash.String()))
			dumpUast(change.After, record.UastAfter)
			blob, _ = saver.repository.BlobObject(change.Change.To.TreeEntry.Hash)
			s, _ = (&object.File{Blob: *blob}).Contents()
			record.SrcAfter = path.Join(saver.OutputPath, fmt.Sprintf(
				"%d_%d_after_%s.src", i, j, change.Change.To.TreeEntry.Hash.String()))
			err = ioutil.WriteFile(record.SrcAfter, []byte(s), 0666)
			if err != nil {
				panic(err)
			}
			fileNames = append(fileNames, record)
		}
	}
	return fileNames
}

func (saver *ChangesSaver) serializeText(result []*pb.UASTChange, writer io.Writer) {
	for _, sc := range result {
		kv := [...]string{
			"file: " + sc.FileName,
			"src0: " + sc.SrcBefore, "src1: " + sc.SrcAfter,
			"uast0: " + sc.UastBefore, "uast1: " + sc.UastAfter,
		}
		fmt.Fprintf(writer, "  - {%s}\n", strings.Join(kv[:], ", "))
	}
}

func (saver *ChangesSaver) serializeBinary(result []*pb.UASTChange, writer io.Writer) error {
	message := pb.UASTChangesSaverResults{Changes: result}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	_, err = writer.Write(serialized)
	return err
}

func init() {
	core.Registry.Register(&Extractor{})
	core.Registry.Register(&Changes{})
	core.Registry.Register(&ChangesSaver{})
}
