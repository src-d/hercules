package uast

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	goioutil "io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jeffail/tunny"
	"gopkg.in/bblfsh/client-go.v2"
	"gopkg.in/bblfsh/sdk.v1/protocol"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/src-d/enry.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v4/internal/core"
	"gopkg.in/src-d/hercules.v4/internal/pb"
	items "gopkg.in/src-d/hercules.v4/internal/plumbing"
)

// Extractor retrieves UASTs from Babelfish server which correspond to changed files in a commit.
// It is a PipelineItem.
type Extractor struct {
	Endpoint       string
	Context        func() (context.Context, context.CancelFunc)
	PoolSize       int
	Languages      map[string]bool
	FailOnErrors   bool
	ProcessedFiles map[string]int

	clients []*bblfsh.Client
	pool    *tunny.Pool
}

const (
	uastExtractionSkipped = -(1 << 31)

	// ConfigUASTEndpoint is the name of the configuration option (Extractor.Configure())
	// which sets the Babelfish server address.
	ConfigUASTEndpoint = "ConfigUASTEndpoint"
	// ConfigUASTTimeout is the name of the configuration option (Extractor.Configure())
	// which sets the maximum amount of time to wait for a Babelfish server response.
	ConfigUASTTimeout = "ConfigUASTTimeout"
	// ConfigUASTPoolSize is the name of the configuration option (Extractor.Configure())
	// which sets the number of goroutines to run for UAST parse queries.
	ConfigUASTPoolSize = "ConfigUASTPoolSize"
	// ConfigUASTFailOnErrors is the name of the configuration option (Extractor.Configure())
	// which enables early exit in case of any Babelfish UAST parsing errors.
	ConfigUASTFailOnErrors = "ConfigUASTFailOnErrors"
	// ConfigUASTLanguages is the name of the configuration option (Extractor.Configure())
	// which sets the list of languages to parse. Language names are at
	// https://doc.bblf.sh/languages.html Names are joined with a comma ",".
	ConfigUASTLanguages = "ConfigUASTLanguages"

	// FeatureUast is the name of the Pipeline feature which activates all the items related to UAST.
	FeatureUast = "uast"
	// DependencyUasts is the name of the dependency provided by Extractor.
	DependencyUasts = "uasts"
)

type uastTask struct {
	Lock   *sync.RWMutex
	Dest   map[plumbing.Hash]*uast.Node
	File   *object.File
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
	arr := [...]string{DependencyUasts}
	return arr[:]
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (exr *Extractor) Requires() []string {
	arr := [...]string{items.DependencyTreeChanges, items.DependencyBlobCache}
	return arr[:]
}

// Features which must be enabled for this PipelineItem to be automatically inserted into the DAG.
func (exr *Extractor) Features() []string {
	arr := [...]string{FeatureUast}
	return arr[:]
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (exr *Extractor) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigUASTEndpoint,
		Description: "How many days there are in a single band.",
		Flag:        "bblfsh",
		Type:        core.StringConfigurationOption,
		Default:     "0.0.0.0:9432"}, {
		Name:        ConfigUASTTimeout,
		Description: "Babelfish's server timeout in seconds.",
		Flag:        "bblfsh-timeout",
		Type:        core.IntConfigurationOption,
		Default:     20}, {
		Name:        ConfigUASTPoolSize,
		Description: "Number of goroutines to extract UASTs.",
		Flag:        "bblfsh-pool-size",
		Type:        core.IntConfigurationOption,
		Default:     runtime.NumCPU() * 2}, {
		Name:        ConfigUASTFailOnErrors,
		Description: "Panic if there is a UAST extraction error.",
		Flag:        "bblfsh-fail-on-error",
		Type:        core.BoolConfigurationOption,
		Default:     false}, {
		Name:        ConfigUASTLanguages,
		Description: "Programming languages from which to extract UASTs. Separated by comma \",\".",
		Flag:        "languages",
		Type:        core.StringConfigurationOption,
		Default:     "Python,Java,Go,JavaScript,Ruby,PHP"},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (exr *Extractor) Configure(facts map[string]interface{}) {
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
	if val, exists := facts[ConfigUASTLanguages].(string); exists {
		exr.Languages = map[string]bool{}
		for _, lang := range strings.Split(val, ",") {
			exr.Languages[strings.TrimSpace(lang)] = true
		}
	}
	if val, exists := facts[ConfigUASTFailOnErrors].(bool); exists {
		exr.FailOnErrors = val
	}
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (exr *Extractor) Initialize(repository *git.Repository) {
	if exr.Context == nil {
		exr.Context = func() (context.Context, context.CancelFunc) {
			return context.Background(), nil
		}
	}
	poolSize := exr.PoolSize
	if poolSize == 0 {
		poolSize = runtime.NumCPU()
	}
	exr.clients = make([]*bblfsh.Client, poolSize)
	for i := 0; i < poolSize; i++ {
		client, err := bblfsh.NewClient(exr.Endpoint)
		if err != nil {
			panic(err)
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
	if exr.Languages == nil {
		exr.Languages = map[string]bool{}
	}
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (exr *Extractor) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	cache := deps[items.DependencyBlobCache].(map[plumbing.Hash]*object.Blob)
	treeDiffs := deps[items.DependencyTreeChanges].(object.Changes)
	uasts := map[plumbing.Hash]*uast.Node{}
	lock := sync.RWMutex{}
	errs := make([]error, 0)
	wg := sync.WaitGroup{}
	submit := func(change *object.Change) {
		{
			reader, err := cache[change.To.TreeEntry.Hash].Reader()
			if err != nil {
				errs = append(errs, err)
				return
			}
			defer ioutil.CheckClose(reader, &err)

			buf := new(bytes.Buffer)
			if _, err := buf.ReadFrom(reader); err != nil {
				errs = append(errs, err)
				return
			}
			lang := enry.GetLanguage(change.To.Name, buf.Bytes())
			if _, exists := exr.Languages[lang]; !exists {
				exr.ProcessedFiles[change.To.Name] = uastExtractionSkipped
				return
			}
			exr.ProcessedFiles[change.To.Name]++
		}
		wg.Add(1)
		go func(task interface{}) {
			exr.pool.Process(task)
			wg.Done()
		}(uastTask{
			Lock:   &lock,
			Dest:   uasts,
			File:   &object.File{Name: change.To.Name, Blob: *cache[change.To.TreeEntry.Hash]},
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
		fmt.Fprintln(os.Stderr, joined)
	}
	return map[string]interface{}{DependencyUasts: uasts}, nil
}

func (exr *Extractor) Fork(n int) []core.PipelineItem {
	exrs := make([]core.PipelineItem, n)
	for i := 0; i < n; i++ {
		exrs[i] = exr
	}
	return exrs
}

func (exr *Extractor) Merge(branches []core.PipelineItem) {
	// no-op
}

func (exr *Extractor) extractUAST(
	client *bblfsh.Client, file *object.File) (*uast.Node, error) {
	request := client.NewParseRequest()
	contents, err := file.Contents()
	if err != nil {
		return nil, err
	}
	request.Content(contents)
	request.Filename(file.Name)
	ctx, cancel := exr.Context()
	if cancel != nil {
		defer cancel()
	}
	response, err := request.DoWithContext(ctx)
	if err != nil {
		if strings.Contains("missing driver", err.Error()) {
			return nil, nil
		}
		return nil, err
	}
	if response.Status != protocol.Ok {
		return nil, errors.New(strings.Join(response.Errors, "\n"))
	}
	if err != nil {
		return nil, err
	}
	return response.UAST, nil
}

func (exr *Extractor) extractTask(client *bblfsh.Client, data interface{}) interface{} {
	task := data.(uastTask)
	node, err := exr.extractUAST(client, task.File)
	task.Lock.Lock()
	defer task.Lock.Unlock()
	if err != nil {
		*task.Errors = append(*task.Errors,
			fmt.Errorf("\nfile %s, blob %s: %v", task.File.Name, task.File.Hash.String(), err))
		return nil
	}
	if node != nil {
		task.Dest[task.File.Hash] = node
	}
	return nil
}

// Change is the type of the items in the list of changes which is provided by Changes.
type Change struct {
	Before *uast.Node
	After  *uast.Node
	Change *object.Change
}

const (
	// DependencyUastChanges is the name of the dependency provided by Changes.
	DependencyUastChanges = "changed_uasts"
)

// Changes is a structured analog of TreeDiff: it provides UASTs for every logical change
// in a commit. It is a PipelineItem.
type Changes struct {
	cache map[plumbing.Hash]*uast.Node
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (uc *Changes) Name() string {
	return "UASTChanges"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (uc *Changes) Provides() []string {
	arr := [...]string{DependencyUastChanges}
	return arr[:]
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (uc *Changes) Requires() []string {
	arr := [...]string{DependencyUasts, items.DependencyTreeChanges}
	return arr[:]
}

// Features which must be enabled for this PipelineItem to be automatically inserted into the DAG.
func (uc *Changes) Features() []string {
	arr := [...]string{FeatureUast}
	return arr[:]
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (uc *Changes) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (uc *Changes) Configure(facts map[string]interface{}) {}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (uc *Changes) Initialize(repository *git.Repository) {
	uc.cache = map[plumbing.Hash]*uast.Node{}
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (uc *Changes) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	uasts := deps[DependencyUasts].(map[plumbing.Hash]*uast.Node)
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

func (uc *Changes) Fork(n int) []core.PipelineItem {
	ucs := make([]core.PipelineItem, n)
	for i := 0; i < n; i++ {
		clone := &Changes{
			cache: map[plumbing.Hash]*uast.Node{},
		}
		for key, val := range uc.cache {
			clone.cache[key] = val
		}
		ucs[i] = clone
	}
	return ucs
}

func (uc *Changes) Merge(branches []core.PipelineItem) {
	// no-op
}

// ChangesSaver dumps changed files and corresponding UASTs for every commit.
// it is a LeafPipelineItem.
type ChangesSaver struct {
	// OutputPath points to the target directory with UASTs
	OutputPath string

	repository *git.Repository
	result     [][]Change
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
	arr := [...]string{DependencyUastChanges}
	return arr[:]
}

// Features which must be enabled for this PipelineItem to be automatically inserted into the DAG.
func (saver *ChangesSaver) Features() []string {
	arr := [...]string{FeatureUast}
	return arr[:]
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (saver *ChangesSaver) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigUASTChangesSaverOutputPath,
		Description: "The target directory where to store the changed UAST files.",
		Flag:        "changed-uast-dir",
		Type:        core.StringConfigurationOption,
		Default:     "."},
	}
	return options[:]
}

// Flag for the command line switch which enables this analysis.
func (saver *ChangesSaver) Flag() string {
	return "dump-uast-changes"
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (saver *ChangesSaver) Configure(facts map[string]interface{}) {
	if val, exists := facts[ConfigUASTChangesSaverOutputPath]; exists {
		saver.OutputPath = val.(string)
	}
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (saver *ChangesSaver) Initialize(repository *git.Repository) {
	saver.repository = repository
	saver.result = [][]Change{}
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (saver *ChangesSaver) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	changes := deps[DependencyUastChanges].([]Change)
	saver.result = append(saver.result, changes)
	return nil, nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (saver *ChangesSaver) Finalize() interface{} {
	return saver.result
}

func (saver *ChangesSaver) Fork(n int) []core.PipelineItem {
	savers := make([]core.PipelineItem, n)
	for i := 0; i < n; i++ {
		savers[i] = saver
	}
	return savers
}

func (saver *ChangesSaver) Merge(branches []core.PipelineItem) {
	// no-op
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
	fileNames := []*pb.UASTChange{}
	for i, changes := range result {
		for j, change := range changes {
			if change.Before == nil || change.After == nil {
				continue
			}
			record := &pb.UASTChange{FileName: change.Change.To.Name}
			bs, _ := change.Before.Marshal()
			record.UastBefore = path.Join(saver.OutputPath, fmt.Sprintf(
				"%d_%d_before_%s.pb", i, j, change.Change.From.TreeEntry.Hash.String()))
			goioutil.WriteFile(record.UastBefore, bs, 0666)
			blob, _ := saver.repository.BlobObject(change.Change.From.TreeEntry.Hash)
			s, _ := (&object.File{Blob: *blob}).Contents()
			record.SrcBefore = path.Join(saver.OutputPath, fmt.Sprintf(
				"%d_%d_before_%s.src", i, j, change.Change.From.TreeEntry.Hash.String()))
			goioutil.WriteFile(record.SrcBefore, []byte(s), 0666)
			bs, _ = change.After.Marshal()
			record.UastAfter = path.Join(saver.OutputPath, fmt.Sprintf(
				"%d_%d_after_%s.pb", i, j, change.Change.To.TreeEntry.Hash.String()))
			goioutil.WriteFile(record.UastAfter, bs, 0666)
			blob, _ = saver.repository.BlobObject(change.Change.To.TreeEntry.Hash)
			s, _ = (&object.File{Blob: *blob}).Contents()
			record.SrcAfter = path.Join(saver.OutputPath, fmt.Sprintf(
				"%d_%d_after_%s.src", i, j, change.Change.To.TreeEntry.Hash.String()))
			goioutil.WriteFile(record.SrcAfter, []byte(s), 0666)
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
	writer.Write(serialized)
	return nil
}

func init() {
	core.Registry.Register(&Extractor{})
	core.Registry.Register(&Changes{})
	core.Registry.Register(&ChangesSaver{})
}
