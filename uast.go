package hercules

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	goioutil "io/ioutil"
	"os"
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
	"gopkg.in/src-d/hercules.v3/pb"
)

type UASTExtractor struct {
	Endpoint       string
	Context        func() (context.Context, context.CancelFunc)
	PoolSize       int
	Languages      map[string]bool
	FailOnErrors   bool
	ProcessedFiles map[string]int

	clients []*bblfsh.Client
	pool    *tunny.WorkPool
}

const (
	UAST_EXTRACTION_SKIPPED = -(1 << 31)

	ConfigUASTEndpoint     = "ConfigUASTEndpoint"
	ConfigUASTTimeout      = "ConfigUASTTimeout"
	ConfigUASTPoolSize     = "ConfigUASTPoolSize"
	ConfigUASTFailOnErrors = "ConfigUASTFailOnErrors"
	ConfigUASTLanguages    = "ConfigUASTLanguages"
)

type uastTask struct {
	Client *bblfsh.Client
	Lock   *sync.RWMutex
	Dest   map[plumbing.Hash]*uast.Node
	File   *object.File
	Errors *[]error
	Status chan int
}

type worker struct {
	Client *bblfsh.Client
	Job    func(interface{}) interface{}
}

func (w worker) TunnyReady() bool {
	return true
}

func (w worker) TunnyJob(data interface{}) interface{} {
	task := data.(uastTask)
	task.Client = w.Client
	return w.Job(task)
}

func (exr *UASTExtractor) Name() string {
	return "UAST"
}

func (exr *UASTExtractor) Provides() []string {
	arr := [...]string{"uasts"}
	return arr[:]
}

func (exr *UASTExtractor) Requires() []string {
	arr := [...]string{"changes", "blob_cache"}
	return arr[:]
}

func (exr *UASTExtractor) Features() []string {
	arr := [...]string{"uast"}
	return arr[:]
}

func (exr *UASTExtractor) ListConfigurationOptions() []ConfigurationOption {
	options := [...]ConfigurationOption{{
		Name:        ConfigUASTEndpoint,
		Description: "How many days there are in a single band.",
		Flag:        "bblfsh",
		Type:        StringConfigurationOption,
		Default:     "0.0.0.0:9432"}, {
		Name:        ConfigUASTTimeout,
		Description: "Babelfish's server timeout in seconds.",
		Flag:        "bblfsh-timeout",
		Type:        IntConfigurationOption,
		Default:     20}, {
		Name:        ConfigUASTPoolSize,
		Description: "Number of goroutines to extract UASTs.",
		Flag:        "bblfsh-pool-size",
		Type:        IntConfigurationOption,
		Default:     runtime.NumCPU()}, {
		Name:        ConfigUASTFailOnErrors,
		Description: "Panic if there is a UAST extraction error.",
		Flag:        "bblfsh-fail-on-error",
		Type:        BoolConfigurationOption,
		Default:     false}, {
		Name:        ConfigUASTLanguages,
		Description: "Programming languages from which to extract UASTs. Separated by comma \",\".",
		Flag:        "languages",
		Type:        StringConfigurationOption,
		Default:     "Python,Java"},
	}
	return options[:]
}

func (exr *UASTExtractor) Configure(facts map[string]interface{}) {
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

func (exr *UASTExtractor) Initialize(repository *git.Repository) {
	if exr.Context == nil {
		exr.Context = func() (context.Context, context.CancelFunc) {
			return context.Background(), nil
		}
	}
	poolSize := exr.PoolSize
	if poolSize == 0 {
		poolSize = runtime.NumCPU()
	}
	var err error
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
	workers := make([]tunny.TunnyWorker, poolSize)
	for i := 0; i < poolSize; i++ {
		workers[i] = worker{Client: exr.clients[i], Job: exr.extractTask}
	}
	exr.pool, err = tunny.CreateCustomPool(workers).Open()
	if err != nil {
		panic(err)
	}
	exr.ProcessedFiles = map[string]int{}
	if exr.Languages == nil {
		exr.Languages = map[string]bool{}
	}
}

func (exr *UASTExtractor) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	cache := deps["blob_cache"].(map[plumbing.Hash]*object.Blob)
	treeDiffs := deps["changes"].(object.Changes)
	uasts := map[plumbing.Hash]*uast.Node{}
	lock := sync.RWMutex{}
	errs := make([]error, 0)
	status := make(chan int)
	pending := 0
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
				exr.ProcessedFiles[change.To.Name] = UAST_EXTRACTION_SKIPPED
				return
			}
			exr.ProcessedFiles[change.To.Name]++
		}
		pending++
		exr.pool.SendWorkAsync(uastTask{
			Lock:   &lock,
			Dest:   uasts,
			File:   &object.File{Name: change.To.Name, Blob: *cache[change.To.TreeEntry.Hash]},
			Errors: &errs, Status: status}, nil)
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
	for i := 0; i < pending; i++ {
		_ = <-status
	}
	if len(errs) > 0 {
		msgs := make([]string, len(errs))
		for i, err := range errs {
			msgs[i] = err.Error()
		}
		joined := strings.Join(msgs, "\n")
		if exr.FailOnErrors {
			return nil, errors.New(joined)
		} else {
			fmt.Fprintln(os.Stderr, joined)
		}
	}
	return map[string]interface{}{"uasts": uasts}, nil
}

func (exr *UASTExtractor) extractUAST(
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

func (exr *UASTExtractor) extractTask(data interface{}) interface{} {
	task := data.(uastTask)
	defer func() { task.Status <- 0 }()
	node, err := exr.extractUAST(task.Client, task.File)
	task.Lock.Lock()
	defer task.Lock.Unlock()
	if err != nil {
		*task.Errors = append(*task.Errors, errors.New(task.File.Name+": "+err.Error()))
		return nil
	}
	if node != nil {
		task.Dest[task.File.Hash] = node
	}
	return nil
}

type UASTChange struct {
	Before *uast.Node
	After  *uast.Node
	Change *object.Change
}

type UASTChanges struct {
	cache map[plumbing.Hash]*uast.Node
}

func (uc *UASTChanges) Name() string {
	return "UASTChanges"
}

func (uc *UASTChanges) Provides() []string {
	arr := [...]string{"changed_uasts"}
	return arr[:]
}

func (uc *UASTChanges) Requires() []string {
	arr := [...]string{"uasts", "changes"}
	return arr[:]
}

func (uc *UASTChanges) Features() []string {
	arr := [...]string{"uast"}
	return arr[:]
}

func (uc *UASTChanges) ListConfigurationOptions() []ConfigurationOption {
	return []ConfigurationOption{}
}

func (uc *UASTChanges) Configure(facts map[string]interface{}) {}

func (uc *UASTChanges) Initialize(repository *git.Repository) {
	uc.cache = map[plumbing.Hash]*uast.Node{}
}

func (uc *UASTChanges) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	uasts := deps["uasts"].(map[plumbing.Hash]*uast.Node)
	treeDiffs := deps["changes"].(object.Changes)
	commit := make([]UASTChange, 0, len(treeDiffs))
	for _, change := range treeDiffs {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		switch action {
		case merkletrie.Insert:
			hashTo := change.To.TreeEntry.Hash
			uastTo := uasts[hashTo]
			commit = append(commit, UASTChange{Before: nil, After: uastTo, Change: change})
			uc.cache[hashTo] = uastTo
		case merkletrie.Delete:
			hashFrom := change.From.TreeEntry.Hash
			commit = append(commit, UASTChange{Before: uc.cache[hashFrom], After: nil, Change: change})
			delete(uc.cache, hashFrom)
		case merkletrie.Modify:
			hashFrom := change.From.TreeEntry.Hash
			hashTo := change.To.TreeEntry.Hash
			uastTo := uasts[hashTo]
			commit = append(commit, UASTChange{Before: uc.cache[hashFrom], After: uastTo, Change: change})
			delete(uc.cache, hashFrom)
			uc.cache[hashTo] = uastTo
		}
	}
	return map[string]interface{}{"changed_uasts": commit}, nil
}

type UASTChangesSaver struct {
	// OutputPath points to the target directory with UASTs
	OutputPath string

	repository *git.Repository
	result     [][]UASTChange
}

const (
	ConfigUASTChangesSaverOutputPath = "UASTChangesSaver.OutputPath"
)

func (saver *UASTChangesSaver) Name() string {
	return "UASTChangesSaver"
}

func (saver *UASTChangesSaver) Provides() []string {
	return []string{}
}

func (saver *UASTChangesSaver) Requires() []string {
	arr := [...]string{"changed_uasts"}
	return arr[:]
}

func (saver *UASTChangesSaver) Features() []string {
	arr := [...]string{"uast"}
	return arr[:]
}

func (saver *UASTChangesSaver) ListConfigurationOptions() []ConfigurationOption {
	options := [...]ConfigurationOption{{
		Name:        ConfigUASTChangesSaverOutputPath,
		Description: "The target directory where to store the changed UAST files.",
		Flag:        "changed-uast-dir",
		Type:        StringConfigurationOption,
		Default:     "."},
	}
	return options[:]
}

func (saver *UASTChangesSaver) Flag() string {
	return "dump-uast-changes"
}

func (saver *UASTChangesSaver) Configure(facts map[string]interface{}) {
	if val, exists := facts[ConfigUASTChangesSaverOutputPath]; exists {
		saver.OutputPath = val.(string)
	}
}

func (saver *UASTChangesSaver) Initialize(repository *git.Repository) {
	saver.repository = repository
	saver.result = [][]UASTChange{}
}

func (saver *UASTChangesSaver) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	changes := deps["changed_uasts"].([]UASTChange)
	saver.result = append(saver.result, changes)
	return nil, nil
}

func (saver *UASTChangesSaver) Finalize() interface{} {
	return saver.result
}

func (saver *UASTChangesSaver) Serialize(result interface{}, binary bool, writer io.Writer) error {
	saverResult := result.([][]UASTChange)
	fileNames := saver.dumpFiles(saverResult)
	if binary {
		return saver.serializeBinary(fileNames, writer)
	}
	saver.serializeText(fileNames, writer)
	return nil
}

func (saver *UASTChangesSaver) dumpFiles(result [][]UASTChange) []*pb.UASTChange {
	fileNames := []*pb.UASTChange{}
	for i, changes := range result {
		for j, change := range changes {
			if change.Before == nil || change.After == nil {
				continue
			}
			record := &pb.UASTChange{FileName: change.Change.To.Name}
			bs, _ := change.Before.Marshal()
			record.UastBefore = fmt.Sprintf(
				"%d_%d_before_%s.pb", i, j, change.Change.From.TreeEntry.Hash.String())
			goioutil.WriteFile(record.UastBefore, bs, 0666)
			blob, _ := saver.repository.BlobObject(change.Change.From.TreeEntry.Hash)
			s, _ := (&object.File{Blob: *blob}).Contents()
			record.SrcBefore = fmt.Sprintf(
				"%d_%d_before_%s.src", i, j, change.Change.From.TreeEntry.Hash.String())
			goioutil.WriteFile(record.SrcBefore, []byte(s), 0666)
			bs, _ = change.After.Marshal()
			record.UastAfter = fmt.Sprintf(
				"%d_%d_after_%s.pb", i, j, change.Change.To.TreeEntry.Hash.String())
			goioutil.WriteFile(record.UastAfter, bs, 0666)
			blob, _ = saver.repository.BlobObject(change.Change.To.TreeEntry.Hash)
			s, _ = (&object.File{Blob: *blob}).Contents()
			record.SrcAfter = fmt.Sprintf(
				"%d_%d_after_%s.src", i, j, change.Change.To.TreeEntry.Hash.String())
			goioutil.WriteFile(record.SrcAfter, []byte(s), 0666)
			fileNames = append(fileNames, record)
		}
	}
	return fileNames
}

func (saver *UASTChangesSaver) serializeText(result []*pb.UASTChange, writer io.Writer) {
	for _, sc := range result {
		kv := [...]string{
			"file: " + sc.FileName,
			"src0: " + sc.SrcBefore, "src1: " + sc.SrcAfter,
			"uast0: " + sc.UastBefore, "uast1: " + sc.UastAfter,
		}
		fmt.Fprintf(writer, "  - {%s}\n", strings.Join(kv[:], ", "))
	}
}

func (saver *UASTChangesSaver) serializeBinary(result []*pb.UASTChange, writer io.Writer) error {
	message := pb.UASTChangesSaverResults{Changes: result}
	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	writer.Write(serialized)
	return nil
}

func init() {
	Registry.Register(&UASTExtractor{})
	Registry.Register(&UASTChanges{})
	Registry.Register(&UASTChangesSaver{})
}
