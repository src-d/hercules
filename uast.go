package hercules

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"

	"github.com/jeffail/tunny"
	"gopkg.in/bblfsh/client-go.v1"
	"gopkg.in/bblfsh/sdk.v1/protocol"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"fmt"
	"os"
)

type UASTExtractor struct {
	Endpoint string
	Context  func() context.Context
	PoolSize int
	Extensions map[string]bool
	FailOnErrors bool

	clients []*bblfsh.BblfshClient
	pool   *tunny.WorkPool
}

type uastTask struct {
	Client *bblfsh.BblfshClient
	Lock   *sync.RWMutex
	Dest   map[plumbing.Hash]*uast.Node
	File   *object.File
	Errors *[]error
	Status chan int
}

type worker struct {
	Client *bblfsh.BblfshClient
	Job func(interface{}) interface{}
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
	arr := [...]string{"renamed_changes", "blob_cache"}
	return arr[:]
}

func (exr *UASTExtractor) Initialize(repository *git.Repository) {
	if exr.Context == nil {
		exr.Context = func() context.Context { return context.Background() }
	}
	poolSize := exr.PoolSize
	if poolSize == 0 {
		poolSize = runtime.NumCPU()
	}
	var err error
	exr.clients = make([]*bblfsh.BblfshClient, poolSize)
	for i := 0; i < poolSize; i++ {
		client, err := bblfsh.NewBblfshClient(exr.Endpoint)
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
}

func (exr *UASTExtractor) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	cache := deps["blob_cache"].(map[plumbing.Hash]*object.Blob)
	treeDiffs := deps["renamed_changes"].(object.Changes)
	uasts := map[plumbing.Hash]*uast.Node{}
	lock := sync.RWMutex{}
	errs := make([]error, 0)
	status := make(chan int)
	pending := 0
	submit := func(change *object.Change) {
		var ext string
		dotpos := strings.LastIndex(change.To.Name, ".")
		if dotpos >= 0 {
			ext = change.To.Name[dotpos + 1:]
		} else {
			ext = change.To.Name
		}
		_, exists := exr.Extensions[ext]
		if !exists {
			return
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

func (exr *UASTExtractor) Finalize() interface{} {
	return nil
}

func (exr *UASTExtractor) extractUAST(
		client *bblfsh.BblfshClient, file *object.File) (*uast.Node, error) {
	request := client.NewParseRequest()
	contents, err := file.Contents()
	if err != nil {
		return nil, err
	}
	request.Content(contents)
	request.Filename(file.Name)
	response, err := request.DoWithContext(exr.Context())
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
	task.Dest[task.File.Hash] = node
	return nil
}

type UASTChange struct {
	Before *uast.Node
	After *uast.Node
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
	arr := [...]string{"uasts", "renamed_changes"}
	return arr[:]
}

func (uc *UASTChanges) Initialize(repository *git.Repository) {
	uc.cache = map[plumbing.Hash]*uast.Node{}
}

func (uc *UASTChanges) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
  uasts := deps["uasts"].(map[plumbing.Hash]*uast.Node)
	treeDiffs := deps["renamed_changes"].(object.Changes)
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

func (uc *UASTChanges) Finalize() interface{} {
	return nil
}

type UASTChangesSaver struct {
  result [][]UASTChange
}

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

func (saver *UASTChangesSaver) Initialize(repository *git.Repository) {
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
