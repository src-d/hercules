package hercules

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"

	"github.com/jeffail/tunny"
	"gopkg.in/bblfsh/client-go.v0"
	"gopkg.in/bblfsh/sdk.v0/protocol"
	"gopkg.in/bblfsh/sdk.v0/uast"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
)

type UASTExtractor struct {
	Endpoint string
	Context  func() context.Context
	PoolSize int

	clients []*bblfsh.BblfshClient
	pool   *tunny.WorkPool
}

type uastTask struct {
	Client *bblfsh.BblfshClient
	Lock   *sync.RWMutex
	Dest   map[string]*uast.Node
	Name   string
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
	arr := [...]string{"changes", "blob_cache"}
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
	treeDiffs := deps["changes"].(object.Changes)
	uasts := map[string]*uast.Node{}
	lock := sync.RWMutex{}
	errs := make([]error, 0)
	status := make(chan int)
	pending := 0
	submit := func(change *object.Change) {
		pending++
		exr.pool.SendWorkAsync(uastTask{
			Lock: &lock, Dest: uasts, Name: change.To.Name,
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
		return nil, errors.New(strings.Join(msgs, "\n"))
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
		*task.Errors = append(*task.Errors, errors.New(task.Name+": "+err.Error()))
		return nil
	}
	task.Dest[task.Name] = node
	return nil
}
