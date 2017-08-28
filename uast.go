package hercules

import (
	"context"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/bblfsh/client-go.v0"
	"github.com/bblfsh/sdk/uast"
	"github.com/bblfsh/sdk/protocol"
	"errors"
	"strings"
)

type UASTExtractor struct {
    Endpoint string
	Context func() context.Context
	client *bblfsh.BblfshClient
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
	client, err := bblfsh.NewBblfshClient(exr.Endpoint)
	if err != nil {
		panic(err)
	}
	exr.client = client
	if exr.Context == nil {
		exr.Context = func() context.Context { return context.Background() }
	}
}

func (exr *UASTExtractor) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	cache := deps["blob_cache"].(map[plumbing.Hash]*object.Blob)
	treeDiffs := deps["changes"].(object.Changes)
	uasts := map[string]*uast.Node{}
	for _, change := range treeDiffs {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		switch action {
		case merkletrie.Insert:
			uasts[change.To.Name], err = exr.extractUAST(&object.File{
				Name: change.To.Name, Blob: *cache[change.To.TreeEntry.Hash]})
		case merkletrie.Delete:
			continue
		case merkletrie.Modify:
			uasts[change.To.Name], err = exr.extractUAST(&object.File{
				Name: change.To.Name, Blob: *cache[change.To.TreeEntry.Hash]})
		}
		if err != nil {
			return nil, err
		}
	}
	return map[string]interface{}{"uasts": uasts}, nil
}

func (exr *UASTExtractor) Finalize() interface{} {
	return nil
}

func (exr *UASTExtractor) extractUAST(file *object.File) (*uast.Node, error) {
	request := exr.client.NewParseRequest()
	contents, err := file.Contents()
	if err != nil {
		return nil, err
	}
	request.Content(contents)
	request.Filename(file.Name)
	response, err := request.DoWithContext(exr.Context())
    if response.Status != protocol.Ok {
		return nil, errors.New(strings.Join(response.Errors, "\n"))
	}
	if err != nil {
		return nil, err
	}
	return response.UAST, nil
}
