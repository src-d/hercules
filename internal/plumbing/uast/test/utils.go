package test

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/bblfsh/client-go.v2"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/src-d/go-git.v4/plumbing"
	core_test "gopkg.in/src-d/hercules.v7/internal/test"
)

// ParseBlobFromTestRepo extracts the UAST from the file by it's hash and name.
func ParseBlobFromTestRepo(hash, name string, client *bblfsh.Client) *uast.Node {
	blob, err := core_test.Repository.BlobObject(plumbing.NewHash(hash))
	if err != nil {
		panic(err)
	}
	reader, err := blob.Reader()
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	request := client.NewParseRequest()
	request.Content(string(data))
	request.Filename(name)
	response, err := request.Do()
	if err != nil {
		panic(err)
	}
	if response.UAST == nil {
		panic(fmt.Sprintf("empty response for %s %s", name, hash))
	}
	return response.UAST
}
