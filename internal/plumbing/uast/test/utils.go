package test

import (
	"io/ioutil"

	"gopkg.in/bblfsh/client-go.v3"
	"gopkg.in/bblfsh/sdk.v2/uast/nodes"
	"gopkg.in/src-d/go-git.v4/plumbing"
	core_test "gopkg.in/src-d/hercules.v10/internal/test"
)

// ParseBlobFromTestRepo extracts the UAST from the file by it's hash and name.
func ParseBlobFromTestRepo(hash, name string, client *bblfsh.Client) nodes.Node {
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
	request := client.NewParseRequest().Content(string(data)).Filename(name).Mode(bblfsh.Semantic)
	response, _, err := request.UAST()
	if err != nil {
		panic(err)
	}
	return response
}
