package internal

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestCreateDummyBlob(t *testing.T) {
	dummy, err := CreateDummyBlob(plumbing.NewHash("334cde09da4afcb74f8d2b3e6fd6cce61228b485"))
	assert.Nil(t, err)
	assert.Equal(t, dummy.Hash.String(), "334cde09da4afcb74f8d2b3e6fd6cce61228b485")
	assert.Equal(t, dummy.Size, int64(0))
	reader, err := dummy.Reader()
	assert.Nil(t, err)
	buffer := make([]byte, 1)
	buffer[0] = 0xff
	n, err := reader.Read(buffer)
	assert.Equal(t, err, io.EOF)
	assert.Equal(t, n, 0)
	assert.Equal(t, buffer[0], byte(0xff))
	reader.Close()
}

func TestCreateDummyBlobFails(t *testing.T) {
	dummy, err := CreateDummyBlob(plumbing.NewHash("334cde09da4afcb74f8d2b3e6fd6cce61228b485"), true)
	assert.Nil(t, err)
	reader, err := dummy.Reader()
	assert.Nil(t, reader)
	assert.NotNil(t, err)
	assert.Panics(t, func() {
		CreateDummyBlob(plumbing.NewHash("334cde09da4afcb74f8d2b3e6fd6cce61228b485"), true, true)
	})
}

func TestNotUsedDummyStuff(t *testing.T) {
	dio := dummyIO{}
	n, err := dio.Write([]byte{})
	assert.Nil(t, err)
	assert.Equal(t, n, 0)
	obj := dummyEncodedObject{}
	obj.SetSize(int64(100))
	obj.SetType(plumbing.CommitObject)
	writer, err := obj.Writer()
	assert.Nil(t, err)
	assert.NotNil(t, writer)
}
