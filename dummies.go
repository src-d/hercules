package hercules

import (
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type dummyIO struct {
}

func (dummyIO) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (dummyIO) Write(p []byte) (int, error) {
	return len(p), nil
}

func (dummyIO) Close() error {
	return nil
}

type dummyEncodedObject struct {
	FakeHash plumbing.Hash
}

func (obj dummyEncodedObject) Hash() plumbing.Hash {
	return obj.FakeHash
}

func (obj dummyEncodedObject) Type() plumbing.ObjectType {
	return plumbing.BlobObject
}

func (obj dummyEncodedObject) SetType(plumbing.ObjectType) {
}

func (obj dummyEncodedObject) Size() int64 {
	return 0
}

func (obj dummyEncodedObject) SetSize(int64) {
}

func (obj dummyEncodedObject) Reader() (io.ReadCloser, error) {
	return dummyIO{}, nil
}

func (obj dummyEncodedObject) Writer() (io.WriteCloser, error) {
	return dummyIO{}, nil
}

func createDummyBlob(hash plumbing.Hash) (*object.Blob, error) {
	return object.DecodeBlob(dummyEncodedObject{hash})
}
