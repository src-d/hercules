package internal

import (
	"io"

	"github.com/pkg/errors"
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
	Fails    bool
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
	if !obj.Fails {
		return dummyIO{}, nil
	}
	return nil, errors.New("dummy failure")
}

func (obj dummyEncodedObject) Writer() (io.WriteCloser, error) {
	if !obj.Fails {
		return dummyIO{}, nil
	}
	return nil, errors.New("dummy failure")
}

// CreateDummyBlob constructs a fake object.Blob with empty contents.
// Optionally returns an error if read or written.
func CreateDummyBlob(hash plumbing.Hash, fails ...bool) (*object.Blob, error) {
	if len(fails) > 1 {
		panic("invalid usage of CreateDummyBlob() - this is a bug")
	}
	var realFails bool
	if len(fails) == 1 {
		realFails = fails[0]
	}
	return object.DecodeBlob(dummyEncodedObject{FakeHash: hash, Fails: realFails})
}
