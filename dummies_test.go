package hercules

import (
	"io"
	"testing"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"github.com/stretchr/testify/assert"
)

func TestCreateDummyBlob(t *testing.T) {
  dummy, err := createDummyBlob(plumbing.NewHash("334cde09da4afcb74f8d2b3e6fd6cce61228b485"))
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
