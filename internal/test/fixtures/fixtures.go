package fixtures

import (
	"gopkg.in/src-d/hercules.v7/internal/plumbing"
	"gopkg.in/src-d/hercules.v7/internal/test"
)

// FileDiff initializes a new plumbing.FileDiff item for testing.
func FileDiff() *plumbing.FileDiff {
	fd := &plumbing.FileDiff{}
	fd.Initialize(test.Repository)
	return fd
}
