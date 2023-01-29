package fixtures

import (
	"github.com/cyraxred/hercules/internal/plumbing"
	"github.com/cyraxred/hercules/internal/test"
)

// FileDiff initializes a new plumbing.FileDiff item for testing.
func FileDiff() *plumbing.FileDiff {
	fd := &plumbing.FileDiff{}
	fd.Initialize(test.Repository)
	return fd
}
