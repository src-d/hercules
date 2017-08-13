package hercules

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBurndownMeta(t *testing.T) {
	burndown := BurndownAnalysis{}
	assert.Equal(t, burndown.Name(), "Burndown")
	assert.Equal(t, len(burndown.Provides()), 0)
	required := [...]string{"renamed_changes", "blob_cache", "day", "author"}
	for _, name := range required {
		assert.Contains(t, burndown.Requires(), name)
	}
}
