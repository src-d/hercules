package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinMaxAbs64Funcs(t *testing.T) {
	var a int64 = 1
	var b int64 = -1
	assert.Equal(t, Min64(a, b), b)
	assert.Equal(t, Max64(a, b), a)
	assert.Equal(t, Min64(b, a), b)
	assert.Equal(t, Max64(b, a), a)
	assert.Equal(t, Abs64(a), a)
	assert.Equal(t, Abs64(b), a)
}
