package yaml

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// SafeString returns a string which is sufficiently quoted and escaped for YAML.
func SafeString(str string) string {
	str = strings.Replace(str, "\\", "\\\\", -1)
	str = strings.Replace(str, "\"", "\\\"", -1)
	return "\"" + str + "\""
}

// PrintMatrix outputs a rectangular integer matrix in YAML text format.
//
// `indent` is the current YAML indentation level - the number of spaces.
// `name` is the name of the corresponding YAML block. If empty, no separate block is created.
// `fixNegative` changes all negative values to 0.
func PrintMatrix(writer io.Writer, matrix [][]int64, indent int, name string, fixNegative bool) {
	// determine the maximum length of each value
	var maxnum int64 = -(1 << 32)
	var minnum int64 = 1 << 32
	for _, status := range matrix {
		for _, val := range status {
			if val > maxnum {
				maxnum = val
			}
			if val < minnum {
				minnum = val
			}
		}
	}
	width := len(strconv.FormatInt(maxnum, 10))
	if !fixNegative && minnum < 0 {
		negativeWidth := len(strconv.FormatInt(minnum, 10))
		if negativeWidth > width {
			width = negativeWidth
		}
	}
	last := len(matrix[len(matrix)-1])
	if name != "" {
		fmt.Fprintf(writer, "%s%s: |-\n", strings.Repeat(" ", indent), SafeString(name))
		indent += 2
	}
	// print the resulting triangular matrix
	first := true
	for _, status := range matrix {
		fmt.Fprint(writer, strings.Repeat(" ", indent-1))
		for i := 0; i < last; i++ {
			var val int64
			if i < len(status) {
				val = status[i]
				// not sure why this sometimes happens...
				// TODO(vmarkovtsev): find the root cause of tiny negative balances
				if fixNegative && val < 0 {
					val = 0
				}
			}
			if !first {
				fmt.Fprintf(writer, " %[1]*[2]d", width, val)
			} else {
				first = false
				fmt.Fprintf(writer, " %d%s", val, strings.Repeat(" ", width-len(strconv.FormatInt(val, 10))))
			}
		}
		fmt.Fprintln(writer)
	}
}
