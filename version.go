package hercules

import (
	"reflect"
	"strconv"
	"strings"
)

var BinaryGitHash = "<unknown>"

var BinaryVersion = 0

type versionProbe struct{}

func init() {
	parts := strings.Split(reflect.TypeOf(versionProbe{}).PkgPath(), ".")
	BinaryVersion, _ = strconv.Atoi(parts[len(parts)-1][1:])
}
