package hercules

import (
	"reflect"
	"strconv"
	"strings"
)

var GIT_HASH = "<unknown>"

var VERSION = 0

type versionProbe struct{}

func init() {
	parts := strings.Split(reflect.TypeOf(versionProbe{}).PkgPath(), ".")
	VERSION, _ = strconv.Atoi(parts[len(parts)-1][1:])
}
