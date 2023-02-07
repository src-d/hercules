package core

import (
	"math"
)

type FileId int32
type AuthorId int32
type TickNumber int32

const (
	FactIdentityResolver    = "Identity.Resolver"
	FactLineHistoryResolver = "LineHistory.Resolver"
)

const (
	// AuthorMissing is the internal author index which denotes any unmatched identities
	// (Detector.Consume()). It may *not* be (1 << 18) - 1, see BurndownAnalysis.packPersonWithDay().
	AuthorMissing = (1 << 18) - 1
	// AuthorMissingName is the string name which corresponds to AuthorMissing.
	AuthorMissingName = "<unmatched>"
)

type IdentityResolver interface {
	Count() int
	FriendlyNameOf(id AuthorId) string
	ForEachIdentity(callback func(AuthorId, string)) bool
	CopyFriendlyNames() []string
}

var _ IdentityResolver = identityResolver{}

func NewIdentityResolver(names []string, toIds map[string]int) IdentityResolver {
	resolver := identityResolver{}

	n := len(names)
	if n == 0 {
		return resolver
	}
	resolver.toNames = make([]string, n)
	copy(resolver.toNames, names)

	if len(toIds) != 0 {
		n = len(toIds)
	}
	resolver.toIds = make(map[string]int, n)

	if len(toIds) != 0 {
		for k, v := range toIds {
			resolver.toIds[k] = v
		}
	} else {
		for k, v := range names {
			resolver.toIds[v] = k
		}
	}

	return resolver
}

type identityResolver struct {
	toIds   map[string]int
	toNames []string
}

func (v identityResolver) Count() int {
	return len(v.toNames)
}

func (v identityResolver) FriendlyNameOf(id AuthorId) string {
	if id == AuthorMissing || id < 0 || int(id) >= len(v.toNames) {
		return AuthorMissingName
	}
	return v.toNames[id]
}

func (v identityResolver) FindIdOf(name string) AuthorId {
	if id, ok := v.toIds[name]; ok {
		return AuthorId(id)
	}
	return AuthorId(-1)
}

func (v identityResolver) ForEachIdentity(callback func(AuthorId, string)) bool {
	for id, name := range v.toNames {
		callback(AuthorId(id), name)
	}
	return true
}

func (v identityResolver) CopyFriendlyNames() []string {
	return append([]string(nil), v.toNames...)
}

type LineHistoryChange struct {
	FileId
	CurrTick, PrevTick     TickNumber
	CurrAuthor, PrevAuthor AuthorId
	Delta                  int
}

func (v LineHistoryChange) IsDelete() bool {
	return v.CurrAuthor == AuthorMissing && v.Delta == math.MinInt
}

func NewLineHistoryDeletion(id FileId, tick TickNumber) LineHistoryChange {
	return LineHistoryChange{
		FileId:     id,
		CurrTick:   tick,
		CurrAuthor: AuthorMissing,
		PrevTick:   tick,
		PrevAuthor: AuthorMissing,
		Delta:      math.MinInt,
	}
}

type LineHistoryChanges struct {
	Changes  []LineHistoryChange
	Resolver FileIdResolver
}

type FileIdResolver interface {
	NameOf(id FileId) string
	MergedWith(id FileId) (FileId, string, bool)
	ForEachFile(callback func(id FileId, name string)) bool
	ScanFile(id FileId, callback func(line int, tick TickNumber, author AuthorId)) bool
}
