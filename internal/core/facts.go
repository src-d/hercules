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
	FindIdOf(name string) AuthorId
	ForEachIdentity(callback func(AuthorId, string)) bool
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
