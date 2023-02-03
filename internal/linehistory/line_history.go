package linehistory

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"runtime/debug"
	"sync/atomic"
	"unicode/utf8"

	"github.com/cyraxred/hercules/internal/core"
	items "github.com/cyraxred/hercules/internal/plumbing"
	"github.com/cyraxred/hercules/internal/plumbing/identity"
	"github.com/cyraxred/hercules/internal/rbtree"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/utils/merkletrie"
	"github.com/sergi/go-diff/diffmatchpatch"
)

func NewLineHistoryAnalyser() *LineHistoryAnalyser {
	return &LineHistoryAnalyser{}
}

// LineHistoryAnalyser allows to gather per-line history and statistics for a Git repository.
// It is a PipelineItem.
type LineHistoryAnalyser struct {
	// HibernationThreshold sets the hibernation threshold for the underlying
	// RBTree allocator. It is useful to trade CPU time for reduced peak memory consumption
	// if there are many branches.
	HibernationThreshold int

	// HibernationToDisk specifies whether the hibernated RBTree allocator must be saved on disk
	// rather than kept in memory.
	HibernationToDisk bool

	// HibernationDirectory is the name of the temporary directory to use for saving hibernated
	// RBTree allocators.
	HibernationDirectory string

	// Debug activates the debugging mode. Analyse() runs slower in this mode
	// but it accurately checks all the intermediate states for invariant
	// violations.
	Debug bool

	// Repository points to the analysed Git repository struct from go-git.
	repository *git.Repository

	fileIdCounter *counterHolder
	// names of unique file ids
	fileNames map[FileId]string
	// names of unique file ids
	fileAbandonedNames         map[FileId]string
	fileAbandonedNamesOfParent map[FileId]string

	// files is the mapping <file path> -> *File.
	files map[string]*File

	// fileAllocator is the allocator for RBTree-s in `files`.
	fileAllocator *rbtree.Allocator
	// hibernatedFileName is the path to the serialized `fileAllocator`.
	hibernatedFileName string

	// tick is the most recent tick index processed.
	tick TickNumber
	// previousTick is the tick from the previous sample period -
	// different from TicksSinceStart.previousTick.
	previousTick TickNumber

	changes []LineHistoryChange

	l core.Logger
}

type counterHolder struct {
	atomicCounter int32
}

func (p *counterHolder) next() FileId {
	return FileId(atomic.AddInt32(&p.atomicCounter, 1))
}

type TickNumber int32
type AuthorId int32

type LineHistoryChange struct {
	FileId
	CurrTick, PrevTick     TickNumber
	CurrAuthor, PrevAuthor AuthorId
	Delta                  int
}

func (v LineHistoryChange) IsDelete() bool {
	return v.CurrAuthor == identity.AuthorMissing && v.Delta == math.MinInt
}

type FileIdResolver struct {
	analyser *LineHistoryAnalyser
}

func (v FileIdResolver) NameOf(id FileId) string {
	if v.analyser == nil {
		return ""
	}

	if n, ok := v.analyser.fileNames[id]; ok {
		return n
	}
	return v.abandonedNameOf(id)
}

func (v FileIdResolver) abandonedNameOf(id FileId) string {
	if n, ok := v.analyser.fileAbandonedNames[id]; ok {
		return n
	}
	return v.analyser.fileAbandonedNamesOfParent[id]
}

func (v FileIdResolver) MergedWith(id FileId) (FileId, string, bool) {
	if v.analyser == nil {
		return 0, "", false
	}

	switch f, n := v.analyser.findFileAndName(id); {
	case f != nil:
		return f.Id, n, true
	case n != "":
		return 0, n, false
	}
	return 0, v.abandonedNameOf(id), false
}

func (v FileIdResolver) ForEachFile(callback func(id FileId, name string)) bool {
	if v.analyser == nil {
		return false
	}

	for name, file := range v.analyser.files {
		callback(file.Id, name)
	}
	return true
}

func (v FileIdResolver) ScanFile(id FileId, callback func(line int, tick TickNumber, author AuthorId)) bool {
	if v.analyser == nil {
		return false
	}

	file, _ := v.analyser.findFileAndName(id)
	if file == nil {
		return false
	}
	file.ForEach(func(line, value int) {
		author, tick := unpackPersonWithTick(value)
		callback(line, tick, author)
	})
	return true
}

type LineHistoryChanges struct {
	Changes  []LineHistoryChange
	Resolver FileIdResolver
}

const (
	DependencyLineHistory = "line_history"

	// ConfigLinesHibernationThreshold sets the hibernation threshold for the underlying
	// RBTree allocator. It is useful to trade CPU time for reduced peak memory consumption
	// if there are many branches.
	ConfigLinesHibernationThreshold = "LineHistory.HibernationThreshold"
	// ConfigLinesHibernationToDisk sets whether the hibernated RBTree allocator must be saved
	// on disk rather than kept in memory.
	ConfigLinesHibernationToDisk = "LineHistory.HibernationOnDisk"
	// ConfigLinesHibernationDirectory sets the name of the temporary directory to use for
	// saving hibernated RBTree allocators.
	ConfigLinesHibernationDirectory = "LineHistory.HibernationDirectory"
	// ConfigLinesDebug enables some extra debug assertions.
	ConfigLinesDebug = "LineHistory.Debug"
)

func (analyser *LineHistoryAnalyser) Name() string {
	return "LineHistory"
}

func (analyser *LineHistoryAnalyser) Provides() []string {
	return []string{DependencyLineHistory}
}

func (analyser *LineHistoryAnalyser) Requires() []string {
	return []string{
		items.DependencyFileDiff, items.DependencyTreeChanges, items.DependencyBlobCache,
		items.DependencyTick, identity.DependencyAuthor}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (analyser *LineHistoryAnalyser) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name: ConfigLinesHibernationThreshold,
		Description: "The minimum size for the allocated memory in each branch to be compressed." +
			"0 disables this optimization. Lower values trade CPU time more. Sane examples: Nx1000.",
		Flag:    "lines-hibernation-threshold",
		Type:    core.IntConfigurationOption,
		Default: 0}, {
		Name:        ConfigLinesHibernationToDisk,
		Description: "Save hibernated RBTree allocators to disk rather than keep it in memory.",
		Flag:        "lines-hibernation-disk",
		Type:        core.BoolConfigurationOption,
		Default:     false}, {
		Name:        ConfigLinesHibernationDirectory,
		Description: "Temporary directory where to save the hibernated RBTree allocators.",
		Flag:        "lines-hibernation-dir",
		Type:        core.PathConfigurationOption,
		Default:     ""}, {
		Name:        ConfigLinesDebug,
		Description: "Validate the trees at each step.",
		Flag:        "lines-debug",
		Type:        core.BoolConfigurationOption,
		Default:     false},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (analyser *LineHistoryAnalyser) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		analyser.l = l
	} else {
		analyser.l = core.NewLogger()
	}
	if val, exists := facts[ConfigLinesHibernationThreshold].(int); exists {
		analyser.HibernationThreshold = val
	}
	if val, exists := facts[ConfigLinesHibernationToDisk].(bool); exists {
		analyser.HibernationToDisk = val
	}
	if val, exists := facts[ConfigLinesHibernationDirectory].(string); exists {
		analyser.HibernationDirectory = val
	}
	if val, exists := facts[ConfigLinesDebug].(bool); exists {
		analyser.Debug = val
	}

	return nil
}

func (analyser *LineHistoryAnalyser) ConfigureUpstream(_ map[string]interface{}) error {
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (analyser *LineHistoryAnalyser) Initialize(repository *git.Repository) error {
	analyser.l = core.NewLogger()
	analyser.repository = repository
	analyser.fileNames = map[FileId]string{}
	analyser.fileIdCounter = &counterHolder{}
	analyser.files = map[string]*File{}
	analyser.fileAllocator = rbtree.NewAllocator()
	analyser.fileAllocator.HibernationThreshold = analyser.HibernationThreshold

	analyser.tick = 0
	analyser.previousTick = 0

	return nil
}

func (analyser *LineHistoryAnalyser) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	if analyser.fileAllocator.Size() == 0 && len(analyser.files) > 0 {
		panic("LineHistoryAnalyser.Consume() was called on a hibernated instance")
	}

	author := AuthorId(deps[identity.DependencyAuthor].(int))
	analyser.tick = TickNumber(deps[items.DependencyTick].(int))
	analyser.onNewTick()

	cache := deps[items.DependencyBlobCache].(map[plumbing.Hash]*items.CachedBlob)
	treeDiffs := deps[items.DependencyTreeChanges].(object.Changes)
	fileDiffs := deps[items.DependencyFileDiff].(map[string]items.FileDiffData)

	analyser.changes = make([]LineHistoryChange, 0, len(treeDiffs)*4)
	for _, change := range treeDiffs {
		action, _ := change.Action()
		var err error
		switch action {
		case merkletrie.Insert:
			err = analyser.handleInsertion(change, author, cache)
		case merkletrie.Delete:
			err = analyser.handleDeletion(change, author, cache)
		case merkletrie.Modify:
			err = analyser.handleModification(change, author, cache, fileDiffs)
		}
		if err != nil {
			return nil, err
		}
	}

	result := map[string]interface{}{DependencyLineHistory: LineHistoryChanges{
		Changes:  analyser.changes,
		Resolver: FileIdResolver{analyser},
	}}

	analyser.changes = nil
	return result, nil
}

func (analyser *LineHistoryAnalyser) findFileAndName(id FileId) (*File, string) {
	if n, ok := analyser.fileNames[id]; ok {
		if f := analyser.files[n]; f != nil {
			return f, n
		}
		analyser.addAbandonedName(id, n)
	}
	return nil, ""
}

func (analyser *LineHistoryAnalyser) addAbandonedName(id FileId, name string) {
	if analyser.fileAbandonedNames == nil {
		analyser.fileAbandonedNames = map[FileId]string{}
	}
	analyser.fileAbandonedNames[id] = name
	delete(analyser.fileNames, id)
}

func (analyser *LineHistoryAnalyser) mergeAbandonedName(id FileId, name string) {
	if analyser.fileAbandonedNames == nil {
		analyser.fileAbandonedNames = map[FileId]string{}
	} else if _, ok := analyser.fileAbandonedNames[id]; ok {
		return
	}
	analyser.fileAbandonedNames[id] = name
}

func (analyser *LineHistoryAnalyser) inheritAbandonedNames() map[FileId]string {
	if len(analyser.fileAbandonedNamesOfParent) == 0 {
		return analyser.fileAbandonedNames
	}
	if len(analyser.fileAbandonedNames) == 0 {
		return analyser.fileAbandonedNamesOfParent
	}
	m := make(map[FileId]string, len(analyser.fileAbandonedNames)+len(analyser.fileAbandonedNamesOfParent))
	for k, v := range analyser.fileAbandonedNamesOfParent {
		m[k] = v
	}
	for k, v := range analyser.fileAbandonedNames {
		m[k] = v
	}
	return m
}

// Fork clones this item. Everything is copied by reference except the files
// which are copied by value.
func (analyser *LineHistoryAnalyser) Fork(n int) []core.PipelineItem {
	result := make([]core.PipelineItem, n)

	for i := range result {
		clone := *analyser

		clone.files = make(map[string]*File, len(analyser.files))
		clone.fileNames = make(map[FileId]string, len(analyser.fileNames))
		clone.fileAbandonedNames = nil
		clone.fileAbandonedNamesOfParent = nil
		clone.fileAllocator = clone.fileAllocator.Clone()
		for key, file := range analyser.files {
			clone.files[key] = file.CloneShallowWithUpdaters(clone.fileAllocator, clone.updateChangeList)
			clone.fileNames[file.Id] = key
		}
		clone.changes = append(make([]LineHistoryChange, 0, cap(analyser.changes)), analyser.changes...)

		result[i] = &clone
	}

	for id, name := range analyser.fileNames {
		if file := analyser.files[name]; file == nil {
			analyser.addAbandonedName(id, name)
		} else if file.Id != id {
			panic("Inconsistent file name mapping")
		}
	}

	if abandonedNames := analyser.inheritAbandonedNames(); len(abandonedNames) > 0 {
		for _, item := range result {
			item.(*LineHistoryAnalyser).fileAbandonedNamesOfParent = abandonedNames
		}
	}

	return result
}

// Merge combines several items together. We apply the special file merging logic here.
func (analyser *LineHistoryAnalyser) Merge(items []core.PipelineItem) {
	analyser.onNewTick()

	//clones := make([]*LineHistoryAnalyser, len(items))
	for _, item := range items {
		clone := item.(*LineHistoryAnalyser)

		for name, file := range clone.files {
			if _, ok := analyser.fileNames[file.Id]; !ok {
				analyser.mergeAbandonedName(file.Id, name)
			}
		}

		for id, name := range clone.fileNames {
			if _, ok := analyser.fileNames[id]; !ok {
				analyser.mergeAbandonedName(id, name)
			}
		}
	}

}

// Hibernate compresses the bound RBTree memory with the files.
func (analyser *LineHistoryAnalyser) Hibernate() error {
	analyser.fileAllocator.Hibernate()
	if analyser.HibernationToDisk {
		file, err := ioutil.TempFile(analyser.HibernationDirectory, "*-hercules.bin")
		if err != nil {
			return err
		}
		analyser.hibernatedFileName = file.Name()
		err = file.Close()
		if err != nil {
			analyser.hibernatedFileName = ""
			return err
		}
		err = analyser.fileAllocator.Serialize(analyser.hibernatedFileName)
		if err != nil {
			analyser.hibernatedFileName = ""
			return err
		}
	}
	return nil
}

// Boot decompresses the bound RBTree memory with the files.
func (analyser *LineHistoryAnalyser) Boot() error {
	if analyser.hibernatedFileName != "" {
		err := analyser.fileAllocator.Deserialize(analyser.hibernatedFileName)
		if err != nil {
			return err
		}
		err = os.Remove(analyser.hibernatedFileName)
		if err != nil {
			return err
		}
		analyser.hibernatedFileName = ""
	}
	analyser.fileAllocator.Boot()
	return nil
}

// We do a hack and store the tick in the first 14 bits and the author index in the last 18.
// Strictly speaking, int can be 64-bit and then the author index occupies 32+18 bits.
// This hack is needed to simplify the values storage inside File-s. We can compare
// different values together and they are compared as ticks for the same author.
func packPersonWithTick(author AuthorId, tick TickNumber) int {

	if author > identity.AuthorMissing {
		log.Fatalf("person > AuthorMissing %d \n%s", author, string(debug.Stack()))
	}
	if tick > TreeMergeMark {
		log.Fatalf("tick > TreeMergeMark %d %d\n%s", tick, TreeMergeMark, string(debug.Stack()))
	}

	result := int(tick) & TreeMergeMark
	result |= int(author) << TreeMaxBinPower

	// This effectively means max (16383 - 1) ticks (>44 years) and (262143 - 3) devs.
	// One tick less because TreeMergeMark = ((1 << 14) - 1) is a special tick.
	// Three devs less because:
	// - math.MaxUint32 is the special rbtree value with tick == TreeMergeMark (-1)
	// - identity.AuthorMissing (-2)
	// - authorSelf (-3)
	return result
}

func unpackPersonWithTick(value int) (author AuthorId, tick TickNumber) {
	return AuthorId(value >> TreeMaxBinPower), TickNumber(value & TreeMergeMark)
}

func (analyser *LineHistoryAnalyser) onNewTick() {
	if analyser.tick > analyser.previousTick {
		analyser.previousTick = analyser.tick
	}
}

func (analyser *LineHistoryAnalyser) updateChangeList(f *File, currentTime, previousTime, delta int) {
	prevAuthor, prevTick := unpackPersonWithTick(previousTime)
	newAuthor, curTick := unpackPersonWithTick(currentTime)
	if delta > 0 && newAuthor != prevAuthor {
		analyser.l.Errorf("insertion must have the same author (%d, %d)", prevAuthor, newAuthor)
		return
	}
	analyser.changes = append(analyser.changes, LineHistoryChange{
		FileId:     f.Id,
		CurrTick:   curTick,
		CurrAuthor: newAuthor,
		PrevTick:   prevTick,
		PrevAuthor: prevAuthor,
		Delta:      delta,
	})
}

func (analyser *LineHistoryAnalyser) newFile(
	_ plumbing.Hash, name string, author AuthorId, tick TickNumber, size int) (*File, error) {

	analyser.forgetFileName(name)

	fileId := analyser.fileIdCounter.next()
	analyser.fileNames[fileId] = name
	file := NewFile(fileId, packPersonWithTick(author, tick), size, analyser.fileAllocator, analyser.updateChangeList)
	analyser.files[name] = file

	return file, nil
}

func (analyser *LineHistoryAnalyser) forgetFileName(name string) *File {
	if file := analyser.files[name]; file != nil {
		analyser.addAbandonedName(file.Id, name)
		delete(analyser.files, name)
		return file
	}
	return nil
}

func (analyser *LineHistoryAnalyser) handleInsertion(
	change *object.Change, author AuthorId, cache map[plumbing.Hash]*items.CachedBlob) error {
	blob := cache[change.To.TreeEntry.Hash]

	name := change.To.Name
	analyser.forgetFileName(name)

	lines, err := blob.CountLines()
	if err != nil {
		// binary
		return nil
	}
	file := analyser.files[name]
	if file != nil {
		return fmt.Errorf("file %s already exists", name)
	}

	hash := blob.Hash
	file, err = analyser.newFile(hash, name, author, analyser.tick, lines)
	return err
}

func (analyser *LineHistoryAnalyser) handleDeletion(
	change *object.Change, author AuthorId, cache map[plumbing.Hash]*items.CachedBlob) error {

	var name string
	if change.To.TreeEntry.Hash != plumbing.ZeroHash {
		// became binary
		name = change.To.Name
	} else {
		name = change.From.Name
	}
	file, exists := analyser.files[name]
	blob := cache[change.From.TreeEntry.Hash]
	lines, err := blob.CountLines()
	if exists && err != nil {
		return fmt.Errorf("previous version of %s unexpectedly became binary", name)
	}
	if !exists {
		return nil
	}
	// Parallel independent file removals are incorrectly handled. The solution seems to be quite
	// complex, but feel free to suggest your ideas.
	// These edge cases happen *very* rarely, so we don't bother for now.
	file.Update(packPersonWithTick(author, analyser.tick), 0, 0, lines)
	file.Delete()

	analyser.changes = append(analyser.changes, LineHistoryChange{
		FileId:     file.Id,
		CurrTick:   analyser.tick,
		CurrAuthor: identity.AuthorMissing,
		PrevTick:   analyser.tick,
		PrevAuthor: identity.AuthorMissing,
		Delta:      math.MinInt,
	})
	analyser.forgetFileName(name)
	return nil
}

func (analyser *LineHistoryAnalyser) handleModification(
	change *object.Change, author AuthorId, cache map[plumbing.Hash]*items.CachedBlob,
	diffs map[string]items.FileDiffData) error {

	file, exists := analyser.files[change.From.Name]
	if !exists {
		// this indeed may happen
		return analyser.handleInsertion(change, author, cache)
	}

	// possible rename
	if change.To.Name != change.From.Name {
		err := analyser.handleRename(change.From.Name, change.To.Name)
		if err != nil {
			return err
		}
	}

	// Check for binary changes
	blobFrom := cache[change.From.TreeEntry.Hash]
	_, errFrom := blobFrom.CountLines()
	blobTo := cache[change.To.TreeEntry.Hash]
	_, errTo := blobTo.CountLines()
	if errFrom != errTo {
		if errFrom != nil {
			// the file is no longer binary
			return analyser.handleInsertion(change, author, cache)
		}
		// the file became binary
		// TODO this is wrong
		return analyser.handleDeletion(change, author, cache)
	} else if errFrom != nil {
		// what are we doing here?!
		return nil
	}

	thisDiffs := diffs[change.To.Name]
	if file.Len() != thisDiffs.OldLinesOfCode {
		analyser.l.Infof("====TREE====\n%s", file.Dump())
		return fmt.Errorf("%s: internal integrity error src %d != %d %s -> %s",
			change.To.Name, thisDiffs.OldLinesOfCode, file.Len(),
			change.From.TreeEntry.Hash.String(), change.To.TreeEntry.Hash.String())
	}

	// we do not call RunesToDiffLines so the number of lines equals
	// to the rune count
	position := 0
	pending := diffmatchpatch.Diff{Text: ""}

	apply := func(edit diffmatchpatch.Diff) {
		length := utf8.RuneCountInString(edit.Text)
		if edit.Type == diffmatchpatch.DiffInsert {
			file.Update(packPersonWithTick(author, analyser.tick), position, length, 0)
			position += length
		} else {
			file.Update(packPersonWithTick(author, analyser.tick), position, 0, length)
		}
		if analyser.Debug {
			file.Validate()
		}
	}

	for _, edit := range thisDiffs.Diffs {
		dumpBefore := ""
		if analyser.Debug {
			dumpBefore = file.Dump()
		}
		length := utf8.RuneCountInString(edit.Text)
		debugError := func() {
			analyser.l.Errorf("%s: internal diff error\n", change.To.Name)
			analyser.l.Errorf("Update(%d, %d, %d (0), %d (0))\n", analyser.tick, position,
				length, utf8.RuneCountInString(pending.Text))
			if dumpBefore != "" {
				analyser.l.Errorf("====TREE BEFORE====\n%s====END====\n", dumpBefore)
			}
			analyser.l.Errorf("====TREE AFTER====\n%s====END====\n", file.Dump())
		}
		switch edit.Type {
		case diffmatchpatch.DiffEqual:
			if pending.Text != "" {
				apply(pending)
				pending.Text = ""
			}
			position += length
		case diffmatchpatch.DiffInsert:
			if pending.Text != "" {
				if pending.Type == diffmatchpatch.DiffInsert {
					debugError()
					return errors.New("DiffInsert may not appear after DiffInsert")
				}
				file.Update(packPersonWithTick(author, analyser.tick), position, length,
					utf8.RuneCountInString(pending.Text))
				if analyser.Debug {
					file.Validate()
				}
				position += length
				pending.Text = ""
			} else {
				pending = edit
			}
		case diffmatchpatch.DiffDelete:
			if pending.Text != "" {
				debugError()
				return errors.New("DiffDelete may not appear after DiffInsert/DiffDelete")
			}
			pending = edit
		default:
			debugError()
			return fmt.Errorf("diff operation is not supported: %d", edit.Type)
		}
	}
	if pending.Text != "" {
		apply(pending)
		pending.Text = ""
	}
	if file.Len() != thisDiffs.NewLinesOfCode {
		return fmt.Errorf("%s: internal integrity error dst %d != %d %s -> %s",
			change.To.Name, thisDiffs.NewLinesOfCode, file.Len(),
			change.From.TreeEntry.Hash.String(), change.To.TreeEntry.Hash.String())
	}
	return nil
}

func (analyser *LineHistoryAnalyser) handleRename(from, to string) error {
	if from == to {
		return nil
	}
	file, exists := analyser.files[from]
	if !exists {
		return fmt.Errorf("file %s > %s does not exist (files)", from, to)
	}
	delete(analyser.files, from)
	analyser.forgetFileName(to)
	analyser.fileNames[file.Id] = to
	analyser.files[to] = file

	return nil
}

func init() {
	core.Registry.Register(NewLineHistoryAnalyser())
}
