package hercules

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"unicode/utf8"

	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
)

// BurndownAnalyser allows to gather the line burndown statistics for a Git repository.
type BurndownAnalysis struct {
	// Granularity sets the size of each band - the number of days it spans.
	// Smaller values provide better resolution but require more work and eat more
	// memory. 30 days is usually enough.
	Granularity int
	// Sampling sets how detailed is the statistic - the size of the interval in
	// days between consecutive measurements. It is usually a good idea to set it
	// <= Granularity. Try 15 or 30.
	Sampling int

	// The number of developers for which to collect the burndown stats. 0 disables it.
	PeopleNumber int

	// Debug activates the debugging mode. Analyse() runs slower in this mode
	// but it accurately checks all the intermediate states for invariant
	// violations.
	Debug bool

	// Repository points to the analysed Git repository struct from go-git.
	repository *git.Repository
	// globalStatus is the current daily alive number of lines; key is the number
	// of days from the beginning of the history.
	globalStatus map[int]int64
	// globalHistory is the weekly snapshots of globalStatus.
	globalHistory [][]int64
	// fileHistories is the weekly snapshots of each file's status.
	fileHistories map[string][][]int64
	// peopleHistories is the weekly snapshots of each person's status.
	peopleHistories [][][]int64
	// fiales is the mapping <file path> -> hercules.File.
	files map[string]*File
	// matrix is the mutual deletions and self insertions.
	matrix []map[int]int64
	// people is the people's individual time stats.
	people []map[int]int64
	// day is the most recent day index processed.
	day int
	// previousDay is the day from the previous sample period -
	// different from DaysSinceStart.previousDay.
	previousDay int
}

type BurndownResult struct {
	GlobalHistory   [][]int64
	FileHistories   map[string][][]int64
	PeopleHistories [][][]int64
	PeopleMatrix    [][]int64
}

func (analyser *BurndownAnalysis) Name() string {
	return "Burndown"
}

func (analyser *BurndownAnalysis) Provides() []string {
	return []string{}
}

func (analyser *BurndownAnalysis) Requires() []string {
	arr := [...]string{"renamed_changes", "blob_cache", "day", "author"}
	return arr[:]
}

func (analyser *BurndownAnalysis) Initialize(repository *git.Repository) {
	analyser.repository = repository
	analyser.globalStatus = map[int]int64{}
	analyser.globalHistory = [][]int64{}
	analyser.fileHistories = map[string][][]int64{}
	analyser.peopleHistories = make([][][]int64, analyser.PeopleNumber)
	analyser.files = map[string]*File{}
	analyser.matrix = make([]map[int]int64, analyser.PeopleNumber)
	analyser.people = make([]map[int]int64, analyser.PeopleNumber)
	analyser.day = 0
	analyser.previousDay = 0
}

func (analyser *BurndownAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	sampling := analyser.Sampling
	if sampling == 0 {
		sampling = 1
	}
	author := deps["author"].(int)
	analyser.day = deps["day"].(int)
	delta := (analyser.day / sampling) - (analyser.previousDay / sampling)
	if delta > 0 {
		analyser.previousDay = analyser.day
		gs, fss, pss := analyser.groupStatus()
		analyser.updateHistories(gs, fss, pss, delta)
	}
	cache := deps["blob_cache"].(map[plumbing.Hash]*object.Blob)
	tree_diff := deps["renamed_changes"].(object.Changes)
	for _, change := range tree_diff {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		switch action {
		case merkletrie.Insert:
			err = analyser.handleInsertion(change, author, cache)
		case merkletrie.Delete:
			err = analyser.handleDeletion(change, author, cache)
		case merkletrie.Modify:
			err = analyser.handleModification(change, author, cache)
		}
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// Finalize() returns the list of snapshots of the cumulative line edit times
// and the similar lists for every file which is alive in HEAD.
// The number of snapshots (the first dimension >[]<[]int64) depends on
// Analyser.Sampling (the more Sampling, the less the value); the length of
// each snapshot depends on Analyser.Granularity (the more Granularity,
// the less the value).
func (analyser *BurndownAnalysis) Finalize() interface{} {
	gs, fss, pss := analyser.groupStatus()
	analyser.updateHistories(gs, fss, pss, 1)
	for key, statuses := range analyser.fileHistories {
		if len(statuses) == len(analyser.globalHistory) {
			continue
		}
		padding := make([][]int64, len(analyser.globalHistory)-len(statuses))
		for i := range padding {
			padding[i] = make([]int64, len(analyser.globalStatus))
		}
		analyser.fileHistories[key] = append(padding, statuses...)
	}
	peopleMatrix := make([][]int64, analyser.PeopleNumber)
	for i, row := range analyser.matrix {
		mrow := make([]int64, analyser.PeopleNumber+2)
		peopleMatrix[i] = mrow
		for key, val := range row {
			if key == MISSING_AUTHOR {
				key = -1
			} else if key == SELF_AUTHOR {
				key = -2
			}
			mrow[key+2] = val
		}
	}
	return BurndownResult{
		GlobalHistory:   analyser.globalHistory,
		FileHistories:   analyser.fileHistories,
		PeopleHistories: analyser.peopleHistories,
		PeopleMatrix:    peopleMatrix}
}

func checkClose(c io.Closer) {
	if err := c.Close(); err != nil {
		panic(err)
	}
}

func countLines(file *object.Blob) (int, error) {
	reader, err := file.Reader()
	if err != nil {
		return 0, err
	}
	defer checkClose(reader)
	var scanner *bufio.Scanner
	buffer := make([]byte, bufio.MaxScanTokenSize)
	counter := 0
	for scanner == nil || scanner.Err() == bufio.ErrTooLong {
		if scanner != nil && !utf8.Valid(scanner.Bytes()) {
			return -1, errors.New("binary")
		}
		scanner = bufio.NewScanner(reader)
		scanner.Buffer(buffer, 0)
		for scanner.Scan() {
			if !utf8.Valid(scanner.Bytes()) {
				return -1, errors.New("binary")
			}
			counter++
		}
	}
	return counter, nil
}

func blobToString(file *object.Blob) (string, error) {
	reader, err := file.Reader()
	if err != nil {
		return "", err
	}
	defer checkClose(reader)
	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	return buf.String(), nil
}

func (analyser *BurndownAnalysis) packPersonWithDay(person int, day int) int {
	if analyser.PeopleNumber == 0 {
		return day
	}
	result := day
	result |= person << 14
	// This effectively means max 16384 days (>44 years) and (131072 - 2) devs
	return result
}

func (analyser *BurndownAnalysis) unpackPersonWithDay(value int) (int, int) {
	if analyser.PeopleNumber == 0 {
		return MISSING_AUTHOR, value
	}
	return value >> 14, value & 0x3FFF
}

func (analyser *BurndownAnalysis) updateStatus(
	status interface{}, _ int, previous_time_ int, delta int) {

	_, previous_time := analyser.unpackPersonWithDay(previous_time_)
	status.(map[int]int64)[previous_time] += int64(delta)
}

func (analyser *BurndownAnalysis) updatePeople(people interface{}, _ int, previous_time_ int, delta int) {
	old_author, previous_time := analyser.unpackPersonWithDay(previous_time_)
	if old_author == MISSING_AUTHOR {
		return
	}
	casted := people.([]map[int]int64)
	stats := casted[old_author]
	if stats == nil {
		stats = map[int]int64{}
		casted[old_author] = stats
	}
	stats[previous_time] += int64(delta)
}

func (analyser *BurndownAnalysis) updateMatrix(
	matrix_ interface{}, current_time int, previous_time int, delta int) {

	matrix := matrix_.([]map[int]int64)
	new_author, _ := analyser.unpackPersonWithDay(current_time)
	old_author, _ := analyser.unpackPersonWithDay(previous_time)
	if old_author == MISSING_AUTHOR {
		return
	}
	if new_author == old_author && delta > 0 {
		new_author = SELF_AUTHOR
	}
	row := matrix[old_author]
	if row == nil {
		row = map[int]int64{}
		matrix[old_author] = row
	}
	cell, exists := row[new_author]
	if !exists {
		row[new_author] = 0
		cell = 0
	}
	row[new_author] = cell + int64(delta)
}

func (analyser *BurndownAnalysis) newFile(
	author int, day int, size int, global map[int]int64, people []map[int]int64,
	matrix []map[int]int64) *File {
	if analyser.PeopleNumber == 0 {
		return NewFile(day, size, NewStatus(global, analyser.updateStatus),
			NewStatus(make(map[int]int64), analyser.updateStatus))
	}
	return NewFile(analyser.packPersonWithDay(author, day), size,
		NewStatus(global, analyser.updateStatus),
		NewStatus(make(map[int]int64), analyser.updateStatus),
		NewStatus(people, analyser.updatePeople),
		NewStatus(matrix, analyser.updateMatrix))
}

func (analyser *BurndownAnalysis) handleInsertion(
	change *object.Change, author int, cache map[plumbing.Hash]*object.Blob) error {
	blob := cache[change.To.TreeEntry.Hash]
	lines, err := countLines(blob)
	if err != nil {
		if err.Error() == "binary" {
			return nil
		}
		return err
	}
	name := change.To.Name
	file, exists := analyser.files[name]
	if exists {
		return errors.New(fmt.Sprintf("file %s already exists", name))
	}
	file = analyser.newFile(
		author, analyser.day, lines, analyser.globalStatus, analyser.people, analyser.matrix)
	analyser.files[name] = file
	return nil
}

func (analyser *BurndownAnalysis) handleDeletion(
	change *object.Change, author int, cache map[plumbing.Hash]*object.Blob) error {

	blob := cache[change.From.TreeEntry.Hash]
	lines, err := countLines(blob)
	if err != nil {
		if err.Error() == "binary" {
			return nil
		}
		return err
	}
	name := change.From.Name
	file := analyser.files[name]
	file.Update(analyser.packPersonWithDay(author, analyser.day), 0, 0, lines)
	delete(analyser.files, name)
	return nil
}

func (analyser *BurndownAnalysis) handleModification(
	change *object.Change, author int, cache map[plumbing.Hash]*object.Blob) error {

	blob_from := cache[change.From.TreeEntry.Hash]
	blob_to := cache[change.To.TreeEntry.Hash]
	// we are not validating UTF-8 here because for example
	// git/git 4f7770c87ce3c302e1639a7737a6d2531fe4b160 fetch-pack.c is invalid UTF-8
	str_from, err := blobToString(blob_from)
	if err != nil {
		return err
	}
	str_to, err := blobToString(blob_to)
	if err != nil {
		return err
	}
	file, exists := analyser.files[change.From.Name]
	if !exists {
		return analyser.handleInsertion(change, author, cache)
	}
	// possible rename
	if change.To.Name != change.From.Name {
		err = analyser.handleRename(change.From.Name, change.To.Name)
		if err != nil {
			return err
		}
	}
	dmp := diffmatchpatch.New()
	src, dst, _ := dmp.DiffLinesToRunes(str_from, str_to)
	if file.Len() != len(src) {
		fmt.Fprintf(os.Stderr, "====TREE====\n%s", file.Dump())
		return errors.New(fmt.Sprintf("%s: internal integrity error src %d != %d %s -> %s",
			change.To.Name, len(src), file.Len(),
			change.From.TreeEntry.Hash.String(), change.To.TreeEntry.Hash.String()))
	}
	diffs := dmp.DiffMainRunes(src, dst, false)
	// we do not call RunesToDiffLines so the number of lines equals
	// to the rune count
	position := 0
	pending := diffmatchpatch.Diff{Text: ""}

	apply := func(edit diffmatchpatch.Diff) {
		length := utf8.RuneCountInString(edit.Text)
		if edit.Type == diffmatchpatch.DiffInsert {
			file.Update(analyser.packPersonWithDay(author, analyser.day), position, length, 0)
			position += length
		} else {
			file.Update(analyser.packPersonWithDay(author, analyser.day), position, 0, length)
		}
		if analyser.Debug {
			file.Validate()
		}
	}

	for _, edit := range diffs {
		dump_before := ""
		if analyser.Debug {
			dump_before = file.Dump()
		}
		length := utf8.RuneCountInString(edit.Text)
		debug_error := func() {
			fmt.Fprintf(os.Stderr, "%s: internal diff error\n", change.To.Name)
			fmt.Fprintf(os.Stderr, "Update(%d, %d, %d (0), %d (0))\n", analyser.day, position,
				length, utf8.RuneCountInString(pending.Text))
			if dump_before != "" {
				fmt.Fprintf(os.Stderr, "====TREE BEFORE====\n%s====END====\n", dump_before)
			}
			fmt.Fprintf(os.Stderr, "====TREE AFTER====\n%s====END====\n", file.Dump())
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
					debug_error()
					return errors.New("DiffInsert may not appear after DiffInsert")
				}
				file.Update(analyser.packPersonWithDay(author, analyser.day), position, length,
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
				debug_error()
				return errors.New("DiffDelete may not appear after DiffInsert/DiffDelete")
			}
			pending = edit
		default:
			debug_error()
			return errors.New(fmt.Sprintf("diff operation is not supported: %d", edit.Type))
		}
	}
	if pending.Text != "" {
		apply(pending)
		pending.Text = ""
	}
	if file.Len() != len(dst) {
		return errors.New(fmt.Sprintf("%s: internal integrity error dst %d != %d",
			change.To.Name, len(dst), file.Len()))
	}
	return nil
}

func (analyser *BurndownAnalysis) handleRename(from, to string) error {
	file, exists := analyser.files[from]
	if !exists {
		return errors.New(fmt.Sprintf("file %s does not exist", from))
	}
	analyser.files[to] = file
	delete(analyser.files, from)
	return nil
}

func (analyser *BurndownAnalysis) groupStatus() ([]int64, map[string][]int64, [][]int64) {
	granularity := analyser.Granularity
	if granularity == 0 {
		granularity = 1
	}
	day := analyser.day
	day++
	adjust := 0
	if day%granularity != 0 {
		adjust = 1
	}
	global := make([]int64, day/granularity+adjust)
	var group int64
	for i := 0; i < day; i++ {
		group += analyser.globalStatus[i]
		if (i % granularity) == (granularity - 1) {
			global[i/granularity] = group
			group = 0
		}
	}
	if day%granularity != 0 {
		global[len(global)-1] = group
	}
	locals := make(map[string][]int64)
	for key, file := range analyser.files {
		status := make([]int64, day/granularity+adjust)
		var group int64
		for i := 0; i < day; i++ {
			group += file.Status(1).(map[int]int64)[i]
			if (i % granularity) == (granularity - 1) {
				status[i/granularity] = group
				group = 0
			}
		}
		if day%granularity != 0 {
			status[len(status)-1] = group
		}
		locals[key] = status
	}
	peoples := make([][]int64, len(analyser.people))
	for key, person := range analyser.people {
		status := make([]int64, day/granularity+adjust)
		var group int64
		for i := 0; i < day; i++ {
			group += person[i]
			if (i % granularity) == (granularity - 1) {
				status[i/granularity] = group
				group = 0
			}
		}
		if day%granularity != 0 {
			status[len(status)-1] = group
		}
		peoples[key] = status
	}
	return global, locals, peoples
}

func (analyser *BurndownAnalysis) updateHistories(
	globalStatus []int64, file_statuses map[string][]int64, people_statuses [][]int64, delta int) {
	for i := 0; i < delta; i++ {
		analyser.globalHistory = append(analyser.globalHistory, globalStatus)
	}
	to_delete := make([]string, 0)
	for key, fh := range analyser.fileHistories {
		ls, exists := file_statuses[key]
		if !exists {
			to_delete = append(to_delete, key)
		} else {
			for i := 0; i < delta; i++ {
				fh = append(fh, ls)
			}
			analyser.fileHistories[key] = fh
		}
	}
	for _, key := range to_delete {
		delete(analyser.fileHistories, key)
	}
	for key, ls := range file_statuses {
		fh, exists := analyser.fileHistories[key]
		if exists {
			continue
		}
		for i := 0; i < delta; i++ {
			fh = append(fh, ls)
		}
		analyser.fileHistories[key] = fh
	}

	for key, ph := range analyser.peopleHistories {
		ls := people_statuses[key]
		for i := 0; i < delta; i++ {
			ph = append(ph, ls)
		}
		analyser.peopleHistories[key] = ph
	}
}
