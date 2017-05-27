package hercules

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
)

type Analyser struct {
	Repository          *git.Repository
	Granularity         int
	Sampling            int
	SimilarityThreshold int
	Debug               bool
	OnProgress          func(int, int)
}

func checkClose(c io.Closer) {
	if err := c.Close(); err != nil {
		panic(err)
	}
}

func loc(file *object.Blob) (int, error) {
	reader, err := file.Reader()
	if err != nil {
		panic(err)
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

func str(file *object.Blob) string {
	reader, err := file.Reader()
	if err != nil {
		panic(err)
	}
	defer checkClose(reader)
	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	return buf.String()
}

type DummyIO struct {
}

func (DummyIO) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (DummyIO) Write(p []byte) (int, error) {
	return len(p), nil
}

func (DummyIO) Close() error {
	return nil
}

type DummyEncodedObject struct {
	FakeHash plumbing.Hash
}

func (obj DummyEncodedObject) Hash() plumbing.Hash {
	return obj.FakeHash
}

func (obj DummyEncodedObject) Type() plumbing.ObjectType {
	return plumbing.BlobObject
}

func (obj DummyEncodedObject) SetType(plumbing.ObjectType) {
}

func (obj DummyEncodedObject) Size() int64 {
	return 0
}

func (obj DummyEncodedObject) SetSize(int64) {
}

func (obj DummyEncodedObject) Reader() (io.ReadCloser, error) {
	return DummyIO{}, nil
}

func (obj DummyEncodedObject) Writer() (io.WriteCloser, error) {
	return DummyIO{}, nil
}

func createDummyBlob(hash *plumbing.Hash) (*object.Blob, error) {
	return object.DecodeBlob(DummyEncodedObject{*hash})
}

func (analyser *Analyser) handleInsertion(
	change *object.Change, day int, status map[int]int64, files map[string]*File,
	cache *map[plumbing.Hash]*object.Blob) {
	blob := (*cache)[change.To.TreeEntry.Hash]
	lines, err := loc(blob)
	if err != nil {
		return
	}
	name := change.To.Name
	file, exists := files[name]
	if exists {
		panic(fmt.Sprintf("file %s already exists", name))
	}
	file = NewFile(day, lines, status)
	files[name] = file
}

func (analyser *Analyser) handleDeletion(
	change *object.Change, day int, status map[int]int64, files map[string]*File,
	cache *map[plumbing.Hash]*object.Blob) {
	blob := (*cache)[change.From.TreeEntry.Hash]
	lines, err := loc(blob)
	if err != nil {
		return
	}
	name := change.From.Name
	file := files[name]
	file.Update(day, 0, 0, lines)
	delete(files, name)
}

func (analyser *Analyser) handleModification(
	change *object.Change, day int, status map[int]int64, files map[string]*File,
	cache *map[plumbing.Hash]*object.Blob) {
	blob_from := (*cache)[change.From.TreeEntry.Hash]
	blob_to := (*cache)[change.To.TreeEntry.Hash]
	// we are not validating UTF-8 here because for example
	// git/git 4f7770c87ce3c302e1639a7737a6d2531fe4b160 fetch-pack.c is invalid UTF-8
	str_from := str(blob_from)
	str_to := str(blob_to)
	file, exists := files[change.From.Name]
	if !exists {
		analyser.handleInsertion(change, day, status, files, cache)
		return
	}
	// possible rename
	if change.To.Name != change.From.Name {
		analyser.handleRename(change.From.Name, change.To.Name, files)
	}
	dmp := diffmatchpatch.New()
	src, dst, _ := dmp.DiffLinesToRunes(str_from, str_to)
	if file.Len() != len(src) {
		fmt.Fprintf(os.Stderr, "====TREE====\n%s", file.Dump())
		panic(fmt.Sprintf("%s: internal integrity error src %d != %d %s -> %s",
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
			file.Update(day, position, length, 0)
			position += length
		} else {
			file.Update(day, position, 0, length)
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
		func() {
			defer func() {
				r := recover()
				if r != nil {
					fmt.Fprintf(os.Stderr, "%s: internal diff error\n", change.To.Name)
					fmt.Fprintf(os.Stderr, "Update(%d, %d, %d (0), %d (0))\n", day, position,
						length, utf8.RuneCountInString(pending.Text))
					if dump_before != "" {
						fmt.Fprintf(os.Stderr, "====TREE BEFORE====\n%s====END====\n", dump_before)
					}
					fmt.Fprintf(os.Stderr, "====TREE AFTER====\n%s====END====\n", file.Dump())
					panic(r)
				}
			}()
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
						panic("DiffInsert may not appear after DiffInsert")
					}
					file.Update(day, position, length, utf8.RuneCountInString(pending.Text))
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
					panic("DiffDelete may not appear after DiffInsert/DiffDelete")
				}
				pending = edit
			default:
				panic(fmt.Sprintf("diff operation is not supported: %d", edit.Type))
			}
		}()
	}
	if pending.Text != "" {
		apply(pending)
		pending.Text = ""
	}
	if file.Len() != len(dst) {
		panic(fmt.Sprintf("%s: internal integrity error dst %d != %d",
			change.To.Name, len(dst), file.Len()))
	}
}

func (analyser *Analyser) handleRename(from, to string, files map[string]*File) {
	file, exists := files[from]
	if !exists {
		panic(fmt.Sprintf("file %s does not exist", from))
	}
	files[to] = file
	delete(files, from)
}

func (analyser *Analyser) Commits() []*object.Commit {
	result := []*object.Commit{}
	repository := analyser.Repository
	head, err := repository.Head()
	if err != nil {
		panic(err)
	}
	commit, err := repository.CommitObject(head.Hash())
	if err != nil {
		panic(err)
	}
	result = append(result, commit)
	for ; err != io.EOF; commit, err = commit.Parents().Next() {
		if err != nil {
			panic(err)
		}
		result = append(result, commit)
	}
	// reverse the order
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

func (analyser *Analyser) groupStatus(status map[int]int64, day int) []int64 {
	granularity := analyser.Granularity
	if granularity == 0 {
		granularity = 1
	}
	day++
	adjust := 0
	if day%granularity < granularity-1 {
		adjust = 1
	}
	result := make([]int64, day/granularity+adjust)
	var group int64
	for i := 0; i < day; i++ {
		group += status[i]
		if i%granularity == (granularity - 1) {
			result[i/granularity] = group
			group = 0
		}
	}
	if day%granularity < granularity-1 {
		result[len(result)-1] = group
	}
	return result
}

type sortableChange struct {
	change *object.Change
	hash   plumbing.Hash
}

type sortableChanges []sortableChange

func (change *sortableChange) Less(other *sortableChange) bool {
	for x := 0; x < 20; x++ {
		if change.hash[x] < other.hash[x] {
			return true
		}
	}
	return false
}

func (slice sortableChanges) Len() int {
	return len(slice)
}

func (slice sortableChanges) Less(i, j int) bool {
	return slice[i].Less(&slice[j])
}

func (slice sortableChanges) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

type sortableBlob struct {
	change *object.Change
	size   int64
}

type sortableBlobs []sortableBlob

func (change *sortableBlob) Less(other *sortableBlob) bool {
	return change.size < other.size
}

func (slice sortableBlobs) Len() int {
	return len(slice)
}

func (slice sortableBlobs) Less(i, j int) bool {
	return slice[i].Less(&slice[j])
}

func (slice sortableBlobs) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func (analyser *Analyser) sizesAreClose(size1 int64, size2 int64) bool {
	return abs64(size1-size2)*100/max64(1, min64(size1, size2)) <=
		int64(100-analyser.SimilarityThreshold)
}

func (analyser *Analyser) blobsAreClose(
	blob1 *object.Blob, blob2 *object.Blob) bool {
	str_from := str(blob1)
	str_to := str(blob2)
	dmp := diffmatchpatch.New()
	src, dst, _ := dmp.DiffLinesToRunes(str_from, str_to)
	diffs := dmp.DiffMainRunes(src, dst, false)
	common := 0
	for _, edit := range diffs {
		if edit.Type == diffmatchpatch.DiffEqual {
			common += utf8.RuneCountInString(edit.Text)
		}
	}
	return common*100/max(1, min(len(src), len(dst))) >=
		analyser.SimilarityThreshold
}

func (analyser *Analyser) getBlob(entry *object.ChangeEntry, commit *object.Commit) (
	*object.Blob, error) {
	blob, err := analyser.Repository.BlobObject(entry.TreeEntry.Hash)
	if err != nil {
		if err.Error() != git.ErrObjectNotFound.Error() {
			fmt.Fprintf(os.Stderr, "getBlob(%s)\n", entry.TreeEntry.Hash.String())
			return nil, err
		}
		file, err_modules := commit.File(".gitmodules")
		if err_modules != nil {
			return nil, err
		}
		contents, err_modules := file.Contents()
		if err_modules != nil {
			return nil, err
		}
		modules := config.NewModules()
		err_modules = modules.Unmarshal([]byte(contents))
		if err_modules != nil {
			return nil, err
		}
		_, exists := modules.Submodules[entry.Name]
		if exists {
			// we found that this is a submodule
			return createDummyBlob(&entry.TreeEntry.Hash)
		}
		return nil, err
	}
	return blob, nil
}

func (analyser *Analyser) cacheBlobs(changes *object.Changes, commit *object.Commit) (
	*map[plumbing.Hash]*object.Blob, error) {
	cache := make(map[plumbing.Hash]*object.Blob)
	for _, change := range *changes {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		switch action {
		case merkletrie.Insert:
			cache[change.To.TreeEntry.Hash], err = analyser.getBlob(&change.To, commit)
			if err != nil {
				fmt.Fprintf(os.Stderr, "file to %s\n", change.To.Name)
			}
		case merkletrie.Delete:
			cache[change.From.TreeEntry.Hash], err = analyser.getBlob(&change.From, commit)
			if err != nil {
				if err.Error() != git.ErrObjectNotFound.Error() {
					fmt.Fprintf(os.Stderr, "file from %s\n", change.From.Name)
				} else {
					cache[change.From.TreeEntry.Hash], err = createDummyBlob(
						&change.From.TreeEntry.Hash)
				}
			}
		case merkletrie.Modify:
			cache[change.To.TreeEntry.Hash], err = analyser.getBlob(&change.To, commit)
			if err != nil {
				fmt.Fprintf(os.Stderr, "file to %s\n", change.To.Name)
			}
			cache[change.From.TreeEntry.Hash], err = analyser.getBlob(&change.From, commit)
			if err != nil {
				fmt.Fprintf(os.Stderr, "file from %s\n", change.From.Name)
			}
		default:
			panic(fmt.Sprintf("unsupported action: %d", change.Action))
		}
		if err != nil {
			return nil, err
		}
	}
	return &cache, nil
}

func (analyser *Analyser) detectRenames(
	changes *object.Changes, cache *map[plumbing.Hash]*object.Blob) object.Changes {
	reduced_changes := make(object.Changes, 0, changes.Len())

	// Stage 1 - find renames by matching the hashes
	// n log(n)
	// We sort additions and deletions by hash and then do the single scan along
	// both slices.
	deleted := make(sortableChanges, 0, changes.Len())
	added := make(sortableChanges, 0, changes.Len())
	for _, change := range *changes {
		action, err := change.Action()
		if err != nil {
			panic(err)
		}
		switch action {
		case merkletrie.Insert:
			added = append(added, sortableChange{change, change.To.TreeEntry.Hash})
		case merkletrie.Delete:
			deleted = append(deleted, sortableChange{change, change.From.TreeEntry.Hash})
		case merkletrie.Modify:
			reduced_changes = append(reduced_changes, change)
		default:
			panic(fmt.Sprintf("unsupported action: %d", change.Action))
		}
	}
	sort.Sort(deleted)
	sort.Sort(added)
	a := 0
	d := 0
	still_deleted := make(object.Changes, 0, deleted.Len())
	still_added := make(object.Changes, 0, added.Len())
	for a < added.Len() && d < deleted.Len() {
		if added[a].hash == deleted[d].hash {
			reduced_changes = append(
				reduced_changes,
				&object.Change{From: deleted[d].change.From, To: added[a].change.To})
			a++
			d++
		} else if added[a].Less(&deleted[d]) {
			still_added = append(still_added, added[a].change)
			a++
		} else {
			still_deleted = append(still_deleted, deleted[d].change)
			d++
		}
	}
	for ; a < added.Len(); a++ {
		still_added = append(still_added, added[a].change)
	}
	for ; d < deleted.Len(); d++ {
		still_deleted = append(still_deleted, deleted[d].change)
	}

	// Stage 2 - apply the similarity threshold
	// n^2 but actually linear
	// We sort the blobs by size and do the single linear scan.
	added_blobs := make(sortableBlobs, 0, still_added.Len())
	deleted_blobs := make(sortableBlobs, 0, still_deleted.Len())
	for _, change := range still_added {
		blob := (*cache)[change.To.TreeEntry.Hash]
		added_blobs = append(
			added_blobs, sortableBlob{change: change, size: blob.Size})
	}
	for _, change := range still_deleted {
		blob := (*cache)[change.From.TreeEntry.Hash]
		deleted_blobs = append(
			deleted_blobs, sortableBlob{change: change, size: blob.Size})
	}
	sort.Sort(added_blobs)
	sort.Sort(deleted_blobs)
	d_start := 0
	for a = 0; a < added_blobs.Len(); a++ {
		my_blob := (*cache)[added_blobs[a].change.To.TreeEntry.Hash]
		my_size := added_blobs[a].size
		for d = d_start; d < deleted_blobs.Len() && !analyser.sizesAreClose(my_size, deleted_blobs[d].size); d++ {
		}
		d_start = d
		found_match := false
		for d = d_start; d < deleted_blobs.Len() && analyser.sizesAreClose(my_size, deleted_blobs[d].size); d++ {
			if analyser.blobsAreClose(
				my_blob, (*cache)[deleted_blobs[d].change.From.TreeEntry.Hash]) {
				found_match = true
				reduced_changes = append(
					reduced_changes,
					&object.Change{From: deleted_blobs[d].change.From,
						To: added_blobs[a].change.To})
				break
			}
		}
		if found_match {
			added_blobs = append(added_blobs[:a], added_blobs[a+1:]...)
			a--
			deleted_blobs = append(deleted_blobs[:d], deleted_blobs[d+1:]...)
		}
	}

	// Stage 3 - we give up, everything left are independent additions and deletions
	for _, blob := range added_blobs {
		reduced_changes = append(reduced_changes, blob.change)
	}
	for _, blob := range deleted_blobs {
		reduced_changes = append(reduced_changes, blob.change)
	}
	return reduced_changes
}

func (analyser *Analyser) Analyse(commits []*object.Commit) [][]int64 {
	sampling := analyser.Sampling
	if sampling == 0 {
		sampling = 1
	}
	onProgress := analyser.OnProgress
	if onProgress == nil {
		onProgress = func(int, int) {}
	}
	if analyser.SimilarityThreshold < 0 || analyser.SimilarityThreshold > 100 {
		panic("hercules.Analyser: an invalid SimilarityThreshold was specified")
	}

	// current daily alive number of lines; key is the number of days from the
	// beginning of the history
	status := map[int]int64{}
	// weekly snapshots of status
	statuses := [][]int64{}
	// mapping <file path> -> hercules.File
	files := map[string]*File{}

	var day0 time.Time // will be initialized in the first iteration
	var prev_tree *object.Tree = nil
	prev_day := 0

	for index, commit := range commits {
		onProgress(index, len(commits))
		tree, err := commit.Tree()
		if err != nil {
			panic(err)
		}
		if index == 0 {
			// first iteration - initialize the file objects from the tree
			day0 = commit.Author.When
			func() {
				file_iter := tree.Files()
				defer file_iter.Close()
				for {
					file, err := file_iter.Next()
					if err != nil {
						if err == io.EOF {
							break
						}
						panic(err)
					}
					lines, err := loc(&file.Blob)
					if err == nil {
						files[file.Name] = NewFile(0, lines, status)
					}
				}
			}()
		} else {
			day := int(commit.Author.When.Sub(day0).Hours() / 24)
			delta := (day / sampling) - (prev_day / sampling)
			if delta > 0 {
				prev_day = day
				gs := analyser.groupStatus(status, day)
				for i := 0; i < delta; i++ {
					statuses = append(statuses, gs)
				}
			}
			tree_diff, err := object.DiffTree(prev_tree, tree)
			if err != nil {
				fmt.Fprintf(os.Stderr, "commit #%d %s\n", index, commit.Hash.String())
				panic(err)
			}
			cache, err := analyser.cacheBlobs(&tree_diff, commit)
			if err != nil {
				fmt.Fprintf(os.Stderr, "commit #%d %s\n", index, commit.Hash.String())
				panic(err)
			}
			tree_diff = analyser.detectRenames(&tree_diff, cache)
			for _, change := range tree_diff {
				action, err := change.Action()
				if err != nil {
					fmt.Fprintf(os.Stderr, "commit #%d %s\n", index, commit.Hash.String())
					panic(err)
				}
				switch action {
				case merkletrie.Insert:
					analyser.handleInsertion(change, day, status, files, cache)
				case merkletrie.Delete:
					analyser.handleDeletion(change, day, status, files, cache)
				case merkletrie.Modify:
					func() {
						defer func() {
							r := recover()
							if r != nil {
								fmt.Fprintf(os.Stderr, "#%d - %s: modification error\n",
									index, commit.Hash.String())
								panic(r)
							}
						}()
						analyser.handleModification(change, day, status, files, cache)
					}()
				}
			}
		}
		prev_tree = tree
	}
	return statuses
}
