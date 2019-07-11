package leaves

import (
	"fmt"
	"io"
	"sort"

	"github.com/gogo/protobuf/proto"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	items "gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v10/internal/yaml"
)

// CouplesAnalysis calculates the number of common commits for files and authors.
// The results are matrices, where cell at row X and column Y is the number of commits which
// changed X and Y together. In case with people, the numbers are summed for every common file.
type CouplesAnalysis struct {
	core.NoopMerger
	core.OneShotMergeProcessor
	// PeopleNumber is the number of developers for which to build the matrix. 0 disables this analysis.
	PeopleNumber int

	// people store how many times every developer committed to every file.
	people []map[string]int
	// peopleCommits is the number of commits each author made.
	peopleCommits []int
	// files store every file occurred in the same commit with every other file.
	files map[string]map[string]int
	// renames point from new file name to old file name.
	renames *[]rename
	// lastCommit is the last commit which was consumed.
	lastCommit *object.Commit
	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string

	l core.Logger
}

// CouplesResult is returned by CouplesAnalysis.Finalize() and carries couples matrices from
// authors and files.
type CouplesResult struct {
	// PeopleMatrix is how many times developers changed files which were also changed by other developers.
	// The mapping's key is the other developer, and the value is the sum over all the files both developers changed.
	// Each element of that sum is min(C1, C2) where Ci is the number of commits developer i made which touched the file.
	PeopleMatrix []map[int]int64
	// PeopleFiles is how many times developers changed files. The first dimension (left []) is developers,
	// and the second dimension (right []) is file indexes.
	PeopleFiles [][]int
	// FilesMatrix is how many times file pairs occurred in the same commit.
	FilesMatrix []map[int]int64
	// FilesLines is the number of lines contained in each file from the last analyzed commit.
	FilesLines []int
	// Files is the names of the files. The order matches PeopleFiles' indexes and FilesMatrix.
	Files []string

	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
}

const (
	// CouplesMaximumMeaningfulContextSize is the threshold on the number of files in a commit to
	// consider them as grouped together.
	CouplesMaximumMeaningfulContextSize = 1000
)

type rename struct {
	FromName string
	ToName   string
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (couples *CouplesAnalysis) Name() string {
	return "Couples"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (couples *CouplesAnalysis) Provides() []string {
	return []string{}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (couples *CouplesAnalysis) Requires() []string {
	return []string{identity.DependencyAuthor, items.DependencyTreeChanges}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (couples *CouplesAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (couples *CouplesAnalysis) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		couples.l = l
	}
	if val, exists := facts[identity.FactIdentityDetectorPeopleCount].(int); exists {
		couples.PeopleNumber = val
		couples.reversedPeopleDict = facts[identity.FactIdentityDetectorReversedPeopleDict].([]string)
	}
	return nil
}

// Flag for the command line switch which enables this analysis.
func (couples *CouplesAnalysis) Flag() string {
	return "couples"
}

// Description returns the text which explains what the analysis is doing.
func (couples *CouplesAnalysis) Description() string {
	return "The result is a square matrix, the value in each cell corresponds to the number " +
		"of times the pair of files appeared in the same commit or pair of developers " +
		"committed to the same file."
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (couples *CouplesAnalysis) Initialize(repository *git.Repository) error {
	couples.l = core.NewLogger()
	couples.people = make([]map[string]int, couples.PeopleNumber+1)
	for i := range couples.people {
		couples.people[i] = map[string]int{}
	}
	couples.peopleCommits = make([]int, couples.PeopleNumber+1)
	couples.files = map[string]map[string]int{}
	couples.renames = &[]rename{}
	couples.OneShotMergeProcessor.Initialize()
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (couples *CouplesAnalysis) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	firstMerge := couples.ShouldConsumeCommit(deps)
	mergeMode := deps[core.DependencyIsMerge].(bool)
	couples.lastCommit = deps[core.DependencyCommit].(*object.Commit)
	author := deps[identity.DependencyAuthor].(int)
	if author == identity.AuthorMissing {
		author = couples.PeopleNumber
	}
	if firstMerge {
		couples.peopleCommits[author]++
	}
	treeDiff := deps[items.DependencyTreeChanges].(object.Changes)
	context := make([]string, 0, len(treeDiff))
	for _, change := range treeDiff {
		action, err := change.Action()
		if err != nil {
			return nil, err
		}
		toName := change.To.Name
		fromName := change.From.Name
		switch action {
		case merkletrie.Insert:
			if !mergeMode || couples.files[toName] == nil {
				context = append(context, toName)
				couples.people[author][toName]++
			}
		case merkletrie.Delete:
			if !mergeMode {
				couples.people[author][fromName]++
			}
		case merkletrie.Modify:
			if fromName != toName {
				// renamed
				*couples.renames = append(
					*couples.renames, rename{ToName: toName, FromName: fromName})
			}
			if !mergeMode || couples.files[toName] == nil {
				context = append(context, toName)
				couples.people[author][toName]++
			}
		}
	}
	if len(context) <= CouplesMaximumMeaningfulContextSize {
		for _, file := range context {
			for _, otherFile := range context {
				lane, exists := couples.files[file]
				if !exists {
					lane = map[string]int{}
					couples.files[file] = lane
				}
				lane[otherFile]++
			}
		}
	}
	return nil, nil
}

// Finalize returns the result of the analysis. Further Consume() calls are not expected.
func (couples *CouplesAnalysis) Finalize() interface{} {
	files, people := couples.propagateRenames(couples.currentFiles())
	filesSequence := make([]string, len(files))
	i := 0
	for file := range files {
		filesSequence[i] = file
		i++
	}
	sort.Strings(filesSequence)
	filesIndex := map[string]int{}
	for i, file := range filesSequence {
		filesIndex[file] = i
	}
	filesLines := make([]int, len(filesSequence))
	for i, name := range filesSequence {
		file, err := couples.lastCommit.File(name)
		if err != nil {
			err := fmt.Errorf("cannot find file %s in commit %s: %v",
				name, couples.lastCommit.Hash.String(), err)
			couples.l.Critical(err)
			return err
		}
		blob := items.CachedBlob{Blob: file.Blob}
		err = blob.Cache()
		if err != nil {
			err := fmt.Errorf("cannot read blob %s of file %s: %v",
				blob.Hash.String(), name, err)
			couples.l.Critical(err)
			return err
		}
		filesLines[i], _ = blob.CountLines()
	}

	peopleMatrix := make([]map[int]int64, couples.PeopleNumber+1)
	peopleFiles := make([][]int, couples.PeopleNumber+1)
	for i := range peopleMatrix {
		peopleMatrix[i] = map[int]int64{}
		for file, commits := range people[i] {
			if fi, exists := filesIndex[file]; exists {
				peopleFiles[i] = append(peopleFiles[i], fi)
			}
			for j, otherFiles := range people {
				otherCommits := otherFiles[file]
				delta := otherCommits
				if otherCommits > commits {
					delta = commits
				}
				if delta > 0 {
					peopleMatrix[i][j] += int64(delta)
				}
			}
		}
		sort.Ints(peopleFiles[i])
	}

	filesMatrix := make([]map[int]int64, len(filesIndex))
	for i := range filesMatrix {
		filesMatrix[i] = map[int]int64{}
		for otherFile, cooccs := range files[filesSequence[i]] {
			filesMatrix[i][filesIndex[otherFile]] = int64(cooccs)
		}
	}
	return CouplesResult{
		PeopleMatrix:       peopleMatrix,
		PeopleFiles:        peopleFiles,
		Files:              filesSequence,
		FilesLines:         filesLines,
		FilesMatrix:        filesMatrix,
		reversedPeopleDict: couples.reversedPeopleDict,
	}
}

// Fork clones this pipeline item.
func (couples *CouplesAnalysis) Fork(n int) []core.PipelineItem {
	return core.ForkCopyPipelineItem(couples, n)
}

// Serialize converts the analysis result as returned by Finalize() to text or bytes.
// The text format is YAML and the bytes format is Protocol Buffers.
func (couples *CouplesAnalysis) Serialize(result interface{}, binary bool, writer io.Writer) error {
	couplesResult := result.(CouplesResult)
	if binary {
		return couples.serializeBinary(&couplesResult, writer)
	}
	couples.serializeText(&couplesResult, writer)
	return nil
}

// Deserialize converts the specified protobuf bytes to CouplesResult.
func (couples *CouplesAnalysis) Deserialize(pbmessage []byte) (interface{}, error) {
	message := pb.CouplesAnalysisResults{}
	err := proto.Unmarshal(pbmessage, &message)
	if err != nil {
		return nil, err
	}
	result := CouplesResult{
		Files:              message.FileCouples.Index,
		FilesLines:         make([]int, len(message.FileCouples.Index)),
		FilesMatrix:        make([]map[int]int64, message.FileCouples.Matrix.NumberOfRows),
		PeopleFiles:        make([][]int, len(message.PeopleCouples.Index)),
		PeopleMatrix:       make([]map[int]int64, message.PeopleCouples.Matrix.NumberOfRows),
		reversedPeopleDict: message.PeopleCouples.Index,
	}
	for i, files := range message.PeopleFiles {
		result.PeopleFiles[i] = make([]int, len(files.Files))
		for j, val := range files.Files {
			result.PeopleFiles[i][j] = int(val)
		}
	}
	if len(message.FileCouples.Index) != len(message.FilesLines) {
		err := fmt.Errorf("Couples PB message integrity violation: file_couples (%d) != file_lines (%d)",
			len(message.FileCouples.Index), len(message.FilesLines))
		couples.l.Critical(err)
		return nil, err
	}
	for i, v := range message.FilesLines {
		result.FilesLines[i] = int(v)
	}
	convertCSR := func(dest []map[int]int64, src *pb.CompressedSparseRowMatrix) {
		for indptr := range src.Indptr {
			if indptr == 0 {
				continue
			}
			dest[indptr-1] = map[int]int64{}
			for j := src.Indptr[indptr-1]; j < src.Indptr[indptr]; j++ {
				dest[indptr-1][int(src.Indices[j])] = src.Data[j]
			}
		}
	}
	convertCSR(result.FilesMatrix, message.FileCouples.Matrix)
	convertCSR(result.PeopleMatrix, message.PeopleCouples.Matrix)
	return result, nil
}

// MergeResults combines two CouplesAnalysis-s together.
func (couples *CouplesAnalysis) MergeResults(r1, r2 interface{}, c1, c2 *core.CommonAnalysisResult) interface{} {
	cr1 := r1.(CouplesResult)
	cr2 := r2.(CouplesResult)
	merged := CouplesResult{}
	var people, files map[string]identity.MergedIndex
	people, merged.reversedPeopleDict = identity.MergeReversedDictsIdentities(
		cr1.reversedPeopleDict, cr2.reversedPeopleDict)
	files, merged.Files = identity.MergeReversedDictsLiteral(cr1.Files, cr2.Files)
	merged.FilesLines = make([]int, len(merged.Files))
	for i, name := range merged.Files {
		idxs := files[name]
		if idxs.First >= 0 {
			merged.FilesLines[i] += cr1.FilesLines[idxs.First]
		}
		if idxs.Second >= 0 {
			merged.FilesLines[i] += cr2.FilesLines[idxs.Second]
		}
	}
	merged.PeopleFiles = make([][]int, len(merged.reversedPeopleDict))
	peopleFilesDicts := make([]map[int]bool, len(merged.reversedPeopleDict))
	addPeopleFiles := func(peopleFiles [][]int, reversedPeopleDict []string,
		reversedFilesDict []string) {
		for pi, fs := range peopleFiles {
			idx := people[reversedPeopleDict[pi]].Final
			m := peopleFilesDicts[idx]
			if m == nil {
				m = map[int]bool{}
				peopleFilesDicts[idx] = m
			}
			for _, f := range fs {
				m[files[reversedFilesDict[f]].Final] = true
			}
		}
	}
	addPeopleFiles(cr1.PeopleFiles, cr1.reversedPeopleDict, cr1.Files)
	addPeopleFiles(cr2.PeopleFiles, cr2.reversedPeopleDict, cr2.Files)
	for i, m := range peopleFilesDicts {
		merged.PeopleFiles[i] = make([]int, len(m))
		j := 0
		for f := range m {
			merged.PeopleFiles[i][j] = f
			j++
		}
		sort.Ints(merged.PeopleFiles[i])
	}
	merged.PeopleMatrix = make([]map[int]int64, len(merged.reversedPeopleDict)+1)
	addPeople := func(peopleMatrix []map[int]int64, reversedPeopleDict []string) {
		for pi, pc := range peopleMatrix {
			var idx int
			if pi < len(reversedPeopleDict) {
				idx = people[reversedPeopleDict[pi]].Final
			} else {
				idx = len(merged.reversedPeopleDict)
			}
			m := merged.PeopleMatrix[idx]
			if m == nil {
				m = map[int]int64{}
				merged.PeopleMatrix[idx] = m
			}
			for otherDev, val := range pc {
				var otherIdx int
				if otherDev < len(reversedPeopleDict) {
					otherIdx = people[reversedPeopleDict[otherDev]].Final
				} else {
					otherIdx = len(merged.reversedPeopleDict)
				}
				m[otherIdx] += val
			}
		}
	}
	addPeople(cr1.PeopleMatrix, cr1.reversedPeopleDict)
	addPeople(cr2.PeopleMatrix, cr2.reversedPeopleDict)
	merged.FilesMatrix = make([]map[int]int64, len(merged.Files))
	addFiles := func(filesMatrix []map[int]int64, reversedFilesDict []string) {
		for fi, fc := range filesMatrix {
			idx := people[reversedFilesDict[fi]].Final
			m := merged.FilesMatrix[idx]
			if m == nil {
				m = map[int]int64{}
				merged.FilesMatrix[idx] = m
			}
			for file, val := range fc {
				m[files[reversedFilesDict[file]].Final] += val
			}
		}
	}
	addFiles(cr1.FilesMatrix, cr1.Files)
	addFiles(cr2.FilesMatrix, cr2.Files)
	return merged
}

func (couples *CouplesAnalysis) serializeText(result *CouplesResult, writer io.Writer) {
	fmt.Fprintln(writer, "  files_coocc:")
	fmt.Fprintln(writer, "    index:")
	for _, file := range result.Files {
		fmt.Fprintf(writer, "      - %s\n", yaml.SafeString(file))
	}
	fmt.Fprintln(writer, "    lines:")
	for _, l := range result.FilesLines {
		fmt.Fprintf(writer, "      - %d\n", l)
	}

	fmt.Fprintln(writer, "    matrix:")
	for _, files := range result.FilesMatrix {
		fmt.Fprint(writer, "      - {")
		var indices []int
		for file := range files {
			indices = append(indices, file)
		}
		sort.Ints(indices)
		for i, file := range indices {
			fmt.Fprintf(writer, "%d: %d", file, files[file])
			if i < len(indices)-1 {
				fmt.Fprint(writer, ", ")
			}
		}
		fmt.Fprintln(writer, "}")
	}

	fmt.Fprintln(writer, "  people_coocc:")
	fmt.Fprintln(writer, "    index:")
	for _, person := range result.reversedPeopleDict {
		fmt.Fprintf(writer, "      - %s\n", yaml.SafeString(person))
	}

	fmt.Fprintln(writer, "    matrix:")
	for _, people := range result.PeopleMatrix {
		fmt.Fprint(writer, "      - {")
		var indices []int
		for file := range people {
			indices = append(indices, file)
		}
		sort.Ints(indices)
		for i, person := range indices {
			fmt.Fprintf(writer, "%d: %d", person, people[person])
			if i < len(indices)-1 {
				fmt.Fprint(writer, ", ")
			}
		}
		fmt.Fprintln(writer, "}")
	}

	fmt.Fprintln(writer, "    author_files:") // sorted by number of files each author changed
	peopleFiles := sortByNumberOfFiles(result.PeopleFiles, result.reversedPeopleDict, result.Files)
	for _, authorFiles := range peopleFiles {
		fmt.Fprintf(writer, "      - %s:\n", yaml.SafeString(authorFiles.Author))
		sort.Strings(authorFiles.Files)
		for _, file := range authorFiles.Files {
			fmt.Fprintf(writer, "        - %s\n", yaml.SafeString(file)) // sorted by path
		}
	}
}

func sortByNumberOfFiles(
	peopleFiles [][]int, peopleDict []string, filesDict []string) authorFilesList {
	var pfl authorFilesList
	for peopleIdx, files := range peopleFiles {
		if peopleIdx < len(peopleDict) {
			fileNames := make([]string, len(files))
			for i, fi := range files {
				fileNames[i] = filesDict[fi]
			}
			pfl = append(pfl, authorFiles{peopleDict[peopleIdx], fileNames})
		}
	}
	sort.Sort(pfl)
	return pfl
}

type authorFiles struct {
	Author string
	Files  []string
}

type authorFilesList []authorFiles

func (s authorFilesList) Len() int {
	return len(s)
}
func (s authorFilesList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s authorFilesList) Less(i, j int) bool {
	return len(s[i].Files) < len(s[j].Files)
}

func (couples *CouplesAnalysis) serializeBinary(result *CouplesResult, writer io.Writer) error {
	message := pb.CouplesAnalysisResults{}

	message.FileCouples = &pb.Couples{
		Index:  result.Files,
		Matrix: pb.MapToCompressedSparseRowMatrix(result.FilesMatrix),
	}
	message.PeopleCouples = &pb.Couples{
		Index:  result.reversedPeopleDict,
		Matrix: pb.MapToCompressedSparseRowMatrix(result.PeopleMatrix),
	}
	message.PeopleFiles = make([]*pb.TouchedFiles, len(result.reversedPeopleDict))
	for key := range result.reversedPeopleDict {
		files := result.PeopleFiles[key]
		int32Files := make([]int32, len(files))
		for i, f := range files {
			int32Files[i] = int32(f)
		}
		message.PeopleFiles[key] = &pb.TouchedFiles{
			Files: int32Files,
		}
	}
	message.FilesLines = make([]int32, len(result.FilesLines))
	for i, l := range result.FilesLines {
		message.FilesLines[i] = int32(l)
	}

	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	_, err = writer.Write(serialized)
	return err
}

// currentFiles return the list of files in the last consumed commit.
func (couples *CouplesAnalysis) currentFiles() map[string]bool {
	files := map[string]bool{}
	if couples.lastCommit == nil {
		for key := range couples.files {
			files[key] = true
		}
	}
	tree, _ := couples.lastCommit.Tree()
	fileIter := tree.Files()
	fileIter.ForEach(func(fobj *object.File) error {
		files[fobj.Name] = true
		return nil
	})
	return files
}

// propagateRenames applies `renames` over the files from `lastCommit`.
func (couples *CouplesAnalysis) propagateRenames(files map[string]bool) (
	map[string]map[string]int, []map[string]int) {

	renames := *couples.renames
	reducedFiles := map[string]map[string]int{}
	for file := range files {
		fmap := map[string]int{}
		refmap := couples.files[file]
		for other := range files {
			refval := refmap[other]
			if refval > 0 {
				fmap[other] = refval
			}
		}
		if len(fmap) > 0 {
			reducedFiles[file] = fmap
		}
	}
	// propagate renames
	aliases := map[string]map[string]bool{}
	pointers := map[string]string{}
	for i := range renames {
		rename := renames[len(renames)-i-1]
		toName := rename.ToName
		if newTo, exists := pointers[toName]; exists {
			toName = newTo
		}
		if _, exists := reducedFiles[toName]; exists {
			if rename.FromName != toName {
				var set map[string]bool
				if set, exists = aliases[toName]; !exists {
					set = map[string]bool{}
					aliases[toName] = set
				}
				set[rename.FromName] = true
				pointers[rename.FromName] = toName
			}
			continue
		}
	}
	adjustments := map[string]map[string]int{}
	for final, set := range aliases {
		adjustment := map[string]int{}
		for alias := range set {
			for k, v := range couples.files[alias] {
				adjustment[k] += v
			}
		}
		adjustments[final] = adjustment
	}
	for _, adjustment := range adjustments {
		for final, set := range aliases {
			for alias := range set {
				adjustment[final] += adjustment[alias]
				delete(adjustment, alias)
			}
		}
	}
	for final, adjustment := range adjustments {
		for key, val := range adjustment {
			if coocc, exists := reducedFiles[final][key]; exists {
				reducedFiles[final][key] = coocc + val
				reducedFiles[key][final] = coocc + val
			}
		}
	}
	people := make([]map[string]int, len(couples.people))
	for i, counts := range couples.people {
		reducedCounts := map[string]int{}
		people[i] = reducedCounts
		for file := range files {
			count := counts[file]
			for alias := range aliases[file] {
				count += counts[alias]
			}
			if count > 0 {
				reducedCounts[file] = count
			}
		}
		for key, val := range counts {
			if _, exists := files[key]; !exists {
				if _, exists = pointers[key]; !exists {
					reducedCounts[key] = val
				}
			}
		}
	}
	return reducedFiles, people
}

func init() {
	core.Registry.Register(&CouplesAnalysis{})
}
