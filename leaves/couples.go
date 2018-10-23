package leaves

import (
	"fmt"
	"io"
	"sort"

	"github.com/gogo/protobuf/proto"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/utils/merkletrie"
	"gopkg.in/src-d/hercules.v5/internal/core"
	"gopkg.in/src-d/hercules.v5/internal/pb"
	items "gopkg.in/src-d/hercules.v5/internal/plumbing"
	"gopkg.in/src-d/hercules.v5/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v5/internal/yaml"
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
}

// CouplesResult is returned by CouplesAnalysis.Finalize() and carries couples matrices from
// authors and files.
type CouplesResult struct {
	PeopleMatrix []map[int]int64
	PeopleFiles  [][]int
	FilesMatrix  []map[int]int64
	Files        []string

	// reversedPeopleDict references IdentityDetector.ReversedPeopleDict
	reversedPeopleDict []string
}

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
	arr := [...]string{identity.DependencyAuthor, items.DependencyTreeChanges}
	return arr[:]
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (couples *CouplesAnalysis) ListConfigurationOptions() []core.ConfigurationOption {
	return []core.ConfigurationOption{}
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (couples *CouplesAnalysis) Configure(facts map[string]interface{}) {
	if val, exists := facts[identity.FactIdentityDetectorPeopleCount].(int); exists {
		couples.PeopleNumber = val
		couples.reversedPeopleDict = facts[identity.FactIdentityDetectorReversedPeopleDict].([]string)
	}
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
func (couples *CouplesAnalysis) Initialize(repository *git.Repository) {
	couples.people = make([]map[string]int, couples.PeopleNumber+1)
	for i := range couples.people {
		couples.people[i] = map[string]int{}
	}
	couples.peopleCommits = make([]int, couples.PeopleNumber+1)
	couples.files = map[string]map[string]int{}
	couples.renames = &[]rename{}
	couples.OneShotMergeProcessor.Initialize()
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
			if !mergeMode {
				context = append(context, toName)
				couples.people[author][toName]++
			} else if couples.people[author][toName] == 0 {
				couples.people[author][toName] = 1
			}
		case merkletrie.Delete:
			if !mergeMode {
				couples.people[author][fromName]++
			} else if couples.people[author][fromName] == 0 {
				couples.people[author][fromName] = 1
			}
		case merkletrie.Modify:
			if fromName != toName {
				// renamed
				*couples.renames = append(
					*couples.renames, rename{ToName: toName, FromName: fromName})
			}
			if !mergeMode {
				context = append(context, toName)
				couples.people[author][toName]++
			}
		}
	}
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
	var people, files map[string][3]int
	people, merged.reversedPeopleDict = identity.Detector{}.MergeReversedDicts(
		cr1.reversedPeopleDict, cr2.reversedPeopleDict)
	files, merged.Files = identity.Detector{}.MergeReversedDicts(cr1.Files, cr2.Files)
	merged.PeopleFiles = make([][]int, len(merged.reversedPeopleDict))
	peopleFilesDicts := make([]map[int]bool, len(merged.reversedPeopleDict))
	addPeopleFiles := func(peopleFiles [][]int, reversedPeopleDict []string,
		reversedFilesDict []string) {
		for pi, fs := range peopleFiles {
			idx := people[reversedPeopleDict[pi]][0]
			m := peopleFilesDicts[idx]
			if m == nil {
				m = map[int]bool{}
				peopleFilesDicts[idx] = m
			}
			for _, f := range fs {
				m[files[reversedFilesDict[f]][0]] = true
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
	addPeople := func(peopleMatrix []map[int]int64, reversedPeopleDict []string,
		reversedFilesDict []string) {
		for pi, pc := range peopleMatrix {
			var idx int
			if pi < len(reversedPeopleDict) {
				idx = people[reversedPeopleDict[pi]][0]
			} else {
				idx = len(merged.reversedPeopleDict)
			}
			m := merged.PeopleMatrix[idx]
			if m == nil {
				m = map[int]int64{}
				merged.PeopleMatrix[idx] = m
			}
			for file, val := range pc {
				m[files[reversedFilesDict[file]][0]] += val
			}
		}
	}
	addPeople(cr1.PeopleMatrix, cr1.reversedPeopleDict, cr1.Files)
	addPeople(cr2.PeopleMatrix, cr2.reversedPeopleDict, cr2.Files)
	merged.FilesMatrix = make([]map[int]int64, len(merged.Files))
	addFiles := func(filesMatrix []map[int]int64, reversedFilesDict []string) {
		for fi, fc := range filesMatrix {
			idx := people[reversedFilesDict[fi]][0]
			m := merged.FilesMatrix[idx]
			if m == nil {
				m = map[int]int64{}
				merged.FilesMatrix[idx] = m
			}
			for file, val := range fc {
				m[files[reversedFilesDict[file]][0]] += val
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

	serialized, err := proto.Marshal(&message)
	if err != nil {
		return err
	}
	writer.Write(serialized)
	return nil
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
