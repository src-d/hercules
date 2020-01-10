package identity

import (
	"bufio"
	"os"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
)

// Detector determines the author of a commit. Same person can commit under different
// signatures, and we apply some heuristics to merge those together.
// It is a PipelineItem.
type Detector struct {
	core.NoopMerger
	// PeopleDict maps email || name  -> developer id
	PeopleDict map[string]int
	// ReversedPeopleDict maps developer id -> description
	ReversedPeopleDict []string
	// ExactSignatures chooses the matching algorithm: opportunistic email || name
	// or exact email && name
	ExactSignatures bool

	l core.Logger
}

const (
	// AuthorMissing is the internal author index which denotes any unmatched identities
	// (Detector.Consume()). It may *not* be (1 << 18) - 1, see BurndownAnalysis.packPersonWithDay().
	AuthorMissing = (1 << 18) - 2
	// AuthorMissingName is the string name which corresponds to AuthorMissing.
	AuthorMissingName = "<unmatched>"

	// FactIdentityDetectorPeopleDict is the name of the fact which is inserted in
	// Detector.Configure(). It corresponds to Detector.PeopleDict - the mapping
	// from the signatures to the author indices.
	FactIdentityDetectorPeopleDict = "IdentityDetector.PeopleDict"
	// FactIdentityDetectorReversedPeopleDict is the name of the fact which is inserted in
	// Detector.Configure(). It corresponds to Detector.ReversedPeopleDict -
	// the mapping from the author indices to the main signature.
	FactIdentityDetectorReversedPeopleDict = "IdentityDetector.ReversedPeopleDict"
	// ConfigIdentityDetectorPeopleDictPath is the name of the configuration option
	// (Detector.Configure()) which allows to set the external PeopleDict mapping from a file.
	ConfigIdentityDetectorPeopleDictPath = "IdentityDetector.PeopleDictPath"
	// ConfigIdentityDetectorExactSignatures is the name of the configuration option
	// (Detector.Configure()) which changes the matching algorithm to exact signature (name + email)
	// correspondence.
	ConfigIdentityDetectorExactSignatures = "IdentityDetector.ExactSignatures"
	// FactIdentityDetectorPeopleCount is the name of the fact which is inserted in
	// Detector.Configure(). It is equal to the overall number of unique authors
	// (the length of ReversedPeopleDict).
	FactIdentityDetectorPeopleCount = "IdentityDetector.PeopleCount"

	// DependencyAuthor is the name of the dependency provided by Detector.
	DependencyAuthor = "author"
)

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (detector *Detector) Name() string {
	return "IdentityDetector"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (detector *Detector) Provides() []string {
	return []string{DependencyAuthor}
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (detector *Detector) Requires() []string {
	return []string{}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (detector *Detector) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigIdentityDetectorPeopleDictPath,
		Description: "Path to the file with developer -> name|email associations.",
		Flag:        "people-dict",
		Type:        core.PathConfigurationOption,
		Default:     ""}, {
		Name: ConfigIdentityDetectorExactSignatures,
		Description: "Disable separate name/email matching. This will lead to considerbly more " +
			"identities and should not be normally used.",
		Flag:    "exact-signatures",
		Type:    core.BoolConfigurationOption,
		Default: false},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (detector *Detector) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		detector.l = l
	} else {
		detector.l = core.NewLogger()
	}
	if val, exists := facts[FactIdentityDetectorPeopleDict].(map[string]int); exists {
		detector.PeopleDict = val
	}
	if val, exists := facts[FactIdentityDetectorReversedPeopleDict].([]string); exists {
		detector.ReversedPeopleDict = val
	}
	if val, exists := facts[ConfigIdentityDetectorExactSignatures].(bool); exists {
		detector.ExactSignatures = val
	}
	if detector.PeopleDict == nil || detector.ReversedPeopleDict == nil {
		peopleDictPath, _ := facts[ConfigIdentityDetectorPeopleDictPath].(string)
		if peopleDictPath != "" {
			err := detector.LoadPeopleDict(peopleDictPath)
			if err != nil {
				return errors.Errorf("failed to load %s: %v", peopleDictPath, err)
			}
			facts[FactIdentityDetectorPeopleCount] = len(detector.ReversedPeopleDict) - 1
		} else {
			if _, exists := facts[core.ConfigPipelineCommits]; !exists {
				panic("IdentityDetector needs a list of commits to initialize.")
			}
			detector.GeneratePeopleDict(facts[core.ConfigPipelineCommits].([]*object.Commit))
			facts[FactIdentityDetectorPeopleCount] = len(detector.ReversedPeopleDict)
		}
	} else {
		facts[FactIdentityDetectorPeopleCount] = len(detector.ReversedPeopleDict)
	}
	facts[FactIdentityDetectorPeopleDict] = detector.PeopleDict
	facts[FactIdentityDetectorReversedPeopleDict] = detector.ReversedPeopleDict
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (detector *Detector) Initialize(repository *git.Repository) error {
	detector.l = core.NewLogger()
	return nil
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (detector *Detector) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps[core.DependencyCommit].(*object.Commit)
	var authorID int
	var exists bool
	signature := commit.Author
	if !detector.ExactSignatures {
		authorID, exists = detector.PeopleDict[strings.ToLower(signature.Email)]
		if !exists {
			authorID, exists = detector.PeopleDict[strings.ToLower(signature.Name)]
		}
	} else {
		authorID, exists = detector.PeopleDict[strings.ToLower(signature.String())]
	}
	if !exists {
		authorID = AuthorMissing
	}
	return map[string]interface{}{DependencyAuthor: authorID}, nil
}

// Fork clones this PipelineItem.
func (detector *Detector) Fork(n int) []core.PipelineItem {
	return core.ForkSamePipelineItem(detector, n)
}

// LoadPeopleDict loads author signatures from a text file.
// The format is one signature per line, and the signature consists of several
// keys separated by "|". The first key is the main one and used to reference all the rest.
func (detector *Detector) LoadPeopleDict(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	dict := make(map[string]int)
	var reverseDict []string
	size := 0
	for scanner.Scan() {
		ids := strings.Split(scanner.Text(), "|")
		for _, id := range ids {
			dict[strings.ToLower(id)] = size
		}
		reverseDict = append(reverseDict, ids[0])
		size++
	}
	reverseDict = append(reverseDict, AuthorMissingName)
	detector.PeopleDict = dict
	detector.ReversedPeopleDict = reverseDict
	return nil
}

// GeneratePeopleDict loads author signatures from the specified list of Git commits.
func (detector *Detector) GeneratePeopleDict(commits []*object.Commit) {
	dict := map[string]int{}
	emails := map[int][]string{}
	names := map[int][]string{}
	size := 0

	mailmapFile, err := commits[len(commits)-1].File(".mailmap")
	// TODO(vmarkovtsev): properly handle .mailmap if ExactSignatures
	if !detector.ExactSignatures && err == nil {
		mailMapContents, err := mailmapFile.Contents()
		if err == nil {
			mailmap := ParseMailmap(mailMapContents)
			for key, val := range mailmap {
				key = strings.ToLower(key)
				toEmail := strings.ToLower(val.Email)
				toName := strings.ToLower(val.Name)
				id, exists := dict[toEmail]
				if !exists {
					id, exists = dict[toName]
				}
				if exists {
					dict[key] = id
				} else {
					id = size
					size++
					if toEmail != "" {
						dict[toEmail] = id
						emails[id] = append(emails[id], toEmail)
					}
					if toName != "" {
						dict[toName] = id
						names[id] = append(names[id], toName)
					}
					dict[key] = id
				}
				if strings.Contains(key, "@") {
					exists := false
					for _, val := range emails[id] {
						if key == val {
							exists = true
							break
						}
					}
					if !exists {
						emails[id] = append(emails[id], key)
					}
				} else {
					exists := false
					for _, val := range names[id] {
						if key == val {
							exists = true
							break
						}
					}
					if !exists {
						names[id] = append(names[id], key)
					}
				}
			}
		}
	}

	for _, commit := range commits {
		if !detector.ExactSignatures {
			email := strings.ToLower(commit.Author.Email)
			name := strings.ToLower(commit.Author.Name)
			id, exists := dict[email]
			if exists {
				_, exists := dict[name]
				if !exists {
					dict[name] = id
					names[id] = append(names[id], name)
				}
				continue
			}
			id, exists = dict[name]
			if exists {
				dict[email] = id
				emails[id] = append(emails[id], email)
				continue
			}
			dict[email] = size
			dict[name] = size
			emails[size] = append(emails[size], email)
			names[size] = append(names[size], name)
			size++
		} else { // !detector.ExactSignatures
			sig := strings.ToLower(commit.Author.String())
			if _, exists := dict[sig]; !exists {
				dict[sig] = size
				size++
			}
		}
	}
	reverseDict := make([]string, size)
	if !detector.ExactSignatures {
		for _, val := range dict {
			sort.Strings(names[val])
			sort.Strings(emails[val])
			reverseDict[val] = strings.Join(names[val], "|") + "|" + strings.Join(emails[val], "|")
		}
	} else {
		for key, val := range dict {
			reverseDict[val] = key
		}
	}
	detector.PeopleDict = dict
	detector.ReversedPeopleDict = reverseDict
}

// MergedIndex is the result of merging `rd1[First]` and `rd2[Second]`: the index in the final reversed
// dictionary. -1 for `First` or `Second` means that the corresponding string does not exist
// in respectively `rd1` and `rd2`.
// See also:
// * MergeReversedDictsLiteral()
// * MergeReversedDictsIdentities()
type MergedIndex struct {
	Final  int
	First  int
	Second int
}

// MergeReversedDictsLiteral joins two string lists together, excluding duplicates, in-order.
// The string comparisons are the usual ones.
// The returned mapping's keys are the unique strings in `rd1 ∪ rd2`, and the values are:
// 1. Index after merging.
// 2. Corresponding index in the first array - `rd1`. -1 means that it does not exist.
// 3. Corresponding index in the second array - `rd2`. -1 means that it does not exist.
func MergeReversedDictsLiteral(rd1, rd2 []string) (map[string]MergedIndex, []string) {

	people := map[string]MergedIndex{}
	for i, pid := range rd1 {
		people[pid] = MergedIndex{len(people), i, -1}
	}
	for i, pid := range rd2 {
		if ptrs, exists := people[pid]; !exists {
			people[pid] = MergedIndex{len(people), -1, i}
		} else {
			people[pid] = MergedIndex{ptrs.Final, ptrs.First, i}
		}
	}
	mrd := make([]string, len(people))
	for name, ptrs := range people {
		mrd[ptrs.Final] = name
	}
	return people, mrd
}

type identityPair struct {
	Index1 int
	Index2 int
}

// MergeReversedDictsIdentities joins two identity lists together, excluding duplicates.
// The strings are split by "|" and we find the connected components..
// The returned mapping's keys are the unique strings in `rd1 ∪ rd2`, and the values are:
// 1. Index after merging.
// 2. Corresponding index in the first array - `rd1`. -1 means that it does not exist.
// 3. Corresponding index in the second array - `rd2`. -1 means that it does not exist.
func MergeReversedDictsIdentities(rd1, rd2 []string) (map[string]MergedIndex, []string) {

	vocabulary := map[string]identityPair{}
	vertices1 := make([][]string, len(rd1))
	for i, s := range rd1 {
		parts := strings.Split(s, "|")
		vertices1[i] = parts
		for _, p := range parts {
			vocabulary[p] = identityPair{i, -1}
		}
	}
	vertices2 := make([][]string, len(rd2))
	for i, s := range rd2 {
		parts := strings.Split(s, "|")
		vertices2[i] = parts
		for _, p := range parts {
			if ip, exists := vocabulary[p]; !exists {
				vocabulary[p] = identityPair{-1, i}
			} else {
				ip.Index2 = i
				vocabulary[p] = ip
			}
		}
	}

	// find the connected components by walking the graph
	var walks []map[string]bool
	visited := map[string]bool{}

	walkFromVertex := func(root []string) {
		walk := map[string]bool{}
		pending := map[string]bool{}
		for _, p := range root {
			pending[p] = true
		}
		for len(pending) > 0 {
			var element string
			for e := range pending {
				element = e
				delete(pending, e)
				break
			}
			if !walk[element] {
				walk[element] = true
				ip := vocabulary[element]
				if ip.Index1 >= 0 {
					for _, p := range vertices1[ip.Index1] {
						if !walk[p] {
							pending[p] = true
						}
					}
				}
				if ip.Index2 >= 0 {
					for _, p := range vertices2[ip.Index2] {
						if !walk[p] {
							pending[p] = true
						}
					}
				}
			}
		}
		for e := range walk {
			visited[e] = true
		}
		walks = append(walks, walk)
	}

	for i1 := range rd1 {
		var skip bool
		for _, p := range vertices1[i1] {
			if visited[p] {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		walkFromVertex(vertices1[i1])
	}
	for i2 := range rd2 {
		var skip bool
		for _, p := range vertices2[i2] {
			if visited[p] {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		walkFromVertex(vertices2[i2])
	}

	mergedStrings := make([]string, 0, len(walks))
	mergedIndex := map[string]MergedIndex{}
	// convert each walk from strings to indexes
	for walkIndex, walk := range walks {
		ids := make([]string, 0, len(walk))
		for key := range walk {
			ids = append(ids, key)
		}
		// place emails after names
		sort.Slice(ids, func(i, j int) bool {
			iid := ids[i]
			jid := ids[j]
			iHasAt := strings.ContainsRune(iid, '@')
			jHasAt := strings.ContainsRune(jid, '@')
			if iHasAt == jHasAt {
				return iid < jid
			}
			return jHasAt
		})
		mergedStrings = append(mergedStrings, strings.Join(ids, "|"))
		for _, key := range ids {
			ipair := vocabulary[key]
			if ipair.Index1 >= 0 {
				s1 := rd1[ipair.Index1]
				if mi, exists := mergedIndex[s1]; !exists {
					mergedIndex[s1] = MergedIndex{walkIndex, ipair.Index1, -1}
				} else {
					mergedIndex[s1] = MergedIndex{walkIndex, ipair.Index1, mi.Second}
				}
			}
			if ipair.Index2 >= 0 {
				s2 := rd2[ipair.Index2]
				if mi, exists := mergedIndex[s2]; !exists {
					mergedIndex[s2] = MergedIndex{walkIndex, -1, ipair.Index2}
				} else {
					mergedIndex[s2] = MergedIndex{walkIndex, mi.First, ipair.Index2}
				}
			}
		}
	}
	return mergedIndex, mergedStrings
}

func init() {
	core.Registry.Register(&Detector{})
}
