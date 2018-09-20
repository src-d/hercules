package plumbing

import (
	"fmt"
	"gopkg.in/src-d/enry.v1"
	"io"
	"log"
	"strings"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v4/internal/core"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

// TreeDiff generates the list of changes for a commit. A change can be either one or two blobs
// under the same path: "before" and "after". If "before" is nil, the change is an addition.
// If "after" is nil, the change is a removal. Otherwise, it is a modification.
// TreeDiff is a PipelineItem.
type TreeDiff struct {
	core.NoopMerger
	SkipDirs     []string
	Languages    map[string]bool

	previousTree *object.Tree
	previousCommit plumbing.Hash
	repository *git.Repository
}

const (
	// DependencyTreeChanges is the name of the dependency provided by TreeDiff.
	DependencyTreeChanges = "changes"
	// ConfigTreeDiffEnableBlacklist is the name of the configuration option
	// (TreeDiff.Configure()) which allows to skip blacklisted directories.
	ConfigTreeDiffEnableBlacklist = "TreeDiff.EnableBlacklist"
	// ConfigTreeDiffBlacklistedPrefixes s the name of the configuration option
	// (TreeDiff.Configure()) which allows to set blacklisted path prefixes -
	// directories or complete file names.
	ConfigTreeDiffBlacklistedPrefixes = "TreeDiff.BlacklistedPrefixes"
	// ConfigTreeDiffLanguages is the name of the configuration option (TreeDiff.Configure())
	// which sets the list of programming languages to analyze. Language names are at
	// https://doc.bblf.sh/languages.html Names are joined with a comma ",".
	// "all" is the special name which disables this filter.
	ConfigTreeDiffLanguages = "TreeDiff.Languages"
	// allLanguages denotes passing all files in.
	allLanguages = "all"
)

// defaultBlacklistedPrefixes is the list of file path prefixes which should be skipped by default.
var defaultBlacklistedPrefixes = []string{
	"vendor/",
	"vendors/",
	"node_modules/",
	"package-lock.json",
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (treediff *TreeDiff) Name() string {
	return "TreeDiff"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (treediff *TreeDiff) Provides() []string {
	arr := [...]string{DependencyTreeChanges}
	return arr[:]
}

// Requires returns the list of names of entities which are needed by this PipelineItem.
// Each requested entity will be inserted into `deps` of Consume(). In turn, those
// entities are Provides() upstream.
func (treediff *TreeDiff) Requires() []string {
	return []string{}
}

// ListConfigurationOptions returns the list of changeable public properties of this PipelineItem.
func (treediff *TreeDiff) ListConfigurationOptions() []core.ConfigurationOption {
	options := [...]core.ConfigurationOption{{
		Name:        ConfigTreeDiffEnableBlacklist,
		Description: "Skip blacklisted directories.",
		Flag:        "skip-blacklist",
		Type:        core.BoolConfigurationOption,
		Default:     false}, {
		Name:        ConfigTreeDiffBlacklistedPrefixes,
		Description: "List of blacklisted path prefixes (e.g. directories or specific files). " +
			"Values are in the UNIX format (\"path/to/x\"). Values should *not* start with \"/\". " +
			"Separated with commas \",\".",
		Flag:        "blacklisted-prefixes",
		Type:        core.StringsConfigurationOption,
		Default:     defaultBlacklistedPrefixes}, {
		Name:        ConfigTreeDiffLanguages,
		Description: fmt.Sprintf(
			"List of programming languages to analyze. Separated by comma \",\". " +
			"Names are at https://doc.bblf.sh/languages.html \"%s\" is the special name " +
			"which disables this filter and lets all the files through.", allLanguages),
		Flag:        "languages",
		Type:        core.StringsConfigurationOption,
		Default:     []string{allLanguages}},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (treediff *TreeDiff) Configure(facts map[string]interface{}) {
	if val, exist := facts[ConfigTreeDiffEnableBlacklist]; exist && val.(bool) {
		treediff.SkipDirs = facts[ConfigTreeDiffBlacklistedPrefixes].([]string)
	}
	if val, exists := facts[ConfigTreeDiffLanguages].(string); exists {
		treediff.Languages = map[string]bool{}
		for _, lang := range strings.Split(val, ",") {
			treediff.Languages[strings.TrimSpace(lang)] = true
		}
	} else if treediff.Languages == nil {
		treediff.Languages = map[string]bool{}
		treediff.Languages[allLanguages] = true
	}
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (treediff *TreeDiff) Initialize(repository *git.Repository) {
	treediff.previousTree = nil
	treediff.repository = repository
	if treediff.Languages == nil {
		treediff.Languages = map[string]bool{}
		treediff.Languages[allLanguages] = true
	}
}

// Consume runs this PipelineItem on the next commit data.
// `deps` contain all the results from upstream PipelineItem-s as requested by Requires().
// Additionally, DependencyCommit is always present there and represents the analysed *object.Commit.
// This function returns the mapping with analysis results. The keys must be the same as
// in Provides(). If there was an error, nil is returned.
func (treediff *TreeDiff) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	commit := deps[core.DependencyCommit].(*object.Commit)
	pass := false
	for _, hash := range commit.ParentHashes {
		if hash == treediff.previousCommit {
			pass = true
		}
	}
	if !pass && treediff.previousCommit != plumbing.ZeroHash {
		log.Panicf("%s > %s", treediff.previousCommit.String(), commit.Hash.String())
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, err
	}
	var diff object.Changes
	if treediff.previousTree != nil {
		diff, err = object.DiffTree(treediff.previousTree, tree)
		if err != nil {
			return nil, err
		}
	} else {
		diff = []*object.Change{}
		err = func() error {
			fileIter := tree.Files()
			defer fileIter.Close()
			for {
				file, err := fileIter.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				pass, err := treediff.checkLanguage(file.Name, file.Hash)
				if err != nil {
					return err
				}
				if !pass {
					continue
				}
				diff = append(diff, &object.Change{
					To: object.ChangeEntry{Name: file.Name, Tree: tree, TreeEntry: object.TreeEntry{
						Name: file.Name, Mode: file.Mode, Hash: file.Hash}}})
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}
	treediff.previousTree = tree
	treediff.previousCommit = commit.Hash

	// filter without allocation
	filteredDiff := make([]*object.Change, 0, len(diff))
OUTER:
	for _, change := range diff {
		for _, dir := range treediff.SkipDirs {
			if strings.HasPrefix(change.To.Name, dir) || strings.HasPrefix(change.From.Name, dir) {
				continue OUTER
			}
		}
		var changeEntry object.ChangeEntry
		if change.To.Tree == nil {
			changeEntry = change.From
		} else {
			changeEntry = change.To
		}
		pass, _ := treediff.checkLanguage(changeEntry.Name, changeEntry.TreeEntry.Hash)
		if !pass {
			continue
		}
		filteredDiff = append(filteredDiff, change)
	}

	diff = filteredDiff
	return map[string]interface{}{DependencyTreeChanges: diff}, nil
}

// Fork clones this PipelineItem.
func (treediff *TreeDiff) Fork(n int) []core.PipelineItem {
	return core.ForkCopyPipelineItem(treediff, n)
}

// checkLanguage returns whether the blob corresponds to the list of required languages.
func (treediff *TreeDiff) checkLanguage(name string, blobHash plumbing.Hash) (bool, error) {
	if treediff.Languages[allLanguages] {
		return true, nil
	}
	blob, err := treediff.repository.BlobObject(blobHash)
	if err != nil {
		return false, err
	}
	reader, err := blob.Reader()
	if err != nil {
		return false, err
	}
	buffer := make([]byte, 1024)
	_, err = reader.Read(buffer)
	if err != nil {
		return false, err
	}
	lang := enry.GetLanguage(name, buffer)
	return treediff.Languages[lang], nil
}

func init() {
	core.Registry.Register(&TreeDiff{})
}
