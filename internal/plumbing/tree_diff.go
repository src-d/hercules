package plumbing

import (
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"

	"github.com/src-d/enry/v2"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"

	"gopkg.in/src-d/hercules.v10/internal/core"
)

// TreeDiff generates the list of changes for a commit. A change can be either one or two blobs
// under the same path: "before" and "after". If "before" is nil, the change is an addition.
// If "after" is nil, the change is a removal. Otherwise, it is a modification.
// TreeDiff is a PipelineItem.
type TreeDiff struct {
	core.NoopMerger
	SkipFiles  []string
	NameFilter *regexp.Regexp
	// Languages is the set of allowed languages. The values must be lower case. The default
	// (empty) set disables the language filter.
	Languages map[string]bool

	previousTree   *object.Tree
	previousCommit plumbing.Hash
	repository     *git.Repository

	l core.Logger
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
	ConfigTreeDiffLanguages = "TreeDiff.LanguagesDetection"
	// allLanguages denotes passing all files in.
	allLanguages = "all"

	// ConfigTreeDiffFilterRegexp is the name of the configuration option
	// (TreeDiff.Configure()) which makes FileDiff consider only those files which have names matching this regexp.
	ConfigTreeDiffFilterRegexp = "TreeDiff.FilteredRegexes"
)

// defaultBlacklistedPrefixes is the list of file path prefixes which should be skipped by default.
var defaultBlacklistedPrefixes = []string{
	"vendor/",
	"vendors/",
	"package-lock.json",
	"Gopkg.lock",
}

// Name of this PipelineItem. Uniquely identifies the type, used for mapping keys, etc.
func (treediff *TreeDiff) Name() string {
	return "TreeDiff"
}

// Provides returns the list of names of entities which are produced by this PipelineItem.
// Each produced entity will be inserted into `deps` of dependent Consume()-s according
// to this list. Also used by core.Registry to build the global map of providers.
func (treediff *TreeDiff) Provides() []string {
	return []string{DependencyTreeChanges}
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
		Name: ConfigTreeDiffEnableBlacklist,
		Description: "Skip blacklisted directories and vendored files (according to " +
			"src-d/enry.IsVendor).",
		Flag:    "skip-blacklist",
		Type:    core.BoolConfigurationOption,
		Default: false}, {

		Name: ConfigTreeDiffBlacklistedPrefixes,
		Description: "List of blacklisted path prefixes (e.g. directories or specific files). " +
			"Values are in the UNIX format (\"path/to/x\"). Values should *not* start with \"/\". " +
			"Separated with commas \",\".",
		Flag:    "blacklisted-prefixes",
		Type:    core.StringsConfigurationOption,
		Default: defaultBlacklistedPrefixes}, {

		Name: ConfigTreeDiffLanguages,
		Description: fmt.Sprintf(
			"List of programming languages to analyze. Separated by comma \",\". "+
				"The names are the keys in https://github.com/github/linguist/blob/master/lib/linguist/languages.yml "+
				"\"%s\" is the special name which disables this filter and lets all the files through.",
			allLanguages),
		Flag:    "languages",
		Type:    core.StringsConfigurationOption,
		Default: []string{allLanguages}}, {

		Name:        ConfigTreeDiffFilterRegexp,
		Description: "Whitelist regexp to determine which files to analyze.",
		Flag:        "whitelist",
		Type:        core.StringConfigurationOption,
		Default:     ""},
	}
	return options[:]
}

// Configure sets the properties previously published by ListConfigurationOptions().
func (treediff *TreeDiff) Configure(facts map[string]interface{}) error {
	if l, exists := facts[core.ConfigLogger].(core.Logger); exists {
		treediff.l = l
	}
	if val, exists := facts[ConfigTreeDiffEnableBlacklist].(bool); exists && val {
		treediff.SkipFiles = facts[ConfigTreeDiffBlacklistedPrefixes].([]string)
	}
	if val, exists := facts[ConfigTreeDiffLanguages].([]string); exists {
		treediff.Languages = map[string]bool{}
		for _, lang := range val {
			treediff.Languages[strings.ToLower(strings.TrimSpace(lang))] = true
		}
	} else if treediff.Languages == nil {
		treediff.Languages = map[string]bool{}
		treediff.Languages[allLanguages] = true
	}

	if val, exists := facts[ConfigTreeDiffFilterRegexp].(string); exists {
		treediff.NameFilter = regexp.MustCompile(val)
	}
	return nil
}

// Initialize resets the temporary caches and prepares this PipelineItem for a series of Consume()
// calls. The repository which is going to be analysed is supplied as an argument.
func (treediff *TreeDiff) Initialize(repository *git.Repository) error {
	treediff.l = core.NewLogger()
	treediff.previousTree = nil
	treediff.repository = repository
	if treediff.Languages == nil {
		treediff.Languages = map[string]bool{}
		treediff.Languages[allLanguages] = true
	}
	return nil
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
		err := fmt.Errorf("%s > %s", treediff.previousCommit.String(), commit.Hash.String())
		treediff.l.Critical(err)
		return nil, err
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, err
	}
	var diffs object.Changes
	if treediff.previousTree != nil {
		diffs, err = object.DiffTree(treediff.previousTree, tree)
		if err != nil {
			return nil, err
		}
	} else {
		diffs = []*object.Change{}
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
				diffs = append(diffs, &object.Change{
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
	diffs = treediff.filterDiffs(diffs)
	return map[string]interface{}{DependencyTreeChanges: diffs}, nil
}

func (treediff *TreeDiff) filterDiffs(diffs object.Changes) object.Changes {
	// filter without allocation
	filteredDiffs := make(object.Changes, 0, len(diffs))
OUTER:
	for _, change := range diffs {
		if len(treediff.SkipFiles) > 0 && (enry.IsVendor(change.To.Name) || enry.IsVendor(change.From.Name)) {
			continue
		}
		for _, dir := range treediff.SkipFiles {
			if strings.HasPrefix(change.To.Name, dir) || strings.HasPrefix(change.From.Name, dir) {
				continue OUTER
			}
		}
		if treediff.NameFilter != nil {
			matchedTo := treediff.NameFilter.MatchString(change.To.Name)
			matchedFrom := treediff.NameFilter.MatchString(change.From.Name)

			if !matchedTo && !matchedFrom {
				continue
			}
		}
		var changeEntry object.ChangeEntry
		if change.To.Tree == nil {
			changeEntry = change.From
		} else {
			changeEntry = change.To
		}
		if pass, _ := treediff.checkLanguage(changeEntry.Name, changeEntry.TreeEntry.Hash); !pass {
			continue
		}
		filteredDiffs = append(filteredDiffs, change)
	}
	return filteredDiffs
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
	n, err := reader.Read(buffer)
	if err != nil && (blob.Size != 0 || err != io.EOF) {
		return false, err
	}
	if n < len(buffer) {
		buffer = buffer[:n]
	}
	lang := strings.ToLower(enry.GetLanguage(path.Base(name), buffer))
	return treediff.Languages[lang], nil
}

func init() {
	core.Registry.Register(&TreeDiff{})
}
