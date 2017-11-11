package hercules

import (
	"gopkg.in/src-d/go-git.v4"
)

type FileDiffRefiner struct {
}

func (ref *FileDiffRefiner) Name() string {
	return "FileDiffRefiner"
}

func (ref *FileDiffRefiner) Provides() []string {
	arr := [...]string{"file_diff"}
	return arr[:]
}

func (ref *FileDiffRefiner) Requires() []string {
	arr := [...]string{"file_diff", "changed_uasts"}
	return arr[:]
}

func (ref *FileDiffRefiner) Features() []string {
	arr := [...]string{"uast"}
	return arr[:]
}

func (ref *FileDiffRefiner) ListConfigurationOptions() []ConfigurationOption {
	return []ConfigurationOption{}
}

func (ref *FileDiffRefiner) Configure(facts map[string]interface{}) {}

func (ref *FileDiffRefiner) Initialize(repository *git.Repository) {
}

func (ref *FileDiffRefiner) Consume(deps map[string]interface{}) (map[string]interface{}, error) {
	changesList := deps["changed_uasts"].([]UASTChange)
	changes := map[string]UASTChange{}
	for _, change := range changesList {
		if change.Before != nil && change.After != nil {
			changes[change.Change.To.Name] = change
		}
	}
	diffs := deps["file_diff"].(map[string]FileDiffData)
	for fileName, _ /*diff*/ := range diffs {
		_ /*change*/ = changes[fileName]
		// TODO: scan diff line by line
	}
	result := map[string]FileDiffData{}
	return map[string]interface{}{"file_diff": result}, nil
}

func init() {
	Registry.Register(&FileDiffRefiner{})
}
