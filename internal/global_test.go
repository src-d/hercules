package internal_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/cyraxred/hercules/internal/core"
	"github.com/cyraxred/hercules/internal/plumbing/uast"
	"github.com/cyraxred/hercules/internal/test"
	"github.com/cyraxred/hercules/leaves"
	"github.com/stretchr/testify/assert"
)

func TestPipelineSerialize(t *testing.T) {
	pipeline := core.NewPipeline(test.Repository)
	pipeline.SetFeature(uast.FeatureUast)
	pipeline.DeployItem(&leaves.LegacyBurndownAnalysis{})
	facts := map[string]interface{}{}
	facts[core.ConfigPipelineDryRun] = true
	tmpdir, _ := ioutil.TempDir("", "hercules-")
	defer os.RemoveAll(tmpdir)
	dotpath := path.Join(tmpdir, "graph.dot")
	facts[core.ConfigPipelineDAGPath] = dotpath
	pipeline.Initialize(facts)
	bdot, _ := ioutil.ReadFile(dotpath)
	dot := string(bdot)
	println()
	println(dot)
	println()
	assert.Equal(t, `digraph Hercules {
  "5 BlobCache_1" -> "6 [blob_cache]"
  "9 FileDiff_1" -> "14 FileDiffRefiner_1"
  "14 FileDiffRefiner_1" -> "15 [file_diff]"
  "0 PeopleDetector_1" -> "3 [author]"
  "7 RenameAnalysis_1" -> "8 [changes]"
  "1 TicksSinceStart_1" -> "4 [tick]"
  "2 TreeDiff_1" -> "5 BlobCache_1"
  "2 TreeDiff_1" -> "7 RenameAnalysis_1"
  "10 UAST_1" -> "11 [uasts]"
  "12 UASTChanges_1" -> "13 [changed_uasts]"
  "3 [author]" -> "16 LegacyBurndown_1"
  "6 [blob_cache]" -> "9 FileDiff_1"
  "6 [blob_cache]" -> "16 LegacyBurndown_1"
  "6 [blob_cache]" -> "7 RenameAnalysis_1"
  "6 [blob_cache]" -> "10 UAST_1"
  "13 [changed_uasts]" -> "14 FileDiffRefiner_1"
  "8 [changes]" -> "9 FileDiff_1"
  "8 [changes]" -> "16 LegacyBurndown_1"
  "8 [changes]" -> "10 UAST_1"
  "8 [changes]" -> "12 UASTChanges_1"
  "15 [file_diff]" -> "16 LegacyBurndown_1"
  "4 [tick]" -> "16 LegacyBurndown_1"
  "11 [uasts]" -> "12 UASTChanges_1"
}`, dot)
}

func TestPipelineSerializeNoUast(t *testing.T) {
	pipeline := core.NewPipeline(test.Repository)
	pipeline.DeployItem(&leaves.LegacyBurndownAnalysis{})
	facts := map[string]interface{}{}
	facts[core.ConfigPipelineDryRun] = true
	tmpdir, _ := ioutil.TempDir("", "hercules-")
	defer os.RemoveAll(tmpdir)
	dotpath := path.Join(tmpdir, "graph.dot")
	facts[core.ConfigPipelineDAGPath] = dotpath
	pipeline.Initialize(facts)
	bdot, _ := ioutil.ReadFile(dotpath)
	dot := string(bdot)
	println()
	println(dot)
	println()
	assert.Equal(t, `digraph Hercules {
  "5 BlobCache_1" -> "6 [blob_cache]"
  "9 FileDiff_1" -> "10 [file_diff]"
  "0 PeopleDetector_1" -> "3 [author]"
  "7 RenameAnalysis_1" -> "8 [changes]"
  "1 TicksSinceStart_1" -> "4 [tick]"
  "2 TreeDiff_1" -> "5 BlobCache_1"
  "2 TreeDiff_1" -> "7 RenameAnalysis_1"
  "3 [author]" -> "11 LegacyBurndown_1"
  "6 [blob_cache]" -> "9 FileDiff_1"
  "6 [blob_cache]" -> "11 LegacyBurndown_1"
  "6 [blob_cache]" -> "7 RenameAnalysis_1"
  "8 [changes]" -> "9 FileDiff_1"
  "8 [changes]" -> "11 LegacyBurndown_1"
  "10 [file_diff]" -> "11 LegacyBurndown_1"
  "4 [tick]" -> "11 LegacyBurndown_1"
}`, dot)
}

func TestPipelineResolveIntegration(t *testing.T) {
	pipeline := core.NewPipeline(test.Repository)
	pipeline.DeployItem(&leaves.LegacyBurndownAnalysis{})
	pipeline.DeployItem(&leaves.CouplesAnalysis{})
	pipeline.Initialize(nil)
}
