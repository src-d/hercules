package leaves

import (
	"bytes"
	"io/ioutil"
	"path"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	gitplumbing "gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	"gopkg.in/src-d/hercules.v10/internal/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func fixtureCouples() *CouplesAnalysis {
	c := CouplesAnalysis{PeopleNumber: 3}
	c.Initialize(test.Repository)
	return &c
}

func TestCouplesMeta(t *testing.T) {
	c := fixtureCouples()
	assert.Equal(t, c.Name(), "Couples")
	assert.Equal(t, len(c.Provides()), 0)
	assert.Equal(t, len(c.Requires()), 2)
	assert.Equal(t, c.Requires()[0], identity.DependencyAuthor)
	assert.Equal(t, c.Requires()[1], plumbing.DependencyTreeChanges)
	assert.Equal(t, c.Flag(), "couples")
	assert.Len(t, c.ListConfigurationOptions(), 0)
	logger := core.NewLogger()
	assert.NoError(t, c.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, c.l)
}

func TestCouplesRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&CouplesAnalysis{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "Couples")
	leaves := core.Registry.GetLeaves()
	matched := false
	for _, tp := range leaves {
		if tp.Flag() == (&CouplesAnalysis{}).Flag() {
			matched = true
			break
		}
	}
	assert.True(t, matched)
}

func generateChanges(names ...string) object.Changes {
	changes := make(object.Changes, 0, len(names))
	for _, name := range names {
		action := name[:1]
		name = name[1:]
		var change object.Change
		if action == "+" {
			change = object.Change{
				From: object.ChangeEntry{},
				To:   object.ChangeEntry{Name: name},
			}
		} else if action == "-" {
			change = object.Change{
				From: object.ChangeEntry{Name: name},
				To:   object.ChangeEntry{},
			}
		} else if action == "=" {
			change = object.Change{
				From: object.ChangeEntry{Name: name},
				To:   object.ChangeEntry{Name: name},
			}
		} else {
			if action != ">" {
				panic("Invalid action.")
			}
			parts := strings.Split(name, ">")
			change = object.Change{
				From: object.ChangeEntry{Name: parts[0]},
				To:   object.ChangeEntry{Name: parts[1]},
			}
		}
		changes = append(changes, &change)
	}
	return changes
}

func TestCouplesConsumeFinalize(t *testing.T) {
	c := fixtureCouples()
	deps := map[string]interface{}{}
	deps[identity.DependencyAuthor] = 0
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(gitplumbing.NewHash(
		"a3ee37f91f0d705ec9c41ae88426f0ae44b2fbc3"))
	deps[core.DependencyIsMerge] = false
	deps[plumbing.DependencyTreeChanges] = generateChanges("+LICENSE2", "+file2.go", "+rbtree2.go")
	c.Consume(deps)
	deps[plumbing.DependencyTreeChanges] = generateChanges("+README.md", "-LICENSE2", "=analyser.go", ">file2.go>file_test.go")
	c.Consume(deps)
	deps[identity.DependencyAuthor] = 1
	deps[plumbing.DependencyTreeChanges] = generateChanges("=README.md", "=analyser.go", "-rbtree2.go")
	c.Consume(deps)
	deps[identity.DependencyAuthor] = 2
	deps[plumbing.DependencyTreeChanges] = generateChanges("=file_test.go")
	c.Consume(deps)
	assert.Equal(t, len(c.people[0]), 6)
	assert.Equal(t, c.people[0]["README.md"], 1)
	assert.Equal(t, c.people[0]["LICENSE2"], 2)
	assert.Equal(t, c.people[0]["analyser.go"], 1)
	assert.Equal(t, c.people[0]["file2.go"], 1)
	assert.Equal(t, c.people[0]["file_test.go"], 1)
	assert.Equal(t, c.people[0]["rbtree2.go"], 1)
	assert.Equal(t, len(c.people[1]), 3)
	assert.Equal(t, c.people[1]["README.md"], 1)
	assert.Equal(t, c.people[1]["analyser.go"], 1)
	assert.Equal(t, c.people[1]["rbtree2.go"], 1)
	assert.Equal(t, len(c.people[2]), 1)
	assert.Equal(t, c.people[2]["file_test.go"], 1)
	assert.Equal(t, len(c.files["README.md"]), 3)
	assert.Equal(t, c.files["README.md"], map[string]int{
		"README.md":    2,
		"analyser.go":  2,
		"file_test.go": 1,
	})
	assert.Equal(t, c.files["LICENSE2"], map[string]int{
		"LICENSE2":   1,
		"file2.go":   1,
		"rbtree2.go": 1,
	})
	assert.Equal(t, c.files["file2.go"], map[string]int{
		"LICENSE2":   1,
		"file2.go":   1,
		"rbtree2.go": 1,
	})
	assert.Equal(t, c.files["rbtree2.go"], map[string]int{
		"LICENSE2":   1,
		"file2.go":   1,
		"rbtree2.go": 1,
	})
	assert.Equal(t, c.files["analyser.go"], map[string]int{
		"analyser.go":  2,
		"README.md":    2,
		"file_test.go": 1,
	})
	assert.Equal(t, c.files["file_test.go"], map[string]int{
		"file_test.go": 2,
		"README.md":    1,
		"analyser.go":  1,
	})
	assert.Equal(t, c.peopleCommits[0], 2)
	assert.Equal(t, c.peopleCommits[1], 1)
	assert.Equal(t, c.peopleCommits[2], 1)
	cr := c.Finalize().(CouplesResult)
	assert.Equal(t, len(cr.Files), 3)
	assert.Equal(t, cr.Files[0], "README.md")
	assert.Equal(t, cr.Files[1], "analyser.go")
	assert.Equal(t, cr.Files[2], "file_test.go")
	assert.Equal(t, len(cr.FilesLines), 3)
	assert.Equal(t, cr.FilesLines[0], 15)
	assert.Equal(t, cr.FilesLines[1], 252)
	assert.Equal(t, cr.FilesLines[2], 238)
	assert.Equal(t, len(cr.PeopleFiles[0]), 3)
	assert.Equal(t, cr.PeopleFiles[0][0], 0)
	assert.Equal(t, cr.PeopleFiles[0][1], 1)
	assert.Equal(t, cr.PeopleFiles[0][2], 2)
	assert.Equal(t, len(cr.PeopleFiles[1]), 2)
	assert.Equal(t, cr.PeopleFiles[1][0], 0)
	assert.Equal(t, cr.PeopleFiles[1][1], 1)
	assert.Equal(t, len(cr.PeopleFiles[2]), 1)
	assert.Equal(t, cr.PeopleFiles[2][0], 2)
	assert.Equal(t, len(cr.PeopleMatrix[0]), 3)
	assert.Equal(t, cr.PeopleMatrix[0][0], int64(7))
	assert.Equal(t, cr.PeopleMatrix[0][1], int64(3))
	assert.Equal(t, cr.PeopleMatrix[0][2], int64(1))
	assert.Equal(t, len(cr.PeopleMatrix[1]), 2)
	assert.Equal(t, cr.PeopleMatrix[1][0], int64(3))
	assert.Equal(t, cr.PeopleMatrix[1][1], int64(3))
	assert.Equal(t, len(cr.PeopleMatrix[2]), 2)
	assert.Equal(t, cr.PeopleMatrix[2][0], int64(1))
	assert.Equal(t, cr.PeopleMatrix[2][2], int64(1))
	assert.Equal(t, len(cr.FilesMatrix), 3)
	assert.Equal(t, len(cr.FilesMatrix[0]), 3)
	assert.Equal(t, cr.FilesMatrix[0][2], int64(1))
	assert.Equal(t, cr.FilesMatrix[0][0], int64(2))
	assert.Equal(t, cr.FilesMatrix[0][1], int64(2))
	assert.Equal(t, len(cr.FilesMatrix[1]), 3)
	assert.Equal(t, cr.FilesMatrix[1][2], int64(1))
	assert.Equal(t, cr.FilesMatrix[1][0], int64(2))
	assert.Equal(t, cr.FilesMatrix[1][1], int64(2))
	assert.Equal(t, len(cr.FilesMatrix[2]), 3)
	assert.Equal(t, cr.FilesMatrix[2][0], int64(1))
	assert.Equal(t, cr.FilesMatrix[2][1], int64(1))
	assert.Equal(t, cr.FilesMatrix[2][2], int64(3))
}

func TestCouplesConsumeFinalizeMerge(t *testing.T) {
	c := fixtureCouples()
	deps := map[string]interface{}{}
	deps[identity.DependencyAuthor] = 0
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(gitplumbing.NewHash(
		"a3ee37f91f0d705ec9c41ae88426f0ae44b2fbc3"))
	deps[core.DependencyIsMerge] = true
	deps[plumbing.DependencyTreeChanges] = generateChanges("+LICENSE2", "+file2.go")
	c.Consume(deps)
	deps[plumbing.DependencyTreeChanges] = generateChanges("+file2.go", "-LICENSE2", ">file2.go>file_test.go")
	c.Consume(deps)
	assert.Equal(t, len(c.people[0]), 3)
	assert.Equal(t, c.people[0]["LICENSE2"], 1)
	assert.Equal(t, c.people[0]["file2.go"], 1)
	assert.Equal(t, c.people[0]["file_test.go"], 1)
	for i := 1; i < 3; i++ {
		assert.Equal(t, len(c.people[i]), 0)
	}
	assert.Equal(t, c.files["LICENSE2"], map[string]int{
		"LICENSE2": 1,
		"file2.go": 1,
	})
	assert.Equal(t, c.files["file2.go"], map[string]int{
		"file2.go": 1,
		"LICENSE2": 1,
	})
	assert.Equal(t, c.files["file_test.go"], map[string]int{
		"file_test.go": 1,
	})
}

func TestCouplesConsumeFinalizeAuthorMissing(t *testing.T) {
	c := fixtureCouples()
	deps := map[string]interface{}{}
	deps[identity.DependencyAuthor] = 0
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(gitplumbing.NewHash(
		"a3ee37f91f0d705ec9c41ae88426f0ae44b2fbc3"))
	deps[core.DependencyIsMerge] = false
	deps[plumbing.DependencyTreeChanges] = generateChanges("+LICENSE2", "+file2.go", "+rbtree2.go")
	c.Consume(deps)
	deps[plumbing.DependencyTreeChanges] = generateChanges("+README.md", "-LICENSE2", "=analyser.go", ">file2.go>file_test.go")
	c.Consume(deps)
	deps[identity.DependencyAuthor] = 1
	deps[plumbing.DependencyTreeChanges] = generateChanges("=README.md", "=analyser.go", "-rbtree2.go")
	c.Consume(deps)
	deps[identity.DependencyAuthor] = identity.AuthorMissing
	deps[plumbing.DependencyTreeChanges] = generateChanges("=file_test.go")
	c.Consume(deps)
	assert.Equal(t, len(c.people[0]), 6)
	assert.Equal(t, c.people[0]["README.md"], 1)
	assert.Equal(t, c.people[0]["LICENSE2"], 2)
	assert.Equal(t, c.people[0]["analyser.go"], 1)
	assert.Equal(t, c.people[0]["file2.go"], 1)
	assert.Equal(t, c.people[0]["file_test.go"], 1)
	assert.Equal(t, c.people[0]["rbtree2.go"], 1)
	assert.Equal(t, len(c.people[1]), 3)
	assert.Equal(t, c.people[1]["README.md"], 1)
	assert.Equal(t, c.people[1]["analyser.go"], 1)
	assert.Equal(t, c.people[1]["rbtree2.go"], 1)
	assert.Equal(t, len(c.people[2]), 0)
	assert.Equal(t, len(c.files["README.md"]), 3)
	assert.Equal(t, c.files["README.md"], map[string]int{
		"README.md":    2,
		"analyser.go":  2,
		"file_test.go": 1,
	})
	assert.Equal(t, c.files["LICENSE2"], map[string]int{
		"LICENSE2":   1,
		"file2.go":   1,
		"rbtree2.go": 1,
	})
	assert.Equal(t, c.files["file2.go"], map[string]int{
		"LICENSE2":   1,
		"file2.go":   1,
		"rbtree2.go": 1,
	})
	assert.Equal(t, c.files["rbtree2.go"], map[string]int{
		"LICENSE2":   1,
		"file2.go":   1,
		"rbtree2.go": 1,
	})
	assert.Equal(t, c.files["analyser.go"], map[string]int{
		"analyser.go":  2,
		"README.md":    2,
		"file_test.go": 1,
	})
	assert.Equal(t, c.files["file_test.go"], map[string]int{
		"file_test.go": 2,
		"README.md":    1,
		"analyser.go":  1,
	})
	assert.Equal(t, c.peopleCommits[0], 2)
	assert.Equal(t, c.peopleCommits[1], 1)
	assert.Equal(t, c.peopleCommits[2], 0)
	cr := c.Finalize().(CouplesResult)
	assert.Equal(t, len(cr.Files), 3)
	assert.Equal(t, cr.Files[0], "README.md")
	assert.Equal(t, cr.Files[1], "analyser.go")
	assert.Equal(t, cr.Files[2], "file_test.go")
	assert.Equal(t, len(cr.FilesLines), 3)
	assert.Equal(t, cr.FilesLines[0], 15)
	assert.Equal(t, cr.FilesLines[1], 252)
	assert.Equal(t, cr.FilesLines[2], 238)
	assert.Equal(t, len(cr.PeopleFiles[0]), 3)
	assert.Equal(t, cr.PeopleFiles[0][0], 0)
	assert.Equal(t, cr.PeopleFiles[0][1], 1)
	assert.Equal(t, cr.PeopleFiles[0][2], 2)
	assert.Equal(t, len(cr.PeopleFiles[1]), 2)
	assert.Equal(t, cr.PeopleFiles[1][0], 0)
	assert.Equal(t, cr.PeopleFiles[1][1], 1)
	assert.Equal(t, len(cr.PeopleFiles[2]), 0)
	assert.Equal(t, len(cr.PeopleMatrix[0]), 3)
	assert.Equal(t, cr.PeopleMatrix[0][0], int64(7))
	assert.Equal(t, cr.PeopleMatrix[0][1], int64(3))
	assert.Equal(t, cr.PeopleMatrix[0][2], int64(0))
	assert.Equal(t, len(cr.PeopleMatrix[1]), 2)
	assert.Equal(t, cr.PeopleMatrix[1][0], int64(3))
	assert.Equal(t, cr.PeopleMatrix[1][1], int64(3))
	assert.Equal(t, len(cr.PeopleMatrix[2]), 0)
	assert.Equal(t, len(cr.FilesMatrix), 3)
	assert.Equal(t, len(cr.FilesMatrix[0]), 3)
	assert.Equal(t, cr.FilesMatrix[0][2], int64(1))
	assert.Equal(t, cr.FilesMatrix[0][0], int64(2))
	assert.Equal(t, cr.FilesMatrix[0][1], int64(2))
	assert.Equal(t, len(cr.FilesMatrix[1]), 3)
	assert.Equal(t, cr.FilesMatrix[1][2], int64(1))
	assert.Equal(t, cr.FilesMatrix[1][0], int64(2))
	assert.Equal(t, cr.FilesMatrix[1][1], int64(2))
	assert.Equal(t, len(cr.FilesMatrix[2]), 3)
	assert.Equal(t, cr.FilesMatrix[2][0], int64(1))
	assert.Equal(t, cr.FilesMatrix[2][1], int64(1))
	assert.Equal(t, cr.FilesMatrix[2][2], int64(3))
}

func TestCouplesConsumeManyFiles(t *testing.T) {
	c := fixtureCouples()
	deps := map[string]interface{}{}
	deps[identity.DependencyAuthor] = 0
	deps[core.DependencyCommit], _ = test.Repository.CommitObject(gitplumbing.NewHash(
		"a3ee37f91f0d705ec9c41ae88426f0ae44b2fbc3"))
	deps[core.DependencyIsMerge] = false
	changes := make(object.Changes, CouplesMaximumMeaningfulContextSize+1)
	for i := 0; i < len(changes); i++ {
		changes[i] = &object.Change{
			From: object.ChangeEntry{},
			To:   object.ChangeEntry{Name: string(i)},
		}
	}
	deps[plumbing.DependencyTreeChanges] = changes
	_, err := c.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, len(c.people[0]), len(changes))
	assert.Len(t, c.files, 0)
}

func TestCouplesFork(t *testing.T) {
	couples1 := fixtureCouples()
	clones := couples1.Fork(1)
	assert.Len(t, clones, 1)
	couples2 := clones[0].(*CouplesAnalysis)
	assert.True(t, couples1 != couples2)
	assert.Equal(t, *couples1, *couples2)
	couples1.Merge([]core.PipelineItem{couples2})
}

func TestCouplesSerialize(t *testing.T) {
	c := fixtureCouples()
	result := CouplesResult{
		PeopleMatrix: []map[int]int64{
			{0: 7, 1: 3, 2: 1}, {0: 3, 1: 3}, {0: 1, 2: 1}, {},
		},
		PeopleFiles: [][]int{
			{0, 1, 2}, {1, 2}, {0}, {},
		},
		FilesMatrix: []map[int]int64{
			{1: 1, 2: 1, 0: 3}, {1: 2, 2: 2, 0: 1}, {2: 2, 0: 1, 1: 2},
		},
		Files:              []string{"five", "one", "three"},
		FilesLines:         []int{9, 8, 7},
		reversedPeopleDict: []string{"p1", "p2", "p3"},
	}
	buffer := &bytes.Buffer{}
	assert.Nil(t, c.Serialize(result, false, buffer))
	assert.Equal(t, buffer.String(), `  files_coocc:
    index:
      - "five"
      - "one"
      - "three"
    lines:
      - 9
      - 8
      - 7
    matrix:
      - {0: 3, 1: 1, 2: 1}
      - {0: 1, 1: 2, 2: 2}
      - {0: 1, 1: 2, 2: 2}
  people_coocc:
    index:
      - "p1"
      - "p2"
      - "p3"
    matrix:
      - {0: 7, 1: 3, 2: 1}
      - {0: 3, 1: 3}
      - {0: 1, 2: 1}
      - {}
    author_files:
      - "p3":
        - "five"
      - "p2":
        - "one"
        - "three"
      - "p1":
        - "five"
        - "one"
        - "three"
`)
	buffer = &bytes.Buffer{}
	assert.Nil(t, c.Serialize(result, true, buffer))
	msg := pb.CouplesAnalysisResults{}
	assert.Nil(t, proto.Unmarshal(buffer.Bytes(), &msg))
	assert.Equal(t, msg.FilesLines, []int32{9, 8, 7})
	assert.Len(t, msg.PeopleFiles, 3)
	tmp1 := [...]int32{0, 1, 2}
	assert.Equal(t, msg.PeopleFiles[0].Files, tmp1[:])
	tmp2 := [...]int32{1, 2}
	assert.Equal(t, msg.PeopleFiles[1].Files, tmp2[:])
	tmp3 := [...]int32{0}
	assert.Equal(t, msg.PeopleFiles[2].Files, tmp3[:])
	assert.Equal(t, msg.PeopleCouples.Index, result.reversedPeopleDict)
	assert.Equal(t, msg.PeopleCouples.Matrix.NumberOfRows, int32(4))
	assert.Equal(t, msg.PeopleCouples.Matrix.NumberOfColumns, int32(4))
	data := [...]int64{7, 3, 1, 3, 3, 1, 1}
	assert.Equal(t, msg.PeopleCouples.Matrix.Data, data[:])
	indices := [...]int32{0, 1, 2, 0, 1, 0, 2}
	assert.Equal(t, msg.PeopleCouples.Matrix.Indices, indices[:])
	indptr := [...]int64{0, 3, 5, 7, 7}
	assert.Equal(t, msg.PeopleCouples.Matrix.Indptr, indptr[:])
	files := [...]string{"five", "one", "three"}
	assert.Equal(t, msg.FileCouples.Index, files[:])
	assert.Equal(t, msg.FileCouples.Matrix.NumberOfRows, int32(3))
	assert.Equal(t, msg.FileCouples.Matrix.NumberOfColumns, int32(3))
	data2 := [...]int64{3, 1, 1, 1, 2, 2, 1, 2, 2}
	assert.Equal(t, msg.FileCouples.Matrix.Data, data2[:])
	indices2 := [...]int32{0, 1, 2, 0, 1, 2, 0, 1, 2}
	assert.Equal(t, msg.FileCouples.Matrix.Indices, indices2[:])
	indptr2 := [...]int64{0, 3, 6, 9}
	assert.Equal(t, msg.FileCouples.Matrix.Indptr, indptr2[:])
}

func TestCouplesDeserialize(t *testing.T) {
	message, err := ioutil.ReadFile(path.Join("..", "internal", "test_data", "couples.pb"))
	assert.Nil(t, err)
	couples := CouplesAnalysis{}
	iresult, err := couples.Deserialize(message)
	assert.Nil(t, err)
	result := iresult.(CouplesResult)
	assert.Len(t, result.reversedPeopleDict, 2)
	assert.Len(t, result.PeopleFiles, 2)
	assert.Len(t, result.PeopleMatrix, 3)
	assert.Len(t, result.Files, 74)
	assert.Len(t, result.FilesLines, 74)
	for _, v := range result.FilesLines {
		assert.True(t, v > 0)
	}
	assert.Len(t, result.FilesMatrix, 74)
}

func TestCouplesMerge(t *testing.T) {
	r1, r2 := CouplesResult{}, CouplesResult{}
	people1 := [...]string{"one", "two"}
	people2 := [...]string{"two", "three"}
	r1.reversedPeopleDict = people1[:]
	r2.reversedPeopleDict = people2[:]
	r1.Files = people1[:]
	r2.Files = people2[:]
	r1.FilesLines = []int{1, 2}
	r2.FilesLines = []int{2, 3}
	r1.PeopleFiles = make([][]int, 2)
	r1.PeopleFiles[0] = make([]int, 2)
	r1.PeopleFiles[0][0] = 0
	r1.PeopleFiles[0][1] = 1
	r1.PeopleFiles[1] = make([]int, 1)
	r1.PeopleFiles[1][0] = 0
	r2.PeopleFiles = make([][]int, 2)
	r2.PeopleFiles[0] = make([]int, 1)
	r2.PeopleFiles[0][0] = 1
	r2.PeopleFiles[1] = make([]int, 2)
	r2.PeopleFiles[1][0] = 0
	r2.PeopleFiles[1][1] = 1
	r1.FilesMatrix = make([]map[int]int64, 2)
	r1.FilesMatrix[0] = map[int]int64{}
	r1.FilesMatrix[1] = map[int]int64{}
	r1.FilesMatrix[0][1] = 100
	r1.FilesMatrix[1][0] = 100
	r2.FilesMatrix = make([]map[int]int64, 2)
	r2.FilesMatrix[0] = map[int]int64{}
	r2.FilesMatrix[1] = map[int]int64{}
	r2.FilesMatrix[0][1] = 200
	r2.FilesMatrix[1][0] = 200
	r1.PeopleMatrix = make([]map[int]int64, 3)
	r1.PeopleMatrix[0] = map[int]int64{}
	r1.PeopleMatrix[1] = map[int]int64{}
	r1.PeopleMatrix[2] = map[int]int64{}
	r1.PeopleMatrix[0][1] = 100
	r1.PeopleMatrix[1][0] = 100
	r1.PeopleMatrix[2][0] = 300
	r1.PeopleMatrix[2][1] = 400
	r2.PeopleMatrix = make([]map[int]int64, 3)
	r2.PeopleMatrix[0] = map[int]int64{}
	r2.PeopleMatrix[1] = map[int]int64{}
	r2.PeopleMatrix[2] = map[int]int64{}
	r2.PeopleMatrix[0][1] = 10
	r2.PeopleMatrix[1][0] = 10
	r2.PeopleMatrix[2][0] = 30
	r2.PeopleMatrix[2][1] = 40
	couples := CouplesAnalysis{}
	merged := couples.MergeResults(r1, r2, nil, nil).(CouplesResult)
	mergedPeople := [...]string{"one", "two", "three"}
	assert.Equal(t, merged.reversedPeopleDict, mergedPeople[:])
	assert.Equal(t, merged.Files, mergedPeople[:])
	assert.Equal(t, merged.FilesLines, []int{1, 4, 3})
	assert.Len(t, merged.PeopleFiles, 3)
	assert.Equal(t, merged.PeopleFiles[0], getSlice(0, 1))
	assert.Equal(t, merged.PeopleFiles[1], getSlice(0, 2))
	assert.Equal(t, merged.PeopleFiles[2], getSlice(1, 2))
	assert.Len(t, merged.PeopleMatrix, 4)
	assert.Equal(t, merged.PeopleMatrix[0], getCouplesMap(1, 100))
	assert.Equal(t, merged.PeopleMatrix[1], getCouplesMap(0, 100, 2, 10))
	assert.Equal(t, merged.PeopleMatrix[2], getCouplesMap(1, 10))
	assert.Equal(t, merged.PeopleMatrix[3], getCouplesMap(0, 300, 1, 430, 2, 40))
	assert.Len(t, merged.FilesMatrix, 3)
	assert.Equal(t, merged.FilesMatrix[0], getCouplesMap(1, 100))
	assert.Equal(t, merged.FilesMatrix[1], getCouplesMap(0, 100, 2, 200))
	assert.Equal(t, merged.FilesMatrix[2], getCouplesMap(1, 200))
}

func TestCouplesCurrentFiles(t *testing.T) {
	c := fixtureCouples()
	c.lastCommit, _ = test.Repository.CommitObject(gitplumbing.NewHash(
		"cce947b98a050c6d356bc6ba95030254914027b1"))
	files := c.currentFiles()
	assert.Equal(t, files, map[string]bool{".gitignore": true, "LICENSE": true})
}

func TestCouplesPropagateRenames(t *testing.T) {
	c := fixtureCouples()
	c.files["one"] = map[string]int{
		"one":   1,
		"two":   2,
		"three": 3,
	}
	c.files["two"] = map[string]int{
		"one":   2,
		"two":   10,
		"three": 1,
		"four":  7,
	}
	c.files["three"] = map[string]int{
		"one":   3,
		"two":   1,
		"three": 3,
		"four":  2,
	}
	c.files["four"] = map[string]int{
		"two":   7,
		"three": 3,
		"four":  1,
	}
	c.PeopleNumber = 1
	c.people = make([]map[string]int, 1)
	c.people[0] = map[string]int{}
	c.people[0]["one"] = 1
	c.people[0]["two"] = 2
	c.people[0]["three"] = 3
	c.people[0]["four"] = 4
	*c.renames = []rename{{ToName: "four", FromName: "one"}}
	files, people := c.propagateRenames(map[string]bool{"two": true, "three": true, "four": true})
	assert.Len(t, files, 3)
	assert.Len(t, people, 1)
	assert.Equal(t, files["two"], map[string]int{"two": 10, "three": 1, "four": 9})
	assert.Equal(t, files["three"], map[string]int{"two": 1, "three": 3, "four": 6})
	assert.Equal(t, files["four"], map[string]int{"two": 9, "three": 6, "four": 2})
	assert.Equal(t, people[0], map[string]int{"two": 2, "three": 3, "four": 5})
}

func getSlice(vals ...int) []int {
	return vals
}

func getCouplesMap(vals ...int) map[int]int64 {
	res := map[int]int64{}
	for i := 0; i < len(vals); i += 2 {
		res[vals[i]] = int64(vals[i+1])
	}
	return res
}
