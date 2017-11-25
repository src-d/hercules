package hercules

import (
	"bytes"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v3/pb"
)

func fixtureCouples() *CouplesAnalysis {
	c := CouplesAnalysis{PeopleNumber: 3}
	c.Initialize(testRepository)
	return &c
}

func TestCouplesMeta(t *testing.T) {
	c := fixtureCouples()
	assert.Equal(t, c.Name(), "Couples")
	assert.Equal(t, len(c.Provides()), 0)
	assert.Equal(t, len(c.Requires()), 2)
	assert.Equal(t, c.Requires()[0], "author")
	assert.Equal(t, c.Requires()[1], "changes")
	assert.Equal(t, c.Flag(), "couples")
	assert.Len(t, c.ListConfigurationOptions(), 0)
}

func TestCouplesRegistration(t *testing.T) {
	tp, exists := Registry.registered[(&CouplesAnalysis{}).Name()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "CouplesAnalysis")
	tp, exists = Registry.flags[(&CouplesAnalysis{}).Flag()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "CouplesAnalysis")
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
	deps["author"] = 0
	deps["changes"] = generateChanges("+two", "+four", "+six")
	c.Consume(deps)
	deps["changes"] = generateChanges("+one", "-two", "=three", ">four>five")
	c.Consume(deps)
	deps["author"] = 1
	deps["changes"] = generateChanges("=one", "=three", "-six")
	c.Consume(deps)
	deps["author"] = 2
	deps["changes"] = generateChanges("=five")
	c.Consume(deps)
	assert.Equal(t, len(c.people[0]), 5)
	assert.Equal(t, c.people[0]["one"], 1)
	assert.Equal(t, c.people[0]["two"], 2)
	assert.Equal(t, c.people[0]["three"], 1)
	assert.Equal(t, c.people[0]["five"], 2)
	assert.Equal(t, c.people[0]["six"], 1)
	assert.Equal(t, len(c.people[1]), 3)
	assert.Equal(t, c.people[1]["one"], 1)
	assert.Equal(t, c.people[1]["three"], 1)
	assert.Equal(t, c.people[1]["six"], 1)
	assert.Equal(t, len(c.people[2]), 1)
	assert.Equal(t, c.people[2]["five"], 1)
	assert.Equal(t, len(c.files["one"]), 3)
	assert.Equal(t, c.files["one"]["one"], 2)
	assert.Equal(t, c.files["one"]["three"], 2)
	assert.Equal(t, c.files["one"]["five"], 1)
	assert.NotContains(t, c.files, "two")
	assert.NotContains(t, c.files, "four")
	assert.NotContains(t, c.files, "six")
	assert.Equal(t, len(c.files["three"]), 3)
	assert.Equal(t, c.files["three"]["three"], 2)
	assert.Equal(t, c.files["three"]["one"], 2)
	assert.Equal(t, c.files["three"]["five"], 1)
	assert.Equal(t, len(c.files["five"]), 3)
	assert.Equal(t, c.files["five"]["five"], 3)
	assert.Equal(t, c.files["five"]["one"], 1)
	assert.Equal(t, c.files["five"]["three"], 1)
	assert.Equal(t, c.people_commits[0], 2)
	assert.Equal(t, c.people_commits[1], 1)
	assert.Equal(t, c.people_commits[2], 1)
	cr := c.Finalize().(CouplesResult)
	assert.Equal(t, len(cr.Files), 3)
	assert.Equal(t, cr.Files[0], "five")
	assert.Equal(t, cr.Files[1], "one")
	assert.Equal(t, cr.Files[2], "three")
	assert.Equal(t, len(cr.PeopleFiles[0]), 3)
	assert.Equal(t, cr.PeopleFiles[0][0], 0)
	assert.Equal(t, cr.PeopleFiles[0][1], 1)
	assert.Equal(t, cr.PeopleFiles[0][2], 2)
	assert.Equal(t, len(cr.PeopleFiles[1]), 2)
	assert.Equal(t, cr.PeopleFiles[1][0], 1)
	assert.Equal(t, cr.PeopleFiles[1][1], 2)
	assert.Equal(t, len(cr.PeopleFiles[2]), 1)
	assert.Equal(t, cr.PeopleFiles[2][0], 0)
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
	assert.Equal(t, cr.FilesMatrix[0][0], int64(3))
	assert.Equal(t, cr.FilesMatrix[0][1], int64(1))
	assert.Equal(t, cr.FilesMatrix[0][2], int64(1))
	assert.Equal(t, len(cr.FilesMatrix[1]), 3)
	assert.Equal(t, cr.FilesMatrix[1][0], int64(1))
	assert.Equal(t, cr.FilesMatrix[1][1], int64(2))
	assert.Equal(t, cr.FilesMatrix[1][2], int64(2))
	assert.Equal(t, len(cr.FilesMatrix[2]), 3)
	assert.Equal(t, cr.FilesMatrix[2][0], int64(1))
	assert.Equal(t, cr.FilesMatrix[2][1], int64(2))
	assert.Equal(t, cr.FilesMatrix[2][2], int64(2))
}

func TestCouplesSerialize(t *testing.T) {
	c := fixtureCouples()
	c.PeopleNumber = 1
	people := [...]string{"p1", "p2", "p3"}
	facts := map[string]interface{}{}
	c.Configure(facts)
	assert.Equal(t, c.PeopleNumber, 1)
	facts[FactIdentityDetectorPeopleCount] = 3
	facts[FactIdentityDetectorReversedPeopleDict] = people[:]
	c.Configure(facts)
	assert.Equal(t, c.PeopleNumber, 3)
	deps := map[string]interface{}{}
	deps["author"] = 0
	deps["changes"] = generateChanges("+two", "+four", "+six")
	c.Consume(deps)
	deps["changes"] = generateChanges("+one", "-two", "=three", ">four>five")
	c.Consume(deps)
	deps["author"] = 1
	deps["changes"] = generateChanges("=one", "=three", "-six")
	c.Consume(deps)
	deps["author"] = 2
	deps["changes"] = generateChanges("=five")
	c.Consume(deps)
	result := c.Finalize().(CouplesResult)
	buffer := &bytes.Buffer{}
	c.Serialize(result, false, buffer)
	assert.Equal(t, buffer.String(), `  files_coocc:
    index:
      - "five"
      - "one"
      - "three"
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
	c.Serialize(result, true, buffer)
	msg := pb.CouplesAnalysisResults{}
	proto.Unmarshal(buffer.Bytes(), &msg)
	assert.Len(t, msg.TouchedFiles.Developers, 3)
	tmp1 := [...]int32{0, 1, 2}
	assert.Equal(t, msg.TouchedFiles.Developers[0].Files, tmp1[:])
	tmp2 := [...]int32{1, 2}
	assert.Equal(t, msg.TouchedFiles.Developers[1].Files, tmp2[:])
	tmp3 := [...]int32{0}
	assert.Equal(t, msg.TouchedFiles.Developers[2].Files, tmp3[:])
	assert.Equal(t, msg.DeveloperCouples.Index, people[:])
	assert.Equal(t, msg.DeveloperCouples.Matrix.NumberOfRows, int32(4))
	assert.Equal(t, msg.DeveloperCouples.Matrix.NumberOfColumns, int32(4))
	data := [...]int64{7, 3, 1, 3, 3, 1, 1}
	assert.Equal(t, msg.DeveloperCouples.Matrix.Data, data[:])
	indices := [...]int32{0, 1, 2, 0, 1, 0, 2}
	assert.Equal(t, msg.DeveloperCouples.Matrix.Indices, indices[:])
	indptr := [...]int64{0, 3, 5, 7, 7}
	assert.Equal(t, msg.DeveloperCouples.Matrix.Indptr, indptr[:])
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
