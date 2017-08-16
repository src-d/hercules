package hercules

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

func fixtureCouples() *Couples {
	c := Couples{PeopleNumber: 3}
	c.Initialize(testRepository)
	return &c
}

func TestCouplesMeta(t *testing.T) {
	c := fixtureCouples()
	assert.Equal(t, c.Name(), "Couples")
	assert.Equal(t, len(c.Provides()), 0)
	assert.Equal(t, len(c.Requires()), 2)
	assert.Equal(t, c.Requires()[0], "author")
	assert.Equal(t, c.Requires()[1], "renamed_changes")
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
	deps["renamed_changes"] = generateChanges("+two", "+four", "+six")
	c.Consume(deps)
	deps["renamed_changes"] = generateChanges("+one", "-two", "=three", ">four>five")
	c.Consume(deps)
	deps["author"] = 1
	deps["renamed_changes"] = generateChanges("=one", "=three", "-six")
	c.Consume(deps)
	deps["author"] = 2
	deps["renamed_changes"] = generateChanges("=five")
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
