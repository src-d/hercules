package leaves

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	imports2 "github.com/src-d/imports"
	"github.com/stretchr/testify/assert"
	gitplumbing "gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/pb"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/identity"
	"gopkg.in/src-d/hercules.v10/internal/plumbing/imports"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func fixtureImportsPerDev() *ImportsPerDeveloper {
	d := ImportsPerDeveloper{}
	d.Initialize(test.Repository)
	people := [...]string{"one@srcd", "two@srcd"}
	d.reversedPeopleDict = people[:]
	return &d
}

func TestImportsPerDeveloperMeta(t *testing.T) {
	ipd := fixtureImportsPerDev()
	assert.Equal(t, ipd.Name(), "ImportsPerDeveloper")
	assert.Equal(t, len(ipd.Provides()), 0)
	assert.Equal(t, len(ipd.Requires()), 2)
	assert.Equal(t, ipd.Requires()[0], imports.DependencyImports)
	assert.Equal(t, ipd.Requires()[1], identity.DependencyAuthor)
	assert.Equal(t, ipd.Flag(), "imports-per-dev")
	assert.Len(t, ipd.ListConfigurationOptions(), 0)
	assert.True(t, len(ipd.Description()) > 0)
	logger := core.NewLogger()
	assert.NoError(t, ipd.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
		identity.FactIdentityDetectorReversedPeopleDict: []string{"1", "2"},
	}))
	assert.Equal(t, logger, ipd.l)
	assert.Equal(t, []string{"1", "2"}, ipd.reversedPeopleDict)
}

func TestImportsPerDeveloperRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&ImportsPerDeveloper{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "ImportsPerDeveloper")
	leaves := core.Registry.GetLeaves()
	matched := false
	for _, tp := range leaves {
		if tp.Flag() == (&ImportsPerDeveloper{}).Flag() {
			matched = true
			break
		}
	}
	assert.True(t, matched)
}

func TestImportsPerDeveloperInitialize(t *testing.T) {
	ipd := fixtureImportsPerDev()
	assert.NotNil(t, ipd.imports)
}

func TestImportsPerDeveloperConsumeFinalize(t *testing.T) {
	deps := map[string]interface{}{}
	deps[core.DependencyIsMerge] = false
	deps[identity.DependencyAuthor] = 0
	imps := map[gitplumbing.Hash]imports2.File{}
	imps[gitplumbing.NewHash("291286b4ac41952cbd1389fda66420ec03c1a9fe")] =
		imports2.File{Lang: "Go", Path: "test.go", Imports: []string{"sys"}}
	imps[gitplumbing.NewHash("c29112dbd697ad9b401333b80c18a63951bc18d9")] =
		imports2.File{Lang: "Python", Path: "test.py", Imports: []string{"sys"}}
	deps[imports.DependencyImports] = imps
	ipd := fixtureImportsPerDev()
	ipd.reversedPeopleDict = []string{"1", "2"}
	_, err := ipd.Consume(deps)
	assert.NoError(t, err)
	assert.Equal(t, map[int]map[string]map[string]int{
		0: {"Go": {"sys": 1}, "Python": {"sys": 1}},
	}, ipd.imports)
	_, err = ipd.Consume(deps)
	assert.NoError(t, err)
	assert.Equal(t, map[int]map[string]map[string]int{
		0: {"Go": {"sys": 2}, "Python": {"sys": 2}},
	}, ipd.imports)
	deps[identity.DependencyAuthor] = 1
	_, err = ipd.Consume(deps)
	assert.NoError(t, err)
	assert.Equal(t, map[int]map[string]map[string]int{
		0: {"Go": {"sys": 2}, "Python": {"sys": 2}},
		1: {"Go": {"sys": 1}, "Python": {"sys": 1}},
	}, ipd.imports)
	deps[core.DependencyIsMerge] = true
	_, err = ipd.Consume(deps)
	assert.NoError(t, err)
	assert.Equal(t, map[int]map[string]map[string]int{
		0: {"Go": {"sys": 2}, "Python": {"sys": 2}},
		1: {"Go": {"sys": 1}, "Python": {"sys": 1}},
	}, ipd.imports)
	result := ipd.Finalize().(ImportsPerDeveloperResult)
	assert.Equal(t, ipd.reversedPeopleDict, result.reversedPeopleDict)
	assert.Equal(t, ipd.imports, result.Imports)
}

func TestImportsPerDeveloperSerializeText(t *testing.T) {
	ipd := fixtureImportsPerDev()
	res := ImportsPerDeveloperResult{Imports: map[int]map[string]map[string]int{
		0: {"Go": {"sys": 2}, "Python": {"sys": 2}},
		1: {"Go": {"sys": 1}, "Python": {"sys": 1}},
	}, reversedPeopleDict: []string{"one", "two"}}
	buffer := &bytes.Buffer{}
	assert.NoError(t, ipd.Serialize(res, false, buffer))
	assert.Equal(t, `  - "one": {"Go":{"sys":2},"Python":{"sys":2}}
  - "two": {"Go":{"sys":1},"Python":{"sys":1}}
`, buffer.String())
}

func TestImportsPerDeveloperSerializeBinary(t *testing.T) {
	ipd := fixtureImportsPerDev()
	res := ImportsPerDeveloperResult{Imports: map[int]map[string]map[string]int{
		0: {"Go": {"sys": 2}, "Python": {"sys": 2}},
		1: {"Go": {"sys": 1}, "Python": {"sys": 1}},
	}, reversedPeopleDict: []string{"one", "two"}}
	buffer := &bytes.Buffer{}
	assert.NoError(t, ipd.Serialize(res, true, buffer))
	msg := pb.ImportsPerDeveloperResults{}
	assert.Nil(t, proto.Unmarshal(buffer.Bytes(), &msg))
	assert.Equal(t, `{[languages:<key:"Go" value:<counts:<key:"sys" value:2 > > > languages:<key:"Python" value:<counts:<key:"sys" value:2 > > >  languages:<key:"Go" value:<counts:<key:"sys" value:1 > > > languages:<key:"Python" value:<counts:<key:"sys" value:1 > > > ] [one two] {} [] 0}`, fmt.Sprint(msg))
}
