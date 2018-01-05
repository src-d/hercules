package hercules

import (
	"io/ioutil"
	"path"
	"testing"

	"bytes"
	"github.com/gogo/protobuf/proto"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/stretchr/testify/assert"
	"gopkg.in/bblfsh/sdk.v1/uast"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/hercules.v3/pb"
)

func fixtureShotness() *ShotnessAnalysis {
	sh := &ShotnessAnalysis{}
	sh.Initialize(testRepository)
	sh.Configure(nil)
	return sh
}

func TestShotnessMeta(t *testing.T) {
	sh := &ShotnessAnalysis{}
	sh.Initialize(testRepository)
	assert.NotNil(t, sh.nodes)
	assert.NotNil(t, sh.files)
	assert.Equal(t, sh.Name(), "Shotness")
	assert.Len(t, sh.Provides(), 0)
	assert.Equal(t, len(sh.Requires()), 2)
	assert.Equal(t, sh.Requires()[0], DependencyFileDiff)
	assert.Equal(t, sh.Requires()[1], DependencyUastChanges)
	assert.Len(t, sh.ListConfigurationOptions(), 2)
	assert.Equal(t, sh.ListConfigurationOptions()[0].Name, ConfigShotnessXpathStruct)
	assert.Equal(t, sh.ListConfigurationOptions()[1].Name, ConfigShotnessXpathName)
	sh.Configure(nil)
	assert.Equal(t, sh.XpathStruct, DefaultShotnessXpathStruct)
	assert.Equal(t, sh.XpathName, DefaultShotnessXpathName)
	facts := map[string]interface{}{}
	facts[ConfigShotnessXpathStruct] = "xpath!"
	facts[ConfigShotnessXpathName] = "another!"
	sh.Configure(facts)
	assert.Equal(t, sh.XpathStruct, "xpath!")
	assert.Equal(t, sh.XpathName, "another!")
	features := sh.Features()
	assert.Len(t, features, 1)
	assert.Equal(t, features[0], FeatureUast)
}

func TestShotnessRegistration(t *testing.T) {
	tp, exists := Registry.registered[(&ShotnessAnalysis{}).Name()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "ShotnessAnalysis")
	tp, exists = Registry.flags[(&ShotnessAnalysis{}).Flag()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "ShotnessAnalysis")
}

func bakeShotness(t *testing.T, eraseEndPosition bool) (*ShotnessAnalysis, ShotnessResult) {
	sh := fixtureShotness()
	bytes1, err := ioutil.ReadFile(path.Join("test_data", "1.java"))
	assert.Nil(t, err)
	bytes2, err := ioutil.ReadFile(path.Join("test_data", "2.java"))
	assert.Nil(t, err)
	dmp := diffmatchpatch.New()
	src, dst, _ := dmp.DiffLinesToRunes(string(bytes1), string(bytes2))
	state := map[string]interface{}{}
	state["commit"] = &object.Commit{}
	fileDiffs := map[string]FileDiffData{}
	const fileName = "test.java"
	fileDiffs[fileName] = FileDiffData{
		OldLinesOfCode: len(src),
		NewLinesOfCode: len(dst),
		Diffs:          dmp.DiffMainRunes(src, dst, false),
	}
	state[DependencyFileDiff] = fileDiffs
	uastChanges := make([]UASTChange, 1)
	loadUast := func(name string) *uast.Node {
		bytes, err := ioutil.ReadFile(path.Join("test_data", name))
		assert.Nil(t, err)
		node := uast.Node{}
		proto.Unmarshal(bytes, &node)
		if eraseEndPosition {
			VisitEachNode(&node, func(child *uast.Node) {
				child.EndPosition = nil
			})
		}
		return &node
	}
	state[DependencyUastChanges] = uastChanges
	uastChanges[0] = UASTChange{
		Change: &object.Change{
			From: object.ChangeEntry{},
			To:   object.ChangeEntry{Name: fileName}},
		Before: nil, After: loadUast("uast1.pb"),
	}
	iresult, err := sh.Consume(state)
	assert.Nil(t, err)
	assert.Nil(t, iresult)
	uastChanges[0] = UASTChange{
		Change: &object.Change{
			From: object.ChangeEntry{Name: fileName},
			To:   object.ChangeEntry{Name: fileName}},
		Before: loadUast("uast1.pb"), After: loadUast("uast2.pb"),
	}
	iresult, err = sh.Consume(state)
	assert.Nil(t, err)
	assert.Nil(t, iresult)
	return sh, sh.Finalize().(ShotnessResult)
}

func TestShotnessConsume(t *testing.T) {
	sh := fixtureShotness()
	bytes1, err := ioutil.ReadFile(path.Join("test_data", "1.java"))
	assert.Nil(t, err)
	bytes2, err := ioutil.ReadFile(path.Join("test_data", "2.java"))
	assert.Nil(t, err)
	dmp := diffmatchpatch.New()
	src, dst, _ := dmp.DiffLinesToRunes(string(bytes1), string(bytes2))
	state := map[string]interface{}{}
	state["commit"] = &object.Commit{}
	fileDiffs := map[string]FileDiffData{}
	const fileName = "test.java"
	const newfileName = "new.java"
	fileDiffs[fileName] = FileDiffData{
		OldLinesOfCode: len(src),
		NewLinesOfCode: len(dst),
		Diffs:          dmp.DiffMainRunes(src, dst, false),
	}
	state[DependencyFileDiff] = fileDiffs
	uastChanges := make([]UASTChange, 1)
	loadUast := func(name string) *uast.Node {
		bytes, err := ioutil.ReadFile(path.Join("test_data", name))
		assert.Nil(t, err)
		node := uast.Node{}
		proto.Unmarshal(bytes, &node)
		return &node
	}
	state[DependencyUastChanges] = uastChanges
	uastChanges[0] = UASTChange{
		Change: &object.Change{
			From: object.ChangeEntry{},
			To:   object.ChangeEntry{Name: fileName}},
		Before: nil, After: loadUast("uast1.pb"),
	}
	iresult, err := sh.Consume(state)
	assert.Nil(t, err)
	assert.Nil(t, iresult)
	uastChanges[0] = UASTChange{
		Change: &object.Change{
			From: object.ChangeEntry{Name: fileName},
			To:   object.ChangeEntry{Name: newfileName}},
		Before: loadUast("uast1.pb"), After: loadUast("uast2.pb"),
	}
	fileDiffs[newfileName] = fileDiffs[fileName]
	delete(fileDiffs, fileName)
	iresult, err = sh.Consume(state)
	assert.Nil(t, err)
	assert.Nil(t, iresult)
	assert.Len(t, sh.nodes, 18)
	assert.Len(t, sh.files, 1)
	assert.Len(t, sh.files["new.java"], 18)
	for _, node := range sh.nodes {
		assert.Equal(t, node.Summary.InternalRole, "MethodDeclaration")
		if node.Summary.Name != "testUnpackEntryFromFile" {
			assert.Equal(t, node.Count, 1)
			if node.Summary.Name != "testUnpackEntryFromStreamToFile" {
				assert.Len(t, node.Couples, 16)
			} else {
				assert.Len(t, node.Couples, 1)
			}
		} else {
			assert.Equal(t, node.Count, 2)
			assert.Len(t, node.Couples, 17)
		}
	}
	result := sh.Finalize().(ShotnessResult)
	assert.Len(t, result.Nodes, 18)
	assert.Len(t, result.Counters, 18)
	assert.Equal(t, result.Nodes[14].String(),
		"MethodDeclaration_testUnpackEntryFromStreamToFile_"+newfileName)
	assert.Equal(t, result.Counters[14], map[int]int{14: 1, 13: 1})
	assert.Equal(t, result.Nodes[15].String(),
		"MethodDeclaration_testUnpackEntryFromStream_"+newfileName)
	assert.Equal(t, result.Counters[15], map[int]int{
		8: 1, 0: 1, 5: 1, 6: 1, 11: 1, 1: 1, 13: 1, 17: 1, 3: 1, 15: 1, 9: 1, 4: 1, 7: 1, 16: 1, 2: 1, 12: 1, 10: 1})
	uastChanges[0] = UASTChange{
		Change: &object.Change{
			From: object.ChangeEntry{Name: newfileName},
			To:   object.ChangeEntry{}},
		Before: loadUast("uast2.pb"), After: nil,
	}
	iresult, err = sh.Consume(state)
	assert.Nil(t, err)
	assert.Nil(t, iresult)
	assert.Len(t, sh.nodes, 0)
	assert.Len(t, sh.files, 0)
}

func TestShotnessConsumeNoEnd(t *testing.T) {
	_, result1 := bakeShotness(t, false)
	_, result2 := bakeShotness(t, true)
	assert.Equal(t, result1, result2)
}

func TestShotnessSerializeText(t *testing.T) {
	sh, result := bakeShotness(t, false)
	buffer := &bytes.Buffer{}
	sh.Serialize(result, false, buffer)
	assert.Equal(t, buffer.String(), `  - name: testAddEntry
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testArchiveEquals
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testContainsAnyEntry
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testDuplicateEntryAtAddOrReplace
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testDuplicateEntryAtAdd
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testDuplicateEntryAtReplace
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testPackEntries
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testPackEntry
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testPreserveRoot
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testRemoveDirs
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testRemoveEntry
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testRepackArchive
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testUnexplode
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testUnpackEntryFromFile
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":2,"14":1,"15":1,"16":1,"17":1}
  - name: testUnpackEntryFromStreamToFile
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"13":1,"14":1}
  - name: testUnpackEntryFromStream
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: testZipException
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,59,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
  - name: unexplodeWithException
    file: test.java
    internal_role: MethodDeclaration
    roles: [111,100,41,45]
    counters: {"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1,"10":1,"11":1,"12":1,"13":1,"15":1,"16":1,"17":1}
`)
}

func TestShotnessSerializeBinary(t *testing.T) {
	sh, result := bakeShotness(t, false)
	buffer := &bytes.Buffer{}
	sh.Serialize(result, true, buffer)
	message := pb.ShotnessAnalysisResults{}
	err := proto.Unmarshal(buffer.Bytes(), &message)
	assert.Nil(t, err)
	assert.Len(t, message.Records, 18)
	assert.Equal(t, message.Records[14].Name, "testUnpackEntryFromStreamToFile")
	assert.Equal(t, message.Records[14].Counters, map[int32]int32{14: 1, 13: 1})
}
