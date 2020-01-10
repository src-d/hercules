package identity

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/hercules.v10/internal/core"
	"gopkg.in/src-d/hercules.v10/internal/test"
)

func fixtureIdentityDetector() *Detector {
	peopleDict := map[string]int{}
	peopleDict["vadim@sourced.tech"] = 0
	peopleDict["gmarkhor@gmail.com"] = 0
	reversePeopleDict := make([]string, 1)
	reversePeopleDict[0] = "Vadim"
	id := Detector{
		PeopleDict:         peopleDict,
		ReversedPeopleDict: reversePeopleDict,
	}
	id.Initialize(test.Repository)
	return &id
}

func TestIdentityDetectorMeta(t *testing.T) {
	id := fixtureIdentityDetector()
	assert.Equal(t, id.Name(), "IdentityDetector")
	assert.Equal(t, len(id.Requires()), 0)
	assert.Equal(t, len(id.Provides()), 1)
	assert.Equal(t, id.Provides()[0], DependencyAuthor)
	opts := id.ListConfigurationOptions()
	assert.Len(t, opts, 2)
	assert.Equal(t, opts[0].Name, ConfigIdentityDetectorPeopleDictPath)
	assert.Equal(t, opts[1].Name, ConfigIdentityDetectorExactSignatures)
	logger := core.NewLogger()
	assert.NoError(t, id.Configure(map[string]interface{}{
		core.ConfigLogger: logger,
	}))
	assert.Equal(t, logger, id.l)
}

func TestIdentityDetectorConfigure(t *testing.T) {
	id := fixtureIdentityDetector()
	facts := map[string]interface{}{}
	m1 := map[string]int{"one": 0}
	m2 := []string{"one"}
	facts[FactIdentityDetectorPeopleDict] = m1
	facts[FactIdentityDetectorReversedPeopleDict] = m2
	assert.Nil(t, id.Configure(facts))
	assert.Equal(t, m1, facts[FactIdentityDetectorPeopleDict])
	assert.Equal(t, m2, facts[FactIdentityDetectorReversedPeopleDict])
	assert.Equal(t, id.PeopleDict, facts[FactIdentityDetectorPeopleDict])
	assert.Equal(t, id.ReversedPeopleDict, facts[FactIdentityDetectorReversedPeopleDict])
	id = fixtureIdentityDetector()
	tmpf, err := ioutil.TempFile("", "hercules-test-")
	assert.Nil(t, err)
	defer os.Remove(tmpf.Name())
	_, err = tmpf.WriteString(`Egor|egor@sourced.tech
Vadim|vadim@sourced.tech`)
	assert.Nil(t, err)
	assert.Nil(t, tmpf.Close())
	delete(facts, FactIdentityDetectorPeopleDict)
	delete(facts, FactIdentityDetectorReversedPeopleDict)
	facts[ConfigIdentityDetectorPeopleDictPath] = tmpf.Name()
	assert.Nil(t, id.Configure(facts))
	assert.Len(t, id.PeopleDict, 2)
	assert.Len(t, id.ReversedPeopleDict, 1)
	assert.Equal(t, id.ReversedPeopleDict[0], "Vadim")
	delete(facts, FactIdentityDetectorPeopleDict)
	delete(facts, FactIdentityDetectorReversedPeopleDict)
	id = fixtureIdentityDetector()
	id.PeopleDict = nil
	assert.Nil(t, id.Configure(facts))
	assert.Equal(t, id.PeopleDict, facts[FactIdentityDetectorPeopleDict])
	assert.Equal(t, id.ReversedPeopleDict, facts[FactIdentityDetectorReversedPeopleDict])
	assert.Len(t, id.PeopleDict, 4)
	assert.Len(t, id.ReversedPeopleDict, 3)
	assert.Equal(t, id.ReversedPeopleDict[0], "Egor")
	assert.Equal(t, facts[FactIdentityDetectorPeopleCount], 2)
	delete(facts, FactIdentityDetectorPeopleDict)
	delete(facts, FactIdentityDetectorReversedPeopleDict)
	id = fixtureIdentityDetector()
	id.ReversedPeopleDict = nil
	assert.Nil(t, id.Configure(facts))
	assert.Equal(t, id.PeopleDict, facts[FactIdentityDetectorPeopleDict])
	assert.Equal(t, id.ReversedPeopleDict, facts[FactIdentityDetectorReversedPeopleDict])
	assert.Len(t, id.PeopleDict, 4)
	assert.Len(t, id.ReversedPeopleDict, 3)
	assert.Equal(t, id.ReversedPeopleDict[0], "Egor")
	assert.Equal(t, facts[FactIdentityDetectorPeopleCount], 2)
	delete(facts, FactIdentityDetectorPeopleDict)
	delete(facts, FactIdentityDetectorReversedPeopleDict)
	delete(facts, ConfigIdentityDetectorPeopleDictPath)
	commits := make([]*object.Commit, 0)
	iter, err := test.Repository.CommitObjects()
	commit, err := iter.Next()
	for ; err != io.EOF; commit, err = iter.Next() {
		if err != nil {
			panic(err)
		}
		commits = append(commits, commit)
	}
	facts[core.ConfigPipelineCommits] = commits
	id = fixtureIdentityDetector()
	id.PeopleDict = nil
	id.ReversedPeopleDict = nil
	assert.Nil(t, id.Configure(facts))
	assert.Equal(t, id.PeopleDict, facts[FactIdentityDetectorPeopleDict])
	assert.Equal(t, id.ReversedPeopleDict, facts[FactIdentityDetectorReversedPeopleDict])
	assert.True(t, len(id.PeopleDict) >= 3)
	assert.True(t, len(id.ReversedPeopleDict) >= 4)
}

func TestIdentityDetectorRegistration(t *testing.T) {
	summoned := core.Registry.Summon((&Detector{}).Name())
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "IdentityDetector")
	summoned = core.Registry.Summon((&Detector{}).Provides()[0])
	assert.Len(t, summoned, 1)
	assert.Equal(t, summoned[0].Name(), "IdentityDetector")
}

func TestIdentityDetectorConfigureEmpty(t *testing.T) {
	id := Detector{}
	assert.Panics(t, func() { id.Configure(map[string]interface{}{}) })
}

func TestIdentityDetectorConsume(t *testing.T) {
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"5c0e755dd85ac74584d9988cc361eccf02ce1a48"))
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	res, err := fixtureIdentityDetector().Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res[DependencyAuthor].(int), 0)
	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"8a03b5620b1caa72ec9cb847ea88332621e2950a"))
	deps[core.DependencyCommit] = commit
	res, err = fixtureIdentityDetector().Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res[DependencyAuthor].(int), AuthorMissing)
}

func TestIdentityDetectorConsumeExact(t *testing.T) {
	commit, _ := test.Repository.CommitObject(plumbing.NewHash(
		"5c0e755dd85ac74584d9988cc361eccf02ce1a48"))
	deps := map[string]interface{}{}
	deps[core.DependencyCommit] = commit
	id := fixtureIdentityDetector()
	id.ExactSignatures = true
	id.PeopleDict = map[string]int{
		"vadim markovtsev <gmarkhor@gmail.com>": 0,
		"vadim markovtsev <vadim@sourced.tech>": 1,
	}
	res, err := id.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res[DependencyAuthor].(int), 1)
	commit, _ = test.Repository.CommitObject(plumbing.NewHash(
		"8a03b5620b1caa72ec9cb847ea88332621e2950a"))
	deps[core.DependencyCommit] = commit
	res, err = id.Consume(deps)
	assert.Nil(t, err)
	assert.Equal(t, res[DependencyAuthor].(int), AuthorMissing)
}

func TestIdentityDetectorLoadPeopleDict(t *testing.T) {
	id := fixtureIdentityDetector()
	err := id.LoadPeopleDict(path.Join("..", "..", "test_data", "identities"))
	assert.Nil(t, err)
	assert.Equal(t, len(id.PeopleDict), 7)
	assert.Contains(t, id.PeopleDict, "linus torvalds")
	assert.Contains(t, id.PeopleDict, "torvalds@linux-foundation.org")
	assert.Contains(t, id.PeopleDict, "vadim markovtsev")
	assert.Contains(t, id.PeopleDict, "vadim@sourced.tech")
	assert.Contains(t, id.PeopleDict, "another@one.com")
	assert.Contains(t, id.PeopleDict, "máximo cuadros")
	assert.Contains(t, id.PeopleDict, "maximo@sourced.tech")
	assert.Equal(t, len(id.ReversedPeopleDict), 4)
	assert.Equal(t, id.ReversedPeopleDict[0], "Linus Torvalds")
	assert.Equal(t, id.ReversedPeopleDict[1], "Vadim Markovtsev")
	assert.Equal(t, id.ReversedPeopleDict[2], "Máximo Cuadros")
	assert.Equal(t, id.ReversedPeopleDict[3], AuthorMissingName)
}

func TestIdentityDetectorLoadPeopleDictWrongPath(t *testing.T) {
	id := fixtureIdentityDetector()
	err := id.LoadPeopleDict(path.Join("identities"))
	assert.NotNil(t, err)
}

func TestIdentityDetectorGeneratePeopleDict(t *testing.T) {
	id := fixtureIdentityDetector()
	commits := make([]*object.Commit, 0)
	iter, err := test.Repository.CommitObjects()
	commit, err := iter.Next()
	for ; err != io.EOF; commit, err = iter.Next() {
		if err != nil {
			panic(err)
		}
		commits = append(commits, commit)
	}
	{
		i := 0
		for ; commits[i].Author.Name != "Vadim Markovtsev"; i++ {
		}
		if i > 0 {
			commit := commits[0]
			commits[0] = commits[i]
			commits[i] = commit
		}
		i = 1
		for ; commits[i].Author.Name != "Alexander Bezzubov"; i++ {
		}
		if i > 0 {
			commit := commits[1]
			commits[1] = commits[i]
			commits[i] = commit
		}
		i = 2
		for ; commits[i].Author.Name != "Máximo Cuadros"; i++ {
		}
		if i > 0 {
			commit := commits[2]
			commits[2] = commits[i]
			commits[i] = commit
		}
	}
	id.GeneratePeopleDict(commits)
	assert.True(t, len(id.PeopleDict) >= 7)
	assert.True(t, len(id.ReversedPeopleDict) >= 3)
	assert.Equal(t, id.PeopleDict["vadim markovtsev"], 0)
	assert.Equal(t, id.PeopleDict["vadim@sourced.tech"], 0)
	assert.Equal(t, id.PeopleDict["gmarkhor@gmail.com"], 0)
	assert.Equal(t, id.PeopleDict["alexander bezzubov"], 1)
	assert.Equal(t, id.PeopleDict["bzz@apache.org"], 1)
	assert.Equal(t, id.PeopleDict["máximo cuadros"], 2)
	assert.Equal(t, id.PeopleDict["mcuadros@gmail.com"], 2)
	assert.Equal(t, id.ReversedPeopleDict[0], "vadim markovtsev|gmarkhor@gmail.com|vadim@athenian.co|vadim@sourced.tech")
	assert.Equal(t, id.ReversedPeopleDict[1], "alexander bezzubov|bzz@apache.org")
	assert.Equal(t, id.ReversedPeopleDict[2], "máximo cuadros|mcuadros@gmail.com")
	assert.NotEqual(t, id.ReversedPeopleDict[len(id.ReversedPeopleDict)-1], AuthorMissingName)
}

func TestIdentityDetectorGeneratePeopleDictExact(t *testing.T) {
	id := fixtureIdentityDetector()
	id.ExactSignatures = true
	commits := make([]*object.Commit, 0)
	iter, err := test.Repository.CommitObjects()
	commit, err := iter.Next()
	for ; err != io.EOF; commit, err = iter.Next() {
		if err != nil {
			panic(err)
		}
		commits = append(commits, commit)
	}
	id.GeneratePeopleDict(commits)
	ass := assert.New(t)
	ass.Equal(len(id.PeopleDict), len(id.ReversedPeopleDict))
	ass.True(len(id.ReversedPeopleDict) >= 24)
	ass.Contains(id.PeopleDict, "vadim markovtsev <vadim@sourced.tech>")
	ass.Contains(id.PeopleDict, "vadim markovtsev <vadim@athenian.co>")
	ass.NotEqual(id.ReversedPeopleDict[len(id.ReversedPeopleDict)-1], AuthorMissingName)
}

func TestIdentityDetectorLoadPeopleDictInvalidPath(t *testing.T) {
	id := fixtureIdentityDetector()
	ipath := "/xxxyyyzzzInvalidPath!hehe"
	err := id.LoadPeopleDict(ipath)
	assert.NotNil(t, err)
	assert.Equal(t, err.(*os.PathError).Path, ipath)
}

type fakeBlobEncodedObject struct {
	Contents string
}

func (obj fakeBlobEncodedObject) Hash() plumbing.Hash {
	return plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff")
}

func (obj fakeBlobEncodedObject) Type() plumbing.ObjectType {
	return plumbing.BlobObject
}

func (obj fakeBlobEncodedObject) SetType(plumbing.ObjectType) {}

func (obj fakeBlobEncodedObject) Size() int64 {
	return int64(len(obj.Contents))
}

func (obj fakeBlobEncodedObject) SetSize(int64) {}

func (obj fakeBlobEncodedObject) Reader() (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader(obj.Contents)), nil
}

func (obj fakeBlobEncodedObject) Writer() (io.WriteCloser, error) {
	return nil, nil
}

type fakeTreeEncodedObject struct {
	Name string
}

func (obj fakeTreeEncodedObject) Hash() plumbing.Hash {
	return plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff")
}

func (obj fakeTreeEncodedObject) Type() plumbing.ObjectType {
	return plumbing.TreeObject
}

func (obj fakeTreeEncodedObject) SetType(plumbing.ObjectType) {}

func (obj fakeTreeEncodedObject) Size() int64 {
	return 1
}

func (obj fakeTreeEncodedObject) SetSize(int64) {}

func (obj fakeTreeEncodedObject) Reader() (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader(
		"100644 " + obj.Name + "\x00ffffffffffffffffffffffffffffffffffffffff")), nil
}

func (obj fakeTreeEncodedObject) Writer() (io.WriteCloser, error) {
	return nil, nil
}

type fakeEncodedObjectStorer struct {
	Name     string
	Contents string
}

func (strr fakeEncodedObjectStorer) NewEncodedObject() plumbing.EncodedObject {
	return nil
}

func (strr fakeEncodedObjectStorer) HasEncodedObject(plumbing.Hash) error {
	return nil
}

func (strr fakeEncodedObjectStorer) SetEncodedObject(plumbing.EncodedObject) (plumbing.Hash, error) {
	return plumbing.NewHash("0000000000000000000000000000000000000000"), nil
}

func (strr fakeEncodedObjectStorer) EncodedObject(objType plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	if objType == plumbing.TreeObject {
		return fakeTreeEncodedObject{Name: strr.Name}, nil
	} else if objType == plumbing.BlobObject {
		return fakeBlobEncodedObject{Contents: strr.Contents}, nil
	}
	return nil, nil
}

func (strr fakeEncodedObjectStorer) IterEncodedObjects(plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	return nil, nil
}

func (strr fakeEncodedObjectStorer) EncodedObjectSize(plumbing.Hash) (int64, error) {
	return 0, nil
}

func getFakeCommitWithFile(name string, contents string) *object.Commit {
	c := object.Commit{
		Hash: plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff"),
		Author: object.Signature{
			Name:  "Vadim Markovtsev",
			Email: "vadim@sourced.tech",
		},
		Committer: object.Signature{
			Name:  "Vadim Markovtsev",
			Email: "vadim@sourced.tech",
		},
		Message:  "Virtual file " + name,
		TreeHash: plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff"),
	}
	voc := reflect.ValueOf(&c)
	voc = voc.Elem()
	f := voc.FieldByName("s")
	ptr := unsafe.Pointer(f.UnsafeAddr())
	strr := fakeEncodedObjectStorer{Name: name, Contents: contents}
	*(*storer.EncodedObjectStorer)(ptr) = strr
	return &c
}

func TestIdentityDetectorGeneratePeopleDictMailmap(t *testing.T) {
	id := fixtureIdentityDetector()
	commits := make([]*object.Commit, 0)
	iter, err := test.Repository.CommitObjects()
	commit, err := iter.Next()
	for ; err != io.EOF; commit, err = iter.Next() {
		if err != nil {
			panic(err)
		}
		commits = append(commits, commit)
	}
	fake := getFakeCommitWithFile(
		".mailmap",
		"Strange Guy <vadim@sourced.tech>\nVadim Markovtsev <vadim@sourced.tech> Strange Guy <vadim@sourced.tech>")
	commits = append(commits, fake)
	id.GeneratePeopleDict(commits)
	assert.Contains(t, id.ReversedPeopleDict,
		"strange guy|vadim markovtsev|gmarkhor@gmail.com|vadim@athenian.co|vadim@sourced.tech")
}

func TestIdentityDetectorMergeReversedDictsLiteral(t *testing.T) {
	pa1 := [...]string{"one|one@one", "two|aaa@two"}
	pa2 := [...]string{"two|aaa@two", "three|one@one"}
	people, merged := MergeReversedDictsLiteral(pa1[:], pa2[:])
	assert.Len(t, people, 3)
	assert.Len(t, merged, 3)
	assert.Equal(t, people["one|one@one"], MergedIndex{0, 0, -1})
	assert.Equal(t, people["two|aaa@two"], MergedIndex{1, 1, 0})
	assert.Equal(t, people["three|one@one"], MergedIndex{2, -1, 1})
	assert.Equal(t, merged, []string{"one|one@one", "two|aaa@two", "three|one@one"})
	pa1 = [...]string{"two|aaa@two", "one|one@one"}
	people, merged = MergeReversedDictsLiteral(pa1[:], pa2[:])
	assert.Len(t, people, 3)
	assert.Len(t, merged, 3)
	assert.Equal(t, people["one|one@one"], MergedIndex{1, 1, -1})
	assert.Equal(t, people["two|aaa@two"], MergedIndex{0, 0, 0})
	assert.Equal(t, people["three|one@one"], MergedIndex{2, -1, 1})
	assert.Equal(t, merged, []string{"two|aaa@two", "one|one@one", "three|one@one"})
}

func TestIdentityDetectorMergeReversedDictsIdentities(t *testing.T) {
	pa1 := [...]string{"one|one@one", "two|aaa@two"}
	pa2 := [...]string{"two|aaa@two", "three|one@one"}
	people, merged := MergeReversedDictsIdentities(pa1[:], pa2[:])
	assert.Len(t, people, 3)
	assert.Len(t, merged, 2)
	assert.Equal(t, people["one|one@one"], MergedIndex{0, 0, -1})
	assert.Equal(t, people["two|aaa@two"], MergedIndex{1, 1, 0})
	assert.Equal(t, people["three|one@one"], MergedIndex{0, -1, 1})
	assert.Equal(t, merged, []string{"one|three|one@one", "two|aaa@two"})
}

func TestIdentityDetectorMergeReversedDictsIdentitiesStrikeBack(t *testing.T) {
	pa1 := [...]string{"one|one@one", "two|aaa@two", "three|three@three"}
	pa2 := [...]string{"two|aaa@two", "three|one@one"}
	people, merged := MergeReversedDictsIdentities(pa1[:], pa2[:])
	assert.Len(t, people, 4)
	assert.Len(t, merged, 2)
	assert.Equal(t, people["one|one@one"], MergedIndex{0, 0, -1})
	assert.Equal(t, people["two|aaa@two"], MergedIndex{1, 1, 0})
	assert.Equal(t, people["three|one@one"], MergedIndex{0, -1, 1})
	assert.Equal(t, people["three|three@three"], MergedIndex{0, 2, -1})
	assert.Equal(t, merged, []string{"one|three|one@one|three@three", "two|aaa@two"})

	pa1 = [...]string{"one|one@one", "two|aaa@two", "three|aaa@two"}
	people, merged = MergeReversedDictsIdentities(pa1[:], pa2[:])
	assert.Len(t, people, 4)
	assert.Len(t, merged, 1)
	assert.Equal(t, people["one|one@one"], MergedIndex{0, 0, -1})
	assert.Equal(t, people["two|aaa@two"], MergedIndex{0, 1, 0})
	assert.Equal(t, people["three|one@one"], MergedIndex{0, -1, 1})
	assert.Equal(t, people["three|aaa@two"], MergedIndex{0, 2, -1})
	assert.Equal(t, merged, []string{"one|three|two|aaa@two|one@one"})
}

func TestIdentityDetectorFork(t *testing.T) {
	id1 := fixtureIdentityDetector()
	clones := id1.Fork(1)
	assert.Len(t, clones, 1)
	id2 := clones[0].(*Detector)
	assert.True(t, id1 == id2)
	id1.Merge([]core.PipelineItem{id2})
}
