package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/stretchr/testify/assert"
)

func TestLoadRepository(t *testing.T) {
	repo := loadRepository("https://github.com/src-d/hercules", "", true, "")
	assert.NotNil(t, repo)
	log.Println("TestLoadRepository: 1/3")

	tempdir, err := ioutil.TempDir("", "hercules-")
	assert.Nil(t, err)
	if err != nil {
		assert.FailNow(t, "ioutil.TempDir")
	}
	defer os.RemoveAll(tempdir)
	backend := filesystem.NewStorage(osfs.New(tempdir), cache.NewObjectLRUDefault())
	cloneOptions := &git.CloneOptions{URL: "https://github.com/src-d/hercules"}
	_, err = git.Clone(backend, nil, cloneOptions)
	assert.Nil(t, err)
	if err != nil {
		assert.FailNow(t, "filesystem.NewStorage")
	}

	repo = loadRepository(tempdir, "", true, "")
	assert.NotNil(t, repo)
	log.Println("TestLoadRepository: 2/3")

	_, filename, _, _ := runtime.Caller(0)
	sivafile := filepath.Join(filepath.Dir(filename), "test_data", "hercules.siva")
	repo = loadRepository(sivafile, "", true, "")
	assert.NotNil(t, repo)
	log.Println("TestLoadRepository: 3/3")

	assert.Panics(t, func() { loadRepository("https://github.com/src-d/porn", "", true, "") })
	assert.Panics(t, func() { loadRepository(filepath.Dir(filename), "", true, "") })
	assert.Panics(t, func() { loadRepository("/xxx", "", true, "") })
}
