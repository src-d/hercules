package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4"
)

func TestLoadRepository(t *testing.T) {
	repo := loadRepository("https://github.com/src-d/hercules", "", true)
	assert.NotNil(t, repo)
	log.Println("TestLoadRepository: 1/3")

	tempdir, err := ioutil.TempDir("", "hercules-")
	assert.Nil(t, err)
	if err != nil {
		assert.FailNow(t, "ioutil.TempDir")
	}
	defer os.RemoveAll(tempdir)
	backend, err := filesystem.NewStorage(osfs.New(tempdir))
	assert.Nil(t, err)
	if err != nil {
		assert.FailNow(t, "filesystem.NewStorage")
	}
	cloneOptions := &git.CloneOptions{URL: "https://github.com/src-d/hercules"}
	_, err = git.Clone(backend, nil, cloneOptions)
	assert.Nil(t, err)
	if err != nil {
		assert.FailNow(t, "filesystem.NewStorage")
	}

	repo = loadRepository(tempdir, "", true)
	assert.NotNil(t, repo)
	log.Println("TestLoadRepository: 2/3")

	_, filename, _, _ := runtime.Caller(0)
	if runtime.GOOS != "windows" {
		// TODO(vmarkovtsev): uncomment once https://github.com/src-d/go-billy-siva/issues/29 is resolved
		sivafile := filepath.Join(filepath.Dir(filename), "test_data", "hercules.siva")
		repo = loadRepository(sivafile, "", true)
		assert.NotNil(t, repo)
		log.Println("TestLoadRepository: 3/3")
	}

	assert.Panics(t, func() { loadRepository("https://github.com/src-d/porn", "", true) })
	assert.Panics(t, func() { loadRepository(filepath.Dir(filename), "", true) })
	assert.Panics(t, func() { loadRepository("/xxx", "", true) })
}
