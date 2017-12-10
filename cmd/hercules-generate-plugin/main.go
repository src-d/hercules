package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"text/template"

	"github.com/fatih/camelcase"
	"gopkg.in/src-d/hercules.v3"
)

//go:generate go run embed.go

var SHLIB_EXT = map[string]string{
	"window":  "dll",
	"linux":   "so",
	"darwin":  "dylib",
	"freebsd": "dylib",
}

func main() {
	var outputDir, name, varname, _flag, pkg string
	var printVersion, disableMakefile bool
	flag.StringVar(&name, "n", "", "Name of the plugin, CamelCase. Required.")
	flag.StringVar(&outputDir, "o", ".", "Output directory for the generated plugin files.")
	flag.StringVar(&varname, "varname", "", "Name of the plugin instance variable, If not "+
		"specified, inferred from -n.")
	flag.StringVar(&_flag, "flag", "", "Name of the plugin activation cmdline flag, If not "+
		"specified, inferred from -varname.")
	flag.BoolVar(&printVersion, "version", false, "Print version information and exit.")
	flag.BoolVar(&disableMakefile, "no-makefile", false, "Do not generate the Makefile.")
	flag.StringVar(&pkg, "package", "main", "Name of the package.")
	flag.Parse()
	if printVersion {
		fmt.Printf("Version: 3\nGit:     %s\n", hercules.GIT_HASH)
		return
	}
	if name == "" {
		fmt.Fprintln(os.Stderr, "-n must be specified")
		flag.PrintDefaults()
		os.Exit(1)
	}
	splitted := camelcase.Split(name)
	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		panic(err)
	}
	outputPath := path.Join(outputDir, strings.ToLower(strings.Join(splitted, "_"))+".go")
	gen := template.Must(template.New("plugin").Parse(PLUGIN_TEMPLATE_SOURCE))
	outFile, err := os.Create(outputPath)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()
	if varname == "" {
		varname = strings.ToLower(splitted[0])
	}
	if _flag == "" {
		_flag = strings.ToLower(strings.Join(splitted, "-"))
	}
	outputBase := path.Base(outputPath)
	shlib := outputBase[:len(outputBase)-2] + SHLIB_EXT[runtime.GOOS]
	protoBuf := outputPath[:len(outputPath)-3] + ".proto"
	pbGo := outputPath[:len(outputPath)-3] + ".pb.go"
	dict := map[string]string{
		"name": name, "varname": varname, "flag": _flag, "package": pkg,
		"output": outputPath, "shlib": shlib, "proto": protoBuf, "protogo": pbGo,
		"outdir": outputDir}
	err = gen.Execute(outFile, dict)
	if err != nil {
		panic(err)
	}
	// write pb file
	ioutil.WriteFile(protoBuf, []byte(fmt.Sprintf(`syntax = "proto3";
option go_package = "%s";

message %sResultMessage {
  // add fields here
  // reference: https://developers.google.com/protocol-buffers/docs/proto3
  // example: pb/pb.proto https://github.com/src-d/hercules/blob/master/pb/pb.proto
}
`, pkg, name)), 0666)
	// generate the pb Go file
	protoc, err := exec.LookPath("protoc")
	args := [...]string{
		protoc,
		"--gogo_out=" + outputDir,
		"--proto_path=" + outputDir,
		protoBuf,
	}
	env := os.Environ()
	env = append(env, fmt.Sprintf(
		"PATH=%s:%s", os.Getenv("PATH"), path.Join(os.Getenv("GOPATH"), "bin")))
	if err != nil {
		panic("protoc was not found at " + env[len(env)-1])
	}
	cmd := exec.Cmd{Path: protoc, Args: args[:], Env: env, Stdout: os.Stdout, Stderr: os.Stderr}
	err = cmd.Run()
	if err != nil {
		panic(err)
	}
	if !disableMakefile {
		makefile := path.Join(outputDir, "Makefile")
		gen = template.Must(template.New("plugin").Parse(`all: {{.shlib}}

{{.shlib}}: {{.output}} {{.protogo}}
` + "\t" + `go build -buildmode=plugin {{.output}} {{.protogo}}

{{.protogo}}: {{.proto}}
` + "\t" + `PATH=$$PATH:$$GOPATH/bin protoc --gogo_out=. --proto_path=. {{.proto}}
`))
		buffer := new(bytes.Buffer)
		mkrelative := func(name string) {
			dict[name] = path.Base(dict[name])
		}
		mkrelative("output")
		mkrelative("protogo")
		mkrelative("proto")
		gen.Execute(buffer, dict)
		ioutil.WriteFile(makefile, buffer.Bytes(), 0666)
	}
}
