package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"

	"github.com/fatih/camelcase"
	"github.com/spf13/cobra"
)

//go:generate go run embed.go

// ShlibExts is the mapping between platform names and shared library file name extensions.
var ShlibExts = map[string]string{
	"window":  "dll",
	"linux":   "so",
	"darwin":  "dylib",
	"freebsd": "dylib",
}

// generatePluginCmd represents the generatePlugin command
var generatePluginCmd = &cobra.Command{
	Use:   "generate-plugin",
	Short: "Write the plugin source skeleton.",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		flags := cmd.Flags()
		name, _ := flags.GetString("name")
		outputDir, _ := flags.GetString("output")
		varname, _ := flags.GetString("varname")
		flag, _ := flags.GetString("flag")
		disableMakefile, _ := flags.GetBool("no-makefile")
		pkg, _ := flags.GetString("package")

		splitted := camelcase.Split(name)
		err := os.MkdirAll(outputDir, os.ModePerm)
		if err != nil {
			panic(err)
		}
		outputPath := path.Join(outputDir, strings.ToLower(strings.Join(splitted, "_"))+".go")
		gen := template.Must(template.New("plugin").Parse(PluginTemplateSource))
		outFile, err := os.Create(outputPath)
		if err != nil {
			panic(err)
		}
		defer outFile.Close()
		if varname == "" {
			varname = strings.ToLower(splitted[0])
		}
		if flag == "" {
			flag = strings.ToLower(strings.Join(splitted, "-"))
		}
		outputBase := path.Base(outputPath)
		shlib := outputBase[:len(outputBase)-2] + ShlibExts[runtime.GOOS]
		protoBuf := outputPath[:len(outputPath)-3] + ".proto"
		pbGo := outputPath[:len(outputPath)-3] + ".pb.go"
		dict := map[string]string{
			"name": name, "varname": varname, "flag": flag, "package": pkg,
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
		cmdargs := [...]string{
			protoc,
			"--gogo_out=" + outputDir,
			"--proto_path=" + outputDir,
			protoBuf,
		}
		env := os.Environ()
		extraPath, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		gobin := os.Getenv("GOBIN")
		if gobin != "" {
			extraPath = gobin + ":" + extraPath
		}
		env = append(env, fmt.Sprintf("PATH=%s:%s", os.Getenv("PATH"), extraPath))
		if err != nil {
			panic("protoc was not found at " + env[len(env)-1])
		}
		protocmd := exec.Cmd{
			Path: protoc, Args: cmdargs[:], Env: env, Stdout: os.Stdout, Stderr: os.Stderr}
		err = protocmd.Run()
		if err != nil {
			panic(err)
		}
		if !disableMakefile {
			makefile := path.Join(outputDir, "Makefile")
			gen = template.Must(template.New("plugin").Parse(`GO111MODULE = on

all: {{.shlib}}

{{.shlib}}: {{.output}} {{.protogo}}
` + "\t" + `go build -buildmode=plugin -linkshared {{.output}} {{.protogo}}

{{.protogo}}: {{.proto}}
` + "\t" + `PATH=$$PATH:$$GOBIN protoc --gogo_out=. --proto_path=. {{.proto}}
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
	},
}

func init() {
	rootCmd.AddCommand(generatePluginCmd)
	generatePluginCmd.SetUsageFunc(generatePluginCmd.UsageFunc())
	gpFlags := generatePluginCmd.Flags()
	gpFlags.StringP("name", "n", "", "Name of the plugin, CamelCase. Required.")
	generatePluginCmd.MarkFlagRequired("name")
	gpFlags.StringP("output", "o", ".", "Output directory for the generated plugin files.")
	gpFlags.String("varname", "", "Name of the plugin instance variable, If not "+
		"specified, inferred from -n.")
	gpFlags.String("flag", "", "Name of the plugin activation cmdline flag, If not "+
		"specified, inferred from -varname.")
	gpFlags.Bool("no-makefile", false, "Do not generate the Makefile.")
	gpFlags.String("package", "main", "Name of the package.")
}
