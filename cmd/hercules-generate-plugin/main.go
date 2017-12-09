package main

import (
  "flag"
  "fmt"
  "os"
  "path"
  "runtime"
  "strings"
  "text/template"

  "gopkg.in/src-d/hercules.v3"
  "github.com/fatih/camelcase"
)

//go:generate go run embed.go

var SHLIB_EXT = map[string]string {
  "window": "dll",
  "linux": "so",
  "darwin": "dylib",
  "freebsd": "dylib",
}

func main() {
  var outputPath, name, varname, _flag, pkg string
  var printVersion bool
  flag.StringVar(&name, "n", "", "Name of the plugin, CamelCase. Required.")
  flag.StringVar(&outputPath, "o", "", "Output path of the generated plugin code. If not " +
      "specified, inferred from -n.")
  flag.StringVar(&varname, "varname", "", "Name of the plugin instance variable, If not " +
      "specified, inferred from -n.")
  flag.StringVar(&_flag, "flag", "", "Name of the plugin activation cmdline flag, If not " +
      "specified, inferred from -varname.")
  flag.BoolVar(&printVersion, "version", false, "Print version information and exit.")
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
  if outputPath == "" {
    outputPath = strings.ToLower(strings.Join(splitted, "_")) + ".go"
  } else if !strings.HasSuffix(outputPath, ".go") {
    panic("-o must end with \".go\"")
  }
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
    _flag = strings.Join(splitted, "-")
  }
  outputBase := path.Base(outputPath)
  shlib := outputBase[:len(outputBase)-2] + SHLIB_EXT[runtime.GOOS]
  dict := map[string]string{
    "name": name, "varname": varname, "flag": _flag, "package": pkg,
    "output": outputPath, "shlib": shlib}
  err = gen.Execute(outFile, dict)
  if err != nil {
    panic(err)
  }
}
