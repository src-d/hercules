package main

import (
  "flag"
  "fmt"
  "os"
  "strings"
  "text/template"

  "gopkg.in/src-d/hercules.v3"
  "github.com/fatih/camelcase"
)

//go:generate go run embed.go

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
  flag.StringVar(&pkg, "package", "contrib", "Name of the package.")
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
    _flag = varname
  }
  dict := map[string]string{"name": name, "varname": varname, "flag": _flag, "package": pkg}
  err = gen.Execute(outFile, dict)
  if err != nil {
    panic(err)
  }
}
