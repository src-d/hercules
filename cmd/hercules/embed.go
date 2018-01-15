// +build ignore

package main

import (
	"io/ioutil"
	"os"
	"text/template"
)

func main() {
	contents, err := ioutil.ReadFile("plugin.template")
	if err != nil {
		panic(err)
	}
	template.Must(template.New("plugin").Parse(string(contents)))
	file, err := os.Create("plugin_template_source.go")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.WriteString("package main\n\n" +
		"// PluginTemplateSource is the source code template of a Hercules plugin.\n" +
		"const PluginTemplateSource = `")
	file.Write(contents)
	file.WriteString("`\n")
}
