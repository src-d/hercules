package core

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unsafe"

	"github.com/spf13/pflag"
)

// PipelineItemRegistry contains all the known PipelineItem-s.
type PipelineItemRegistry struct {
	provided     map[string][]reflect.Type
	registered   map[string]reflect.Type
	flags        map[string]reflect.Type
	featureFlags arrayFeatureFlags
}

// Register adds another PipelineItem to the registry.
func (registry *PipelineItemRegistry) Register(example PipelineItem) {
	t := reflect.TypeOf(example)
	registry.registered[example.Name()] = t
	if fpi, ok := example.(LeafPipelineItem); ok {
		registry.flags[fpi.Flag()] = t
	}
	for _, dep := range example.Provides() {
		ts := registry.provided[dep]
		if ts == nil {
			ts = []reflect.Type{}
		}
		ts = append(ts, t)
		registry.provided[dep] = ts
	}
}

// Summon searches for PipelineItem-s which provide the specified entity or named after
// the specified string. It materializes all the found types and returns them.
func (registry *PipelineItemRegistry) Summon(providesOrName string) []PipelineItem {
	if registry.provided == nil {
		return []PipelineItem{}
	}
	ts := registry.provided[providesOrName]
	items := []PipelineItem{}
	for _, t := range ts {
		items = append(items, reflect.New(t.Elem()).Interface().(PipelineItem))
	}
	if t, exists := registry.registered[providesOrName]; exists {
		items = append(items, reflect.New(t.Elem()).Interface().(PipelineItem))
	}
	return items
}

// GetLeaves returns all LeafPipelineItem-s registered.
func (registry *PipelineItemRegistry) GetLeaves() []LeafPipelineItem {
	keys := []string{}
	for key := range registry.flags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := []LeafPipelineItem{}
	for _, key := range keys {
		items = append(items, reflect.New(registry.flags[key].Elem()).Interface().(LeafPipelineItem))
	}
	return items
}

// GetPlumbingItems returns all non-LeafPipelineItem-s registered.
func (registry *PipelineItemRegistry) GetPlumbingItems() []PipelineItem {
	keys := []string{}
	for key := range registry.registered {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := []PipelineItem{}
	for _, key := range keys {
		iface := reflect.New(registry.registered[key].Elem()).Interface()
		if _, ok := iface.(LeafPipelineItem); !ok {
			items = append(items, iface.(PipelineItem))
		}
	}
	return items
}

type orderedFeaturedItems []FeaturedPipelineItem

func (ofi orderedFeaturedItems) Len() int {
	return len([]FeaturedPipelineItem(ofi))
}

func (ofi orderedFeaturedItems) Less(i, j int) bool {
	cofi := []FeaturedPipelineItem(ofi)
	return cofi[i].Name() < cofi[j].Name()
}

func (ofi orderedFeaturedItems) Swap(i, j int) {
	cofi := []FeaturedPipelineItem(ofi)
	cofi[i], cofi[j] = cofi[j], cofi[i]
}

// GetFeaturedItems returns all FeaturedPipelineItem-s registered.
func (registry *PipelineItemRegistry) GetFeaturedItems() map[string][]FeaturedPipelineItem {
	features := map[string][]FeaturedPipelineItem{}
	for _, t := range registry.registered {
		if fiface, ok := reflect.New(t.Elem()).Interface().(FeaturedPipelineItem); ok {
			for _, f := range fiface.Features() {
				list := features[f]
				if list == nil {
					list = []FeaturedPipelineItem{}
				}
				list = append(list, fiface)
				features[f] = list
			}
		}
	}
	for _, vals := range features {
		sort.Sort(orderedFeaturedItems(vals))
	}
	return features
}

type arrayFeatureFlags struct {
	// Flags contains the features activated through the command line.
	Flags []string
	// Choices contains all registered features.
	Choices map[string]bool
}

func (acf *arrayFeatureFlags) String() string {
	return strings.Join([]string(acf.Flags), ", ")
}

func (acf *arrayFeatureFlags) Set(value string) error {
	if _, exists := acf.Choices[value]; !exists {
		return fmt.Errorf("feature \"%s\" is not registered", value)
	}
	acf.Flags = append(acf.Flags, value)
	return nil
}

func (acf *arrayFeatureFlags) Type() string {
	return "string"
}

// AddFlags inserts the cmdline options from PipelineItem.ListConfigurationOptions(),
// FeaturedPipelineItem().Features() and LeafPipelineItem.Flag() into the global "flag" parser
// built into the Go runtime.
// Returns the "facts" which can be fed into PipelineItem.Configure() and the dictionary of
// runnable analysis (LeafPipelineItem) choices. E.g. if "BurndownAnalysis" was activated
// through "-burndown" cmdline argument, this mapping would contain ["BurndownAnalysis"] = *true.
func (registry *PipelineItemRegistry) AddFlags(flagSet *pflag.FlagSet) (
	map[string]interface{}, map[string]*bool) {
	flags := map[string]interface{}{}
	deployed := map[string]*bool{}
	for name, it := range registry.registered {
		formatHelp := func(desc string) string {
			return fmt.Sprintf("%s [%s]", desc, name)
		}
		itemIface := reflect.New(it.Elem()).Interface()
		for _, opt := range itemIface.(PipelineItem).ListConfigurationOptions() {
			var iface interface{}
			getPtr := func() unsafe.Pointer {
				return unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface))
			}
			switch opt.Type {
			case BoolConfigurationOption:
				iface = interface{}(true)
				ptr := (**bool)(getPtr())
				*ptr = flagSet.Bool(opt.Flag, opt.Default.(bool), formatHelp(opt.Description))
			case IntConfigurationOption:
				iface = interface{}(0)
				ptr := (**int)(getPtr())
				*ptr = flagSet.Int(opt.Flag, opt.Default.(int), formatHelp(opt.Description))
			case StringConfigurationOption:
				iface = interface{}("")
				ptr := (**string)(getPtr())
				*ptr = flagSet.String(opt.Flag, opt.Default.(string), formatHelp(opt.Description))
			case FloatConfigurationOption:
				iface = interface{}(float32(0))
				ptr := (**float32)(getPtr())
				*ptr = flagSet.Float32(opt.Flag, opt.Default.(float32), formatHelp(opt.Description))
			case StringsConfigurationOption:
				iface = interface{}([]string{})
				ptr := (**[]string)(getPtr())
				*ptr = flagSet.StringSlice(opt.Flag, opt.Default.([]string), formatHelp(opt.Description))
			}
			flags[opt.Name] = iface
		}
		if fpi, ok := itemIface.(FeaturedPipelineItem); ok {
			for _, f := range fpi.Features() {
				registry.featureFlags.Choices[f] = true
			}
		}
		if fpi, ok := itemIface.(LeafPipelineItem); ok {
			deployed[fpi.Name()] = flagSet.Bool(
				fpi.Flag(), false, fmt.Sprintf("Runs %s analysis.", fpi.Name()))
		}
	}
	{
		// Pipeline flags
		iface := interface{}("")
		ptr1 := (**string)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
		*ptr1 = flagSet.String("dump-dag", "", "Write the pipeline DAG to a Graphviz file.")
		flags[ConfigPipelineDumpPath] = iface
		iface = interface{}(true)
		ptr2 := (**bool)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
		*ptr2 = flagSet.Bool("dry-run", false, "Do not run any analyses - only resolve the DAG. "+
			"Useful for -dump-dag.")
		flags[ConfigPipelineDryRun] = iface
	}
	features := []string{}
	for f := range registry.featureFlags.Choices {
		features = append(features, f)
	}
	flagSet.Var(&registry.featureFlags, "feature",
		fmt.Sprintf("Enables the items which depend on the specified features. Can be specified "+
			"multiple times. Available features: [%s] (see --feature below).",
			strings.Join(features, ", ")))
	return flags, deployed
}

// Registry contains all known pipeline item types.
var Registry = &PipelineItemRegistry{
	provided:     map[string][]reflect.Type{},
	registered:   map[string]reflect.Type{},
	flags:        map[string]reflect.Type{},
	featureFlags: arrayFeatureFlags{Flags: []string{}, Choices: map[string]bool{}},
}
