package core

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unsafe"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// PipelineItemRegistry contains all the known PipelineItem-s.
type PipelineItemRegistry struct {
	provided     map[string][]reflect.Type
	registered   map[string]reflect.Type
	preferred    map[string]struct{}
	flags        map[string]reflect.Type
	featureFlags arrayFeatureFlags
}

// Register adds another PipelineItem to the registry.
func (registry *PipelineItemRegistry) Register(example PipelineItem) {
	registry.RegisterPreferred(example, false)
}

func (registry *PipelineItemRegistry) RegisterPreferred(example PipelineItem, preferred bool) {
	t := reflect.TypeOf(example)
	exampleName := example.Name()
	registry.registered[exampleName] = t
	if fpi, ok := example.(LeafPipelineItem); ok {
		registry.flags[fpi.Flag()] = t
		if preferred {
			registry.preferred[exampleName] = struct{}{}
		} else {
			delete(registry.preferred, exampleName)
		}
	}

	for _, dep := range example.Provides() {
		ts := registry.provided[dep]
		if preferred && len(ts) > 0 {
			ts = append(ts, ts[0])
			ts[0] = t
		} else {
			ts = append(ts, t)
		}
		registry.provided[dep] = ts
	}
}

// Summon searches for PipelineItem-s which provide the specified entity or named after
// the specified string. It materializes all the found types and returns them.
func (registry *PipelineItemRegistry) Summon(providesOrNames ...string) []PipelineItem {
	if registry.provided == nil {
		return nil
	}

	var items []PipelineItem
	for _, providesOrName := range providesOrNames {
		ts := registry.provided[providesOrName]
		for _, t := range ts {
			items = append(items, reflect.New(t.Elem()).Interface().(PipelineItem))
		}
		if t, exists := registry.registered[providesOrName]; exists {
			items = append(items, reflect.New(t.Elem()).Interface().(PipelineItem))
		}
	}
	return items
}

// GetLeaves returns all LeafPipelineItem-s registered.
func (registry *PipelineItemRegistry) GetLeaves() []LeafPipelineItem {
	var keys []string
	for key := range registry.flags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var items []LeafPipelineItem
	for _, key := range keys {
		items = append(items, reflect.New(registry.flags[key].Elem()).Interface().(LeafPipelineItem))
	}
	return items
}

// GetPlumbingItems returns all non-LeafPipelineItem-s registered.
func (registry *PipelineItemRegistry) GetPlumbingItems() []PipelineItem {
	var keys []string
	for key := range registry.registered {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]PipelineItem, 0, len(keys))
	for _, key := range keys {
		iface := reflect.New(registry.registered[key].Elem()).Interface()
		if _, ok := iface.(LeafPipelineItem); !ok {
			items = append(items, iface.(PipelineItem))
		}
	}
	return items
}

// GetFeaturedItems returns all FeaturedPipelineItem-s registered.
func (registry *PipelineItemRegistry) GetFeaturedItems() map[string][]PipelineItem {
	features := map[string][]PipelineItem{}
	for _, t := range registry.registered {
		item := reflect.New(t.Elem()).Interface().(PipelineItem)
		deps := registry.CollectAllDependencies(item)
		deps = append(deps, item)
		depFeatures := map[string]bool{}
		for _, dep := range deps {
			if fiFace, ok := dep.(FeaturedPipelineItem); ok {
				for _, f := range fiFace.Features() {
					depFeatures[f] = true
				}
			}
		}
		for f := range depFeatures {
			features[f] = append(features[f], item)
		}
	}

	for _, vals := range features {
		sort.Slice(vals, func(i, j int) bool {
			return vals[i].Name() < vals[j].Name()
		})
	}
	return features
}

// CollectAllDependencies recursively builds the list of all the items on which the specified item
// depends.
func (registry *PipelineItemRegistry) CollectAllDependencies(item PipelineItem) []PipelineItem {
	deps := map[string]PipelineItem{}
	for stack := []PipelineItem{item}; len(stack) > 0; {
		head := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, reqID := range head.Requires() {
			req := registry.Summon(reqID)[0]
			if _, exists := deps[reqID]; !exists {
				deps[reqID] = req
				stack = append(stack, req)
			}
		}
	}
	result := make([]PipelineItem, 0, len(deps))
	for _, val := range deps {
		result = append(result, val)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name() < result[j].Name()
	})
	return result
}

var pathFlagTypeMasquerade bool

// EnablePathFlagTypeMasquerade changes the type of all "path" command line arguments from "string"
// to "path". This operation cannot be canceled and is intended to be used for better --help output.
func EnablePathFlagTypeMasquerade() {
	pathFlagTypeMasquerade = true
}

type pathValue struct {
	origin pflag.Value
}

func wrapPathValue(val pflag.Value) pflag.Value {
	return &pathValue{val}
}

func (s *pathValue) Set(val string) error {
	return s.origin.Set(val)
}
func (s *pathValue) Type() string {
	if pathFlagTypeMasquerade {
		return "path"
	}
	return "string"
}

func (s *pathValue) String() string {
	return s.origin.String()
}

// PathifyFlagValue changes the type of string command line argument to "path".
func PathifyFlagValue(flag *pflag.Flag) {
	flag.Value = wrapPathValue(flag.Value)
}

type arrayFeatureFlags struct {
	// Flags contains the features activated through the command line.
	Flags []string
	// Choices contains all registered features.
	Choices map[string]bool
}

func (acf *arrayFeatureFlags) String() string {
	return strings.Join(acf.Flags, ", ")
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
	flags map[string]interface{}, deployed map[string]*bool, activations map[string][]string) {

	flags = map[string]interface{}{}
	deployed = map[string]*bool{}
	activations = map[string][]string{}
	reusableOptions := map[string]ConfigurationOption{}

	for name, it := range registry.registered {
		formatHelp := func(desc string) string {
			return fmt.Sprintf("%s [%s]", desc, name)
		}
		itemIface := reflect.New(it.Elem()).Interface()

		if fpi, ok := itemIface.(FeaturedPipelineItem); ok {
			for _, f := range fpi.Features() {
				registry.featureFlags.Choices[f] = true
			}
		}

		leafFlag := ""
		if fpi, ok := itemIface.(LeafPipelineItem); ok {
			leafFlag = fpi.Flag()
			deployed[fpi.Name()] = flagSet.Bool(
				leafFlag, false, fmt.Sprintf("Runs %s analysis.", fpi.Name()))
		}

		addFlagActivation := func(optFlag string) {
			if leafFlag == "" {
				return
			}
			flagName := flagSet.Lookup(optFlag).Name
			list := activations[flagName]
			if _, ok := registry.preferred[name]; !ok || len(list) == 0 {
				activations[flagName] = append(list, name)
			} else {
				activations[flagName] = append([]string{name}, list...)
			}
		}

		for _, opt := range itemIface.(PipelineItem).ListConfigurationOptions() {
			if opt.Shared {
				optCopy := opt
				if reused, ok := reusableOptions[opt.Flag]; !ok {
					optCopy.Description = name
					reusableOptions[opt.Flag] = optCopy
				} else {
					optCopy.Description = reused.Description
					if reflect.DeepEqual(reused, optCopy) {
						addFlagActivation(opt.Flag)
						continue
					}
					s := fmt.Sprintf("Param conflict of the option %s from: %s, %s", opt.Flag, reused.Description, name)
					fmt.Println(s)
					panic(s)
				}
			}

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
			case StringConfigurationOption, PathConfigurationOption:
				iface = interface{}("")
				ptr := (**string)(getPtr())
				*ptr = flagSet.String(opt.Flag, opt.Default.(string), formatHelp(opt.Description))
				if opt.Type == PathConfigurationOption {
					err := cobra.MarkFlagFilename(flagSet, opt.Flag)
					if err != nil {
						panic(err)
					}
					PathifyFlagValue(flagSet.Lookup(opt.Flag))
				}
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
			addFlagActivation(opt.Flag)
		}
	}
	{
		// Pipeline flags
		iface := interface{}("")
		ptr1 := (**string)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
		*ptr1 = flagSet.String("dump-dag", "", "Write the pipeline DAG to a Graphviz file.")
		flags[ConfigPipelineDAGPath] = iface
		PathifyFlagValue(flagSet.Lookup("dump-dag"))
		iface = interface{}(true)
		ptr2 := (**bool)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
		*ptr2 = flagSet.Bool("dry-run", false, "Do not run any analyses - only resolve the DAG. "+
			"Useful for --dump-dag or --dump-plan.")
		flags[ConfigPipelineDryRun] = iface
		iface = interface{}(true)
		ptr3 := (**bool)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
		*ptr3 = flagSet.Bool("dump-plan", false, "Print the pipeline execution plan to stderr.")
		flags[ConfigPipelineDumpPlan] = iface
		iface = interface{}(0)
		ptr4 := (**int)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
		*ptr4 = flagSet.Int("hibernation-distance", 0,
			"Minimum number of actions between two sequential usages of a branch to activate "+
				"the hibernation optimization (cpu-memory trade-off). 0 disables.")
		flags[ConfigPipelineHibernationDistance] = iface
		iface = interface{}(true)
		ptr5 := (**bool)(unsafe.Pointer(uintptr(unsafe.Pointer(&iface)) + unsafe.Sizeof(&iface)))
		*ptr5 = flagSet.Bool("print-actions", false, "Print the executed actions to stderr.")
		flags[ConfigPipelinePrintActions] = iface
	}
	var features []string
	for f := range registry.featureFlags.Choices {
		features = append(features, f)
	}
	flagSet.Var(&registry.featureFlags, "feature",
		fmt.Sprintf("Enables the items which depend on the specified features. Can be specified "+
			"multiple times. Available features: [%s] (see --feature below).",
			strings.Join(features, ", ")))

	return
}

// Registry contains all known pipeline item types.
var Registry = &PipelineItemRegistry{
	provided:     map[string][]reflect.Type{},
	registered:   map[string]reflect.Type{},
	preferred:    map[string]struct{}{},
	flags:        map[string]reflect.Type{},
	featureFlags: arrayFeatureFlags{Flags: []string{}, Choices: map[string]bool{}},
}
