Pipeline items
==============

### PipelineItem lifecycle

```go
type PipelineItem interface {
	// Name returns the name of the analysis.
	Name() string
	// Provides returns the list of keys of reusable calculated entities.
	// Other items may depend on them.
	Provides() []string
	// Requires returns the list of keys of needed entities which must be supplied in Consume().
	Requires() []string
	// ListConfigurationOptions returns the list of available options which can be consumed by Configure().
	ListConfigurationOptions() []ConfigurationOption
	// Configure performs the initial setup of the object by applying parameters from facts.
	// It allows to create PipelineItems in a universal way.
	Configure(facts map[string]interface{})
	// Initialize prepares and resets the item. Consume() requires Initialize()
	// to be called at least once beforehand.
	Initialize(*git.Repository)
	// Consume processes the next commit.
	// deps contains the required entities which match Depends(). Besides, it always includes
	// "commit" and "index".
	// Returns the calculated entities which match Provides().
	Consume(deps map[string]interface{}) (map[string]interface{}, error)
}
```

![PipelineItem](pipeline_item.png)

### LeafPipelineItem lifecycle

```go
type LeafPipelineItem interface {
	PipelineItem
	// Flag returns the cmdline name of the item.
	Flag() string
	// Finalize returns the result of the analysis.
	Finalize() interface{}
	// Serialize encodes the object returned by Finalize() to Text or Protocol Buffers.
	Serialize(result interface{}, binary bool, writer io.Writer) error
}
```

![LeafPipelineItem](leaf_pipeline_item.png)
