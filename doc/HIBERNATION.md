# Hibernation

Hercules supports signalling pipeline items when they are not going to be needed for some period
n the future and when they are going to be used after that period.
Pipeline items which support hibernation are expected to compress and decompress their data
corresponding to the described signals.
This mechanism is called *hibernation*. It can be used in the cases when there many parallel
branches and the free operating memory runs too small.
Hibernation is a special analysis mode and is disabled by default. It can be enabled with

```
hercules --burndown-distance N
```

where N is the minimum distance between two sequential usages of a branch to hibernate it.
The distance is measured in the number of commits, forks, merges. etc. in the linear execution plan.
Usually 10 is a good default; the bigger N, the less hibernation operations,
the faster the analysis but the bigger memory pressure.

There is also `--hibernate-disk` flag which maintains 

## Burndown

The burndown analysis' hibernation compresses the blame information about files with LZ4 algorithm.
It works very effectively and is actually better than zlib according to the tests.
There are some further defined flags:

`--burndown-hibernation-threshold N` is the minimum number of files registered in a branch to start hibernating.

`--burndown-hibernation-disk` dumps the compressed blame info on disk instead of keeping them in memory.

`--burndown-hibernation-dir` sets the path for the previous feature.