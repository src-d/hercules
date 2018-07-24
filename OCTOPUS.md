# Forks and merges in commit history

Hercules expects the commit history to be linear.
It follows the main (zero index) branch when it encounters a fork.
This behavior ignores all the side branches, and we are currently
thinking how to include them into the analysis.

### Plan - done

* Commits must be ordered topologically.
* When a fork is hit, clone the pipeline. Assign the old instance to the main branch and new
instances to the sprouts. BurndownAnalysis should share the same counters for efficiency
and simplicity, but the files must be copied.
* Follow each branch independently. Clone side pipelines as needed.
* Join pipelines on merge commits. Side pipelines are killed, the main instance survives.
This will be tricky for Burndown because we need to join the files together while preserving
the line annotations. The plan is to calculate the separate line annotations for each branch and blend them,
the oldest timestamp winning.
* Merge commits should have diffs which correspond to CGit diffs. So far they represent the diff
with the previous commit in the main branch.
* The sequence of commits must be the analysis scenario: it must inform when to fork and to merge,
which pipeline instance to apply.

### New APIs - done

* PipelineItem
  * `Fork()`
  * `Merge()`
  
### Major changes

* `Pipeline`
  * `Commits()` - done
  * `Run()` - done
* `Burndown` - done
* `Couples`
* `FileDiff` - done