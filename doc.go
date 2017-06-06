/*
Package hercules contains the functions which are needed to gather the line
burndown statistics from a Git repository.

Analyser is the main object which concentrates the high level logic. It
provides Commits() and Analyse() methods to get the work done. The following
example was taken from cmd/hercules:

	var repository *git.Repository
	// ... initialize repository ...
	analyser := hercules.Analyser{
		Repository: repository,
		OnProgress: func(commit, length int) {
			fmt.Fprintf(os.Stderr, "%d / %d\r", commit, length)
		},
		Granularity:         30,
		Sampling:            15,
		SimilarityThreshold: 90,
		Debug:               false,
	}
	commits := analyser.Commits()  // or specify a custom list
	statuses := analyser.Analyse(commits)
	// [y][x]int64 where y is the snapshot index and x is the granulated time index.

As commented in the code, the list of commits can be any valid slice of *object.Commit.
The returned statuses slice of slices is a rectangular 2D matrix where
the number of rows equals to the repository's lifetime divided by the sampling
value (detail factor) and the number of columns is the repository's lifetime
divided by the granularity value (number of bands).

Analyser depends heavily on https://github.com/src-d/go-git and leverages the
diff algorithm through https://github.com/sergi/go-diff.

Besides, hercules defines File and RBTree. These are low level data structures
required by Analyser. File carries an instance of RBTree and the current line
burndown state. RBTree implements the red-black balanced binary tree and is
based on https://github.com/yasushi-saito/rbtree.
*/
package hercules
