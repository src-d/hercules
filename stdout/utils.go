package stdout

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/src-d/hercules.v2"
)

func SafeString(str string) string {
	str = strings.Replace(str, "\\", "\\\\", -1)
	str = strings.Replace(str, "\"", "\\\"", -1)
	return "\"" + str + "\""
}

func PrintMatrix(matrix [][]int64, name string, fixNegative bool) {
	// determine the maximum length of each value
	var maxnum int64 = -(1 << 32)
	var minnum int64 = 1 << 32
	for _, status := range matrix {
		for _, val := range status {
			if val > maxnum {
				maxnum = val
			}
			if val < minnum {
				minnum = val
			}
		}
	}
	width := len(strconv.FormatInt(maxnum, 10))
	if !fixNegative && minnum < 0 {
		negativeWidth := len(strconv.FormatInt(minnum, 10))
		if negativeWidth > width {
			width = negativeWidth
		}
	}
	last := len(matrix[len(matrix)-1])
	indent := 2
	if name != "" {
		fmt.Printf("  %s: |-\n", SafeString(name))
		indent += 2
	}
	// print the resulting triangular matrix
	first := true
	for _, status := range matrix {
		fmt.Print(strings.Repeat(" ", indent-1))
		for i := 0; i < last; i++ {
			var val int64
			if i < len(status) {
				val = status[i]
				// not sure why this sometimes happens...
				// TODO(vmarkovtsev): find the root cause of tiny negative balances
				if fixNegative && val < 0 {
					val = 0
				}
			}
			if !first {
				fmt.Printf(" %[1]*[2]d", width, val)
			} else {
				first = false
				fmt.Printf("%d%s", val, strings.Repeat(" ", width-len(strconv.FormatInt(val, 10))))
			}
		}
		fmt.Println()
	}
}

func PrintCouples(result *hercules.CouplesResult, peopleDict []string) {
	fmt.Println("files_coocc:")
	fmt.Println("  index:")
	for _, file := range result.Files {
		fmt.Printf("    - %s\n", SafeString(file))
	}

	fmt.Println("  matrix:")
	for _, files := range result.FilesMatrix {
		fmt.Print("    - {")
		indices := []int{}
		for file := range files {
			indices = append(indices, file)
		}
		sort.Ints(indices)
		for i, file := range indices {
			fmt.Printf("%d: %d", file, files[file])
			if i < len(indices)-1 {
				fmt.Print(", ")
			}
		}
		fmt.Println("}")
	}

	fmt.Println("people_coocc:")
	fmt.Println("  index:")
	for _, person := range peopleDict {
		fmt.Printf("    - %s\n", SafeString(person))
	}

	fmt.Println("  matrix:")
	for _, people := range result.PeopleMatrix {
		fmt.Print("    - {")
		indices := []int{}
		for file := range people {
			indices = append(indices, file)
		}
		sort.Ints(indices)
		for i, person := range indices {
			fmt.Printf("%d: %d", person, people[person])
			if i < len(indices)-1 {
				fmt.Print(", ")
			}
		}
		fmt.Println("}")
	}

	fmt.Println("  author_files:") // sorted by number of files each author changed
	peopleFiles := sortByNumberOfFiles(result.PeopleFiles, peopleDict, result.Files)
	for _, authorFiles := range peopleFiles {
		fmt.Printf("    - %s:\n", SafeString(authorFiles.Author))
		sort.Strings(authorFiles.Files)
		for _, file := range authorFiles.Files {
			fmt.Printf("      - %s\n", SafeString(file)) // sorted by path
		}
	}
}

func sortByNumberOfFiles(
	peopleFiles [][]int, peopleDict []string, filesDict []string) authorFilesList {
	var pfl authorFilesList
	for peopleIdx, files := range peopleFiles {
		if peopleIdx < len(peopleDict) {
			fileNames := make([]string, len(files))
			for i, fi := range files {
				fileNames[i] = filesDict[fi]
			}
			pfl = append(pfl, authorFiles{peopleDict[peopleIdx], fileNames})
		}
	}
	sort.Sort(pfl)
	return pfl
}

type authorFiles struct {
	Author string
	Files  []string
}

type authorFilesList []authorFiles

func (s authorFilesList) Len() int {
	return len(s)
}
func (s authorFilesList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s authorFilesList) Less(i, j int) bool {
	return len(s[i].Files) < len(s[j].Files)
}
