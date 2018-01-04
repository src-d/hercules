package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"
	"gopkg.in/src-d/hercules.v3"
	"gopkg.in/src-d/hercules.v3/pb"
)

// combineCmd represents the combine command
var combineCmd = &cobra.Command{
	Use:   "combine",
	Short: "Merge several binary analysis results together.",
	Long:  ``,
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, files []string) {
		if len(files) == 1 {
			file, err := os.Open(files[0])
			if err != nil {
				panic(err)
			}
			defer file.Close()
			io.Copy(os.Stdout, bufio.NewReader(file))
			return
		}
		repos := []string{}
		allErrors := map[string][]string{}
		mergedResults := map[string]interface{}{}
		mergedMetadata := &hercules.CommonAnalysisResult{}
		for _, fileName := range files {
			anotherResults, anotherMetadata, errs := loadMessage(fileName, &repos)
			if anotherMetadata != nil {
				mergeResults(mergedResults, mergedMetadata, anotherResults, anotherMetadata)
			}
			allErrors[fileName] = errs
		}
		printErrors(allErrors)
		sort.Strings(repos)
		if mergedMetadata == nil {
			return
		}
		mergedMessage := pb.AnalysisResults{
			Header: &pb.Metadata{
				Version:    2,
				Hash:       hercules.GIT_HASH,
				Repository: strings.Join(repos, " & "),
			},
			Contents: map[string][]byte{},
		}
		mergedMetadata.FillMetadata(mergedMessage.Header)
		for key, val := range mergedResults {
			buffer := bytes.Buffer{}
			hercules.Registry.Summon(key)[0].(hercules.LeafPipelineItem).Serialize(
				val, true, &buffer)
			mergedMessage.Contents[key] = buffer.Bytes()
		}
		serialized, err := proto.Marshal(&mergedMessage)
		if err != nil {
			panic(err)
		}
		os.Stdout.Write(serialized)
	},
}

func loadMessage(fileName string, repos *[]string) (
	map[string]interface{}, *hercules.CommonAnalysisResult, []string) {
	errs := []string{}
	buffer, err := ioutil.ReadFile(fileName)
	if err != nil {
		errs = append(errs, "Cannot read "+fileName+": "+err.Error())
		return nil, nil, errs
	}
	message := pb.AnalysisResults{}
	err = proto.Unmarshal(buffer, &message)
	if err != nil {
		errs = append(errs, "Cannot parse "+fileName+": "+err.Error())
		return nil, nil, errs
	}
	*repos = append(*repos, message.Header.Repository)
	results := map[string]interface{}{}
	for key, val := range message.Contents {
		summoned := hercules.Registry.Summon(key)
		if len(summoned) == 0 {
			errs = append(errs, fileName+": item not found: "+key)
			continue
		}
		mpi, ok := summoned[0].(hercules.MergeablePipelineItem)
		if !ok {
			errs = append(errs, fileName+": "+key+": MergeablePipelineItem is not implemented")
			continue
		}
		msg, err := mpi.Deserialize(val)
		if err != nil {
			errs = append(errs, fileName+": deserialization failed: "+key+": "+err.Error())
			continue
		}
		results[key] = msg
	}
	return results, hercules.MetadataToCommonAnalysisResult(message.Header), errs
}

func printErrors(allErrors map[string][]string) {
	needToPrintErrors := false
	for _, errs := range allErrors {
		if len(errs) > 0 {
			needToPrintErrors = true
			break
		}
	}
	if !needToPrintErrors {
		return
	}
	fmt.Fprintln(os.Stderr, "Errors:")
	for key, errs := range allErrors {
		if len(errs) > 0 {
			fmt.Fprintln(os.Stderr, "  "+key)
			for _, err := range errs {
				fmt.Fprintln(os.Stderr, "    "+err)
			}
		}
	}
}

func mergeResults(mergedResults map[string]interface{},
	mergedCommons *hercules.CommonAnalysisResult,
	anotherResults map[string]interface{},
	anotherCommons *hercules.CommonAnalysisResult) {
	for key, val := range anotherResults {
		mergedResult, exists := mergedResults[key]
		if !exists {
			mergedResults[key] = val
			continue
		}
		item := hercules.Registry.Summon(key)[0].(hercules.MergeablePipelineItem)
		mergedResult = item.MergeResults(mergedResult, val, mergedCommons, anotherCommons)
		mergedResults[key] = mergedResult
	}
	if mergedCommons.CommitsNumber == 0 {
		*mergedCommons = *anotherCommons
	} else {
		mergedCommons.Merge(anotherCommons)
	}
}

func init() {
	rootCmd.AddCommand(combineCmd)
	combineCmd.SetUsageFunc(combineCmd.UsageFunc())
}
