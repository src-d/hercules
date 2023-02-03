package burndown

import (
	"github.com/cyraxred/hercules/internal/core"
	"github.com/cyraxred/hercules/internal/plumbing"
	"log"
	"math"
	"runtime"
	"time"
)

// DenseHistory is the matrix [number of samples][number of bands] -> number of lines.
//                                    y                  x
type DenseHistory = [][]int64

// AddBurndownMatrix explodes `matrix` so that it is daily sampled and has daily bands, shift by `offset` ticks
// and add to the accumulator. `daily` size is square and is guaranteed to fit `matrix` by
// the caller.
// Rows: *at least* len(matrix) * sampling + offset
// Columns: *at least* len(matrix[...]) * granularity + offset
// `matrix` can be sparse, so that the last columns which are equal to 0 are truncated.
func AddBurndownMatrix(matrix DenseHistory, granularity, sampling int, accPerTick [][]float32, offset int) {

	//	defer print("AddBurndownMatrix exit\n")
	//	print("AddBurndownMatrix enter\n")

	// Determine the maximum number of bands; the actual one may be larger but we do not care
	maxCols := 0
	for _, row := range matrix {
		if maxCols < len(row) {
			maxCols = len(row)
		}
	}
	neededRows := len(matrix)*sampling + offset
	if len(accPerTick) < neededRows {
		log.Panicf("merge bug: too few per-tick rows: required %d, have %d",
			neededRows, len(accPerTick))
	}
	if len(accPerTick[0]) < maxCols {
		log.Panicf("merge bug: too few per-tick cols: required %d, have %d",
			maxCols, len(accPerTick[0]))
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	perTick := make([][]float32, len(accPerTick))
	for i, row := range accPerTick {
		perTick[i] = make([]float32, len(row))
	}

	//	print("AddBurndownMatrix Allocating: ", len(accPerTick), " x ", len(perTick[0]), " = ", len(accPerTick)*len(perTick[0])*4/1024/1024, ", total ", m.Alloc/1024/1024, "\n")

	for x := 0; x < maxCols; x++ {
		for y := 0; y < len(matrix); y++ {
			if x*granularity > (y+1)*sampling {
				// the future is zeros
				continue
			}
			decay := func(startIndex int, startVal float32) {
				if startVal == 0 {
					return
				}
				k := float32(matrix[y][x]) / startVal // <= 1
				scale := float32((y+1)*sampling - startIndex)
				for i := x * granularity; i < (x+1)*granularity; i++ {
					initial := perTick[startIndex-1+offset][i+offset]
					for j := startIndex; j < (y+1)*sampling; j++ {
						perTick[j+offset][i+offset] = initial * (1 + (k-1)*float32(j-startIndex+1)/scale)
					}
				}
			}
			raise := func(finishIndex int, finishVal float32) {
				var initial float32
				if y > 0 {
					initial = float32(matrix[y-1][x])
				}
				startIndex := y * sampling
				if startIndex < x*granularity {
					startIndex = x * granularity
				}
				if startIndex == finishIndex {
					return
				}
				avg := (finishVal - initial) / float32(finishIndex-startIndex)
				for j := y * sampling; j < finishIndex; j++ {
					for i := startIndex; i <= j; i++ {
						perTick[j+offset][i+offset] = avg
					}
				}
				// copy [x*g..y*s)
				for j := y * sampling; j < finishIndex; j++ {
					for i := x * granularity; i < y*sampling; i++ {
						perTick[j+offset][i+offset] = perTick[j-1+offset][i+offset]
					}
				}
			}
			if (x+1)*granularity >= (y+1)*sampling {
				// x*granularity <= (y+1)*sampling
				// 1. x*granularity <= y*sampling
				//    y*sampling..(y+1)sampling
				//
				//       x+1
				//        /
				//       /
				//      / y+1  -|
				//     /        |
				//    / y      -|
				//   /
				//  / x
				//
				// 2. x*granularity > y*sampling
				//    x*granularity..(y+1)sampling
				//
				//       x+1
				//        /
				//       /
				//      / y+1  -|
				//     /        |
				//    / x      -|
				//   /
				//  / y
				if x*granularity <= y*sampling {
					raise((y+1)*sampling, float32(matrix[y][x]))
				} else if (y+1)*sampling > x*granularity {
					raise((y+1)*sampling, float32(matrix[y][x]))
					avg := float32(matrix[y][x]) / float32((y+1)*sampling-x*granularity)
					for j := x * granularity; j < (y+1)*sampling; j++ {
						for i := x * granularity; i <= j; i++ {
							perTick[j+offset][i+offset] = avg
						}
					}
				}
			} else if (x+1)*granularity >= y*sampling {
				// y*sampling <= (x+1)*granularity < (y+1)sampling
				// y*sampling..(x+1)*granularity
				// (x+1)*granularity..(y+1)sampling
				//        x+1
				//         /\
				//        /  \
				//       /    \
				//      /    y+1
				//     /
				//    y
				v1 := float32(matrix[y-1][x])
				v2 := float32(matrix[y][x])
				var peak float32
				delta := float32((x+1)*granularity - y*sampling)
				var scale float32
				var previous float32
				if y > 0 && (y-1)*sampling >= x*granularity {
					// x*g <= (y-1)*s <= y*s <= (x+1)*g <= (y+1)*s
					//           |________|.......^
					if y > 1 {
						previous = float32(matrix[y-2][x])
					}
					scale = float32(sampling)
				} else {
					// (y-1)*s < x*g <= y*s <= (x+1)*g <= (y+1)*s
					//            |______|.......^
					if y == 0 {
						scale = float32(sampling)
					} else {
						scale = float32(y*sampling - x*granularity)
					}
				}
				peak = v1 + (v1-previous)/scale*delta
				if v2 > peak {
					// we need to adjust the peak, it may not be less than the decayed value
					if y < len(matrix)-1 {
						// y*s <= (x+1)*g <= (y+1)*s < (y+2)*s
						//           ^.........|_________|
						k := (v2 - float32(matrix[y+1][x])) / float32(sampling) // > 0
						peak = float32(matrix[y][x]) + k*float32((y+1)*sampling-(x+1)*granularity)
						// peak > v2 > v1
					} else {
						peak = v2
						// not enough data to interpolate; this is at least not restricted
					}
				}
				raise((x+1)*granularity, peak)
				decay((x+1)*granularity, peak)
			} else {
				// (x+1)*granularity < y*sampling
				// y*sampling..(y+1)sampling
				decay(y*sampling, float32(matrix[y-1][x]))
			}
		}
	}
	for y := len(matrix) * sampling; y+offset < len(perTick); y++ {
		copy(perTick[y+offset], perTick[len(matrix)*sampling-1+offset])
	}
	// the original matrix has been resampled by tick
	// add it to the accumulator
	for y, row := range perTick {
		for x, val := range row {
			accPerTick[y][x] += val
		}
	}

	runtime.ReadMemStats(&m)
	for i := range perTick {
		perTick[i] = nil
	}
	perTick = nil
	runtime.GC()
	var a runtime.MemStats
	runtime.ReadMemStats(&a)

	//	print("AddBurndownMatrix Deallocated: ", (m.Alloc-a.Alloc)/1024/1024, "\n")

}

func roundTime(t time.Time, d time.Duration, dir bool) int {
	if !dir {
		t = plumbing.FloorTime(t, d)
	}
	ticks := float64(t.Unix()) / d.Seconds()
	if dir {
		return int(math.Ceil(ticks))
	}
	return int(math.Floor(ticks))
}

// MergeBurndownMatrices takes two [number of samples][number of bands] matrices,
// resamples them to ticks so that they become square, sums and resamples back to the
// least of (sampling1, sampling2) and (granularity1, granularity2).
func MergeBurndownMatrices(
	m1, m2 DenseHistory, granularity1, sampling1, granularity2, sampling2 int, tickSize time.Duration,
	c1, c2 *core.CommonAnalysisResult) DenseHistory {

	//	defer print("MergeBurndownMatrices exit\n\n\n")
	//	print("MergeBurndownMatrices enter\n\n\n")

	commonMerged := c1.Copy()
	commonMerged.Merge(c2)

	var granularity, sampling int
	if sampling1 < sampling2 {
		sampling = sampling1
	} else {
		sampling = sampling2
	}
	if granularity1 < granularity2 {
		granularity = granularity1
	} else {
		granularity = granularity2
	}

	size := roundTime(commonMerged.EndTimeAsTime(), tickSize, true) -
		roundTime(commonMerged.BeginTimeAsTime(), tickSize, false)

	perTick := make([][]float32, size+granularity)
	for i := range perTick {
		perTick[i] = make([]float32, size+sampling)
	}

	//	var m runtime.MemStats
	//	runtime.ReadMemStats(&m)

	//	print("MergeBurndownMatrices Allocating: ", size+granularity, " x ", size+sampling, " = ", (size+granularity)*(size+sampling)*4/1024/1024, ", total ", m.Alloc/1024/1024, "\n")

	if len(m1) > 0 {
		AddBurndownMatrix(m1, granularity1, sampling1, perTick,
			roundTime(c1.BeginTimeAsTime(), tickSize, false)-roundTime(commonMerged.BeginTimeAsTime(), tickSize, false))
	}
	if len(m2) > 0 {
		AddBurndownMatrix(m2, granularity2, sampling2, perTick,
			roundTime(c2.BeginTimeAsTime(), tickSize, false)-roundTime(commonMerged.BeginTimeAsTime(), tickSize, false))
	}

	// convert daily to [][]int64
	result := make(DenseHistory, (size+sampling-1)/sampling)
	for i := range result {
		result[i] = make([]int64, (size+granularity-1)/granularity)
		sampledIndex := (i+1)*sampling - 1
		for j := 0; j < len(result[i]); j++ {
			accum := float32(0)
			for k := j * granularity; k < (j+1)*granularity; k++ {
				accum += perTick[sampledIndex][k]
			}
			result[i][j] = int64(accum)
		}
	}

	//	runtime.ReadMemStats(&m)

	for i := range perTick {
		perTick[i] = nil
	}
	perTick = nil

	runtime.GC()

	//	var a runtime.MemStats
	//	runtime.ReadMemStats(&a)

	//	print("MergeBurndownMatrices Deallocated: ", (m.Alloc-a.Alloc)/1024/1024, "\n")

	return result
}
