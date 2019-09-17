package pb

import (
	"sort"
)

// ToBurndownSparseMatrix converts a rectangular integer matrix to the corresponding Protobuf object.
// It is specific to hercules.BurndownAnalysis.
func ToBurndownSparseMatrix(matrix [][]int64, name string) *BurndownSparseMatrix {
	if len(matrix) == 0 {
		panic("matrix may not be nil or empty")
	}
	r := BurndownSparseMatrix{
		Name:            name,
		NumberOfRows:    int32(len(matrix)),
		NumberOfColumns: int32(len(matrix[len(matrix)-1])),
		Rows:            make([]*BurndownSparseMatrixRow, len(matrix)),
	}
	for i, status := range matrix {
		nnz := make([]uint32, 0, len(status))
		changed := false
		for j := range status {
			v := status[len(status)-1-j]
			if v < 0 {
				v = 0
			}
			if !changed {
				changed = v != 0
			}
			if changed {
				nnz = append(nnz, uint32(v))
			}
		}
		r.Rows[i] = &BurndownSparseMatrixRow{
			Columns: make([]uint32, len(nnz)),
		}
		for j := range nnz {
			r.Rows[i].Columns[j] = nnz[len(nnz)-1-j]
		}
	}
	return &r
}

// DenseToCompressedSparseRowMatrix takes an integer matrix and converts it to a Protobuf CSR.
// CSR format: https://en.wikipedia.org/wiki/Sparse_matrix#Compressed_sparse_row_.28CSR.2C_CRS_or_Yale_format.29
func DenseToCompressedSparseRowMatrix(matrix [][]int64) *CompressedSparseRowMatrix {
	r := CompressedSparseRowMatrix{
		NumberOfRows:    int32(len(matrix)),
		NumberOfColumns: int32(len(matrix[0])),
		Data:            make([]int64, 0),
		Indices:         make([]int32, 0),
		Indptr:          make([]int64, 1),
	}
	r.Indptr[0] = 0
	for _, row := range matrix {
		nnz := 0
		for x, col := range row {
			if col != 0 {
				r.Data = append(r.Data, col)
				r.Indices = append(r.Indices, int32(x))
				nnz++
			}
		}
		r.Indptr = append(r.Indptr, r.Indptr[len(r.Indptr)-1]+int64(nnz))
	}
	return &r
}

// MapToCompressedSparseRowMatrix takes an integer matrix and converts it to a Protobuf CSR.
// In contrast to DenseToCompressedSparseRowMatrix, a matrix here is already in DOK format.
// CSR format: https://en.wikipedia.org/wiki/Sparse_matrix#Compressed_sparse_row_.28CSR.2C_CRS_or_Yale_format.29
func MapToCompressedSparseRowMatrix(matrix []map[int]int64) *CompressedSparseRowMatrix {
	r := CompressedSparseRowMatrix{
		NumberOfRows:    int32(len(matrix)),
		NumberOfColumns: int32(len(matrix)),
		Data:            make([]int64, 0),
		Indices:         make([]int32, 0),
		Indptr:          make([]int64, 1),
	}
	r.Indptr[0] = 0
	for _, row := range matrix {
		order := make([]int, len(row))
		i := 0
		for col := range row {
			order[i] = col
			i++
		}
		sort.Ints(order)
		for _, col := range order {
			val := row[col]
			r.Data = append(r.Data, val)
			r.Indices = append(r.Indices, int32(col))
		}
		r.Indptr = append(r.Indptr, r.Indptr[len(r.Indptr)-1]+int64(len(row)))
	}
	return &r
}
