package rbtree

/*
#cgo CFLAGS: -std=c99
int LZ4_compressBound(int isize);
int LZ4_compress_HC(const void* src, void* dst, int srcSize, int dstCapacity, int compressionLevel);
int LZ4_decompress_fast(const void* source, void* dest, int originalSize);
*/
import "C"
import "unsafe"

// CompressUInt32Slice compresses a slice of uint32-s with LZ4.
func CompressUInt32Slice(data []uint32) []byte {
	dstSize := C.LZ4_compressBound(C.int(len(data) * 4))
	dst := make([]byte, dstSize)
	dstSize = C.LZ4_compress_HC(
		unsafe.Pointer(&data[0]),
		unsafe.Pointer(&dst[0]),
		C.int(len(data)*4),
		dstSize,
		12)
	finalDst := make([]byte, dstSize)
	copy(finalDst, dst[:dstSize])
	return finalDst
}

// DecompressUInt32Slice decompresses a slice of uint32-s previously compressed with LZ4.
// `result` must be preallocated.
func DecompressUInt32Slice(data []byte, result []uint32) {
	C.LZ4_decompress_fast(
		unsafe.Pointer(&data[0]),
		unsafe.Pointer(&result[0]),
		C.int(len(result)*4))
}
