package internal

// The ugly side of Go.
// template <typename T> please!

// Min calculates the minimum of two 32-bit integers.
func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// Min64 calculates the minimum of two 64-bit integers.
func Min64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Max calculates the maximum of two 32-bit integers.
func Max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

// Max64 calculates the maximum of two 64-bit integers.
func Max64(a int64, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

// Abs64 calculates the absolute value of a 64-bit integer.
func Abs64(v int64) int64 {
	if v <= 0 {
		return -v
	}
	return v
}
