package helpers

func CeilDivInt64(a int64, b int64) int64 {
	if a <= 0 || b <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

func MaxInt64(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
