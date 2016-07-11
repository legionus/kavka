package util

import (
	"strconv"
)

func ToInt32(s string) int32 {
	if s == "" {
		return 0
	}
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(i)
}

func ToInt64(s string) int64 {
	if s == "" {
		return 0
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func ToUint64(s string) uint64 {
	if s == "" {
		return 0
	}
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}
