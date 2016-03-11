package util

import (
	"strings"
)

func NormalizePath(path string) string {
	if len(path) > 0 && path[0] != '/' {
		path = "/" + path
	}
	path = strings.TrimRight(path, "/")
	return path
}
