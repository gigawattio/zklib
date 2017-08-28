package candidate

import (
	"strings"
)

type ZNodes []string

func (zNodes ZNodes) Len() int {
	return len(zNodes)
}

func (zNodes ZNodes) Less(i, j int) bool {
	var (
		idx = strings.LastIndex(zNodes[i], "_")
		jdx = strings.LastIndex(zNodes[j], "_")
	)
	if idx < 0 && jdx < 0 {
		return false
	} else if idx < 0 {
		return false
	} else if jdx < 0 {
		return true
	}
	if iLen0, jLen0 := idx == len(zNodes[i])-1, jdx == len(zNodes[j])-1; iLen0 && jLen0 {
		return false
	} else if iLen0 && !jLen0 {
		return false
	} else if !iLen0 && jLen0 {
		return true
	}
	return zNodes[i][idx+1:] < zNodes[j][jdx+1:]
}

func (zNodes ZNodes) Swap(i, j int) {
	zNodes[i], zNodes[j] = zNodes[j], zNodes[i]
}
