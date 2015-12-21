package candidate

import (
	"strings"
)

type ZxIds []string

func (zxIds ZxIds) Len() int {
	return len(zxIds)
}

func (zxIds ZxIds) Less(i, j int) bool {
	var (
		idx = strings.LastIndex(zxIds[i], "_")
		jdx = strings.LastIndex(zxIds[i], "_")
	)
	if idx < 0 || jdx < 0 || idx == len(zxIds[i])-1 || jdx == len(zxIds[j])-1 {
		return false
	}
	return zxIds[i][idx+1:] < zxIds[j][jdx+1:]
}

func (zxIds ZxIds) Swap(i, j int) {
	zxIds[i], zxIds[j] = zxIds[j], zxIds[i]
}
