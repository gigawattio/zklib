package candidate_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/gigawattio/zklib/candidate"
)

func TestZxIdsSort(t *testing.T) {
	testCases := []struct {
		zxIds  candidate.ZxIds
		sorted candidate.ZxIds
	}{
		{
			zxIds: candidate.ZxIds([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
			}),
			sorted: candidate.ZxIds([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
			}),
		},
		{
			zxIds: candidate.ZxIds([]string{
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
			}),
			sorted: candidate.ZxIds([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
			}),
		},
		{
			zxIds: candidate.ZxIds([]string{
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
			}),
			sorted: candidate.ZxIds([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
			}),
		},
		{
			zxIds: candidate.ZxIds([]string{
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"f",
			}),
			sorted: candidate.ZxIds([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
				"f",
			}),
		},
		{
			zxIds: candidate.ZxIds([]string{
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"f",
			}),
			sorted: candidate.ZxIds([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
				"f",
			}),
		},
		{
			zxIds: candidate.ZxIds([]string{
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"f",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
			}),
			sorted: candidate.ZxIds([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"f",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
			}),
		},
		{
			zxIds: candidate.ZxIds([]string{
				"_c_53c7a51efe38eb708bc8f49a4eb749ce-n_0000000056",
				"_c_8e72068f50d33e2efbee037cf4bda745-n_0000000054",
			}),
			sorted: candidate.ZxIds([]string{
				"_c_8e72068f50d33e2efbee037cf4bda745-n_0000000054",
				"_c_53c7a51efe38eb708bc8f49a4eb749ce-n_0000000056",
			}),
		},
		{
			zxIds: candidate.ZxIds([]string{
				"_c_8e72068f50d33e2efbee037cf4bda745-n_0000000054",
				"_c_53c7a51efe38eb708bc8f49a4eb749ce-n_0000000056",
			}),
			sorted: candidate.ZxIds([]string{
				"_c_8e72068f50d33e2efbee037cf4bda745-n_0000000054",
				"_c_53c7a51efe38eb708bc8f49a4eb749ce-n_0000000056",
			}),
		},
	}
	for i, testCase := range testCases {
		copiedZxIds := make(candidate.ZxIds, 0, len(testCase.zxIds))
		for _, zxId := range testCase.zxIds {
			copiedZxIds = append(copiedZxIds, zxId)
		}

		sort.Sort(copiedZxIds)

		if actual, expected := fmt.Sprintf("%+v", copiedZxIds), fmt.Sprintf("%+v", testCase.sorted); actual != expected {
			t.Errorf("Expected sorted copiedZxIds=%v but actual=%v for i=%v/testCase=%+v", expected, actual, i, testCase)
		}
	}
}
