package candidate_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/gigawattio/zklib/candidate"
)

func TestZNodesSort(t *testing.T) {
	testCases := []struct {
		zNodes candidate.ZNodes
		sorted candidate.ZNodes
	}{
		{
			zNodes: candidate.ZNodes([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
			}),
			sorted: candidate.ZNodes([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
			}),
		},
		{
			zNodes: candidate.ZNodes([]string{
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
			}),
			sorted: candidate.ZNodes([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
			}),
		},
		{
			zNodes: candidate.ZNodes([]string{
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
			}),
			sorted: candidate.ZNodes([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
			}),
		},
		{
			zNodes: candidate.ZNodes([]string{
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"f",
			}),
			sorted: candidate.ZNodes([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026",
				"f",
			}),
		},
		{
			zNodes: candidate.ZNodes([]string{
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"f",
			}),
			sorted: candidate.ZNodes([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
				"f",
			}),
		},
		{
			zNodes: candidate.ZNodes([]string{
				"f",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
			}),
			sorted: candidate.ZNodes([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
				"f",
			}),
		},
		{
			zNodes: candidate.ZNodes([]string{
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"f",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
			}),
			sorted: candidate.ZNodes([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
				"f",
			}),
		},
		{
			zNodes: candidate.ZNodes([]string{
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"f",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
			}),
			sorted: candidate.ZNodes([]string{
				"_c_1a80c660a1a8b8882a8dffcafbafc8d9-n_0000000002",
				"_c_9ad2eeac78901f5bf35285f97be4a331-n_0000000005",
				"_c_9f71a47ef75f33dd0847439c5a1e8289-n_0000000011",
				"_c_73d9a263008f90c421535da44d6a54bb-n_0000000026_",
				"f",
			}),
		},
		{
			zNodes: candidate.ZNodes([]string{
				"_c_53c7a51efe38eb708bc8f49a4eb749ce-n_0000000056",
				"_c_8e72068f50d33e2efbee037cf4bda745-n_0000000054",
			}),
			sorted: candidate.ZNodes([]string{
				"_c_8e72068f50d33e2efbee037cf4bda745-n_0000000054",
				"_c_53c7a51efe38eb708bc8f49a4eb749ce-n_0000000056",
			}),
		},
		{
			zNodes: candidate.ZNodes([]string{
				"_c_8e72068f50d33e2efbee037cf4bda745-n_0000000054",
				"_c_53c7a51efe38eb708bc8f49a4eb749ce-n_0000000056",
			}),
			sorted: candidate.ZNodes([]string{
				"_c_8e72068f50d33e2efbee037cf4bda745-n_0000000054",
				"_c_53c7a51efe38eb708bc8f49a4eb749ce-n_0000000056",
			}),
		},
	}
	for i, testCase := range testCases {
		copiedZNodes := make(candidate.ZNodes, 0, len(testCase.zNodes))
		for _, zNode := range testCase.zNodes {
			copiedZNodes = append(copiedZNodes, zNode)
		}

		sort.Sort(copiedZNodes)

		if actual, expected := fmt.Sprintf("%+v", copiedZNodes), fmt.Sprintf("%+v", testCase.sorted); actual != expected {
			t.Errorf("[i=%v] Expected sorted copiedZNodes=%v but actual=%v", i, expected, actual)
		}
	}
}
