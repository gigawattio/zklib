package testutil_test

import (
	"os/exec"
	"testing"

	"github.com/gigawattio/zklib/testutil"
)

func TestNestedUsage(t *testing.T) {
	testutil.WithTestZkCluster(t, 1, func(zkServers []string) {
		t.Log("Inside WithTestZkCluster Function #1")
		testutil.WithTestZkCluster(t, 1, func(zkServers []string) {
			t.Log("Inside WithTestZkCluster Function #2")
		})
	})
}

func TestCommandNotFoundDetection(t *testing.T) {
	testCases := []struct {
		Cmd            string
		ExpectNotFound bool
	}{
		{
			Cmd:            "/bin/bash",
			ExpectNotFound: false,
		},
		{
			Cmd:            "ksadfalskdjfal99",
			ExpectNotFound: true,
		},
		{
			Cmd:            "/xmnv09d9asddkdka",
			ExpectNotFound: true,
		},
	}
	for i, testCase := range testCases {
		cmd := exec.Command(testCase.Cmd)
		output, err := cmd.CombinedOutput()
		result := testutil.IsCommandNotFound(output, err)
		if testCase.ExpectNotFound {
			if result == nil {
				t.Errorf("Expected to detect command not found but result=%v (should have been non-nil) i=%v testCase=%+v", result, i, testCase)
			}
		} else {
			if result != nil {
				t.Errorf("Expected command found but result=%v (should have been nil) i=%v testCase=%+v", result, i, testCase)
			}
		}
	}
}
