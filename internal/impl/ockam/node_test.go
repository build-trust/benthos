package ockam_test

import (
	"testing"

	"github.com/benthosdev/benthos/v4/internal/impl/ockam"
)

func TestNodeCreate(t *testing.T) {
	config := `
		{
			"tcp-outlets": {
				"an-outlet": {
					"to": 9999
				}
			}
		}
	`

	node, err := ockam.NewNode(config)
	if err != nil {
		t.Error("failed to create a node, error: ", err)
	}

	isRunning := node.IsRunning()
	if !isRunning {
		t.Error("node is not running")
	}

	err = node.Delete()
	if err != nil {
		t.Error("failed to delete the node")
	}

	isRunning = node.IsRunning()
	if isRunning {
		t.Error("node is still running, after being deleted")
	}
}
