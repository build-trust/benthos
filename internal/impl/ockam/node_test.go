package ockam_test

import (
	"testing"

	"github.com/benthosdev/benthos/v4/internal/impl/ockam"
)

func TestInstallCommand(t *testing.T) {
	err := ockam.InstallCommand()
	if err != nil {
		t.Error("failed to install command")
	}

	if !ockam.IsCommandInPath() {
		t.Error("ockam command is not in PATH")
	}
}

func TestNodeCreate(t *testing.T) {
	config := "{name: $NODE_NAME, tcp-outlets: {an-outlet: {to: 9999}}}"
	node := ockam.Node{Name: "a", Config: config}
	err := node.Create()
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
