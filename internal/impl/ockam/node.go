package ockam

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"syscall"
)

func InstallCommand() error {
	// Download the install script.
	resp, err := http.Get("https://install.command.ockam.io")
	if err != nil {
		return fmt.Errorf("failed to download the install script: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got HTTP response with status code != 200, while downloading the install script: %v", resp.StatusCode)
	}

	// Save the install script to a temporary file.
	tmpFile, err := os.CreateTemp("", "install-ockam-*.sh")
	if err != nil {
		return fmt.Errorf("failed to create temporary file for the install script: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to copy install script to a temporary file: %v", err)
	}
	err = os.Chmod(tmpFile.Name(), 0700)
	if err != nil {
		return fmt.Errorf("failed to change permissions onthe install script to 0700: %v", err)
	}

	// Run the install script.
	cmd := exec.Command(tmpFile.Name())
	return RunCommand(cmd)
}

func IsCommandInPath() bool {
	_, err := exec.LookPath("ockam")
	return err == nil
}

func RunCommand(cmd *exec.Cmd) error {
	stdout, err := os.CreateTemp("", "stdout-*.log")
	if err != nil {
		return fmt.Errorf("failed to create a temporary file to store the command's stdout: %v", err)
	}
	defer stdout.Close()

	stderr, err := os.CreateTemp("", "stderr-*.log")
	if err != nil {
		return fmt.Errorf("failed to create a temporary file to store the command's stderr: %v", err)
	}
	defer stderr.Close()

	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = append(os.Environ(),
		"NO_INPUT=true",
		"NO_COLOR=true",
		"OCKAM_DISABLE_UPGRADE_CHECK=true",
		"OCKAM_OPENTELEMETRY_EXPORT=false",
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to run the command: %s, error: %v", cmd.String(), err)
	}

	return nil
}

type Node struct {
	Name   string
	Config string
}

func (n *Node) Create() error {
	cmd := exec.Command("ockam", "node", "create", "--node-config", n.Config)
	return RunCommand(cmd)
}

func (n *Node) Delete() error {
	cmd := exec.Command("ockam", "node", "delete", n.Name, "--yes")
	return RunCommand(cmd)
}

func (n *Node) IsRunning() bool {
	cmd := exec.Command("ockam", "node", "show", n.Name, "--output", "json")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	var result map[string]interface{}
	err = json.Unmarshal(output, &result)
	if err != nil {
		return false
	}

	status, ok := result["status"].(string)
	if !ok {
		return false
	}

	return status == "Up"
}
