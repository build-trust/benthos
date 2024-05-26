package ockam

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

func GetOckamBin() string {
	ockamBin := os.Getenv("OCKAM")
	if ockamBin == "" {
		ockamBin = "ockam"
	}
	return ockamBin
}

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
	return RunCommand(tmpFile.Name())
}

func IsCommandInPath() bool {
	_, err := exec.LookPath("ockam")
	return err == nil
}

func RunCommand(name string, arg ...string) error {
	quotedArgs := make([]string, len(arg))
	for i, a := range arg {
		quotedArgs[i] = fmt.Sprintf("'%s'", a)
	}
	cmd := exec.Command("sh", "-c", "source ~/.ockam/env && "+name+" "+strings.Join(quotedArgs, " "))
	cmd.Env = append(os.Environ(),
		"NO_INPUT=true",
		"NO_COLOR=true",
		"OCKAM_DISABLE_UPGRADE_CHECK=true",
		"OCKAM_OPENTELEMETRY_EXPORT=false",
	)

	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer devNull.Close()

	cmd.Stdout = devNull
	cmd.Stderr = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	err = cmd.Run()
	if err != nil {
		errMsg := "failed to run the command: " + cmd.String() + ",\nerror: " + err.Error()
		return errors.New(errMsg)
	}
	return nil
}
