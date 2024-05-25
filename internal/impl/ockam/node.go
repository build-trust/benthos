package ockam

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os/exec"
	"strings"
	"time"
)

type Node struct {
	Name   string
	Config string
}

func NewNode(config string) (*Node, error) {
	var cfg map[string]interface{}
	err := json.Unmarshal([]byte(config), &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshall config as json: %v", err)
	}

	name := "benthos-" + generateName()
	cfg["name"] = name

	updatedConfig, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated config to json string: %v", err)
	}

	node := &Node{Name: name, Config: string(updatedConfig)}

	err = node.Create()
	if err != nil {
		return nil, err
	}

	return node, nil
}

func generateName() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomNumber := r.Intn(1 << 32)
	return fmt.Sprintf("%08x", randomNumber)
}

func (n *Node) Create() error {
	return RunCommand("ockam", "node", "create", "--node-config", n.Config)
}

func (n *Node) Delete() error {
	return RunCommand("ockam", "node", "delete", n.Name, "--yes")
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

	return strings.ToLower(status) == "up"
}
