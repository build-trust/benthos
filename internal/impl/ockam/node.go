package ockam

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
)

type Node struct {
	OckamBin string
	Name     string
	Config   string
}

func NewNode(cfg map[string]interface{}, log *service.Logger) (*Node, error) {
	name := "benthos-" + generateName()
	cfg["name"] = name

	updatedConfig, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated node config to json string: %v", err)
	}

	node := &Node{OckamBin: GetOckamBin(), Name: name, Config: string(updatedConfig)}

	err = node.Create(log)
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

func (n *Node) Create(log *service.Logger) error {
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Run ockam in foreground, piping its log into benthos. Ockam process exit whenever benthos exit.
	cmd := exec.CommandContext(ctx, n.OckamBin, "node", "create", "-f", "--node-config", n.Config)
	log.Infof("Creating node with command: %v", cmd.String())
	cmd.Env = append(os.Environ(),
		"NO_INPUT=true",
		"NO_COLOR=true",
		"OCKAM_DISABLE_UPGRADE_CHECK=true",
		"OCKAM_OPENTELEMETRY_EXPORT=false",
	)

	cmd.Stdout = devNull
	cmd.Stderr = devNull
	err = cmd.Start()
	go func() {
		// Wait for the ockam process to exit, then delete the node.
		err := cmd.Wait()
		if err != nil {
			log.Errorf("%v", err)
		}
		_ = n.Delete()
		devNull.Close()
		cancel()
	}()
	return err
}

func (n *Node) Delete() error {
	return RunCommand(n.OckamBin, "node", "delete", n.Name, "--force", "--yes")
}

func (n *Node) IsRunning() bool {
	cmd := exec.Command(n.OckamBin, "node", "show", n.Name, "--output", "json")
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

	return strings.ToLower(status) == "running" || strings.ToLower(status) == "up"
}

func GetOrCreateIdentifier(name string) (string, error) {
	cmd := exec.Command(GetOckamBin(), "identity", "create", name)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	} else {
		return strings.TrimSpace(string(output)), nil
	}
}

func GetKafkaInletAddressFrom(nodeAddress string) (*string, *string, error) {
	cmd := exec.Command(GetOckamBin(), "node", "list", "--output", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, nil, err
	}
	var nodes []map[string]interface{}
	err = json.Unmarshal(output, &nodes)
	if err != nil {
		return nil, nil, err
	}
	var nodeNames []string
	for _, node := range nodes {
		status, ok := node["status"].(map[string]interface{})
		if !ok {
			continue
		}

		if status["status"] == "running" {
			nodeName, ok := node["node_name"].(string)
			if !ok {
				continue
			}
			nodeNames = append(nodeNames, nodeName)
		}
	}

	for _, nodeName := range nodeNames {
		cmd := exec.Command(GetOckamBin(), "node", "show", nodeName, "--output", "json")
		output, err := cmd.Output()
		if err != nil {
			continue
		}

		var data map[string]interface{}
		err = json.Unmarshal(output, &data)
		if err != nil {
			continue
		}

		transports, ok := data["transports"].([]interface{})
		if !ok {
			continue
		}

		nodeFound := false
		for _, transport := range transports {
			transportMap, ok := transport.(map[string]interface{})
			if !ok {
				continue
			}

			socketAddress, ok := transportMap["socket_addr"].(string)
			if !ok {
				continue
			}

			if socketAddress == nodeAddress {
				nodeFound = true
				break
			}
		}
		if !nodeFound {
			continue
		}

		inlets, ok := data["inlets"].([]interface{})
		if !ok {
			continue
		}

		for _, inlet := range inlets {
			inletMap, ok := inlet.(map[string]interface{})
			if !ok {
				continue
			}

			outletRoute, ok := inletMap["outlet_route"].(string)
			if !ok {
				continue
			}

			if strings.Contains(outletRoute, "0#kafka_inlet") {
				bindAddr, ok := inletMap["bind_addr"].(string)
				if !ok {
					continue
				}
				return &nodeName, &bindAddr, nil
			}
		}
	}
	return nil, nil, fmt.Errorf("no kafka inlet found")
}

func LocalAddressIsTaken(address string) bool {
	_, err := net.Listen("tcp", address)
	return err != nil
}

func GetFreeLocalAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	return listener.Addr().String(), err
}
