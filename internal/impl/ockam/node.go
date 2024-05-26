package ockam

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os/exec"
	"strings"
	"time"

	"regexp"

	"github.com/benthosdev/benthos/v4/public/service"
)



type Node struct {
	OckamBin string
	Name   string
	Config string
}

func NewNode(ockam_bin string, cfg map[string]interface{}, log *service.Logger) (*Node, error) {

	name := "benthos-" + generateName()
	cfg["name"] = name

	updatedConfig, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated config to json string: %v", err)
	}

	node := &Node{OckamBin: ockam_bin, Name: name, Config: string(updatedConfig)}

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

/*
func (n *Node) Create() error {
	return RunCommand("ockam", "node", "create", "--node-config", n.Config)
}
*/
func (n *Node) Create(log *service.Logger) error {
	//return RunCommand("ockam", "node", "create", "--node-config", n.Config)

	ctx, _ := context.WithCancel(context.Background())

	// Run ockam in foreground, piping its log into benthos.  Ockam process exit whenever benthos exit.
	// TODO: LOG_LEVEL should be set at the value set on benthos, otherwise we are wasting cpu logging at
	// debug level just to be discarded latter.
	fmt.Printf("Command: %s : %s", n.OckamBin, n.Config)
	cmd := exec.CommandContext(ctx, n.OckamBin, "node", "create", "-f", "--node-config", n.Config )
	cmd.Env = []string{"OCKAM_LOGGING=true", "OCKAM_LOG_LEVEL=debug", "CONSUMER_IDENTIFIER=11", "PRODUCER_IDENTIFIER=11"}
	stdout, err := cmd.StdoutPipe()
		if err != nil {
			return err
		}
	scanner := bufio.NewScanner(stdout)
	splitter := regexp.MustCompile(`\s+`)
	go func() {
		// Just pipe the logs from ockam into benthos
		for scanner.Scan() {
			// timestamp level line
			log_fields := splitter.Split(scanner.Text(), 3)
			if len(log_fields) == 3 {
				switch log_fields[1] {
				case "DEBUG":
					log.Debugf(log_fields[2])
				case "INFO":
					log.Infof(log_fields[2])
				case "WARN":
					log.Warnf(log_fields[2])
				case "ERROR":
					log.Errorf(log_fields[2])
				}
			}
		}
		if scanner.Err() != nil {
			cmd.Process.Kill()
			cmd.Wait()
			log.Errorf("%v", scanner.Err())
			return
		}
		cmd.Wait()
	}()
	return cmd.Start()
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
