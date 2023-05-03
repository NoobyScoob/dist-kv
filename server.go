package distkv

import (
	"encoding/json"
	"os"

	u "dist-kv/utils"
	s "dist-kv/services"
)

const (
	Linearizable = 1
	Sequential = 2
	Eventual = 3
	Causal = 4
)

var ServersUp = false
var Cfg u.ServerConfig

func StartServers(consistency int) {
	loadServerConfig()
	Cfg = u.Config

	var server func(string, string, string) error

	switch consistency {
	case Linearizable:
		server = s.StartLinearizableServer
	case Sequential:
		server = s.StartSequentialServer
	case Eventual:
		server = s.StartEventualServer
	case Causal:
		server = s.StartCausalServer
	default:
		server = s.StartLinearizableServer
	}

	for i := 0; i < u.Config.NumServers; i++ {
		// starting three servers
		go server(Cfg.ClientPorts[i], Cfg.ServerPorts[i], Cfg.KvStorePorts[i])
	}

	ServersUp = true
}

func Shutdown() {
	s.KillAll()
}

func loadServerConfig() {
	bytes, _ := os.ReadFile("./config.json")
	json.Unmarshal(bytes, &u.Config)
}