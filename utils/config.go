package utils

type ServerConfig struct {
	NetAddr 		string		`json:"netAddr"`
    NetType 		string		`json:"netType"`
    PayloadSize 	int			`json:"payloadSize"`
	NumServers		int			`json:"numServers"`
    ClientPorts 	[]string	`json:"clientPorts"`
    ServerPorts 	[]string	`json:"serverPorts"`
    KvStorePorts 	[]string	`json:"kvStorePorts"`
}

var Config ServerConfig