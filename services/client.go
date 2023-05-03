package services

import (
	u "dist-kv/utils"
	"encoding/json"
	"log"
	"net"
)

type Client struct {
	ServerIface string
	TrackVersion bool
	versions map[string]string
}

func (c *Client) Init(serverIface string, trackVersion bool) {
	c.ServerIface = serverIface
	c.TrackVersion = trackVersion
	c.versions = make(map[string]string)
}

func (c *Client) Write(key string, value string) string {
	conn, err := net.Dial(u.Config.NetType, u.Config.NetAddr + ":" + c.ServerIface)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	payload := map[string]string {
		"op": "set",
		"key": key,
		"value": value,
	}

	// for testing this injects random dependency!
	if c.TrackVersion {
		dependency := make(map[string]string)
		for k, v := range c.versions {
			dependency["key"] = k
			dependency["version"] = v
			break
		}
		rawObj, _ := json.Marshal(dependency)
		payload["dependency"] = string(rawObj)
	}
	jsonPayload, _ := json.Marshal(payload)
	conn.Write(jsonPayload)

	// blocking write!
	buffer := make([]byte, u.Config.PayloadSize)
	size, _  := conn.Read(buffer)

	if c.TrackVersion {
		response := make(map[string]string, 1)
		json.Unmarshal(buffer[:size], &response)
		c.versions[key] = response["version"]
		return response["version"]
	}

	return ""
}

func (c *Client) Read(key string) (value, version string) {
	conn, err := net.Dial(u.Config.NetType, u.Config.NetAddr + ":" + c.ServerIface)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	payload := map[string]string {
		"op": "get",
		"key": key,
	}
	if c.TrackVersion {
		minVersion, ok := c.versions[key]
		if ok {
			payload["minVersion"] = minVersion
		} else {
			payload["minVersion"] = "0"
		}
	}

	jsonPayload, _ := json.Marshal(payload)
	conn.Write(jsonPayload)

	buffer := make([]byte, u.Config.PayloadSize)
	size, _  := conn.Read(buffer)

	response := make(map[string]string, 1)
	json.Unmarshal(buffer[:size], &response)

	value, ok := response["value"]
	version, vOk := response["version"]
	if ok && vOk {
		c.versions[key] = version
		return value, version
	} else if ok {
		return value, ""
	}

	return "", ""
}