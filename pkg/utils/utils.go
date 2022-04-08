package utils

import (
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

// GetNodeName gets the node name from env, else
// returns an error
func GetNodeName() string {
	nodeName, ok := os.LookupEnv("NODENAME")
	if !ok {
		log.Errorf("Failed to get NODENAME from ENV")
		return ""
	}

	return nodeName
}

// GetNamespace get Namespace from env, else it returns error
func GetNamespace() string {
	ns, ok := os.LookupEnv("NAMESPACE")
	if !ok {
		log.Errorf("Failed to get NameSpace from ENV")
		return ""
	}

	return ns
}

// ConvertNodeName e.g.(10.23.10.12 => 10-23-10-12)
func ConvertNodeName(node string) string {
	return strings.Replace(node, ".", "-", -1)
}
