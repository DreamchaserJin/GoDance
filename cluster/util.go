package cluster

import (
	"GoDance/configs"
	"fmt"
	"net"
	"strings"
)

// GetOutBoundIP 获取公网IP
func GetOutBoundIP() (ip string, err error) {
	if configs.Config.Cluster.Address != "" {
		ip = configs.Config.Cluster.Address
		return
	}
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		fmt.Println(err)
		return
	}
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	fmt.Println(localAddr.String())
	ip = strings.Split(localAddr.String(), ":")[0]
	return
}
