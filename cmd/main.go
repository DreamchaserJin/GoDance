package main

import (
	"GoDance/engine"
	"GoDance/utils"
	"GoDance/web"
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	"runtime"
)

func main() {
	var cores int
	var mport int
	var lport int
	var master int
	var localip string
	var masterip string
	flag.IntVar(&cores, "core", runtime.NumCPU(), "CPU 核心数量")
	flag.IntVar(&lport, "p", 9090, "启动端口，默认9991")
	flag.IntVar(&master, "m", 0, "启动master，默认启动的为searcher")
	flag.StringVar(&localip, "lip", "127.0.0.1", "本机ip地址，默认127.0.0.1")
	flag.StringVar(&masterip, "mip", "127.0.0.1", "主节点ip地址，默认127.0.0.1")
	flag.IntVar(&mport, "mp", 9990, "主节点端口，默认9990")

	flag.Parse()
	logger, err := utils.NewLogger("GoDanceEngine")
	if err != nil {
		fmt.Printf("[ERROR] Create logger Error: %v\n", err)
		return
	}

	engine.Engine = engine.NewDefaultEngine(logger)

	router := gin.Default()

	// 注册API
	web.Register(router)

	addr := fmt.Sprintf("%v%v%v", localip, ":", lport)
	fmt.Println(addr)

	router.Run(addr)

}
