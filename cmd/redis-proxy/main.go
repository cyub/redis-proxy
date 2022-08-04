package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	proxy "github.com/cyub/redis-proxy"
)

var (
	addr        = flag.String("addr", ":6380", "the address of redis-proxy")
	clusterAddr = flag.String("cluster-addr", "localhost:6379", "the address of redis cluster")
)

func init() {
	flag.Parse()
}

func main() {
	conf := proxy.NewConfig()
	conf.Addr = *addr
	clusterAddrs := strings.Split(*clusterAddr, ",")
	conf.ClusterAddrs = conf.ClusterAddrs[:0]
	for _, addr := range clusterAddrs {
		conf.ClusterAddrs = append(conf.ClusterAddrs, addr)
	}

	p := proxy.New(conf)
	errChan := make(chan error)
	go func() {
		errChan <- p.Run()
	}()

	defer p.Close()
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case s := <-c:
			switch s {
			case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
				return
			default:
			}
		case err := <-errChan:
			fmt.Printf("redis-proxy will close: %s", err.Error())
			return
		}
	}
}
