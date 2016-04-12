package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/yinghuocho/golibfq/sockstun"
	"github.com/yinghuocho/gosocks"
)

func main() {
	socksSvr := gosocks.NewBasicServer("127.0.0.1:1080", 5*time.Minute)
	go socksSvr.ListenAndServe()

	tunSvr, err := net.Listen("tcp", ":2000")
	if err != nil {
		log.Fatal(err)
	}
	defer tunSvr.Close()

	auth := sockstun.NewTunnelAnonymousAuthenticator()
	for {
		// Wait for a connection.
		conn, err := tunSvr.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func(c net.Conn) {
			cc, err := net.DialTimeout("tcp", "127.0.0.1:1080", socksSvr.GetTimeout())
			if err != nil {
				fmt.Println("error connecting SOCKS server", err)
				c.Close()
				return
			}
			socks := &gosocks.SocksConn{cc.(net.Conn), socksSvr.GetTimeout()}
			if auth.ServerAuthenticate(c, socks) != nil {
				c.Close()
				socks.Close()
				return
			}
			sockstun.TunnelServer(c, socks)
		}(conn)
	}
}
