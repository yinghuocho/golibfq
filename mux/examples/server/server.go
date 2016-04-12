package main

import (
	"fmt"
	"log"
	"net"
	"time"
	
	"github.com/yinghuocho/golibfq/mux"
	"github.com/yinghuocho/golibfq/sockstun"
	"github.com/yinghuocho/gosocks"
)

func main() {
	socksAddr := "127.0.0.1:1080"
	socksSvr := gosocks.NewBasicServer(socksAddr, 5*time.Minute)
	go socksSvr.ListenAndServe()

	auth := sockstun.NewTunnelAnonymousAuthenticator()
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// a mux
		go func(c net.Conn) {
			server := mux.NewServer(c)
			for {
				stream, err := server.Accept()
				if err != nil {
					fmt.Println("error accepting Stream", err)
					server.Close()
					return
				}
				// tunnel for single connection
				go func(cc net.Conn) {
					ccc, err := net.DialTimeout("tcp", socksAddr, socksSvr.GetTimeout())
					if err != nil {
						fmt.Println("error connecting SOCKS server", err)
						cc.Close()
						return
					}
					socks := &gosocks.SocksConn{ccc.(net.Conn), socksSvr.GetTimeout()}
					if auth.ServerAuthenticate(cc, socks) != nil {
						cc.Close()
						socks.Close()
						return
					}
					sockstun.TunnelServer(cc, socks)
				}(stream)
			}
		}(conn)
	}
}
