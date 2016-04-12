package main

import (
	"log"
	"net"
	"net/http"
	"time"
	
	"github.com/yinghuocho/golibfq/mux"
	"github.com/yinghuocho/golibfq/sockstun"
	"github.com/yinghuocho/golibfq/transport/httptran"
	"github.com/yinghuocho/gosocks"
)

func main() {
	socksSvr := gosocks.NewBasicServer("127.0.0.1:1080", 5*time.Minute)
	go socksSvr.ListenAndServe()

	auth := sockstun.NewTunnelAnonymousAuthenticator()
	httpTunHandler := httptran.NewPollServerHandler(func(tunnel net.Conn) {
		server := mux.NewServer(tunnel)
		for {
			stream, err := server.Accept()
			if err != nil {
				log.Printf("error to accept new mux streams: %s", err)
				server.Close()
				return
			}
			go func(cc net.Conn) {
				s, err := net.DialTimeout("tcp", "127.0.0.1:1080", socksSvr.GetTimeout())
				if err != nil {
					log.Printf("error connecting SOCKS server: %s", err)
					cc.Close()
					return
				}
				socks := &gosocks.SocksConn{s.(net.Conn), socksSvr.GetTimeout()}
				if auth.ServerAuthenticate(cc, socks) != nil {
					cc.Close()
					socks.Close()
					return
				}
				sockstun.TunnelServer(cc, socks)
			}(stream)
		}
	})
	httpTunHandler.Run()
	err := http.ListenAndServe("127.0.0.1:8888", httpTunHandler)
	if err != nil {
		log.Fatal(err)
	}
}
