package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/yinghuocho/golibfq/mux"
	"github.com/yinghuocho/golibfq/obf"
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
			// add a layer of obfuscation
			var mask [obf.XorMaskLength]byte
			rand.Read(mask[:])
			obfedConn := obf.NewXorObfConn(c, mask)

			muxServer := mux.NewServer(obfedConn)
			for {
				stream, err := muxServer.Accept()
				if err != nil {
					fmt.Println("error accepting Stream", err)
					return
				}
				c, err := net.DialTimeout("tcp", "127.0.0.1:1080", socksSvr.GetTimeout())
				if err != nil {
					fmt.Println("error connecting SOCKS server", err)
					stream.Close()
					return
				}
				socks := &gosocks.SocksConn{c.(net.Conn), socksSvr.GetTimeout()}
				if auth.ServerAuthenticate(stream, socks) != nil {
					stream.Close()
					socks.Close()
					return
				}
				go sockstun.TunnelServer(stream, socks)
			}
		}(conn)
	}
}
