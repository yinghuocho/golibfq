package main

import (
	"log"
	"net"
	"time"
	
	
	"github.com/yinghuocho/golibfq/mux"
	"github.com/yinghuocho/golibfq/sockstun"
	"github.com/yinghuocho/gosocks"
)

type handler struct {
	muxTunnel  *mux.Client
	tunnelAuth sockstun.TunnelAuthenticator
}

func (h *handler) ServeSocks(client *gosocks.SocksConn) {
	stream, err := h.muxTunnel.OpenStream()
	if err != nil {
		log.Printf("error to create a tunnel connection: %s", err)
		client.Close()
		return
	}
	if h.tunnelAuth.ClientAuthenticate(client, stream) != nil {
		client.Close()
		stream.Close()
		return
	}
	sockstun.TunnelClient(client, stream)
}

func (h *handler) Quit() {}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:2000")
	if err != nil {
		log.Fatalf("error to connect server: %s", err)
	}
	client := mux.NewClient(conn)
	client.SetIdleTime(10 * time.Second)
	server := gosocks.NewServer(
		"127.0.0.1:10800",
		5*time.Minute,
		&handler{
			muxTunnel:  client,
			tunnelAuth: sockstun.NewTunnelAnonymousAuthenticator(),
		},
		nil,
	)
	server.ListenAndServe()
}
