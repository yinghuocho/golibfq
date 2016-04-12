package main

import (
	"net"
	"time"

	"github.com/yinghuocho/golibfq/sockstun"
	"github.com/yinghuocho/gosocks"
)

type handler struct {
	tunnelAuth sockstun.TunnelAuthenticator
}

func (h *handler) ServeSocks(client *gosocks.SocksConn) {
	conn, err := net.Dial("tcp", "127.0.0.1:2000")
	if err != nil {
		client.Close()
		return
	}
	if h.tunnelAuth.ClientAuthenticate(client, conn) != nil {
		client.Close()
		conn.Close()
		return
	}
	sockstun.TunnelClient(client, conn)
}

func (h *handler) Quit() {}

func main() {
	server := gosocks.NewServer(
		"127.0.0.1:10800",
		5*time.Minute,
		&handler{
			tunnelAuth: sockstun.NewTunnelAnonymousAuthenticator(),
		},
		nil,
	)
	server.ListenAndServe()
}
