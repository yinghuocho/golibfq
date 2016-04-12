package main

import (
	"crypto/rand"
	"net"
	"time"
	
	"github.com/yinghuocho/golibfq/mux"
	"github.com/yinghuocho/golibfq/obf"
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
		client.Close()
		return
	}
	if h.tunnelAuth.ClientAuthenticate(client, stream) != nil {
		client.Close()
		stream.Close()
		return
	}
	go sockstun.TunnelClient(client, stream)
}

func (h *handler) Quit() {}

func main() {
	conn, _ := net.Dial("tcp", "127.0.0.1:2000")
	var mask [obf.XorMaskLength]byte
	rand.Read(mask[:])
	obfedConn := obf.NewXorObfConn(conn, mask)
	tunnel := mux.NewClient(obfedConn)

	server := gosocks.NewServer(
		"127.0.0.1:10800",
		5*time.Minute,
		&handler{
			muxTunnel:  tunnel,
			tunnelAuth: sockstun.NewTunnelAnonymousAuthenticator(),
		},
		nil,
	)
	server.ListenAndServe()
}
