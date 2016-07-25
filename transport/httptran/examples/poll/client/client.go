package main

import (
	"log"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/yinghuocho/golibfq/mux"
	"github.com/yinghuocho/golibfq/sockstun"
	"github.com/yinghuocho/golibfq/transport/httptran"
	"github.com/yinghuocho/gosocks"
)

type handler struct {
	muxTunnel  *mux.Client
	tunnelAuth sockstun.TunnelAuthenticator
}

var transport *http.Transport = &http.Transport{
	Proxy: nil,
	Dial: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Minute,
	}).Dial,
	TLSHandshakeTimeout: 10 * time.Second,
}

func (h *handler) ServeSocks(client *gosocks.SocksConn) {
	log.Printf("new socks connection")
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
	URL, _ := url.Parse("http://127.0.0.1:8888")
	httpConn, err := httptran.NewPollClientSession(
		transport,
		&httptran.DomainFrontingPollRequestGenerator{URL: URL},
	)
	if err != nil {
		log.Fatalf("fail to establish http tunnel: %s", err)
	}
	muxClient := mux.NewClient(httpConn)
	server := gosocks.NewServer(
		"127.0.0.1:10800",
		5*time.Minute,
		&handler{
			muxTunnel:  muxClient,
			tunnelAuth: sockstun.NewTunnelAnonymousAuthenticator(),
		},
		nil,
	)
	server.ListenAndServe()
}
