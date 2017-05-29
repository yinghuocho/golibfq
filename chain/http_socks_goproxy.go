package chain

import (
	"net"
	"net/http"

	"github.com/elazarl/goproxy"
)

type GoproxySocksChain struct {
	Chain HTTPSocksChain
}

func (c *GoproxySocksChain) HTTP(r *http.Request, ctx *goproxy.ProxyCtx) (*http.Request, *http.Response) {
	resp, err := c.Chain.HTTP(r)
	if err != nil {
		goproxy.NewResponse(r, goproxy.ContentTypeText, http.StatusBadGateway, "Failed to forward to SOCKS proxy")
	}
	return r, resp
}

func (c *GoproxySocksChain) HTTPS(host string, ctx *goproxy.ProxyCtx) (*goproxy.ConnectAction, string) {
	r := ctx.Req
	resp, socksConn := c.Chain.HTTPSConnect(r)
	if resp.StatusCode != 200 {
		return goproxy.RejectConnect, host
	}
	return &goproxy.ConnectAction{Action: goproxy.ConnectHijack, Hijack: func(r *http.Request, client net.Conn, ctx *goproxy.ProxyCtx) {
		c.Chain.HTTPS(client, socksConn)
	}}, host
}
