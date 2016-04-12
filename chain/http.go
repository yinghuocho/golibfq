package chain

import (
	"net"
	"net/http"
)

type HTTPChain interface {
	HTTP(*http.Request) (*http.Response, error)
	HTTPSConnect(req *http.Request) (*http.Response, net.Conn)
	HTTPS(net.Conn, net.Conn)
}
