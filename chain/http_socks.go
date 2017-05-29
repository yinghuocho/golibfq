package chain

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/yinghuocho/gosocks"
)

type HTTPSocksChain struct {
	SocksDialer *gosocks.SocksDialer
	SocksAddr   string
}

type HTTPtoSocks struct {
	Chain HTTPSocksChain
}

func (hts *HTTPtoSocks) Serve(l net.Listener) error {
	defer l.Close()

	var tempDelay time.Duration
	for {
		conn, e := l.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				time.Sleep(tempDelay)
				continue
			} else {
				return e
			}
		}
		tempDelay = 0
		go hts.serve(conn)
	}
}

func (hts *HTTPtoSocks) serve(c net.Conn) {
	c.SetReadDeadline(time.Now().Add(hts.Chain.SocksDialer.Timeout))
	r := bufio.NewReader(c)
	req, e := http.ReadRequest(r)
	if e != nil {
		c.Close()
		return
	}
	socksConn, e := hts.Chain.dial(req)
	if e != nil {
		resp := &http.Response{
			StatusCode: 502,
			Status:     "502 Bad Gateway",
			Proto:      req.Proto,
			ProtoMajor: req.ProtoMajor,
			ProtoMinor: req.ProtoMinor,
		}
		resp.Write(c)
		c.Close()
		return
	}

	if req.Method == "CONNECT" {
		resp := &http.Response{
			StatusCode: 200,
			Status:     "200 OK",
			Proto:      req.Proto,
			ProtoMajor: req.ProtoMajor,
			ProtoMinor: req.ProtoMinor,
		}
		e = resp.Write(c)
		if e != nil {
			c.Close()
			socksConn.Close()
			return
		}
	} else {
		e = req.Write(socksConn)
		if e != nil {
			resp := &http.Response{
				StatusCode: 502,
				Status:     "502 Bad Gateway",
				Proto:      req.Proto,
				ProtoMajor: req.ProtoMajor,
				ProtoMinor: req.ProtoMinor,
			}
			resp.Write(c)
			socksConn.Close()
			c.Close()
			return
		}
	}
	gosocks.CopyLoopTimeout(c, socksConn, hts.Chain.SocksDialer.Timeout)
}

func hostPort(req *http.Request) (string, string) {
	s := req.Host
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		i, _ := net.LookupPort("tcp", req.URL.Scheme)
		host = req.Host
		port = strconv.Itoa(i)
	}
	return host, port
}

func (hs *HTTPSocksChain) dial(req *http.Request) (*gosocks.SocksConn, error) {
	conn, err := hs.SocksDialer.Dial(hs.SocksAddr)
	if err != nil {
		return nil, fmt.Errorf("fail to connect socks server: %s", err)
	}

	host, port := hostPort(req)
	iPort, _ := strconv.Atoi(port)
	hostType, host := gosocks.ParseHost(host)
	_, err = gosocks.WriteSocksRequest(conn, &gosocks.SocksRequest{
		Cmd:      gosocks.SocksCmdConnect,
		HostType: hostType,
		DstHost:  host,
		DstPort:  uint16(iPort),
	})
	if err != nil {
		conn.Close()
		return nil, err
	}
	reply, err := gosocks.ReadSocksReply(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if reply.Rep != gosocks.SocksSucceeded {
		conn.Close()
		return nil, fmt.Errorf("Socks request failed: %X", reply.Rep)
	}
	return conn, nil
}

func (hs *HTTPSocksChain) HTTP(req *http.Request) (*http.Response, error) {
	socksConn, err := hs.dial(req)
	if err != nil {
		return nil, fmt.Errorf("fail to connect socks server: %s", err)
	}
	err = req.Write(socksConn)
	if err != nil {
		return nil, fmt.Errorf("fail to send request to socks server: %s", err)
	}
	reader := bufio.NewReader(socksConn)
	socksConn.SetReadDeadline(time.Now().Add(socksConn.Timeout))
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		return nil, fmt.Errorf("fail to read response from socks server: %s", err)
	}
	return resp, nil
}

func (hs *HTTPSocksChain) HTTPSConnect(req *http.Request) (*http.Response, net.Conn) {
	socksConn, err := hs.dial(req)
	if err != nil {
		return &http.Response{
			StatusCode: 502,
			Status:     "502 Bad Gateway",
			Proto:      req.Proto,
			ProtoMajor: req.ProtoMajor,
			ProtoMinor: req.ProtoMinor,
		}, nil
	}
	return &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Proto:      req.Proto,
		ProtoMajor: req.ProtoMajor,
		ProtoMinor: req.ProtoMinor,
	}, socksConn
}

func (hs *HTTPSocksChain) HTTPS(client net.Conn, remote net.Conn) {
	go gosocks.CopyLoopTimeout(client, remote, hs.SocksDialer.Timeout)
}
