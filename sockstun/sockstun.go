package sockstun

import (
	"bufio"
	"io"
	"log"
	"net"
	"time"

	"github.com/yinghuocho/gosocks"
)

var (
	basicHandler *gosocks.BasicSocksHandler = &gosocks.BasicSocksHandler{}
)

type datagramFrame struct {
	datagramLen uint16
	datagram    []byte
}

func writeDatagramFrame(conn net.Conn, frame *datagramFrame, timeout time.Duration) (int, error) {
	buf := make([]byte, 2, 0x10002)

	size := gosocks.Htons(frame.datagramLen)
	buf[0] = size[0]
	buf[1] = size[1]
	buf = append(buf, frame.datagram...)

	conn.SetWriteDeadline(time.Now().Add(timeout))
	return conn.Write(buf)
}

func readDatagramFrame(r io.Reader, timeout time.Duration) (frame *datagramFrame, err error) {
	var buf [2]byte
	_, err = io.ReadFull(r, buf[:])
	if err != nil {
		return
	}
	size := gosocks.Ntohs(buf)
	data := make([]byte, size, size)
	_, err = io.ReadFull(r, data)
	if err != nil {
		return
	}
	frame = &datagramFrame{size, data}
	return
}

func datagramFrameReader(conn net.Conn, timeout time.Duration, ch chan<- *datagramFrame, quit chan bool) {
	r := bufio.NewReader(conn)
loop:
	for {
		conn.SetReadDeadline(time.Now().Add(timeout))
		frame, err := readDatagramFrame(r, timeout)
		if err != nil {
			log.Printf("tunnel connection Read error: %s", err)
			break
		}
		select {
		case ch <- frame:
		case <-quit:
			break loop
		}
	}
	close(ch)
}

type TunnelAuthenticator interface {
	ClientAuthenticate(socks *gosocks.SocksConn, tun net.Conn) (err error)
	ServerAuthenticate(tun net.Conn, socks *gosocks.SocksConn) (err error)
}

type TunnelAnonymousAuthenticator struct {
	serverAuth *gosocks.AnonymousServerAuthenticator
	clientAuth *gosocks.AnonymousClientAuthenticator
}

func NewTunnelAnonymousAuthenticator() *TunnelAnonymousAuthenticator {
	return &TunnelAnonymousAuthenticator{
		serverAuth: &gosocks.AnonymousServerAuthenticator{},
		clientAuth: &gosocks.AnonymousClientAuthenticator{},
	}
}

func (a *TunnelAnonymousAuthenticator) ClientAuthenticate(socks *gosocks.SocksConn, tun net.Conn) (err error) {
	err = a.serverAuth.ServerAuthenticate(socks)
	if err != nil {
		log.Printf("error to authenticate: %s", err)
	}
	// save one round-trip by avoiding authentication with remote server.
	// if err != nil {
	//	  return
	// }
	// err = a.clientAuth.ClientAuthenticate(&gosocks.SocksConn{Conn: tun, Timeout: socks.Timeout})
	return
}

func (a *TunnelAnonymousAuthenticator) ServerAuthenticate(tun net.Conn, socks *gosocks.SocksConn) (err error) {
	// save one round-trip by avoiding authentication with remote client.
	// err = a.serverAuth.ServerAuthenticate(&gosocks.SocksConn{Conn: tun, Timeout: socks.Timeout})
	// if err != nil {
	//	return
	// }
	err = a.clientAuth.ClientAuthenticate(socks)
	return
}

func TunnelClient(socks *gosocks.SocksConn, tun net.Conn) {
	socks.SetReadDeadline(time.Now().Add(socks.Timeout))
	req, err := gosocks.ReadSocksRequest(socks)
	if err != nil {
		log.Printf("error in ReadSocksRequest: %s", err)
		socks.Close()
		tun.Close()
		return
	}
	switch req.Cmd {
	case gosocks.SocksCmdConnect:
		log.Printf("cmd connect")
		TunnelTCP(req, socks, tun, socks.Timeout)
		return
	case gosocks.SocksCmdUDPAssociate:
		log.Printf("cmd udp associate")
		TunnelClientUDP(req, socks, tun)
		return
	case gosocks.SocksCmdBind:
		socks.Close()
		tun.Close()
		return
	default:
		return
	}
}

func TunnelTCP(req *gosocks.SocksRequest, socks net.Conn, tun net.Conn, timeout time.Duration) {
	tun.SetWriteDeadline(time.Now().Add(timeout))
	_, err := gosocks.WriteSocksRequest(tun, req)
	if err != nil {
		log.Printf("fail to write Request through tunnel: %s", err)
		socks.Close()
		tun.Close()
		return
	}
	gosocks.CopyLoopTimeout(socks, tun, timeout)
}

func TunnelUDPAssociate(req *gosocks.SocksRequest, clientBind *net.UDPConn, clientAssociate *net.UDPAddr, firstPkt *gosocks.UDPRequest, clientAddr *net.UDPAddr, socks *gosocks.SocksConn, tun net.Conn) {
	// send a request to tunnel, so that server-side knows this is a UDP-relay
	h := "0.0.0.0"
	if req.HostType == gosocks.SocksIPv6Host {
		h = "::"
	}
	tun.SetWriteDeadline(time.Now().Add(socks.Timeout))
	_, err := gosocks.WriteSocksRequest(tun, &gosocks.SocksRequest{
		Cmd: req.Cmd, HostType: req.HostType, DstHost: h, DstPort: 0})
	if err != nil {
		log.Printf("error in tunnelling request: %s", err)
		socks.Close()
		clientBind.Close()
		tun.Close()
		return
	}

	// read reply from server-side
	tun.SetReadDeadline(time.Now().Add(socks.Timeout))
	reply, err := gosocks.ReadSocksReply(tun)
	if err != nil {
		log.Printf("error in reading reply from tunnel: %s", err)
		socks.Close()
		clientBind.Close()
		tun.Close()
		return
	}
	if reply.Rep != gosocks.SocksSucceeded {
		log.Printf("error in reply from tunnel: 0x%02x", reply.Rep)
		socks.Close()
		clientBind.Close()
		tun.Close()
		return
	}

	// write first packet
	datagram := gosocks.PackUDPRequest(firstPkt)
	_, err = writeDatagramFrame(tun, &datagramFrame{uint16(len(datagram)), datagram}, socks.Timeout)
	if err != nil {
		log.Printf("error to write UDP to tunnel: %s", err)
		socks.Close()
		clientBind.Close()
		tun.Close()
		return
	}

	// monitor socks TCP connection
	quitTCP := make(chan bool)
	go gosocks.ConnMonitor(socks, quitTCP)

	quitUDP := make(chan bool)
	// read UDP request from client
	chClientUDP := make(chan *gosocks.UDPPacket, 100)
	go gosocks.UDPReader(clientBind, chClientUDP, quitUDP)

	// read UDP request from tunnel
	chTunnelDatagram := make(chan *datagramFrame, 100)
	go datagramFrameReader(tun, socks.Timeout, chTunnelDatagram, quitUDP)

loop:
	for {
		t := time.NewTimer(socks.Timeout)
		select {
		// packets from client, pack and send through tunnel
		case pkt, ok := <-chClientUDP:
			t.Stop()
			if !ok {
				break loop
			}
			// validation
			// 1) RFC1928 Section-7
			if !gosocks.LegalClientAddr(clientAssociate, pkt.Addr) {
				continue
			}
			// 2) format
			udpReq, err := gosocks.ParseUDPRequest(pkt.Data)
			if err != nil {
				log.Printf("error to parse UDP packet: %s", err)
				continue
				//break loop
			}
			// 3) no fragment
			if udpReq.Frag != gosocks.SocksNoFragment {
				continue
			}
			clientAddr = pkt.Addr
			datagram := gosocks.PackUDPRequest(udpReq)
			_, err = writeDatagramFrame(tun, &datagramFrame{uint16(len(datagram)), datagram}, socks.Timeout)
			if err != nil {
				log.Printf("error to write UDP to tunnel: %s", err)
				break loop
			}

		// requests from tunnel, parse to UDP request, send to client
		case frame, ok := <-chTunnelDatagram:
			t.Stop()
			if !ok {
				log.Printf("tunnel connection ends")
				break loop
			}
			_, err := clientBind.WriteToUDP(frame.datagram, clientAddr)
			if err != nil {
				log.Printf("error to send UDP packet to client: %s", err)
				break loop
			}

		case <-quitTCP:
			t.Stop()
			log.Printf("UDP unexpected event from socks connection")
			break loop

		case <-t.C:
			log.Printf("UDP timeout")
			break loop
		}
		t.Stop()
	}

	// quit
	log.Printf("UDP close local socks connection")
	socks.Close()
	log.Printf("UDP close tunnel connection")
	tun.Close()
	log.Printf("UDP close udp binding socket")
	clientBind.Close()
	close(quitUDP)
}

func TunnelClientUDP(req *gosocks.SocksRequest, socks *gosocks.SocksConn, tun net.Conn) {
	clientBind, clientAssociate, udpReq, clientAddr, err := basicHandler.UDPAssociateFirstPacket(req, socks)
	if err != nil {
		socks.Close()
		tun.Close()
		return
	}
	TunnelUDPAssociate(req, clientBind, clientAssociate, udpReq, clientAddr, socks, tun)
}

func TunnelServer(tun net.Conn, socks *gosocks.SocksConn) {
	tun.SetReadDeadline(time.Now().Add(socks.Timeout))
	req, err := gosocks.ReadSocksRequest(tun)
	if err != nil {
		log.Printf("error in ReadSocksRequest: %s", err)
		socks.Close()
		tun.Close()
		return
	}
	switch req.Cmd {
	case gosocks.SocksCmdConnect:
		log.Printf("cmd connect")
		TunnelTCP(req, tun, socks, socks.Timeout)
		return
	case gosocks.SocksCmdUDPAssociate:
		log.Printf("cmd udp associate")
		tunnelServerUDP(req, tun, socks)
		return
	case gosocks.SocksCmdBind:
		socks.Close()
		tun.Close()
		return
	default:
		return
	}
}

func tunnelServerUDP(req *gosocks.SocksRequest, tun net.Conn, socks *gosocks.SocksConn) {
	// send request to socks server, tunnelClientUDP ensures address and port
	// are all zeros
	socks.SetWriteDeadline(time.Now().Add(socks.Timeout))
	_, err := gosocks.WriteSocksRequest(socks, req)
	if err != nil {
		log.Printf("error in sending request: %s", err)
		socks.Close()
		tun.Close()
		return
	}

	// read reply
	socks.SetReadDeadline(time.Now().Add(socks.Timeout))
	reply, err := gosocks.ReadSocksReply(socks)
	if err != nil {
		log.Printf("error in reading reply from socks: %s", err)
		socks.Close()
		tun.Close()
		return
	}
	if reply.Rep != gosocks.SocksSucceeded {
		log.Printf("error in reply from socks: 0x%02x", reply.Rep)
		socks.Close()
		tun.Close()
		return
	}

	// prepare a UDP socket according to socks's bind address
	socksAddr := gosocks.SocksAddrToNetAddr("udp", reply.BndHost, reply.BndPort).(*net.UDPAddr)
	c, err := net.DialUDP("udp", nil, socksAddr)
	if err != nil {
		log.Printf("error to connect UDP target (%s):%s", socksAddr.String(), err)
		socks.Close()
		tun.Close()
		return
	}
	uaddr := c.LocalAddr().(*net.UDPAddr)
	uaddr.Port = 0
	c.Close()
	socksUDP, err := net.ListenUDP("udp", uaddr)
	if err != nil {
		log.Printf("error to listen on a local UDP: %s", err)
		socks.Close()
		tun.Close()
		return
	}

	// send reply back through tunnel
	tun.SetWriteDeadline(time.Now().Add(socks.Timeout))
	_, err = gosocks.WriteSocksReply(tun, reply)
	if err != nil {
		log.Printf("error in tunnelling reply: %s", err)
		socks.Close()
		socksUDP.Close()
		tun.Close()
		return
	}

	// read from local UDP
	quitUDP := make(chan bool)
	chSocksUDP := make(chan *gosocks.UDPPacket, 100)
	go gosocks.UDPReader(socksUDP, chSocksUDP, quitUDP)

	// read UDP request from tunnel
	chTunnelDatagram := make(chan *datagramFrame, 100)
	go datagramFrameReader(tun, socks.Timeout, chTunnelDatagram, quitUDP)

loop:
	for {
		var pkt *gosocks.UDPPacket
		var frame *datagramFrame
		var ok bool
		t := time.NewTimer(socks.Timeout)
		select {
		// packets from socks server, send to tunnel
		case pkt, ok = <-chSocksUDP:
			if !ok {
				break loop
			}
			_, err = writeDatagramFrame(tun, &datagramFrame{uint16(len(pkt.Data)), pkt.Data}, socks.Timeout)
			if err != nil {
				log.Printf("error to write UDP to tunnel: %s", err)
				break loop
			}

		// requests from tunnel, parse to UDP request, send to socks server
		case frame, ok = <-chTunnelDatagram:
			if !ok {
				log.Printf("UDP unexpected event from tunnel connection")
				break loop
			}
			_, err := socksUDP.WriteToUDP(frame.datagram, socksAddr)
			if err != nil {
				log.Printf("error to send UDP packet to socks server: %s", err)
				break loop
			}

		// timeout
		case <-t.C:
			log.Printf("UDP timeout")
			break loop
		}
		t.Stop()
	}

	// quit
	log.Printf("UDP close local socks connection")
	socks.Close()
	log.Printf("UDP close tunnel connection")
	tun.Close()
	log.Printf("UDP close udp binding socket")
	socksUDP.Close()
	close(quitUDP)
}
