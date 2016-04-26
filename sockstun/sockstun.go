package sockstun

import (
	"bufio"
	"io"
	"log"
	"net"
	"time"

	"github.com/yinghuocho/gosocks"
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

func readDatagramFrame(conn net.Conn, timeout time.Duration) (frame *datagramFrame, err error) {
	conn.SetReadDeadline(time.Now().Add(timeout))

	var buf [2]byte
	r := bufio.NewReader(conn)
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

func datagramFrameReader(conn net.Conn, timeout time.Duration, ch chan<- *datagramFrame) {
	for {
		frame, err := readDatagramFrame(conn, timeout)
		if err != nil {
			break
		}
		ch <- frame
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
		tunnelTCP(req, socks, tun, socks.Timeout)
		return
	case gosocks.SocksCmdUDPAssociate:
		tunnelClientUDP(req, socks, tun)
		return
	case gosocks.SocksCmdBind:
		socks.Close()
		tun.Close()
		return
	default:
		return
	}
}

func tunnelTCP(req *gosocks.SocksRequest, peer1 net.Conn, peer2 net.Conn, timeout time.Duration) {
	peer2.SetWriteDeadline(time.Now().Add(timeout))
	_, err := gosocks.WriteSocksRequest(peer2, req)
	if err != nil {
		log.Printf("fail to write Request through tunnel: %s", err)
		peer1.Close()
		peer2.Close()
		return
	}
	gosocks.CopyLoopTimeout(peer1, peer2, timeout)
}

func tunnelClientUDP(req *gosocks.SocksRequest, socks *gosocks.SocksConn, tun net.Conn) {
	// bind local UDP
	socksAddr := socks.LocalAddr().(*net.TCPAddr)
	clientBind, err := net.ListenUDP("udp", &net.UDPAddr{IP: socksAddr.IP, Port: 0, Zone: socksAddr.Zone})
	if err != nil {
		log.Printf("error in binding local UDP: %s", err)
		gosocks.ReplyGeneralFailure(socks, req)
		socks.Close()
		tun.Close()
		return
	}

	// get local UDP address
	bindAddr := clientBind.LocalAddr()
	hostType, host, port := gosocks.NetAddrToSocksAddr(bindAddr)
	log.Printf("UDP bind local address: %s", bindAddr.String())
	socks.SetWriteDeadline(time.Now().Add(socks.Timeout))
	_, err = gosocks.WriteSocksReply(socks, &gosocks.SocksReply{
		Rep: gosocks.SocksSucceeded, HostType: hostType, BndHost: host, BndPort: port})
	if err != nil {
		log.Printf("error in sending reply: %s", err)
		socks.Close()
		clientBind.Close()
		tun.Close()
		return
	}

	// clientAssociate is used to validate datagrams received by clientBind
	clientAssociate := gosocks.SocksAddrToNetAddr("udp", req.DstHost, req.DstPort).(*net.UDPAddr)

	// send a request to tunnel, so that server-side knows this is a UDP-relay
	h := "0.0.0.0"
	if req.HostType == gosocks.SocksIPv6Host {
		h = "::"
	}
	tun.SetWriteDeadline(time.Now().Add(socks.Timeout))
	_, err = gosocks.WriteSocksRequest(tun, &gosocks.SocksRequest{
		Cmd: req.Cmd, HostType: hostType, DstHost: h, DstPort: 0})
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

	// monitor socks TCP connection
	quit := make(chan bool)
	go gosocks.ConnMonitor(socks, quit)

	// read UDP request from client
	chClientUDP := make(chan *gosocks.UDPPacket)
	go gosocks.UDPReader(clientBind, chClientUDP)

	// read UDP request from tunnel
	chTunnelDatagram := make(chan *datagramFrame)
	go datagramFrameReader(tun, socks.Timeout, chTunnelDatagram)

	clientAddr := clientAssociate

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
				break loop
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
				break loop
			}
			_, err := clientBind.WriteToUDP(frame.datagram, clientAddr)
			if err != nil {
				log.Printf("error to send UDP packet to client: %s", err)
				break loop
			}

		case <-quit:
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
	socks.Close()
	tun.Close()
	clientBind.Close()

	<-chClientUDP
	<-chTunnelDatagram
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
		tunnelTCP(req, tun, socks, socks.Timeout)
		return
	case gosocks.SocksCmdUDPAssociate:
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
	chSocksUDP := make(chan *gosocks.UDPPacket)
	go gosocks.UDPReader(socksUDP, chSocksUDP)

	// read UDP request from tunnel
	chTunnelDatagram := make(chan *datagramFrame)
	go datagramFrameReader(tun, socks.Timeout, chTunnelDatagram)

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
	socks.Close()
	tun.Close()
	socksUDP.Close()

	<-chSocksUDP
	<-chTunnelDatagram
}
