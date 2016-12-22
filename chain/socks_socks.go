package chain

import (
	"log"
	"net"
	"time"

	"github.com/yinghuocho/gosocks"
)

type SocksSocksChain struct {
	SocksDialer *gosocks.SocksDialer
	SocksAddr   string
}

func (ss *SocksSocksChain) TCP(req *gosocks.SocksRequest, src *gosocks.SocksConn) {
	dst, err := ss.SocksDialer.Dial(ss.SocksAddr)
	if err != nil {
		gosocks.ReplyGeneralFailure(src, req)
		src.Close()
		return
	}
	gosocks.WriteSocksRequest(dst, req)
	gosocks.CopyLoopTimeout(src, dst, src.Timeout)
}

func (ss *SocksSocksChain) UDPAssociate(req *gosocks.SocksRequest, src *gosocks.SocksConn, clientBind *net.UDPConn, clientAssociate *net.UDPAddr, firstPkt *gosocks.UDPRequest, clientAddr *net.UDPAddr) {
	dst, err := ss.SocksDialer.Dial(ss.SocksAddr)
	if err != nil {
		src.Close()
		clientBind.Close()
		return
	}

	// bind a UDP port for forwarding
	dstAddr := dst.LocalAddr().(*net.TCPAddr)
	relayBind, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   dstAddr.IP,
		Port: 0,
		Zone: dstAddr.Zone,
	})
	if err != nil {
		log.Printf("error in binding local UDP: %s", err)
		src.Close()
		dst.Close()
		clientBind.Close()
		return
	}

	// send request to forwarding socks connection
	hostType, _, _ := gosocks.NetAddrToSocksAddr(relayBind.LocalAddr())
	h := "0.0.0.0"
	if hostType == gosocks.SocksIPv6Host {
		h = "::"
	}
	dst.SetWriteDeadline(time.Now().Add(dst.Timeout))
	_, err = gosocks.WriteSocksRequest(dst, &gosocks.SocksRequest{
		Cmd: req.Cmd, HostType: hostType, DstHost: h, DstPort: 0})
	if err != nil {
		log.Printf("error in sending request to forwarding socks: %s", err)
		src.Close()
		dst.Close()
		clientBind.Close()
		relayBind.Close()
		return
	}

	// read reply from forwarding socks connection
	dst.SetReadDeadline(time.Now().Add(dst.Timeout))
	reply, err := gosocks.ReadSocksReply(dst)
	if err != nil {
		log.Printf("error in reading reply from forwarding socks: %s", err)
		src.Close()
		dst.Close()
		clientBind.Close()
		relayBind.Close()
		return
	}
	if reply.Rep != gosocks.SocksSucceeded {
		log.Printf("error in reply from forwarding socks: 0x%02x", reply.Rep)
		src.Close()
		dst.Close()
		clientBind.Close()
		relayBind.Close()
		return
	}

	// write first packet
	relayAddr := gosocks.SocksAddrToNetAddr("udp", reply.BndHost, reply.BndPort).(*net.UDPAddr)
	_, err = relayBind.WriteToUDP(gosocks.PackUDPRequest(firstPkt), relayAddr)
	if err != nil {
		log.Printf("error to send UDP packet to forwarding socks: %s", err)
		src.Close()
		dst.Close()
		clientBind.Close()
		relayBind.Close()
		return
	}

	// monitoring socks connections, quit for any reading event
	quitSrc := make(chan bool)
	go gosocks.ConnMonitor(src, quitSrc)
	quitDst := make(chan bool)
	go gosocks.ConnMonitor(dst, quitDst)

	// read client UPD packets
	quitUDP := make(chan bool)
	chClientUDP := make(chan *gosocks.UDPPacket)
	go gosocks.UDPReader(clientBind, chClientUDP, quitUDP)

	// read relay UPD packets
	chRelayUDP := make(chan *gosocks.UDPPacket)
	go gosocks.UDPReader(relayBind, chRelayUDP, quitUDP)

loop:
	for {
		t := time.NewTimer(src.Timeout)
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
			_, err = relayBind.WriteToUDP(gosocks.PackUDPRequest(udpReq), relayAddr)
			if err != nil {
				log.Printf("error to relay UDP to forwarding socks: %s", err)
				break loop
			}

		// requests from forwarding socks, send to client
		case pkt, ok := <-chRelayUDP:
			t.Stop()
			if !ok {
				break loop
			}
			_, err := clientBind.WriteToUDP(pkt.Data, clientAddr)
			if err != nil {
				log.Printf("error to send UDP packet to client: %s", err)
				break loop
			}

		case <-quitSrc:
			t.Stop()
			log.Printf("UDP unexpected event from client socks")
			break loop

		case <-quitDst:
			t.Stop()
			log.Printf("UDP unexpected event from forwarding socks")
			break loop

		case <-t.C:
			log.Printf("UDP timeout")
			break loop
		}
		t.Stop()
	}

	// clean up
	src.Close()
	dst.Close()
	clientBind.Close()
	relayBind.Close()
	close(quitUDP)
}
