// HTTP based data transport
// inspired by:
//     - meek <https://git.torproject.org/pluggable-transports/meek.git>
//     - enproxy <https://github.com/getlantern/enproxy>

package httptran

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	// "sync/atomic"
	"syscall"
	"time"

	"github.com/yinghuocho/golibfq/utils"
)

const (
	maxRequestSize = 0x10000
	largeBufSize   = 0x10000
	// relayBufLimit  = 0

	// some constants copied from meek
	// https://git.torproject.org/pluggable-transports/meek.git
	sessionIDLength        = 8
	initPollInterval       = 100 * time.Millisecond
	maxPollInterval        = 5 * time.Second
	pollIntervalMultiplier = 1.5
	maxTries               = 5
	retryDelay             = 2 * time.Second

	clientTurnaroundTimeout    = 20 * time.Millisecond
	serverTurnaroundTimeout    = 20 * time.Millisecond
	serverMaxTurnaroundTimeout = 200 * time.Millisecond
	maxSessionStaleness        = 120 * time.Second

	maxDuplexRecvTimeout = 30 * time.Second
)

// client side
type PollRequestGenerator interface {
	GenerateRequest([]byte) (*http.Request, error)
}

type DomainFrontingPollRequestGenerator struct {
	URL  *url.URL
	Host string
}

func (df *DomainFrontingPollRequestGenerator) GenerateRequest(data []byte) (*http.Request, error) {
	req, err := http.NewRequest("POST", df.URL.String(), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Connection", "keep-alive")
	if df.Host != "" {
		req.Host = df.Host
	}
	return req, nil
}

type pollSession struct {
	sessionID string

	// up    *utils.BytePipe
	// upCur []byte

	// down *utils.BytePipe
	pc net.Conn
	ps net.Conn

	// readDeadline  time.Time
	// writeDeadline time.Time

	// quitFlag uint32
}

func genSessionID() string {
	buf := make([]byte, sessionIDLength)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err.Error())
	}
	return strings.TrimRight(base64.StdEncoding.EncodeToString(buf), "=")
}

func (s *pollSession) Write(buf []byte) (int, error) {
	// copyed := make([]byte, len(buf))
	// copy(copyed, buf)
	// e := s.down.In(copyed, s.writeDeadline)
	// if e != nil {
	//	return 0, e
	// } else {
	// 	return len(buf), nil
	// }
	return s.pc.Write(buf)
}

func (s *pollSession) Read(buf []byte) (n int, err error) {
	// if len(s.upCur) == 0 {
	//	s.upCur, err = s.up.Out(s.readDeadline)
	//	if err != nil {
	//		return
	//	}
	// }
	// n = copy(buf, s.upCur)
	// s.upCur = s.upCur[n:]
	// return
	return s.pc.Read(buf)
}

func (s *pollSession) LocalAddr() net.Addr {
	// return nil
	return s.pc.LocalAddr()
}

func (s *pollSession) RemoteAddr() net.Addr {
	// return nil
	return s.pc.RemoteAddr()
}

func (s *pollSession) SetDeadline(t time.Time) error {
	// err := s.SetReadDeadline(t)
	// if err != nil {
	//	return err
	// }
	// err = s.SetWriteDeadline(t)
	// if err != nil {
	//	return err
	// }
	// return nil
	e := s.pc.SetReadDeadline(t)
	if e != nil {
		return e
	}
	e = s.pc.SetWriteDeadline(t)
	if e != nil {
		return e
	}
	return nil
}

func (s *pollSession) SetReadDeadline(t time.Time) error {
	// s.readDeadline = t
	// return nil
	return s.pc.SetReadDeadline(t)
}

func (s *pollSession) SetWriteDeadline(t time.Time) error {
	// s.writeDeadline = t
	// return nil
	return s.pc.SetWriteDeadline(t)
}

func (s *pollSession) Close() error {
	// if atomic.AddUint32(&s.quitFlag, 1) <= 1 {
	// s.up.Stop()
	// s.down.Stop()
	// }
	// return nil
	s.pc.Close()
	return nil
}

// func (s *pollSession) isClosed() bool {
//	return atomic.LoadUint32(&s.quitFlag) > 0
// }

type ClosableRoundTripper interface {
	http.RoundTripper
	io.Closer
}

type SimpleSingleThreadTransport struct {
	Dial         func(network, addr string) (net.Conn, error)
	DialTLS      func(network, addr string) (net.Conn, error)
	DialTimeout  time.Duration
	WriteTimeout time.Duration
	ReadTimeout  time.Duration

	conn net.Conn
	r    *bufio.Reader
}

func hasPort(s string) bool { return strings.LastIndex(s, ":") > strings.LastIndex(s, "]") }

var portMap = map[string]string{
	"http":  "80",
	"https": "443",
}

// canonicalAddr returns url.Host but always with a ":port" suffix
func canonicalAddr(url *url.URL) string {
	addr := url.Host
	if !hasPort(addr) {
		return addr + ":" + portMap[url.Scheme]
	}
	return addr
}

func (tp *SimpleSingleThreadTransport) getConn(req *http.Request) error {
	var (
		c net.Conn
		e error
	)
	addr := canonicalAddr(req.URL)
	switch req.URL.Scheme {
	case "http":
		c, e = tp.Dial("tcp", addr)
	case "https":
		c, e = tp.DialTLS("tcp", addr)
	}

	if e == nil {
		tp.conn = c
		tp.r = bufio.NewReader(c)
	}
	return e
}

func closeRequest(req *http.Request) {
	if req.Body != nil {
		req.Body.Close()
	}
}

func (tp *SimpleSingleThreadTransport) Close() error {
	if tp.conn != nil {
		e := tp.conn.Close()
		tp.conn = nil
		return e
	} else {
		return nil
	}
}

// caller must esure to call Response.Body.Close before
// it is caller's responsiblity to retry
func (tp *SimpleSingleThreadTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL == nil {
		closeRequest(req)
		return nil, errors.New("http: nil Request.URL")
	}
	if req.Header == nil {
		closeRequest(req)
		return nil, errors.New("http: nil Request.Header")
	}
	if s := req.URL.Scheme; s != "http" && s != "https" {
		closeRequest(req)
		return nil, fmt.Errorf("unsupported protocol scheme %s", s)
	}
	if req.URL.Host == "" {
		closeRequest(req)
		return nil, errors.New("http: no Host in request URL")
	}

	// retry handles to caller
	if tp.conn == nil {
		err := tp.getConn(req)
		if err != nil {
			closeRequest(req)
			return nil, err
		}
	}
	tp.conn.SetWriteDeadline(time.Now().Add(tp.WriteTimeout))
	err := req.Write(tp.conn)
	if err != nil {
		closeRequest(req)
		tp.conn.Close()
		tp.conn = nil
		return nil, err
	}
	tp.conn.SetReadDeadline(time.Now().Add(tp.ReadTimeout))
	resp, err := http.ReadResponse(tp.r, req)
	if err != nil {
		closeRequest(req)
		tp.conn.Close()
		tp.conn = nil
		return nil, err
	}
	return resp, nil
}

type pollSessionClientHandler struct {
	session       *pollSession
	sendTransport ClosableRoundTripper
	recvTransport ClosableRoundTripper
	transport     ClosableRoundTripper
	gen           PollRequestGenerator
}

func sendRecv(rt http.RoundTripper, req *http.Request, rawBody []byte) (resp *http.Response, err error) {
	defer func() {
		if err != nil {
			if resp != nil {
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
			}
		}
	}()

	for retries := 0; retries < maxTries; retries++ {
		var body io.Reader
		body = bytes.NewReader(rawBody)
		rc, ok := body.(io.ReadCloser)
		if !ok && body != nil {
			rc = ioutil.NopCloser(body)
		}
		req.Body = rc
		resp, err = rt.RoundTrip(req)
		switch {
		case err != nil:
			log.Printf("sendRecv error: %s", err)
		case resp.StatusCode == http.StatusOK:
			return
		case resp.StatusCode == http.StatusResetContent:
			err = &net.OpError{Op: "write", Err: syscall.ECONNRESET}
			return
		}
		time.Sleep(retryDelay)
	}
	err = &utils.TimeoutError{}
	return
}

// func readResponse(resp *http.Response, pipe *utils.BytePipe) (int, error) {
func readResponse(resp *http.Response, c net.Conn) (int, error) {
	var buf [largeBufSize]byte
	var err error
	var total int = 0
	var n int = 0

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	for {
		n, err = resp.Body.Read(buf[:])
		if n > 0 {
			// b := make([]byte, n)
			// copy(b, buf[:n])
			// e := pipe.In(b, time.Time{})
			_, e := c.Write(buf[:n])
			if e != nil {
				return total, e
			} else {
				total += n
			}
		} else {
			break
		}
	}
	if err == io.EOF {
		return total, nil
	}
	return total, err
}

func (ch *pollSessionClientHandler) roundTrip(transport ClosableRoundTripper, data []byte, extraHeaders map[string]string) (*http.Response, error) {
	req, err := ch.gen.GenerateRequest(data)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Session-Id", ch.session.sessionID)
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	return sendRecv(transport, req, data)
}

func (ch *pollSessionClientHandler) handShake() error {
	// a message for handshake, similar as a TCP SYN
	resp, err := ch.roundTrip(ch.transport, nil, map[string]string{"X-Session-Ctrl": "SYN"})
	if resp != nil {
		defer func() {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()
	}
	if err != nil {
		log.Printf("fail to finish a SYN request: %s", err)
		return err
	}
	if resp == nil {
		log.Printf("SYN request got null response")
		return fmt.Errorf("hanshake failed: no valid response")
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("SYN request gets bad response: %d", resp.StatusCode)
		return fmt.Errorf("hanshake failed: status %d", resp.StatusCode)
	}
	return nil
}

func (ch *pollSessionClientHandler) duplexHandShake() error {
	// a message for handshake, similar as a TCP SYN
	resp, err := ch.roundTrip(ch.sendTransport, nil, map[string]string{"X-Session-Ctrl": "SYN-DUPLEX"})
	if resp != nil {
		defer func() {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()
	}
	if err != nil {
		log.Printf("fail to finish a SYN request: %s", err)
		return err
	}
	if resp == nil {
		log.Printf("SYN request got null response")
		return fmt.Errorf("hanshake failed: no valid response")
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("SYN request gets bad response: %d", resp.StatusCode)
		return fmt.Errorf("hanshake failed: status %d", resp.StatusCode)
	}
	return nil
}

func (ch *pollSessionClientHandler) pollLoop() {
	var interval time.Duration = initPollInterval

loop:
	for {
		var buffer [maxRequestSize]byte
		// n, err := ch.session.down.OutToBuf(buffer[:], clientTurnaroundTimeout, time.Now().Add(interval))
		ch.session.ps.SetReadDeadline(time.Now().Add(interval))
		n, e := ch.session.ps.Read(buffer[:])
		// data, err := ch.session.down.Out(time.Now().Add(interval))
		if e == io.EOF {
			resp, _ := ch.roundTrip(ch.transport, nil, map[string]string{"X-Session-Ctrl": "FIN"})
			if resp != nil {
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
			}
			break loop
		}
		resp, err := ch.roundTrip(ch.transport, buffer[:n], nil)
		if err != nil {
			break loop
		}
		rbytes, err := readResponse(resp, ch.session.ps)
		if err != nil {
			break loop
		}
		if resp.Close {
			// server said the underlying connection should be closed
			log.Printf("pollLoop: Transport received Close signal from server")
			ch.transport.Close()
		}

		if rbytes > 0 {
			interval = clientTurnaroundTimeout
		} else if interval == 0 {
			interval = initPollInterval
		} else {
			interval = time.Duration(float64(interval) * pollIntervalMultiplier)
		}
		if interval > maxPollInterval {
			interval = maxPollInterval
		}
	}
	ch.session.ps.Close()
	ch.transport.Close()
}

func (ch *pollSessionClientHandler) duplexRecvLoop() {
loop:
	for {
		resp, err := ch.roundTrip(ch.recvTransport, nil, map[string]string{"X-Session-Ctrl": "DUPLEX-RECV"})
		if err != nil {
			log.Printf("duplexRecvLoop: error roundTrip: %s", err)
			break loop
		}
		// n, err := readResponse(resp, ch.session.up)
		_, err = readResponse(resp, ch.session.ps)
		if err != nil {
			log.Printf("duplexRecvLoop: error readResponse: %s", err)
			break loop
		}
		// log.Printf("duplexRecvLoop: received %d bytes", n)
		if resp.Close {
			// server said the underlying connection should be closed
			log.Printf("duplexRecvLoop: Transport received Close signal from server")
			ch.recvTransport.Close()
		}
	}
	ch.session.ps.Close()
	ch.recvTransport.Close()
}

func (ch *pollSessionClientHandler) duplexSendLoop() {
	txcnt := 0
loop:
	for {
		var buffer [maxRequestSize]byte
		ch.session.ps.SetReadDeadline(time.Time{})
		n, e := ch.session.ps.Read(buffer[:])
		// n, err := ch.session.down.OutToBuf(buffer[:], clientTurnaroundTimeout, time.Time{})
		// data, err := ch.session.down.Out(time.Time{})
		if e == io.EOF {
			log.Printf("duplexSendLoop: outbound stream closed")
			resp, _ := ch.roundTrip(ch.sendTransport, nil, map[string]string{"X-Session-Ctrl": "FIN"})
			if resp != nil {
				resp.Body.Close()
			}
			break loop
		}
		if n > 0 {
			resp, err := ch.roundTrip(ch.sendTransport, buffer[:n], nil)
			if err != nil {
				log.Printf("duplexSendLoop error: %s", err)
				break loop
			}
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
			txcnt += 1
			// log.Printf("duplexSendLoop: sent %d requests, %d bytes in this connection", txcnt, n)
			if resp.Close {
				// server said the underlying connection should be closed
				log.Printf("duplexSendLoop: Transport received Close signal from server")
				ch.sendTransport.Close()
				txcnt = 0
			}
		}
	}
	ch.session.ps.Close()
	ch.sendTransport.Close()
}

func newPollSession(sessionID string) *pollSession {
	pc, ps := utils.SocketPipe()
	s := &pollSession{
		sessionID: sessionID,
		// up:        utils.NewBytePipe(sessionID+"-UP", relayBufLimit),
		// down:      utils.NewBytePipe(sessionID+"-DOWN", relayBufLimit),
		pc: pc,
		ps: ps,
	}
	// go s.up.Start()
	// go s.down.Start()
	return s
}

func NewPollClientSession(transport ClosableRoundTripper, gen PollRequestGenerator) (net.Conn, error) {
	sessionID := genSessionID()
	ch := &pollSessionClientHandler{
		session:   newPollSession(sessionID),
		transport: transport,
		gen:       gen,
	}
	err := ch.handShake()
	if err != nil {
		ch.session.Close()
		return nil, err
	}
	go ch.pollLoop()
	return ch.session, nil
}

func NewDuplexClientSession(sendTransport ClosableRoundTripper, recvTransport ClosableRoundTripper, gen PollRequestGenerator) (net.Conn, error) {
	sessionID := genSessionID()
	ch := &pollSessionClientHandler{
		session:       newPollSession(sessionID),
		sendTransport: sendTransport,
		recvTransport: recvTransport,
		gen:           gen,
	}
	err := ch.duplexHandShake()
	if err != nil {
		ch.session.Close()
		return nil, err
	}
	go ch.duplexSendLoop()
	go ch.duplexRecvLoop()
	return ch.session, nil
}

// server side
type labeledPollSession struct {
	*pollSession
	duplex   bool
	lastSeen time.Time
}

func (ts *labeledPollSession) touch() {
	ts.lastSeen = time.Now()
}

func (ts *labeledPollSession) isExpired() bool {
	return time.Since(ts.lastSeen) > maxSessionStaleness
}

func (ts *labeledPollSession) isDuplex() bool {
	return ts.duplex
}

type pollSessionManager struct {
	sessionMap map[string]*labeledPollSession
	lock       sync.Mutex
}

func (m *pollSessionManager) createSession(sessionID string, duplex bool) (*labeledPollSession, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	session, ok := m.sessionMap[sessionID]
	if ok {
		return session, false
	}
	s := &labeledPollSession{
		pollSession: newPollSession(sessionID),
		duplex:      duplex,
		lastSeen:    time.Now(),
	}
	m.sessionMap[sessionID] = s
	return s, true
}

func (m *pollSessionManager) closeSession(sessionID string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	session, ok := m.sessionMap[sessionID]
	if ok {
		delete(m.sessionMap, sessionID)
		session.ps.Close()
	}
	log.Printf("close session: %s", sessionID)
}

func (m *pollSessionManager) getSession(sessionID string) *labeledPollSession {
	m.lock.Lock()
	defer m.lock.Unlock()
	session := m.sessionMap[sessionID]
	if session != nil {
		session.touch()
	}
	return session
}

func (m *pollSessionManager) expireSessions() {
	for {
		time.Sleep(maxSessionStaleness / 2)
		m.lock.Lock()
		for sessionID, session := range m.sessionMap {
			if session.isExpired() {
				log.Printf("session expired %s", sessionID)
				delete(m.sessionMap, sessionID)
				session.ps.Close()
			}
		}
		log.Printf("alive sessions: %d", len(m.sessionMap))
		m.lock.Unlock()
	}
}

type ServerSessionServeFunc func(net.Conn)

type PollServerHandler struct {
	f ServerSessionServeFunc
	m *pollSessionManager
}

func NewPollServerHandler(f ServerSessionServeFunc) *PollServerHandler {
	return &PollServerHandler{
		f: f,
		m: &pollSessionManager{
			sessionMap: make(map[string]*labeledPollSession),
		},
	}
}

func httpBadRequest(w http.ResponseWriter) {
	http.Error(w, "Bad request.", http.StatusBadRequest)
}

func httpInvalidSession(w http.ResponseWriter) {
	w.WriteHeader(http.StatusResetContent)
	w.Write([]byte("Invalid Session."))
}

func httpSessionAck(w http.ResponseWriter) {
	w.Header().Set("X-Session-Ctrl", "ACK")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(""))
}

func isSYN(req *http.Request) bool {
	return strings.HasPrefix(req.Header.Get("X-Session-Ctrl"), "SYN")
}

func isDUPLEX(req *http.Request) bool {
	return strings.HasPrefix(req.Header.Get("X-Session-Ctrl"), "SYN-DUPLEX")
}

func isRECV(req *http.Request) bool {
	return strings.HasPrefix(req.Header.Get("X-Session-Ctrl"), "DUPLEX-RECV")
}

func isFIN(req *http.Request) bool {
	return strings.HasPrefix(req.Header.Get("X-Session-Ctrl"), "FIN")
}

func (sh *PollServerHandler) get(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("GET is only for testing in a poll server.\n"))
}

func (sh *PollServerHandler) post(w http.ResponseWriter, req *http.Request) {
	// puth Content-Type unconditionally, as per
	// https://trac.torproject.org/projects/tor/ticket/12778
	w.Header().Set("Content-Type", "application/octet-stream")
	sessionID := req.Header.Get("X-Session-Id")
	if len(sessionID) < sessionIDLength {
		log.Printf("invalid sessionID: %s", sessionID)
		httpBadRequest(w)
		return
	}
	session := sh.m.getSession(sessionID)
	if session == nil {
		if !isSYN(req) {
			httpInvalidSession(w)
			return
		}
		duplex := isDUPLEX(req)
		session, created := sh.m.createSession(sessionID, duplex)
		if session == nil {
			httpInvalidSession(w)
			return
		}
		if created {
			log.Printf("new session %s", sessionID)
			go sh.f(session)
		}
		httpSessionAck(w)
		return
	}
	if isFIN(req) {
		sh.m.closeSession(sessionID)
		httpSessionAck(w)
		return
	}
	if session.isDuplex() {
		sh.processDuplexRequest(session.pollSession, w, req)
	} else {
		sh.processRequest(session.pollSession, w, req)
	}
	return
}

func (sh *PollServerHandler) processDuplexRequest(session *pollSession, w http.ResponseWriter, req *http.Request) {
	if isRECV(req) {
		sh.processDuplexRecv(session, w, req)
	} else {
		body := http.MaxBytesReader(w, req.Body, maxRequestSize)
		data, e := ioutil.ReadAll(body)
		if e != nil && e != io.EOF {
			sh.m.closeSession(session.sessionID)
			httpInvalidSession(w)
			return
		}

		if len(data) > 0 {
			session.ps.SetWriteDeadline(time.Time{})
			session.ps.Write(data)
		}
		w.Write([]byte(""))
		return
	}
}

func (sh *PollServerHandler) processDuplexRecv(session *pollSession, w http.ResponseWriter, req *http.Request) {
	start := false
	hardDeadline := time.Now().Add(maxDuplexRecvTimeout)
	var buf [largeBufSize]byte
loop:
	for {
		softDeadline := time.Now().Add(serverTurnaroundTimeout)
		deadline := softDeadline
		if softDeadline.After(hardDeadline) || !start {
			deadline = hardDeadline
		}
		// data, err := session.down.Out(deadline)
		session.ps.SetReadDeadline(deadline)
		n, e := session.ps.Read(buf[:])
		if e != nil {
			// write("") to flush chunks bufferred by CDNs
			w.Write([]byte(""))
			if e == io.EOF {
				sh.m.closeSession(session.sessionID)
			}
			break loop
		}
		_, e = w.Write(buf[:n])
		start = true
		if e != nil {
			log.Println(n, e)
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
}

func (sh *PollServerHandler) processRequest(session *pollSession, w http.ResponseWriter, req *http.Request) {
	var buf [largeBufSize]byte

	body := http.MaxBytesReader(w, req.Body, maxRequestSize)
	data, e := ioutil.ReadAll(body)
	if e != nil && e != io.EOF {
		sh.m.closeSession(session.sessionID)
		httpInvalidSession(w)
		return
	}
	if len(data) > 0 {
		// session.up.In(data, time.Time{})
		session.ps.SetWriteDeadline(time.Time{})
		session.ps.Write(data)
	}
	hardDeadline := time.Now().Add(serverMaxTurnaroundTimeout)
	for {
		softDeadline := time.Now().Add(serverTurnaroundTimeout)
		deadline := softDeadline
		if softDeadline.After(hardDeadline) {
			deadline = hardDeadline
		}
		// data, err := session.down.Out(deadline)
		session.ps.SetReadDeadline(deadline)
		n, e := session.ps.Read(buf[:])
		if e != nil {
			w.Write([]byte(""))
			if e == io.EOF {
				sh.m.closeSession(session.sessionID)
			}
			return
		}
		w.Write(buf[:n])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
}

func (sh *PollServerHandler) Run() {
	go sh.m.expireSessions()
}

func (sh *PollServerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header()["Date"] = nil
	switch req.Method {
	case "GET":
		sh.get(w, req)
	case "POST":
		sh.post(w, req)
	default:
		httpBadRequest(w)
	}
}
