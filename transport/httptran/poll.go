// HTTP based data transport
// inspired by:
//     - meek <https://git.torproject.org/pluggable-transports/meek.git>
//     - enproxy <https://github.com/getlantern/enproxy>

package httptran

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	maxChunkLength = 0x10000

	// some constants copied from meek
	// https://git.torproject.org/pluggable-transports/meek.git
	sessionIDLength        = 8
	initPollInterval       = 100 * time.Millisecond
	maxPollInterval        = 5 * time.Second
	pollIntervalMultiplier = 1.5
	maxTries               = 5
	retryDelay             = 5 * time.Second

	turnaroundTimeout    = 20 * time.Millisecond
	maxTurnaroundTimeout = 200 * time.Millisecond
	maxSessionStaleness  = 120 * time.Second

	maxDuplexRecvTimeout = 2 * time.Minute
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
	if df.Host != "" {
		req.Host = df.Host
	}
	return req, nil
}

type pollSession struct {
	sessionID string

	// for inbound data
	inboundArriving  chan []byte
	inboundDeparture chan []byte
	inboundPending   [][]byte
	inboundCur       []byte

	// for outbound data
	outboundArriving  chan []byte
	outboundDeparture chan []byte
	outboundPending   [][]byte
	outboundCur       []byte

	readDeadline  time.Time
	writeDeadline time.Time

	quit     chan bool
	quitFlag uint32
}

func bufferedRelayLoop(arrivingCh chan []byte, departureCh chan []byte, buffer *[][]byte, quit chan bool) {
loop:
	for {
		var buf [maxChunkLength]byte
		var ch chan []byte
		var pending []byte

		i := 0
		total := len(*buffer)
		if total > 0 {
			ch = departureCh
			size := 0
			for {
				left := maxChunkLength - size
				if len((*buffer)[i]) > left {
					break
				}
				n := copy(buf[size:], (*buffer)[i])
				i++
				size += n
				if i == total {
					break
				}
			}
			pending = buf[0:size]
		}

		select {
		case <-quit:
			break loop
		case data := <-arrivingCh:
			*buffer = append(*buffer, data)
		case ch <- pending:
			*buffer = (*buffer)[i:]
		}
	}
	close(departureCh)
}

func write(ch chan []byte, quit chan bool, deadline time.Time, b []byte) (int, error) {
	var timeout <-chan time.Time
	var chunk []byte
	var sent int = 0
	var total int = len(b)

	if !deadline.IsZero() {
		t := time.NewTimer(deadline.Sub(time.Now()))
		defer t.Stop()
		timeout = t.C
	}

loop:
	for {
		n := total - sent
		if n <= 0 {
			break loop
		}
		if n > maxChunkLength {
			n = maxChunkLength
		}
		chunk = make([]byte, n)
		copy(chunk, b[sent:sent+n])

		select {
		case <-quit:
			return sent, &net.OpError{Op: "write", Err: syscall.EPIPE}
		case <-timeout:
			return sent, &timeoutError{}
		case ch <- chunk:
			sent += len(chunk)
			chunk = nil
			if sent == total {
				break loop
			}
		}
	}
	return total, nil
}

func read(ch chan []byte, cur *[]byte, pending *[][]byte, quit chan bool, deadline time.Time, b []byte) (n int, err error) {
	var buf []byte
	var ok bool
	var timeout <-chan time.Time

	if len(*cur) != 0 {
		buf = *cur
	} else {
		if !deadline.IsZero() {
			t := time.NewTimer(deadline.Sub(time.Now()))
			defer t.Stop()
			timeout = t.C
		}
		select {
		case buf, ok = <-ch:
			if !ok {
				if len(*pending) > 0 {
					buf = (*pending)[0]
					*pending = (*pending)[1:]
				} else {
					err = io.EOF
					return
				}
			}
			break
		case <-timeout:
			err = &timeoutError{}
			return
		}
	}
	n = copy(b, buf)
	*cur = buf[n:]
	return
}

func genSessionID() string {
	buf := make([]byte, sessionIDLength)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err.Error())
	}
	return strings.TrimRight(base64.StdEncoding.EncodeToString(buf), "=")
}

func (s *pollSession) Write(b []byte) (int, error) {
	return write(s.outboundArriving, s.quit, s.writeDeadline, b)
}

func (s *pollSession) Read(b []byte) (n int, err error) {
	return read(s.inboundDeparture, &s.inboundCur, &s.inboundPending, s.quit, s.readDeadline, b)
}

func (s *pollSession) LocalAddr() net.Addr {
	return nil
}

func (s *pollSession) RemoteAddr() net.Addr {
	return nil
}

func (s *pollSession) SetDeadline(t time.Time) error {
	err := s.SetReadDeadline(t)
	if err != nil {
		return err
	}
	err = s.SetWriteDeadline(t)
	if err != nil {
		return err
	}
	return nil
}

func (s *pollSession) SetReadDeadline(t time.Time) error {
	s.readDeadline = t
	return nil
}

func (s *pollSession) SetWriteDeadline(t time.Time) error {
	s.writeDeadline = t
	return nil
}

func (s *pollSession) Close() error {
	if atomic.AddUint32(&s.quitFlag, 1) <= 1 {
		close(s.quit)
	}
	return nil
}

type pollSessionClientHandler struct {
	session *pollSession
	rt      http.RoundTripper
	gen     PollRequestGenerator
}

func sendRecv(rt http.RoundTripper, req *http.Request) (resp *http.Response, err error) {
	defer func() {
		if err != nil {
			if resp != nil {
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
			}
		}
	}()

	for retries := 0; retries < maxTries; retries++ {
		resp, err = rt.RoundTrip(req)
		if err != nil {
			return
		}
		if resp.StatusCode == http.StatusOK {
			return
		}
		if resp.StatusCode == http.StatusResetContent {
			err = &net.OpError{Op: "write", Err: syscall.ECONNRESET}
			return
		}
		time.Sleep(retryDelay)
	}
	err = &timeoutError{}
	return
}

func readResponse(resp *http.Response, dataCh chan<- []byte, quit <-chan bool) (int, error) {
	var buf [maxChunkLength]byte
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
			b := make([]byte, n)
			copy(b, buf[:n])
			select {
			case <-quit:
				return total, io.ErrClosedPipe
			case dataCh <- b:
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

func (ch *pollSessionClientHandler) roundTrip(data []byte, extraHeaders map[string]string) (*http.Response, error) {
	req, err := ch.gen.GenerateRequest(data)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Session-Id", ch.session.sessionID)
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	return sendRecv(ch.rt, req)
}

func (ch *pollSessionClientHandler) handShake() error {
	// a message for handshake, similar as a TCP SYN
	resp, err := ch.roundTrip(nil, map[string]string{"X-Session-Ctrl": "SYN"})
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
	resp, err := ch.roundTrip(nil, map[string]string{"X-Session-Ctrl": "SYN-DUPLEX"})
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
	var (
		interval time.Duration    = initPollInterval
		t        *time.Timer      = time.NewTimer(interval)
		timeout  <-chan time.Time = t.C
		data     []byte
		ok       bool
		err      error
		resp     *http.Response
		n        int
	)
loop:
	for {
		select {
		case <-ch.session.quit:
			break loop
		case <-timeout:
			data = nil
		case data, ok = <-ch.session.outboundDeparture:
			if !ok {
				if len(ch.session.outboundPending) > 0 {
					data = ch.session.outboundPending[0]
					ch.session.outboundPending = ch.session.outboundPending[1:]
				} else {
					resp, _ := ch.roundTrip(nil, map[string]string{"X-Session-Ctrl": "FIN"})
					if resp != nil {
						io.Copy(ioutil.Discard, resp.Body)
						resp.Body.Close()
					}
					break loop
				}
			}
		}

		if t != nil {
			t.Stop()
		}
		resp, err = ch.roundTrip(data, nil)
		if err != nil {
			break loop
		}
		n, err = readResponse(resp, ch.session.inboundArriving, ch.session.quit)
		if err != nil {
			break loop
		}
		if len(data) != 0 || n > 0 {
			interval = 0
		} else if interval == 0 {
			interval = initPollInterval
		} else {
			interval = time.Duration(float64(interval) * pollIntervalMultiplier)
		}
		if interval > maxPollInterval {
			interval = maxPollInterval
		}
		t = time.NewTimer(interval)
		timeout = t.C
	}
	ch.session.Close()
}

func (ch *pollSessionClientHandler) duplexRecvLoop() {
loop:
	for {
		resp, err := ch.roundTrip(nil, map[string]string{"X-Session-Ctrl": "DUPLEX-RECV"})
		if err != nil {
			break loop
		}
		_, err = readResponse(resp, ch.session.inboundArriving, ch.session.quit)
		if err != nil {
			log.Println(err)
			break loop
		}
	}
	ch.session.Close()
}

func (ch *pollSessionClientHandler) duplexSendLoop() {
loop:
	for {
		select {
		case <-ch.session.quit:
			break loop
		case data, ok := <-ch.session.outboundDeparture:
			if !ok {
				if len(ch.session.outboundPending) > 0 {
					data = ch.session.outboundPending[0]
					ch.session.outboundPending = ch.session.outboundPending[1:]
				} else {
					resp, _ := ch.roundTrip(nil, map[string]string{"X-Session-Ctrl": "FIN"})
					if resp != nil {
						io.Copy(ioutil.Discard, resp.Body)
						resp.Body.Close()
					}
					break loop
				}
			}
			resp, err := ch.roundTrip(data, nil)
			if err != nil {
				break loop
			}
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
	}
	ch.session.Close()
}

func newPollSession(sessionID string) *pollSession {
	s := &pollSession{
		sessionID:         sessionID,
		inboundArriving:   make(chan []byte),
		inboundDeparture:  make(chan []byte),
		outboundArriving:  make(chan []byte),
		outboundDeparture: make(chan []byte),
		quit:              make(chan bool),
	}
	go bufferedRelayLoop(s.inboundArriving, s.inboundDeparture, &s.inboundPending, s.quit)
	go bufferedRelayLoop(s.outboundArriving, s.outboundDeparture, &s.outboundPending, s.quit)
	return s
}

func NewPollClientSession(rt http.RoundTripper, gen PollRequestGenerator) (net.Conn, error) {
	sessionID := genSessionID()
	ch := &pollSessionClientHandler{
		session: newPollSession(sessionID),
		rt:      rt,
		gen:     gen,
	}
	err := ch.handShake()
	if err != nil {
		ch.session.Close()
		return nil, err
	}
	go ch.pollLoop()
	return ch.session, nil
}

func NewDuplexClientSession(rt http.RoundTripper, gen PollRequestGenerator) (net.Conn, error) {
	sessionID := genSessionID()
	ch := &pollSessionClientHandler{
		session: newPollSession(sessionID),
		rt:      rt,
		gen:     gen,
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
		session.Close()
	}
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
				// log.Printf("deleting expired session %q", sessionID)
				delete(m.sessionMap, sessionID)
				session.Close()
			}
		}
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
		body := http.MaxBytesReader(w, req.Body, maxChunkLength+1)
		data, err := ioutil.ReadAll(body)
		if err != nil {
			sh.m.closeSession(session.sessionID)
			httpInvalidSession(w)
			return
		}

		if len(data) > 0 {
			select {
			case session.inboundArriving <- data:
			case <-session.quit:
			}
		}
		w.Write([]byte(""))
		return
	}
}

func (sh *PollServerHandler) processDuplexRecv(session *pollSession, w http.ResponseWriter, req *http.Request) {
	t := time.NewTimer(maxDuplexRecvTimeout)
loop:
	for {
		select {
		case resp, ok := <-session.outboundDeparture:
			if !ok {
				if len(session.outboundPending) > 0 {
					resp = session.outboundPending[0]
					session.outboundPending = session.outboundPending[1:]
				} else {
					sh.m.closeSession(session.sessionID)
					w.Write([]byte(""))
					break loop
				}
			}
			n, err := w.Write(resp)
			if err != nil {
				log.Println(n, err)
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case <-t.C:
			w.Write([]byte(""))
			break loop
		}
	}
}

func (sh *PollServerHandler) processRequest(session *pollSession, w http.ResponseWriter, req *http.Request) {
	// read data from request, send to inboundArriving channel
	body := http.MaxBytesReader(w, req.Body, maxChunkLength+1)
	data, err := ioutil.ReadAll(body)
	if err != nil {
		sh.m.closeSession(session.sessionID)
		httpInvalidSession(w)
		return
	}

	if len(data) > 0 {
		select {
		case session.inboundArriving <- data:
		case <-session.quit:
		}
	}
	tt := time.NewTimer(maxTurnaroundTimeout)
	defer tt.Stop()
	for {
		t := time.NewTimer(turnaroundTimeout)
		select {
		case resp, ok := <-session.outboundDeparture:
			t.Stop()
			if !ok {
				if len(session.outboundPending) > 0 {
					resp = session.outboundPending[0]
					session.outboundPending = session.outboundPending[1:]
				} else {
					w.Write([]byte(""))
					sh.m.closeSession(session.sessionID)
					return
				}
			}
			w.Write(resp)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case <-t.C:
			w.Write([]byte(""))
			return
		case <-tt.C:
			t.Stop()
			w.Write([]byte(""))
			return
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

type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }
