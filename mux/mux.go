package mux

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	SYN  = 0x1
	FIN  = 0x2
	RST  = 0x4
	DATA = 0x8

	maxDataLen = 65535
)

type muxFrame struct {
	sID     uint32
	flag    byte
	dataLen uint16
	data    []byte
}

type Session struct {
	isClient bool

	// atomic generation Id for new Streams
	genStreamID uint32

	conn    net.Conn
	quit    chan bool
	writeCh chan *muxFrame

	// mapLock protects operations on streams-map by Session.loop or someone
	// invokes Stream.Close, or Client.OpenStream
	//
	// Closed session has a nil map to prevent new streams by Client.OpenStream
	streams map[uint32]*Stream
	mapLock *sync.Mutex

	// Used by Server to write new Streams to someone waiting on Server.Accept.
	streamCh chan *Stream

	// idle session will be closed
	idle time.Duration
}

type Client struct {
	s *Session
}

type Server struct {
	s *Session
}

// implement net.Conn interface
type Stream struct {
	id uint32

	// checked by Write to ensure first data from Client Stream has a SYN flag.
	synSent bool

	// inbound data is buffered, and the buffered data can be read out after closing.
	inboundArriving  chan []byte
	inboundDeparture chan []byte
	inboundPending   [][]byte
	inboundCur       []byte

	readDeadline  time.Time
	writeDeadline time.Time

	// quitFlag is checked to ensure quit signal only sent once.
	quit     chan bool
	quitFlag uint32

	session *Session
}

func NewClient(c net.Conn) *Client {
	s := &Session{
		isClient: true,
		conn:     c,
		streams:  make(map[uint32]*Stream),
		quit:     make(chan bool),
		writeCh:  make(chan *muxFrame),
		mapLock:  &sync.Mutex{},
		streamCh: make(chan *Stream),
	}
	go s.loop()
	return &Client{s: s}
}

func (c *Client) OpenStream() (*Stream, error) {
	id := atomic.AddUint32(&c.s.genStreamID, 1)
	return c.s.newStream(id)
}

func (c *Client) SetIdleTime(idle time.Duration) {
	c.s.idle = idle
}

func (c *Client) Close() error {
	return c.s.cloze()
}

func NewServer(c net.Conn) *Server {
	s := &Session{
		isClient: false,
		conn:     c,
		streams:  make(map[uint32]*Stream),
		quit:     make(chan bool),
		writeCh:  make(chan *muxFrame),
		mapLock:  &sync.Mutex{},
		streamCh: make(chan *Stream),
	}
	go s.loop()
	return &Server{s: s}
}

func (s *Server) Accept() (*Stream, error) {
	stream, ok := <-s.s.streamCh
	if !ok {
		return nil, io.EOF
	}
	return stream, nil
}

func (s *Server) Close() error {
	return s.s.cloze()
}

func (s *Server) SetIdleTime(idle time.Duration) {
	s.s.idle = idle
}

func (s *Session) readFrame() (frame muxFrame, err error) {
	var buf [7]byte
	_, err = io.ReadFull(s.conn, buf[:])
	if err != nil {
		return
	}
	var sID uint32
	err = binary.Read(bytes.NewReader(buf[:4]), binary.BigEndian, &sID)
	if err != nil {
		return
	}
	flag := buf[4]
	var dataLen uint16
	err = binary.Read(bytes.NewReader(buf[5:]), binary.BigEndian, &dataLen)
	if err != nil {
		return
	}
	frame = muxFrame{sID: sID, flag: flag, dataLen: dataLen}
	if dataLen > 0 {
		data := make([]byte, dataLen)
		_, err = io.ReadFull(s.conn, data[:])
		if err != nil {
			return
		}
		frame.data = data
	}
	return
}

func (s *Session) writeFrame(frame *muxFrame) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, frame.sID)
	buf.WriteByte(frame.flag)
	binary.Write(buf, binary.BigEndian, frame.dataLen)
	if frame.dataLen > 0 {
		buf.Write(frame.data)
	}
	_, err := s.conn.Write(buf.Bytes())
	return err
}

func (s *Session) getStream(sID uint32) *Stream {
	s.mapLock.Lock()
	defer s.mapLock.Unlock()

	return s.streams[sID]
}

func (s *Session) newStream(sID uint32) (*Stream, error) {
	s.mapLock.Lock()
	defer s.mapLock.Unlock()

	// Session already closed
	if s.streams == nil {
		return nil, io.EOF
	}
	stream := &Stream{
		id:               sID,
		inboundArriving:  make(chan []byte),
		inboundDeparture: make(chan []byte),
		quit:             make(chan bool),
		session:          s,
	}

	s.streams[sID] = stream
	go stream.loop()
	return stream, nil
}

func (s *Session) clearStream(sID uint32) {
	s.mapLock.Lock()
	defer s.mapLock.Unlock()

	delete(s.streams, sID)
}

func (s *Session) loop() {
	var pendingStream []*Stream
	var pendingWrite []*muxFrame

	readCh := make(chan *muxFrame)
	// reader
	go func() {
		for {
			frame, err := s.readFrame()
			if err != nil {
				break
			}
			readCh <- &frame
		}
		s.cloze()
		close(s.quit)
		return
	}()

	// writer
	go func() {
	writeLoop:
		for {
			select {
			case frame := <-s.writeCh:
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, frame.sID)
				buf.WriteByte(frame.flag)
				binary.Write(buf, binary.BigEndian, frame.dataLen)
				if frame.dataLen > 0 {
					buf.Write(frame.data)
				}
				_, err := s.conn.Write(buf.Bytes())
				if err != nil {
					// close to notify reader
					s.cloze()
					break writeLoop
				}
			case <-s.quit:
				// signal from reader that this session ends
				break writeLoop
			}
		}
		return
	}()

loop:
	for {
		var writeCh chan *muxFrame
		var streamCh chan *Stream
		var curStream *Stream
		var curFrame *muxFrame
		var timer *time.Timer
		var to <-chan time.Time

		if s.idle != 0 {
			timer = time.NewTimer(s.idle)
			to = timer.C
		}

		if !s.isClient && len(pendingStream) > 0 {
			streamCh = s.streamCh
			curStream = pendingStream[0]
		}

		if len(pendingWrite) > 0 {
			writeCh = s.writeCh
			curFrame = pendingWrite[0]
		}

		select {
		case <-to:
			s.mapLock.Lock()
			for _, stream := range s.streams {
				stream.cloze()
			}
			s.streams = nil
			s.mapLock.Unlock()
			break loop
		case streamCh <- curStream:
			pendingStream = pendingStream[1:]
		case writeCh <- curFrame:
			pendingWrite = pendingWrite[1:]
		case fr := <-readCh:
			stream := s.getStream(fr.sID)
			// SYN
			if (stream == nil) && (fr.flag&SYN != 0) {
				stream, _ = s.newStream(fr.sID)
				if !s.isClient {
					pendingStream = append(pendingStream, stream)
				}
			}

			// DATA
			if (fr.flag&DATA != 0) && (fr.dataLen > 0) {
				if stream != nil {
					stream.newData(fr.data)
				} else {
					// unsolicited frame, respond with RST
					// use a buffer so reading loop　would not be blocked
					pendingWrite = append(pendingWrite, &muxFrame{sID: fr.sID, flag: RST})
				}
			}

			// FIN or RST
			if (fr.flag&FIN != 0) || (fr.flag&RST != 0) {
				if stream != nil {
					s.clearStream(fr.sID)
					stream.cloze()
				}
			}
		case <-s.quit:
			// quit message needs to broadcast to all streams.
			s.mapLock.Lock()
			for _, stream := range s.streams {
				stream.cloze()
			}
			s.streams = nil
			s.mapLock.Unlock()
			break loop
		}
		if timer != nil {
			timer.Stop()
		}
	}
	close(s.streamCh)
	s.cloze()
}

func (s *Session) cloze() error {
	return s.conn.Close()
}

func (s *Session) remoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

func (s *Session) localAddr() net.Addr {
	return s.conn.LocalAddr()
}

func (m *Stream) loop() {
loop:
	for {
		var ch chan []byte
		var pending []byte

		if len(m.inboundPending) > 0 {
			ch = m.inboundDeparture
			pending = m.inboundPending[0]
		}

		select {
		case <-m.quit:
			break loop
		case data := <-m.inboundArriving:
			m.inboundPending = append(m.inboundPending, data)
		case ch <- pending:
			m.inboundPending = m.inboundPending[1:]
		}
	}
	close(m.inboundDeparture)
}

func (m *Stream) newData(data []byte) {
	select {
	case <-m.quit:
	case m.inboundArriving <- data:
	}
}

func (m *Stream) Read(b []byte) (n int, err error) {
	var buf []byte
	var ok bool
	var timeout <-chan time.Time

	if len(m.inboundCur) != 0 {
		buf = m.inboundCur
	} else {
		if !m.readDeadline.IsZero() {
			t := time.NewTimer(m.writeDeadline.Sub(time.Now()))
			defer t.Stop()
			timeout = t.C
		}
		select {
		case buf, ok = <-m.inboundDeparture:
			if !ok {
				if len(m.inboundPending) > 0 {
					buf = m.inboundPending[0]
					m.inboundPending = m.inboundPending[1:]
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
	m.inboundCur = buf[n:]
	return
}

func (m *Stream) Write(b []byte) (int, error) {
	var timeout <-chan time.Time
	var frame *muxFrame
	var sent int = 0
	var total int = len(b)

	if !m.writeDeadline.IsZero() {
		t := time.NewTimer(m.writeDeadline.Sub(time.Now()))
		defer t.Stop()
		timeout = t.C
	}

loop:
	for {
		n := total - sent
		if n <= 0 {
			break loop
		}
		if n > maxDataLen {
			n = maxDataLen
		}
		buf := make([]byte, n)
		copy(buf, b[sent:sent+n])
		frame = &muxFrame{
			sID:     m.id,
			flag:    DATA,
			dataLen: uint16(n),
			data:    buf,
		}
		if !m.synSent && m.session.isClient {
			frame.flag |= SYN
			m.synSent = true
		}

		select {
		case <-m.quit:
			return sent, &net.OpError{Op: "write", Err: syscall.EPIPE}
		case <-m.session.quit:
			return sent, &net.OpError{Op: "write", Err: syscall.EPIPE}
		case m.session.writeCh <- frame:
			frame = nil
			sent += n
			if sent == total {
				break loop
			}
		case <-timeout:
			return sent, &timeoutError{}
		}
	}
	return total, nil
}

func (m *Stream) Close() error {
	m.session.clearStream(m.id)
	if m.cloze() {
		select {
		case <-m.session.quit:
		case m.session.writeCh <- &muxFrame{sID: m.id, flag: FIN}:
		}
	}
	return nil
}

func (m *Stream) cloze() bool {
	if atomic.AddUint32(&m.quitFlag, 1) > 1 {
		return false
	}
	close(m.quit)
	return true
}

func (m *Stream) SetDeadline(t time.Time) error {
	err := m.SetReadDeadline(t)
	if err != nil {
		return err
	}
	err = m.SetWriteDeadline(t)
	if err != nil {
		return err
	}
	return nil
}

func (m *Stream) SetReadDeadline(t time.Time) error {
	m.readDeadline = t
	return nil
}

func (m *Stream) SetWriteDeadline(t time.Time) error {
	m.writeDeadline = t
	return nil
}

func (m *Stream) RemoteAddr() net.Addr {
	return m.session.remoteAddr()
}

func (m *Stream) LocalAddr() net.Addr {
	return m.session.localAddr()
}

type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }
