package mux

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yinghuocho/golibfq/utils"
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

	wlock sync.Mutex
	conn  net.Conn
	quit  chan bool

	// mapLock protects operations on streams-map by Session.loop or someone
	// invokes Stream.Close, or Client.OpenStream
	//
	// Closed session has a nil map to prevent new streams by Client.OpenStream
	streams map[uint32]*Stream
	mapLock sync.Mutex

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

	pc net.Conn
	ps net.Conn
	// up    *utils.BytePipe
	// upCur []byte

	// readDeadline  time.Time
	writeDeadline time.Time
	closed        bool

	// quit     chan bool
	// quitFlag uint32

	session *Session
}

func NewClient(c net.Conn) *Client {
	s := &Session{
		isClient: true,
		conn:     c,
		streams:  make(map[uint32]*Stream),
		quit:     make(chan bool),
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

func (s *Session) readFrame() (*muxFrame, error) {
	var buf [7]byte
	_, err := io.ReadFull(s.conn, buf[:])
	if err != nil {
		return nil, err
	}
	sID := binary.BigEndian.Uint32(buf[0:4])
	flag := buf[4]
	dataLen := binary.BigEndian.Uint16(buf[5:7])
	if dataLen > 0 {
		data := make([]byte, dataLen)
		_, err := io.ReadFull(s.conn, data[0:dataLen])
		if err != nil {
			return nil, err
		}
		return &muxFrame{sID, flag, dataLen, data}, nil
	} else {
		return &muxFrame{sID, flag, dataLen, nil}, nil
	}
}

func (s *Session) writeFrame(frame *muxFrame, deadline time.Time) error {
	s.wlock.Lock()
	defer s.wlock.Unlock()

	var buf [65535 + 8]byte
	binary.BigEndian.PutUint32(buf[0:], frame.sID)
	buf[4] = frame.flag
	binary.BigEndian.PutUint16(buf[5:], frame.dataLen)
	if frame.dataLen > 0 {
		copy(buf[7:], frame.data[0:frame.dataLen])
	}
	s.conn.SetWriteDeadline(deadline)
	_, err := s.conn.Write(buf[0 : uint32(frame.dataLen)+7])
	// log.Printf("frame send: %d %d %d: error(%v)", frame.sID, frame.flag, frame.dataLen, err)
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
	pc, ps := utils.SocketPipe()
	stream := &Stream{
		id: sID,
		// up:      utils.NewBytePipe(fmt.Sprintf("%d-UP", sID), 0),
		// quit:    make(chan bool),
		pc:            pc,
		ps:            ps,
		writeDeadline: time.Time{},
		closed:        false,
		session:       s,
	}

	s.streams[sID] = stream
	// go stream.up.Start()
	return stream, nil
}

func (s *Session) clearStream(sID uint32) {
	s.mapLock.Lock()
	defer s.mapLock.Unlock()

	delete(s.streams, sID)
}

func (s *Session) loop() {
	var pendingStream []*Stream

	readCh := make(chan *muxFrame)
	// reader
	go func() {
		for {
			f, err := s.readFrame()
			if err != nil {
				break
			}
			readCh <- f
		}
		s.cloze()
		close(s.quit)
		return
	}()

loop:
	for {
		var streamCh chan *Stream
		var curStream *Stream
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
		case f := <-readCh:
			stream := s.getStream(f.sID)
			// SYN
			// log.Printf("frame recv: %d %d %d", f.sID, f.flag, f.dataLen)
			if (stream == nil) && (f.flag&SYN != 0) {
				stream, _ = s.newStream(f.sID)
				if !s.isClient {
					pendingStream = append(pendingStream, stream)
				}
			}

			// DATA
			if (f.flag&DATA != 0) && (f.dataLen > 0) {
				if stream != nil {
					// stream.up.In(f.data, time.Time{})
					stream.ps.SetWriteDeadline(time.Time{})
					_, e := stream.ps.Write(f.data)
					if e != nil {
						stream.cloze()
					}
				} else {
					// unsolicited frame, respond with RST
					rst := &muxFrame{f.sID, RST, 0, nil}
					s.writeFrame(rst, time.Time{})
				}
			}

			// FIN or RST
			if (f.flag&FIN != 0) || (f.flag&RST != 0) {
				if stream != nil {
					s.clearStream(f.sID)
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

func (m *Stream) Read(buf []byte) (n int, err error) {
	// if len(m.upCur) == 0 {
	//	m.upCur, err = m.up.Out(m.readDeadline)
	//	if err != nil {
	//		return
	//	}
	// }
	// n = copy(buf, m.upCur)
	// m.upCur = m.upCur[n:]
	// return
	return m.pc.Read(buf)
}

func (m *Stream) Write(b []byte) (int, error) {
	total := len(b)
	sent := 0
	for {
		n := total - sent
		if n == 0 {
			break
		}
		if n > maxDataLen {
			n = maxDataLen
		}
		f := &muxFrame{m.id, DATA, uint16(n), b[sent : sent+n]}
		if !m.synSent && m.session.isClient {
			f.flag |= SYN
			m.synSent = true
		}
		err := m.session.writeFrame(f, m.writeDeadline)
		if err != nil {
			return sent, err
		}
		sent += n
	}
	return total, nil

	// var timeout <-chan time.Time
	// if !m.writeDeadline.IsZero() {
	//	t := time.NewTimer(m.writeDeadline.Sub(time.Now()))
	//	defer t.Stop()
	//	timeout = t.C
	// }

	// sent := 0
	// total := len(b)
	// loop:
	// for {
	//	select {
	//	case <-m.session.quit:
	//		return sent, &net.OpError{Op: "write", Err: syscall.EPIPE}
	//	case <-m.quit:
	//		return sent, &net.OpError{Op: "write", Err: syscall.EPIPE}
	//	case <-timeout:
	//		return sent, &utils.TimeoutError{}
	//	default:
	//		n := total - sent
	//		if n <= 0 {
	//			break loop
	//		}
	//		if n > maxDataLen {
	//			n = maxDataLen
	//		}
	//		f := newFrame(m.id, DATA, uint16(n), b[sent:sent+n])
	//		if !m.synSent && m.session.isClient {
	//			f.flag |= SYN
	//			m.synSent = true
	//		}
	//		err := m.session.writeFrame(f)
	//		if err != nil {
	//			return sent, err
	//		}
	//		releaseFrame(f)
	//		sent += n
	//	}
	// }
	// return total, nil
}

func (m *Stream) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	fin := &muxFrame{m.id, FIN, 0, nil}
	m.session.writeFrame(fin, m.writeDeadline)
	m.session.clearStream(m.id)
	m.pc.Close()
	return m.ps.Close()

	// m.session.clearStream(m.id)
	// if m.cloze() {
	//	fin := newFrame(m.id, FIN, 0, nil)
	//	select {
	//	case <-m.session.quit:
	//	default:
	//		m.session.writeFrame(fin)
	//	}
	//	releaseFrame(fin)
	// }
	// return nil
	// return m.pc.Close()
}

func (m *Stream) cloze() {
	// if atomic.AddUint32(&m.quitFlag, 1) > 1 {
	//	return false
	// }
	// close(m.quit)
	// m.up.Stop()

	// return true
	m.ps.Close()
}

func (m *Stream) SetDeadline(t time.Time) error {
	err := m.pc.SetReadDeadline(t)
	if err != nil {
		return err
	}
	err = m.pc.SetWriteDeadline(t)
	if err != nil {
		return err
	}
	return nil
}

func (m *Stream) SetReadDeadline(t time.Time) error {
	return m.pc.SetReadDeadline(t)
}

func (m *Stream) SetWriteDeadline(t time.Time) error {
	m.writeDeadline = t
	return nil
	// return m.pc.SetWriteDeadline(t)
}

func (m *Stream) RemoteAddr() net.Addr {
	return m.session.remoteAddr()
}

func (m *Stream) LocalAddr() net.Addr {
	return m.session.localAddr()
}
