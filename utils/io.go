package utils

import (
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	lock sync.Mutex

	pipeListener *net.TCPListener
	pipeCnt      int
	pipeCh       chan *net.TCPConn
)

type socketPipeConn struct {
	id     int
	closed bool
	net.Conn
}

func (pc *socketPipeConn) Close() error {
	lock.Lock()
	defer lock.Unlock()
	if pc.closed {
		return nil
	}

	pipeCnt -= 1
	if pipeCnt == 0 {
		pipeListener.Close()
		close(pipeCh)
		pipeListener = nil
	}
	log.Printf("SocketPipe: id %d closed: rest connections: %d", pc.id, pipeCnt)
	pc.closed = true
	return pc.Conn.Close()
}

func servePipe(l *net.TCPListener) {
	go func() {
		log.Printf("SocketPipe: listener starts")
		for {
			c, e := l.AcceptTCP()
			if e != nil {
				log.Printf("SocketPipe: AcceptTCP fail: %s", e)
				if ne, ok := e.(net.Error); ok && ne.Temporary() {
					pipeCh <- nil
					continue
				} else {
					// this should only happend when listener is closed by others.
					break
				}
			} else {
				log.Printf("SocketPipe: AcceptTCP success")
				pipeCh <- c
			}
		}
		log.Printf("SocketPipe: listener ends")
	}()
}

func CreatePipe() (net.Conn, net.Conn) {
	// c, s := SocketPipe()
	// if c != nil && s != nil {
	//	return c, s
	// } else {
	//	log.Printf("fail to create SocketPipe, use BytePipe instead")
	// this is incorrect !!!
	return NewBytePipe(128 * 1024)
	// }
}

func SocketPipe() (net.Conn, net.Conn) {
	lock.Lock()
	defer lock.Unlock()

	var e error
	if pipeCnt == 0 {
		pipeListener, e = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
		if e != nil {
			log.Printf("SocketPipe: ListenTCP fail: %s", e)
			return nil, nil
		}
		pipeCh = make(chan *net.TCPConn)
		servePipe(pipeListener)
	}
	c, e := net.DialTCP("tcp", nil, pipeListener.Addr().(*net.TCPAddr))
	if e != nil {
		log.Printf("SocketPipe: DialTCP fail: %s", e)
		return nil, nil
	}
	log.Printf("SocketPipe: DialTCP success")
	s := <-pipeCh
	if s == nil {
		log.Printf("SocketPipe: server side error")
		c.Close()
		return nil, nil
	}
	pc := &socketPipeConn{closed: false, id: pipeCnt, Conn: c}
	ps := &socketPipeConn{closed: false, id: pipeCnt + 1, Conn: s}
	pipeCnt += 2
	log.Printf("SocketPipe: pipe socket connections: %d", pipeCnt)
	return pc, ps
}

type bytePipeBuf struct {
	limit    int
	size     int
	iCh      chan []byte
	oCh      chan []byte
	rest     []byte
	pending  [][]byte
	quit     chan bool
	quitFlag uint32

	readDeadline  time.Time
	writeDeadline time.Time
}

func NewBytePipe(bufsize int) (net.Conn, net.Conn) {
	ctos := &bytePipeBuf{
		limit: bufsize,
		iCh:   make(chan []byte),
		oCh:   make(chan []byte),
		quit:  make(chan bool),
	}
	go ctos.start()
	stoc := &bytePipeBuf{
		limit: bufsize,
		iCh:   make(chan []byte),
		oCh:   make(chan []byte),
		quit:  make(chan bool),
	}
	go stoc.start()
	return &bytePipeConn{rBuf: stoc, wBuf: ctos}, &bytePipeConn{rBuf: ctos, wBuf: stoc}
}

func (bp *bytePipeBuf) start() {
loop:
	for {
		var out chan []byte
		var in chan []byte
		var b []byte

		if len(bp.pending) > 0 {
			out = bp.oCh
			b = bp.pending[0]
		}
		if bp.limit <= 0 || bp.size < bp.limit {
			in = bp.iCh
		} else {
			log.Printf("buffer too large: %d bytes", bp.size)
		}

		select {
		case <-bp.quit:
			break loop
		case data := <-in:
			bp.size += len(data)
			bp.pending = append(bp.pending, data)
		case out <- b:
			bp.size -= len(b)
			bp.pending = bp.pending[1:]
		}
	}
	close(bp.oCh)
}

func (bp *bytePipeBuf) stop() {
	close(bp.quit)
}

func (bp *bytePipeBuf) write(b []byte) (int, error) {
	var timeout <-chan time.Time
	if !bp.writeDeadline.IsZero() {
		t := time.NewTimer(bp.writeDeadline.Sub(time.Now()))
		defer t.Stop()
		timeout = t.C
	}

	n := len(b)
	buf := make([]byte, n)
	copy(buf, b)
	select {
	case <-bp.quit:
		return 0, &net.OpError{Op: "write", Err: syscall.EPIPE}
	case <-timeout:
		return 0, &TimeoutError{}
	case bp.iCh <- buf:
		return n, nil
	}
}

func (bp *bytePipeBuf) read(b []byte) (int, error) {
	var overall <-chan time.Time
	var turn <-chan time.Time

	turnaround := 10 * time.Millisecond
	if !bp.readDeadline.IsZero() {
		t1 := time.NewTimer(bp.readDeadline.Sub(time.Now()))
		defer t1.Stop()
		overall = t1.C
	}
	max := len(b)
	cnt := 0
	pos := 0
	// first look at the "rest" chunk if any
	if len(bp.rest) > 0 {
		n := copy(b[pos:], bp.rest)
		bp.rest = bp.rest[n:]
		pos += n
		cnt += n
		if cnt >= max {
			return cnt, nil
		}
	}

	for {
		if turn == nil && cnt > 0 {
			// setup turn timer after getting first chunk
			t2 := time.NewTimer(turnaround)
			defer t2.Stop()
			turn = t2.C
		}

		select {
		case buf, ok := <-bp.oCh:
			if !ok {
				if len(bp.pending) > 0 {
					buf = bp.pending[0]
					bp.pending = bp.pending[1:]
				} else {
					return cnt, io.EOF
				}
			}
			n := copy(b[pos:], buf)
			bp.rest = buf[n:]
			pos += n
			cnt += n
			if cnt >= max {
				return cnt, nil
			}
		case <-overall:
			if cnt > 0 {
				return cnt, nil
			} else {
				return 0, &TimeoutError{}
			}
		case <-turn:
			return cnt, nil
		}
	}
	// never reach here
	return cnt, nil
}

func (bp *bytePipeBuf) cloze() error {
	if atomic.AddUint32(&bp.quitFlag, 1) == 1 {
		bp.stop()
	}
	return nil
}

type bytePipeConn struct {
	rBuf *bytePipeBuf
	wBuf *bytePipeBuf
}

func (bc *bytePipeConn) Read(b []byte) (int, error) {
	return bc.rBuf.read(b)
}

func (bc *bytePipeConn) Write(b []byte) (int, error) {
	return bc.wBuf.write(b)
}

func (bc *bytePipeConn) Close() error {
	bc.rBuf.cloze()
	bc.wBuf.cloze()
	return nil
}

func (bc *bytePipeConn) LocalAddr() net.Addr {
	return nil
}

func (bc *bytePipeConn) RemoteAddr() net.Addr {
	return nil
}

func (bc *bytePipeConn) SetReadDeadline(t time.Time) error {
	bc.rBuf.readDeadline = t
	return nil
}

func (bc *bytePipeConn) SetDeadline(t time.Time) error {
	bc.rBuf.readDeadline = t
	bc.wBuf.writeDeadline = t
	return nil
}

func (bc *bytePipeConn) SetWriteDeadline(t time.Time) error {
	bc.wBuf.writeDeadline = t
	return nil
}

type TimeoutError struct{}

func (e *TimeoutError) Error() string   { return "i/o timeout" }
func (e *TimeoutError) Timeout() bool   { return true }
func (e *TimeoutError) Temporary() bool { return true }
