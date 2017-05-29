package utils

import (
	"io"
	"log"
	"net"
	"sync"
	"syscall"
	"time"
)

var (
	lock sync.Mutex

	pipeListener *net.TCPListener
	pipeCnt      int
	pipeCh       chan *net.TCPConn
)

type pipeConn struct {
	id     int
	closed bool
	net.Conn
}

func (pc *pipeConn) Close() error {
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
	log.Printf("SocketPipe %d Close: pipe socket connections: %d", pc.id, pipeCnt)
	pc.closed = true
	return pc.Conn.Close()
}

func servePipe(l *net.TCPListener) {
	go func() {
		log.Printf("pipe listener starts")
		for {
			c, e := l.AcceptTCP()
			if e != nil {
				break
			}
			pipeCh <- c
		}
		log.Printf("pipe listener ends")
	}()
}

func SocketPipe() (net.Conn, net.Conn) {
	lock.Lock()
	defer lock.Unlock()

	var e error
	if pipeCnt == 0 {
		pipeListener, e = net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
		if e != nil {
			return nil, nil
		}
		pipeCh = make(chan *net.TCPConn)
		servePipe(pipeListener)
	}
	c, e := net.DialTCP("tcp", nil, pipeListener.Addr().(*net.TCPAddr))
	if e != nil {
		return nil, nil
	}
	s := <-pipeCh
	pc := &pipeConn{closed: false, id: pipeCnt, Conn: c}
	ps := &pipeConn{closed: false, id: pipeCnt + 1, Conn: s}
	pipeCnt += 2
	log.Printf("SocketPipe Create: pipe socket connections: %d", pipeCnt)
	return pc, ps
}

type BytePipe struct {
	id      string
	limit   int
	size    int
	iCh     chan []byte
	oCh     chan []byte
	rest    []byte
	pending [][]byte
	quit    chan bool
}

func NewBytePipe(id string, limit int) *BytePipe {
	return &BytePipe{
		id:    id,
		limit: limit,
		iCh:   make(chan []byte),
		oCh:   make(chan []byte),
		quit:  make(chan bool),
	}
}

func (bp *BytePipe) Start() {
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
			log.Printf("%s: buffer too large: %d bytes", bp.id, bp.size)
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

func (bp *BytePipe) Stop() {
	close(bp.quit)
}

func (bp *BytePipe) In(buf []byte, deadline time.Time) error {
	var timeout <-chan time.Time
	if !deadline.IsZero() {
		t := time.NewTimer(deadline.Sub(time.Now()))
		defer t.Stop()
		timeout = t.C
	}

	select {
	case <-bp.quit:
		return &net.OpError{Op: "write", Err: syscall.EPIPE}
	case <-timeout:
		return &TimeoutError{}
	case bp.iCh <- buf:
		return nil
	}
}

func (bp *BytePipe) Out(deadline time.Time) ([]byte, error) {
	// first return the "rest" chunk if any
	if len(bp.rest) > 0 {
		b := bp.rest
		bp.rest = nil
		return b, nil
	}

	var timeout <-chan time.Time
	if !deadline.IsZero() {
		t := time.NewTimer(deadline.Sub(time.Now()))
		defer t.Stop()
		timeout = t.C
	}
	select {
	case b, ok := <-bp.oCh:
		if !ok {
			if len(bp.pending) > 0 {
				b = bp.pending[0]
				bp.pending = bp.pending[1:]
			} else {
				return nil, io.EOF
			}
		}
		return b, nil
	case <-timeout:
		return nil, &TimeoutError{}
	}
}

func (bp *BytePipe) OutToBuf(buf []byte, turnaround time.Duration, deadline time.Time) (int, error) {
	var overall <-chan time.Time
	var turn <-chan time.Time
	if !deadline.IsZero() {
		t1 := time.NewTimer(deadline.Sub(time.Now()))
		defer t1.Stop()
		overall = t1.C
	}
	max := len(buf)
	cnt := 0
	pos := 0
	// first look at the "rest" chunk if any
	if len(bp.rest) > 0 {
		n := copy(buf[pos:], bp.rest)
		bp.rest = bp.rest[n:]
		pos += n
		cnt += n
		if cnt >= max {
			return cnt, nil
		}
		// setup turn timer after getting first chunk
		t2 := time.NewTimer(turnaround)
		defer t2.Stop()
		turn = t2.C
	}

	for {
		select {
		case b, ok := <-bp.oCh:
			if !ok {
				if len(bp.pending) > 0 {
					b = bp.pending[0]
					bp.pending = bp.pending[1:]
				} else {
					return cnt, io.EOF
				}
			}
			n := copy(buf[pos:], b)
			bp.rest = b[n:]
			pos += n
			cnt += n
			if cnt >= max {
				return cnt, nil
			}
			// setup turn timer after getting first chunk
			if turn == nil {
				t2 := time.NewTimer(turnaround)
				defer t2.Stop()
				turn = t2.C
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

type TimeoutError struct{}

func (e *TimeoutError) Error() string   { return "i/o timeout" }
func (e *TimeoutError) Timeout() bool   { return true }
func (e *TimeoutError) Temporary() bool { return true }
