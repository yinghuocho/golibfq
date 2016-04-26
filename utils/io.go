package utils

import (
	"io"
	"log"
	"net"
	"syscall"
	"time"
)

type BytePipe struct {
	id      string
	limit   int
	size    int
	iCh     chan []byte
	oCh     chan []byte
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

type TimeoutError struct{}

func (e *TimeoutError) Error() string   { return "i/o timeout" }
func (e *TimeoutError) Timeout() bool   { return true }
func (e *TimeoutError) Temporary() bool { return true }
