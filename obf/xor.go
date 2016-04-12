package obf

import (
	"io"
	"net"
	"time"
)

const (
	XorMaskLength = 4
)

type XorObfConn struct {
	conn           net.Conn
	writeMask      [XorMaskLength]byte
	writeMaskBytes int
	writeMaskIdx   int
	writeMaskSent  bool

	readMask         [XorMaskLength]byte
	readMaskBytes    int
	readMaskIdx      int
	readMaskReceived bool
}

func (x *XorObfConn) recvReadMask() error {
	n, err := io.ReadFull(x.conn, x.readMask[x.readMaskBytes:])
	if n > 0 {
		x.readMaskBytes += n
		if x.readMaskBytes >= XorMaskLength {
			x.readMaskReceived = true
		}
	}
	return err
}

func (x *XorObfConn) sendWriteMask() error {
	n, err := x.conn.Write(x.writeMask[x.writeMaskBytes:])
	if n > 0 {
		x.writeMaskBytes += n
		if x.writeMaskBytes >= XorMaskLength {
			x.writeMaskSent = true
		}
	}
	return err
}

func (x *XorObfConn) deObf(b []byte, n int) {
	for i := 0; i < n; i++ {
		b[i] ^= x.readMask[x.readMaskIdx]
		x.readMaskIdx = (x.readMaskIdx + 1) % XorMaskLength
	}
}

func (x *XorObfConn) obf(b []byte) {
	for i := range b {
		b[i] = b[i] ^ x.writeMask[x.writeMaskIdx]
		x.writeMaskIdx = (x.writeMaskIdx + 1) % XorMaskLength
	}
}

func (x *XorObfConn) Read(b []byte) (n int, err error) {
	if !x.readMaskReceived {
		err = x.recvReadMask()
		if err != nil {
			return
		}
	}
	n, err = x.conn.Read(b)
	if n > 0 {
		x.deObf(b, n)
	}
	return
}

func (x *XorObfConn) Write(b []byte) (int, error) {
	if !x.writeMaskSent {
		err := x.sendWriteMask()
		if err != nil {
			return 0, err
		}
	}

	buf := make([]byte, len(b), cap(b))
	copy(buf, b)
	x.obf(buf)
	return x.conn.Write(buf)
}

func (x *XorObfConn) Close() error {
	return x.conn.Close()
}

func (x *XorObfConn) LocalAddr() net.Addr {
	return x.conn.LocalAddr()
}

func (x *XorObfConn) RemoteAddr() net.Addr {
	return x.conn.RemoteAddr()
}

func (x *XorObfConn) SetDeadline(t time.Time) error {
	return x.conn.SetDeadline(t)
}

func (x *XorObfConn) SetReadDeadline(t time.Time) error {
	return x.conn.SetReadDeadline(t)
}

func (x *XorObfConn) SetWriteDeadline(t time.Time) error {
	return x.conn.SetWriteDeadline(t)
}

// the random mask is used to obfuscate data writing to
// the peer by xoring with raw data.
func NewXorObfConn(conn net.Conn, mask [XorMaskLength]byte) net.Conn {
	return &XorObfConn{conn: conn, writeMask: mask}
}
