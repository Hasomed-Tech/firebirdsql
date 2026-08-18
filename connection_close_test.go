package firebirdsql

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

type closeTrackingConn struct {
	net.Conn
	closed bool
}

func (c *closeTrackingConn) Close() error {
	c.closed = true
	return c.Conn.Close()
}

func TestFirebirdsqlConnCloseClosesSocketWhenDetachFails(t *testing.T) {
	client, server := net.Pipe()
	require.NoError(t, server.Close())

	conn := &closeTrackingConn{Conn: client}
	channel, err := newWireChannel(conn)
	require.NoError(t, err)

	fc := &firebirdsqlConn{
		wp: &wireProtocol{
			conn: channel,
		},
		transactionSet: make(map[*firebirdsqlTx]struct{}),
	}

	err = fc.Close()

	require.Error(t, err)
	require.True(t, conn.closed)
}
