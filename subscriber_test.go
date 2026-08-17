//go:build !plan9

package firebirdsql

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuxiliaryAddressUsesMainConnectionHost(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr net.Addr
		want       string
	}{
		{
			name:       "IPv4",
			remoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3052},
			want:       "127.0.0.1:3053",
		},
		{
			name:       "IPv6",
			remoteAddr: &net.TCPAddr{IP: net.ParseIP("::1"), Port: 3052},
			want:       "[::1]:3053",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			address, err := auxiliaryAddress(test.remoteAddr, 3053)

			require.NoError(t, err)
			require.Equal(t, test.want, address)
		})
	}
}

func TestAuxiliaryAddressRejectsMissingMainConnectionAddress(t *testing.T) {
	_, err := auxiliaryAddress(nil, 3053)

	require.Error(t, err)
}
