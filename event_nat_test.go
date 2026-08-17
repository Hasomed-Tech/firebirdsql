package firebirdsql

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const firebirdNATTestDSN = "FIREBIRD_NAT_TEST_DSN"

func TestEventsThroughNAT(t *testing.T) {
	dsn := os.Getenv(firebirdNATTestDSN)
	if dsn == "" {
		t.Skipf("set %s to run the NAT integration test", firebirdNATTestDSN)
	}

	fbEvent, err := NewFBEvent(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fbEvent.Close() })

	eventName := "nat_event_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	events := make(chan Event, 1)
	subscription, err := fbEvent.SubscribeChan([]string{eventName}, events)
	require.NoError(t, err)
	t.Cleanup(func() { _ = subscription.Unsubscribe() })

	require.NoError(t, fbEvent.PostEvent(eventName))

	select {
	case event := <-events:
		require.Equal(t, eventName, event.Name)
		require.Equal(t, 1, event.Count)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for an event through the NAT auxiliary connection")
	}
}
