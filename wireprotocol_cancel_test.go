package firebirdsql

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
)

func TestWireProtocolSerializesCancelAndDetach(t *testing.T) {
	client, server := net.Pipe()
	channel, err := newWireChannel(client)
	if err != nil {
		t.Fatal(err)
	}
	protocol := &wireProtocol{
		buf:  make([]byte, 0, BUFFER_LEN),
		conn: channel,
	}

	const cancelCount = 20
	packets := make(chan []byte, 1)
	readErrors := make(chan error, 1)
	go func() {
		buf := make([]byte, (cancelCount+1)*8)
		if _, err := io.ReadFull(server, buf); err != nil {
			readErrors <- err
			return
		}
		packets <- buf
	}()

	var operations sync.WaitGroup
	for range cancelCount {
		operations.Add(1)
		go func() {
			defer operations.Done()
			_ = protocol.opCancel(fb_cancel_raise)
		}()
	}
	operations.Add(1)
	go func() {
		defer operations.Done()
		_ = protocol.opDetach()
	}()

	operations.Wait()
	select {
	case err := <-readErrors:
		t.Fatal(err)
	case buf := <-packets:
		cancelPackets := 0
		detachPackets := 0
		for offset := 0; offset < len(buf); offset += 8 {
			operation := int32(binary.BigEndian.Uint32(buf[offset : offset+4]))
			argument := int32(binary.BigEndian.Uint32(buf[offset+4 : offset+8]))
			switch operation {
			case op_cancel:
				cancelPackets++
				if argument != fb_cancel_raise {
					t.Fatalf("cancel argument is %d, want %d", argument, fb_cancel_raise)
				}
			case op_detach:
				detachPackets++
				if argument != protocol.dbHandle {
					t.Fatalf("detach handle is %d, want %d", argument, protocol.dbHandle)
				}
			default:
				t.Fatalf("unexpected operation %d", operation)
			}
		}
		if cancelPackets != cancelCount {
			t.Fatalf("received %d cancel packets, want %d", cancelPackets, cancelCount)
		}
		if detachPackets != 1 {
			t.Fatalf("received %d detach packets, want 1", detachPackets)
		}
	}
	_ = protocol.conn.Close()
	_ = server.Close()
}
