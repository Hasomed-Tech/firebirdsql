package firebirdsql

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestWatchContextStopsCompletedOperation(t *testing.T) {
	ctx, cancelContext := context.WithCancel(context.Background())
	var cancelCalls atomic.Int32

	stop := watchContext(ctx, func() {
		cancelCalls.Add(1)
	})
	stop()
	cancelContext()

	if got := cancelCalls.Load(); got != 0 {
		t.Fatalf("cancel called %d times after the operation completed", got)
	}
}

func TestWatchContextCancelsActiveOperation(t *testing.T) {
	ctx, cancelContext := context.WithCancel(context.Background())
	cancelCalled := make(chan struct{})

	stop := watchContext(ctx, func() {
		close(cancelCalled)
	})
	cancelContext()

	select {
	case <-cancelCalled:
	case <-time.After(time.Second):
		t.Fatal("cancel was not called")
	}
	stop()
}

func TestWatchContextWaitsForCancel(t *testing.T) {
	ctx, cancelContext := context.WithCancel(context.Background())
	cancelStarted := make(chan struct{})
	releaseCancel := make(chan struct{})

	stop := watchContext(ctx, func() {
		close(cancelStarted)
		<-releaseCancel
	})
	cancelContext()
	<-cancelStarted

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("operation stopped before cancel completed")
	case <-time.After(10 * time.Millisecond):
	}

	close(releaseCancel)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("operation did not stop after cancel completed")
	}
}
