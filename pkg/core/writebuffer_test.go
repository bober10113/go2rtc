package core

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recyclableWriter emulates a net/http response writer: after the handler
// returns (i.e. WriteTo unblocks), net/http puts the response bufio.Writer
// back into a pool. Any Write after that moment reproduces the production
// panics (nil deref / slice bounds out of range inside bufio).
type recyclableWriter struct {
	first    sync.Once
	ready    chan struct{}
	recycled atomic.Bool
	violated atomic.Bool
}

func TestWriteBufferClosedBeforeAttach(t *testing.T) {
	wb := NewWriteBuffer(nil)
	_, _ = wb.Write([]byte("init"))
	_ = wb.Close()
	var dst bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, err := wb.WriteTo(&dst)
		done <- err
	}()
	select {
	case err := <-done:
		if err != io.ErrClosedPipe {
			t.Fatalf("got %v, want closed pipe", err)
		}
		if dst.Len() != 0 {
			t.Fatal("closed buffer flushed stale data to a new consumer")
		}
	case <-time.After(time.Second):
		_ = wb.Close() // release the old implementation's waiter before failing
		<-done
		t.Fatal("closed buffer blocked a late consumer")
	}
}

func (w *recyclableWriter) Write(p []byte) (int, error) {
	w.first.Do(func() { close(w.ready) })
	if w.recycled.Load() {
		w.violated.Store(true)
	}
	return len(p), nil
}

func TestWriteBufferCloseBlocksLateWrites(t *testing.T) {
	wb := NewWriteBuffer(nil)
	rw := &recyclableWriter{ready: make(chan struct{})}

	// init segment is buffered before WriteTo, Reset will copy it into rw
	_, _ = wb.Write([]byte("init"))

	handlerDone := make(chan struct{})
	go func() {
		_, _ = wb.WriteTo(rw) // handlerMP4: blocks until Close
		rw.recycled.Store(true)
		close(handlerDone)
	}()
	<-rw.ready // WriteTo has been called, Writer is switched to rw

	// track sender goroutines keep pushing frames
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100000; j++ {
				if _, err := wb.Write([]byte("frame")); err != nil {
					return
				}
			}
		}()
	}

	_ = wb.Close() // client disconnected -> cons.Stop() -> Transport.Close()
	<-handlerDone
	wg.Wait()

	if rw.violated.Load() {
		t.Fatal("Write after handler return: production panic scenario reproduced")
	}
	if _, err := wb.Write([]byte("late")); err == nil {
		t.Fatal("Write after Close must return error")
	}
}
