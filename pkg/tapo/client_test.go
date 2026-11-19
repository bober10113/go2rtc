package tapo

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type readSignalingConn struct {
	net.Conn
	reading chan struct{}
	once    sync.Once
}

func (c *readSignalingConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.reading) })
	return c.Conn.Read(p)
}

// https://github.com/AlexxIT/go2rtc/issues/2690
func TestGetTrackAfterStart(t *testing.T) {
	conn, camera := net.Pipe()
	t.Cleanup(func() { _ = conn.Close() })
	t.Cleanup(func() { _ = camera.Close() })
	go func() { _, _ = io.Copy(io.Discard, camera) }() // reads requests, never answers

	// producer started for the backchannel only: Handle owns conn1, session1 is empty
	reader := &readSignalingConn{Conn: conn, reading: make(chan struct{})}
	client := &Client{conn1: reader}
	stopped := make(chan struct{})
	go func() { _ = client.Start(); close(stopped) }()
	t.Cleanup(func() {
		_ = conn.Close()
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Error("Tapo reader did not stop")
		}
	})
	select {
	case <-reader.reading:
	case <-time.After(time.Second):
		t.Fatal("Tapo reader did not start")
	}

	media := client.GetMedias()[0]
	done := make(chan error, 1)
	go func() {
		_, err := client.GetTrack(media, media.Codecs[0])
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("GetTrack blocked on conn1 while Handle is reading it")
	}
}

func TestSetupStreamKeepsExistingSessionAfterStart(t *testing.T) {
	client := &Client{started: true, session1: "existing-session"}
	require.NoError(t, client.SetupStream())
	require.Equal(t, "existing-session", client.session1)
}
