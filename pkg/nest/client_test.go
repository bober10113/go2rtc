package nest

import (
	"errors"
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/webrtc"
	pion "github.com/pion/webrtc/v4"
)

func testRTCPeer(t *testing.T) (*webrtc.Conn, *pion.PeerConnection) {
	t.Helper()
	api, err := webrtc.NewAPI()
	if err != nil {
		t.Fatal(err)
	}
	pc, err := api.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	conn := webrtc.NewConn(pc)
	conn.Mode = core.ModeActiveProducer
	t.Cleanup(func() { _ = conn.Close() })
	return conn, pc
}

func TestNegotiateRTCClosesEveryFailedAttempt(t *testing.T) {
	for _, stage := range []string{"offer", "exchange", "answer"} {
		t.Run(stage, func(t *testing.T) {
			conn, pc := testRTCPeer(t)
			if stage == "offer" {
				_ = conn.Close()
			}
			exchangeCalled := false
			exchangeFailed, err := negotiateRTC(conn, func(string) (string, error) {
				exchangeCalled = true
				if stage == "exchange" {
					return "", errors.New("test API refusal")
				}
				return "invalid SDP", nil
			})
			if err == nil || exchangeFailed != (stage == "exchange") {
				t.Fatalf("exchangeFailed=%t err=%v", exchangeFailed, err)
			}
			if exchangeCalled != (stage != "offer") {
				t.Fatal("unexpected exchange call")
			}
			if got := pc.ConnectionState(); got != pion.PeerConnectionStateClosed {
				t.Fatalf("failed peer remains %s", got)
			}
		})
	}
}

func TestNegotiateRTCKeepsSuccessfulPeer(t *testing.T) {
	conn, pc := testRTCPeer(t)
	_, remote := testRTCPeer(t)
	exchangeFailed, err := negotiateRTC(conn, func(offer string) (string, error) {
		if err := remote.SetRemoteDescription(pion.SessionDescription{Type: pion.SDPTypeOffer, SDP: offer}); err != nil {
			return "", err
		}
		answer, err := remote.CreateAnswer(nil)
		if err != nil {
			return "", err
		}
		gathered := pion.GatheringCompletePromise(remote)
		if err = remote.SetLocalDescription(answer); err != nil {
			return "", err
		}
		<-gathered
		return remote.LocalDescription().SDP, nil
	})
	if err != nil || exchangeFailed {
		t.Fatalf("exchangeFailed=%t err=%v", exchangeFailed, err)
	}
	if pc.ConnectionState() == pion.PeerConnectionStateClosed {
		t.Fatal("successful peer was closed")
	}
	if len(conn.GetMedias()) == 0 {
		t.Fatal("successful negotiation has no media descriptions")
	}
}
