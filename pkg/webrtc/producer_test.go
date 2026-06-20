package webrtc

import (
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

func TestGetReceiverTrackReusesCompatibleCodec(t *testing.T) {
	media := &core.Media{
		Kind:      core.KindVideo,
		Direction: core.DirectionRecvonly,
		ID:        "video",
		Codecs: []*core.Codec{{
			Name:        core.CodecH264,
			ClockRate:   90000,
			PayloadType: 96,
		}},
	}

	oldCodec := &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 96}
	newCodec := &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 102}
	receiver := core.NewReceiver(media, oldCodec)

	conn := &Conn{}
	conn.Receivers = []*core.Receiver{receiver}

	if got := conn.getReceiverTrack(media, newCodec); got != receiver {
		t.Fatalf("compatible receiver was not reused")
	}
}

func TestGetReceiverTrackPrefersActiveCompatibleReceiver(t *testing.T) {
	media := &core.Media{
		Kind:      core.KindVideo,
		Direction: core.DirectionRecvonly,
		ID:        "video",
	}
	codec := &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 96}
	stale := core.NewReceiver(media, &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 97})
	active := core.NewReceiver(media, &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 98})
	active.Packets = 12

	conn := &Conn{}
	conn.Receivers = []*core.Receiver{stale, active}

	if got := conn.getReceiverTrack(media, codec); got != active {
		t.Fatalf("active compatible receiver was not preferred")
	}
}
