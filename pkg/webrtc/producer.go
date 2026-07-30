package webrtc

import (
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/webrtc/v4"
)

func (c *Conn) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	core.Assert(media.Direction == core.DirectionRecvonly)

	if track := c.getReceiverTrack(media, codec); track != nil {
		return track, nil
	}

	switch c.Mode {
	case core.ModePassiveConsumer: // backchannel from browser
		// set codec for consumer recv track so remote peer should send media with this codec
		params := webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  MimeType(codec),
				ClockRate: codec.ClockRate,
				Channels:  uint16(codec.Channels),
			},
			PayloadType: 0, // don't know if this necessary
		}

		tr := c.getTranseiver(media.ID)

		_ = tr.SetCodecPreferences([]webrtc.RTPCodecParameters{params})

	case core.ModePassiveProducer, core.ModeActiveProducer:
		// Passive producers: OBS Studio via WHIP or Browser
		// Active producers: go2rtc as WebRTC client or WebTorrent

	default:
		panic(core.Caller())
	}

	track := core.NewReceiver(media, codec)
	c.Receivers = append(c.Receivers, track)
	return track, nil
}

func (c *Conn) getReceiverTrack(media *core.Media, codec *core.Codec) *core.Receiver {
	var compatible *core.Receiver

	for _, track := range c.Receivers {
		if track.Codec == codec {
			return track
		}

		if track.Codec == nil || !track.Codec.Match(codec) {
			continue
		}

		if track.Media == nil || !track.Media.Equal(media) {
			continue
		}

		// After a Nest/WebRTC reconnect, the remote track can carry the same
		// H264 media through a new codec pointer. Prefer the receiver that is
		// already getting packets so RTSP/ffmpeg consumers don't stay attached
		// to a stale, headerless receiver.
		if track.Packets > 0 {
			return track
		}

		if compatible == nil {
			compatible = track
		}
	}

	return compatible
}

func (c *Conn) Start() error {
	c.closed.Wait()
	return nil
}
