package streams

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type state byte

const (
	stateNone state = iota
	stateMedias
	stateTracks
	stateStart
	stateExternal
	stateInternal
)

type Producer struct {
	core.Listener

	url      string
	template string

	conn      core.Producer
	receivers []*core.Receiver
	senders   []*core.Receiver

	state     state
	mu        sync.Mutex
	workerID  int
	lastReset time.Time
}

const SourceTemplate = "{input}"

const (
	producerResetMinInterval = 20 * time.Second
	nestReconnectMinBackoff  = 5 * time.Second
	execNestResetBackoff     = 15 * time.Second
)

func NewProducer(source string) *Producer {
	if strings.Contains(source, SourceTemplate) {
		return &Producer{template: source}
	}

	return &Producer{url: source}
}

func (p *Producer) SetSource(s string) {
	if p.template == "" {
		p.url = s
	} else {
		p.url = strings.Replace(p.template, SourceTemplate, s, 1)
	}
}

func (p *Producer) Dial() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		conn, err := GetProducer(p.url)
		if err != nil {
			return err
		}

		p.conn = conn
		p.state = stateMedias
	}

	return nil
}

func (p *Producer) GetMedias() []*core.Media {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil {
		return nil
	}

	return p.conn.GetMedias()
}

func (p *Producer) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		return nil, errors.New("get track from none state")
	}

	for _, track := range p.receivers {
		if track.Codec == codec {
			return track, nil
		}
	}

	track, err := p.conn.GetTrack(media, codec)
	if err != nil {
		return nil, err
	}

	p.receivers = append(p.receivers, track)

	if p.state == stateMedias {
		p.state = stateTracks
	}

	return track, nil
}

func (p *Producer) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		return errors.New("add track from none state")
	}

	if err := p.conn.(core.Consumer).AddTrack(media, codec, track); err != nil {
		return err
	}

	p.senders = append(p.senders, track)

	if p.state == stateMedias {
		p.state = stateTracks
	}

	return nil
}

func (p *Producer) MarshalJSON() ([]byte, error) {
	if conn := p.conn; conn != nil {
		return json.Marshal(conn)
	}
	info := map[string]string{"url": p.url}
	return json.Marshal(info)
}

func (p *Producer) hasSourceScheme(scheme string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return strings.HasPrefix(p.url, scheme+":")
}

func (p *Producer) reset(reason string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	if since := now.Sub(p.lastReset); since < producerResetMinInterval {
		log.Warn().
			Str("url", safeProducerURL(p.url)).
			Str("reason", reason).
			Stringer("wait", (producerResetMinInterval - since).Round(time.Millisecond)).
			Msg("[streams] skip duplicate producer reset")
		return true
	}
	p.lastReset = now

	if p.conn == nil {
		log.Warn().Str("url", safeProducerURL(p.url)).Str("reason", reason).Msg("[streams] mark inactive producer reset")
		return true
	}

	switch p.state {
	case stateMedias, stateTracks, stateStart:
	default:
		return false
	}

	p.workerID++
	workerID := p.workerID
	conn := p.conn
	log.Warn().Str("url", safeProducerURL(p.url)).Str("reason", reason).Msg("[streams] reset producer")

	go func() {
		_ = conn.Stop()
		p.reconnect(workerID, 0)
	}()
	return true
}

func ResetIfSourceScheme(name, scheme, reason string) bool {
	stream := Get(name)
	if stream == nil {
		return false
	}

	stream.mu.Lock()
	producers := append([]*Producer(nil), stream.producers...)
	stream.mu.Unlock()

	var reset bool
	for _, producer := range producers {
		if producer.hasSourceScheme(scheme) && producer.reset(reason) {
			reset = true
		}
	}

	return reset
}

// internals

func (p *Producer) start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state != stateTracks {
		return
	}

	log.Debug().Msgf("[streams] start producer url=%s", safeProducerURL(p.url))

	p.state = stateStart
	p.workerID++

	go p.worker(p.conn, p.workerID)
}

func (p *Producer) worker(conn core.Producer, workerID int) {
	if err := conn.Start(); err != nil {
		p.mu.Lock()
		closed := p.workerID != workerID
		p.mu.Unlock()

		if closed {
			return
		}

		log.Warn().Err(err).Str("url", safeProducerURL(p.url)).Caller().Send()
	}

	p.reconnect(workerID, 0)
}

func (p *Producer) reconnect(workerID, retry int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.workerID != workerID {
		log.Trace().Msgf("[streams] stop reconnect url=%s", safeProducerURL(p.url))
		return
	}

	log.Debug().Msgf("[streams] retry=%d to url=%s", retry, safeProducerURL(p.url))

	conn, err := GetProducer(p.url)
	if err != nil {
		log.Debug().Msgf("[streams] producer=%s", err)

		timeout := p.reconnectBackoff(retry, err)

		time.AfterFunc(timeout, func() {
			p.reconnect(workerID, retry+1)
		})
		return
	}

	for _, media := range conn.GetMedias() {
		switch media.Direction {
		case core.DirectionRecvonly:
			for i, receiver := range p.receivers {
				codec := media.MatchCodec(receiver.Codec)
				if codec == nil {
					continue
				}

				track, err := conn.GetTrack(media, codec)
				if err != nil {
					continue
				}

				receiver.Replace(track)
				p.receivers[i] = track
				break
			}

		case core.DirectionSendonly:
			for _, sender := range p.senders {
				codec := media.MatchCodec(sender.Codec)
				if codec == nil {
					continue
				}

				_ = conn.(core.Consumer).AddTrack(media, codec, sender)
			}
		}
	}

	// stop previous connection after moving tracks (fix ghost exec/ffmpeg)
	_ = p.conn.Stop()
	// swap connections
	p.conn = conn

	go p.worker(conn, workerID)
}

func (p *Producer) reconnectBackoff(retry int, err error) time.Duration {
	if err != nil && strings.Contains(err.Error(), "exec: local nest upstream reset") {
		if retry < 3 {
			return execNestResetBackoff
		}
		if retry < 8 {
			return 30 * time.Second
		}
		return time.Minute
	}

	timeout := time.Minute
	if retry < 5 {
		timeout = time.Second
	} else if retry < 10 {
		timeout = 5 * time.Second
	} else if retry < 20 {
		timeout = 10 * time.Second
	}

	if strings.HasPrefix(p.url, "nest:") && timeout < nestReconnectMinBackoff {
		timeout = nestReconnectMinBackoff
	}

	return timeout
}

func (p *Producer) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch p.state {
	case stateExternal:
		log.Trace().Msgf("[streams] skip stop external producer")
		return
	case stateNone:
		log.Trace().Msgf("[streams] skip stop none producer")
		return
	case stateStart:
		p.workerID++
	}

	log.Debug().Msgf("[streams] stop producer url=%s", safeProducerURL(p.url))

	if p.conn != nil {
		_ = p.conn.Stop()
		p.conn = nil
	}

	p.state = stateNone
	p.receivers = nil
	p.senders = nil
}

func safeProducerURL(rawURL string) string {
	if strings.HasPrefix(rawURL, "nest:") {
		if prefix, _, ok := strings.Cut(rawURL, "?"); ok {
			return prefix + "?<redacted>"
		}
		return "nest:<redacted>"
	}
	return rawURL
}
