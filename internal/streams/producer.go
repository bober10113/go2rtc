package streams

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/shell"
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

	state             state
	mu                sync.Mutex
	workerID          int
	lastReset         time.Time
	execNestFailures  int
	execNestLastFail  time.Time
	execNestStarts    int
	execNestLastStart time.Time
	recoveringUntil   time.Time
	idleRecoverUntil  time.Time
}

const SourceTemplate = "{input}"

const (
	producerResetMinInterval = 20 * time.Second
	nestReconnectMinBackoff  = 5 * time.Second
	execNestResetBackoff     = 15 * time.Second
	execNestBackoffMedium    = 30 * time.Second
	execNestBackoffLong      = time.Minute
	execNestBackoffMax       = 2 * time.Minute
	execNestBackoffWindow    = 10 * time.Minute
	execNestIdleRecoveryMax  = 2 * time.Minute
	deferredStopPadding      = 2 * time.Second
	execNestResetError       = "exec: local nest upstream reset"

	execNestDerivedWarmupTimeout    = 8 * time.Second
	execNestDerivedWarmupCheck      = 500 * time.Millisecond
	execNestDerivedWarmupStable     = 2 * time.Second
	execNestDerivedWarmupMinPackets = 5
	execNestDerivedFlapTimeout      = 20 * time.Second
	execNestDerivedFlapStable       = 5 * time.Second
	execNestDerivedFlapMinPackets   = 25
	execNestDerivedHardTimeout      = 45 * time.Second
	execNestDerivedHardStable       = 10 * time.Second
	execNestDerivedHardMinPackets   = 60
	execNestDerivedStartWindow      = 5 * time.Minute
	execNestDerivedStartFlapAfter   = 2
	execNestDerivedStartHardAfter   = 3
	execNestDerivedFlapSettle       = 5 * time.Second
	execNestDerivedHardSettle       = 10 * time.Second
	execNestDerivedRawResetAfter    = 3
)

type SourceSchemeStatus struct {
	Handled   bool
	Medias    int
	Receivers int
	Packets   int
}

type sourceReceiverStats interface {
	SourceReceivers() []*core.Receiver
}

type execNestDerivedRecoveryCall struct {
	done    chan struct{}
	started time.Time
	err     error
	waiters int
}

var execNestDerivedRecovery = struct {
	sync.Mutex
	calls map[string]*execNestDerivedRecoveryCall
}{
	calls: map[string]*execNestDerivedRecoveryCall{},
}

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

	if p.state != stateNone {
		p.mu.Unlock()
		return nil
	}

	if wait := time.Until(p.recoveringUntil); wait > 0 {
		if inputName, ok := p.localNestDerivedInput(); ok {
			if status, ok := localNestSourceAvailableForDerived(inputName); ok {
				if p.recentExecNestFailuresLocked(time.Now()) > 0 {
					log.Warn().
						Str("url", safeProducerURL(p.url)).
						Str("derived_input", inputName).
						Stringer("wait", wait.Round(time.Millisecond)).
						Int("raw_medias", status.Medias).
						Int("raw_receivers", status.Receivers).
						Int("raw_packets", status.Packets).
						Int("failures", p.execNestFailures).
						Msg("[streams] keep producer dial recovery during derived publish backoff")
					p.mu.Unlock()
					return errors.New(execNestResetError)
				}
				p.recoveringUntil = time.Time{}
				log.Warn().
					Str("url", safeProducerURL(p.url)).
					Str("derived_input", inputName).
					Stringer("wait", wait.Round(time.Millisecond)).
					Int("raw_medias", status.Medias).
					Int("raw_receivers", status.Receivers).
					Int("raw_packets", status.Packets).
					Msg("[streams] clear producer dial recovery because raw nest media is present")
			} else {
				log.Warn().
					Str("url", safeProducerURL(p.url)).
					Stringer("wait", wait.Round(time.Millisecond)).
					Msg("[streams] skip producer dial during local nest recovery")
				p.mu.Unlock()
				return errors.New(execNestResetError)
			}
		} else {
			log.Warn().
				Str("url", safeProducerURL(p.url)).
				Stringer("wait", wait.Round(time.Millisecond)).
				Msg("[streams] skip producer dial during local nest recovery")
			p.mu.Unlock()
			return errors.New(execNestResetError)
		}
	}

	if wait := time.Until(p.recoveringUntil); wait > 0 {
		log.Warn().
			Str("url", safeProducerURL(p.url)).
			Stringer("wait", wait.Round(time.Millisecond)).
			Msg("[streams] skip producer dial during local nest recovery")
		p.mu.Unlock()
		return errors.New(execNestResetError)
	}

	conn, err := GetProducer(p.url)
	if err != nil {
		if strings.Contains(err.Error(), execNestResetError) {
			wait := p.markExecNestBackoffLocked()
			log.Warn().
				Str("url", safeProducerURL(p.url)).
				Int("failures", p.execNestFailures).
				Stringer("wait", wait.Round(time.Millisecond)).
				Msg("[streams] local nest derived producer backoff")
		}
		p.mu.Unlock()
		return err
	}

	p.conn = conn
	p.state = stateMedias
	_, localNestDerived := p.localNestDerivedInput()
	if !localNestDerived {
		p.recoveringUntil = time.Time{}
		p.clearExecNestBackoffLocked()
	}
	p.mu.Unlock()

	if localNestDerived {
		if err := p.waitLocalNestDerivedWarmup(); err != nil {
			p.mu.Lock()
			p.stopLocked()
			p.mu.Unlock()
			return err
		}
		p.mu.Lock()
		p.recoveringUntil = time.Time{}
		p.clearExecNestBackoffLocked()
		p.mu.Unlock()
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

	p.idleRecoverUntil = time.Time{}

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

	p.idleRecoverUntil = time.Time{}

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

func (p *Producer) sourceSchemeStatus(scheme string) SourceSchemeStatus {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !strings.HasPrefix(p.url, scheme+":") {
		return SourceSchemeStatus{}
	}

	status := SourceSchemeStatus{Handled: true}
	if p.conn == nil {
		return status
	}

	for _, media := range p.conn.GetMedias() {
		if media.Direction == core.DirectionRecvonly && media.Kind == core.KindVideo && len(media.Codecs) > 0 {
			status.Medias++
		}
	}

	receivers := p.receivers
	if stats, ok := p.conn.(sourceReceiverStats); ok {
		receivers = stats.SourceReceivers()
	}

	for _, receiver := range receivers {
		if receiver == nil || receiver.Codec == nil || core.GetKind(receiver.Codec.Name) != core.KindVideo {
			continue
		}
		status.Receivers++
		status.Packets += receiver.Packets
	}

	return status
}

func (p *Producer) waitLocalNestDerivedWarmup() error {
	inputName, ok := p.localNestDerivedInput()
	if !ok {
		return nil
	}

	call, owner, waiters, age := beginExecNestDerivedRecovery(inputName, time.Now())
	if !owner {
		log.Warn().
			Str("url", safeProducerURL(p.url)).
			Str("derived_input", inputName).
			Int("waiters", waiters).
			Stringer("existing_recovery_age", age.Round(time.Millisecond)).
			Bool("recovery_owner", false).
			Msg("[streams] join local nest derived media recovery")
		<-call.done
		if call.err != nil {
			return call.err
		}
		if medias, packets := p.videoPacketStatus(); medias > 0 && packets > 0 {
			return nil
		}
		if status, ok := localNestSourceAvailableForDerived(inputName); ok {
			log.Warn().
				Str("url", safeProducerURL(p.url)).
				Str("derived_input", inputName).
				Int("waiters", waiters).
				Int("raw_medias", status.Medias).
				Int("raw_receivers", status.Receivers).
				Int("raw_packets", status.Packets).
				Bool("recovery_owner", false).
				Msg("[streams] local nest derived recovery waiter still has no derived media")
			return errors.New(execNestResetError)
		}
		log.Warn().
			Str("url", safeProducerURL(p.url)).
			Str("derived_input", inputName).
			Int("waiters", waiters).
			Bool("recovery_owner", false).
			Msg("[streams] local nest derived recovery waiter has no media")
		return errors.New(execNestResetError)
	}

	err := p.waitLocalNestDerivedWarmupOwner(inputName)
	finishExecNestDerivedRecovery(inputName, call, err)
	return err
}

func (p *Producer) waitLocalNestDerivedWarmupOwner(inputName string) error {
	now := time.Now()
	p.mu.Lock()
	if wait := p.recoveringUntil.Sub(now); wait > 0 {
		failures := p.recentExecNestFailuresLocked(now)
		if status, ok := localNestSourceAvailableForDerived(inputName); ok {
			if failures > 0 {
				p.mu.Unlock()
				log.Warn().
					Str("url", safeProducerURL(p.url)).
					Str("derived_input", inputName).
					Stringer("wait", wait.Round(time.Millisecond)).
					Int("raw_medias", status.Medias).
					Int("raw_receivers", status.Receivers).
					Int("raw_packets", status.Packets).
					Int("failures", failures).
					Msg("[streams] keep local nest derived recovery hold during publish backoff")
				return errors.New(execNestResetError)
			}
			p.recoveringUntil = time.Time{}
			p.mu.Unlock()
			log.Warn().
				Str("url", safeProducerURL(p.url)).
				Str("derived_input", inputName).
				Stringer("wait", wait.Round(time.Millisecond)).
				Int("raw_medias", status.Medias).
				Int("raw_receivers", status.Receivers).
				Int("raw_packets", status.Packets).
				Msg("[streams] clear local nest derived recovery hold because raw media is present")
		} else {
			p.mu.Unlock()
			log.Warn().
				Str("url", safeProducerURL(p.url)).
				Str("derived_input", inputName).
				Stringer("wait", wait.Round(time.Millisecond)).
				Msg("[streams] wait local nest derived recovery hold")
			return errors.New(execNestResetError)
		}
	} else {
		p.mu.Unlock()
	}
	p.mu.Lock()
	failures := p.recentExecNestFailuresLocked(now)
	p.mu.Unlock()

	startMedias, startPackets := p.videoPacketStatus()
	recentStarts := 0
	if startPackets == 0 {
		p.mu.Lock()
		recentStarts = p.markExecNestDerivedStartLocked(now)
		p.mu.Unlock()
	}

	severity := execNestDerivedWarmupSeverity(failures, recentStarts)
	timeout, stableFor, minPackets := execNestDerivedWarmupPolicy(severity)
	settleFor := execNestDerivedSettleDelay(severity)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(execNestDerivedWarmupCheck)
	defer ticker.Stop()

	var (
		readySince   time.Time
		readyPackets int
		medias       int
		packets      int
	)

	log.Warn().
		Str("url", safeProducerURL(p.url)).
		Int("failures", failures).
		Int("recent_starts", recentStarts).
		Int("severity", severity).
		Stringer("timeout", timeout).
		Stringer("stable", stableFor).
		Stringer("settle", settleFor).
		Int("min_packets", minPackets).
		Int("video_medias", startMedias).
		Int("packets", startPackets).
		Bool("recovery_owner", true).
		Msg("[streams] waiting for local nest derived media")

	for {
		medias, packets = p.videoPacketStatus()
		if medias > 0 && packets >= startPackets+minPackets {
			if readySince.IsZero() {
				readySince = time.Now()
				readyPackets = packets
			} else if time.Since(readySince) >= stableFor && packets > readyPackets {
				log.Info().
					Str("url", safeProducerURL(p.url)).
					Int("failures", failures).
					Int("video_medias", medias).
					Int("packets_start", startPackets).
					Int("packets_now", packets).
					Int("packet_delta", packets-startPackets).
					Stringer("stable_for", time.Since(readySince).Round(time.Millisecond)).
					Bool("recovery_owner", true).
					Msg("[streams] local nest derived media ready")
				if err := p.waitLocalNestDerivedSettle(inputName, settleFor, packets); err != nil {
					return err
				}
				p.mu.Lock()
				p.clearExecNestBackoffLocked()
				p.mu.Unlock()
				return nil
			} else if packets > readyPackets {
				readyPackets = packets
			}
		} else {
			readySince = time.Time{}
			readyPackets = 0
		}

		select {
		case <-deadline.C:
			rawStatus, rawOK := localNestSourceAvailableForDerived(inputName)
			p.mu.Lock()
			wait := p.markExecNestBackoffLocked()
			failures := p.execNestFailures
			p.mu.Unlock()

			resetRaw := execNestShouldResetRawAfterDerivedFailure(failures)
			handled, changed, inactive := false, false, false
			if resetRaw {
				handled, changed, inactive = ResetIfSourceSchemeDetailed(inputName, "nest", "derived media timeout")
			}

			ev := log.Warn().
				Str("url", safeProducerURL(p.url)).
				Str("derived_input", inputName).
				Int("video_medias", medias).
				Int("packets_start", startPackets).
				Int("packets_now", packets).
				Int("packet_delta", packets-startPackets).
				Stringer("backoff", wait.Round(time.Millisecond)).
				Int("failures", failures).
				Bool("raw_available", rawOK).
				Bool("reset_raw", resetRaw).
				Bool("raw_handled", handled).
				Bool("raw_changed", changed).
				Bool("raw_inactive", inactive).
				Bool("recovery_owner", true)
			if rawOK {
				ev.
					Int("raw_medias", rawStatus.Medias).
					Int("raw_receivers", rawStatus.Receivers).
					Int("raw_packets", rawStatus.Packets)
			}
			ev.Msg("[streams] local nest derived media timeout")
			return errors.New(execNestResetError)
		case <-ticker.C:
		}
	}
}

func (p *Producer) waitLocalNestDerivedSettle(inputName string, settleFor time.Duration, startPackets int) error {
	if settleFor <= 0 {
		return nil
	}

	deadline := time.NewTimer(settleFor)
	defer deadline.Stop()
	ticker := time.NewTicker(execNestDerivedWarmupCheck)
	defer ticker.Stop()

	lastPackets := startPackets
	log.Warn().
		Str("url", safeProducerURL(p.url)).
		Str("derived_input", inputName).
		Stringer("settle", settleFor).
		Int("packets_start", startPackets).
		Msg("[streams] settling local nest derived media")

	for {
		select {
		case <-deadline.C:
			medias, packets := p.videoPacketStatus()
			if medias > 0 && packets > startPackets {
				log.Info().
					Str("url", safeProducerURL(p.url)).
					Str("derived_input", inputName).
					Int("video_medias", medias).
					Int("packets_start", startPackets).
					Int("packets_now", packets).
					Int("packet_delta", packets-startPackets).
					Stringer("settled_for", settleFor).
					Msg("[streams] local nest derived media settled")
				return nil
			}
			return p.failLocalNestDerivedSettle(inputName, startPackets, packets, "settle expired without packet growth")
		case <-ticker.C:
			medias, packets := p.videoPacketStatus()
			if medias == 0 || packets <= lastPackets {
				return p.failLocalNestDerivedSettle(inputName, startPackets, packets, "packets stopped during settle")
			}
			lastPackets = packets
		}
	}
}

func (p *Producer) failLocalNestDerivedSettle(inputName string, startPackets, packets int, reason string) error {
	rawStatus, rawOK := localNestSourceAvailableForDerived(inputName)

	p.mu.Lock()
	wait := p.markExecNestBackoffLocked()
	failures := p.execNestFailures
	p.mu.Unlock()

	ev := log.Warn().
		Str("url", safeProducerURL(p.url)).
		Str("derived_input", inputName).
		Str("reason", reason).
		Int("packets_start", startPackets).
		Int("packets_now", packets).
		Stringer("backoff", wait.Round(time.Millisecond)).
		Int("failures", failures).
		Bool("raw_available", rawOK)
	if rawOK {
		ev.
			Int("raw_medias", rawStatus.Medias).
			Int("raw_receivers", rawStatus.Receivers).
			Int("raw_packets", rawStatus.Packets)
	}
	ev.Msg("[streams] local nest derived media settle failed")
	return errors.New(execNestResetError)
}

func beginExecNestDerivedRecovery(inputName string, now time.Time) (*execNestDerivedRecoveryCall, bool, int, time.Duration) {
	execNestDerivedRecovery.Lock()
	defer execNestDerivedRecovery.Unlock()

	if execNestDerivedRecovery.calls == nil {
		execNestDerivedRecovery.calls = map[string]*execNestDerivedRecoveryCall{}
	}

	if call := execNestDerivedRecovery.calls[inputName]; call != nil {
		call.waiters++
		return call, false, call.waiters, now.Sub(call.started)
	}

	call := &execNestDerivedRecoveryCall{
		done:    make(chan struct{}),
		started: now,
	}
	execNestDerivedRecovery.calls[inputName] = call
	return call, true, 0, 0
}

func finishExecNestDerivedRecovery(inputName string, call *execNestDerivedRecoveryCall, err error) {
	execNestDerivedRecovery.Lock()
	defer execNestDerivedRecovery.Unlock()

	if execNestDerivedRecovery.calls[inputName] == call {
		delete(execNestDerivedRecovery.calls, inputName)
	}
	call.err = err
	close(call.done)
}

func (p *Producer) videoPacketStatus() (medias, packets int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil {
		for _, media := range p.conn.GetMedias() {
			if media.Direction == core.DirectionRecvonly && media.Kind == core.KindVideo && len(media.Codecs) > 0 {
				medias++
			}
		}
	}

	for _, receiver := range p.receivers {
		if receiver == nil || receiver.Codec == nil || core.GetKind(receiver.Codec.Name) != core.KindVideo {
			continue
		}
		packets += receiver.Packets
	}

	return medias, packets
}

func execNestDerivedWarmupPolicy(failures int) (timeout time.Duration, stable time.Duration, minPackets int) {
	timeout = execNestDerivedWarmupTimeout
	stable = execNestDerivedWarmupStable
	minPackets = execNestDerivedWarmupMinPackets

	switch {
	case failures >= 6:
		timeout = execNestDerivedHardTimeout
		stable = execNestDerivedHardStable
		minPackets = execNestDerivedHardMinPackets
	case failures >= 2:
		timeout = execNestDerivedFlapTimeout
		stable = execNestDerivedFlapStable
		minPackets = execNestDerivedFlapMinPackets
	}

	return
}

func execNestDerivedWarmupSeverity(failures, recentStarts int) int {
	severity := failures
	switch {
	case recentStarts >= execNestDerivedStartHardAfter:
		if severity < 6 {
			severity = 6
		}
	case recentStarts >= execNestDerivedStartFlapAfter:
		if severity < 2 {
			severity = 2
		}
	}
	return severity
}

func execNestDerivedSettleDelay(severity int) time.Duration {
	switch {
	case severity >= 6:
		return execNestDerivedHardSettle
	case severity >= 2:
		return execNestDerivedFlapSettle
	default:
		return 0
	}
}

func execNestShouldResetRawAfterDerivedFailure(failures int) bool {
	return failures >= execNestDerivedRawResetAfter
}

func sourceSchemeHasMedia(status SourceSchemeStatus) bool {
	return status.Handled && status.Medias > 0 && status.Receivers > 0 && status.Packets > 0
}

func localNestSourceAvailableForDerived(name string) (SourceSchemeStatus, bool) {
	status := SourceSchemeStatusForStream(name, "nest")
	return status, sourceSchemeHasMedia(status)
}

func (p *Producer) localNestDerivedInput() (string, bool) {
	if strings.HasPrefix(p.url, "ffmpeg:") {
		return p.localNestFFmpegInput()
	}

	if !strings.HasPrefix(p.url, "exec:") {
		return "", false
	}

	args := shell.QuoteSplit(strings.TrimPrefix(p.url, "exec:"))
	for i := 0; i < len(args)-1; i++ {
		if args[i] != "-i" {
			continue
		}
		name, ok := localNestRTSPInputName(args[i+1])
		if !ok {
			continue
		}
		if status := SourceSchemeStatusForStream(name, "nest"); status.Handled {
			return name, true
		}
	}

	return "", false
}

func (p *Producer) localNestFFmpegInput() (string, bool) {
	source := strings.TrimPrefix(p.url, "ffmpeg:")
	source, _, _ = strings.Cut(source, "#")

	if name, ok := localNestRTSPInputName(source); ok {
		if status := SourceSchemeStatusForStream(name, "nest"); status.Handled {
			return name, true
		}
		return "", false
	}

	if strings.Contains(source, "://") {
		return "", false
	}

	name := strings.TrimSpace(source)
	if name == "" {
		return "", false
	}

	if status := SourceSchemeStatusForStream(name, "nest"); status.Handled {
		return name, true
	}

	return "", false
}

func localNestRTSPInputName(rawURL string) (string, bool) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "rtsp" || u.Path == "" {
		return "", false
	}

	host := u.Hostname()
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return "", false
	}

	name := strings.TrimPrefix(u.Path, "/")
	if name == "" {
		return "", false
	}

	return name, true
}

func (p *Producer) reset(reason string) (bool, bool, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	if since := now.Sub(p.lastReset); since < producerResetMinInterval {
		log.Warn().
			Str("url", safeProducerURL(p.url)).
			Str("reason", reason).
			Stringer("wait", (producerResetMinInterval - since).Round(time.Millisecond)).
			Msg("[streams] skip duplicate producer reset")
		return true, false, false
	}
	p.lastReset = now

	if p.conn == nil {
		log.Warn().Str("url", safeProducerURL(p.url)).Str("reason", reason).Msg("[streams] mark inactive producer reset")
		return true, true, true
	}

	switch p.state {
	case stateMedias, stateTracks, stateStart:
	default:
		return false, false, false
	}

	p.workerID++
	workerID := p.workerID
	conn := p.conn
	log.Warn().Str("url", safeProducerURL(p.url)).Str("reason", reason).Msg("[streams] reset producer")

	go func() {
		_ = conn.Stop()
		p.reconnect(workerID, 0)
	}()
	return true, true, false
}

func ResetIfSourceScheme(name, scheme, reason string) bool {
	handled, _, _ := ResetIfSourceSchemeDetailed(name, scheme, reason)
	return handled
}

func ResetIfSourceSchemeDetailed(name, scheme, reason string) (bool, bool, bool) {
	stream := Get(name)
	if stream == nil {
		return false, false, false
	}

	stream.mu.Lock()
	producers := append([]*Producer(nil), stream.producers...)
	stream.mu.Unlock()

	var handled bool
	var changed bool
	var inactive bool
	for _, producer := range producers {
		if producer.hasSourceScheme(scheme) {
			producerHandled, producerChanged, producerInactive := producer.reset(reason)
			handled = handled || producerHandled
			changed = changed || producerChanged
			inactive = inactive || producerInactive
		}
	}

	return handled, changed, inactive
}

func HoldSourceScheme(name, scheme, reason string, duration time.Duration) bool {
	stream := Get(name)
	if stream == nil || duration <= 0 {
		return false
	}

	stream.mu.Lock()
	producers := append([]*Producer(nil), stream.producers...)
	stream.mu.Unlock()

	var held bool
	for _, producer := range producers {
		if producer.hasSourceScheme(scheme) && producer.hold(reason, duration) {
			held = true
		}
	}

	return held
}

func (p *Producer) hold(reason string, duration time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil || p.state == stateNone {
		return false
	}

	until := time.Now().Add(duration)
	if p.recoveringUntil.Before(until) {
		p.recoveringUntil = until
	}

	log.Warn().
		Str("url", safeProducerURL(p.url)).
		Str("reason", reason).
		Stringer("hold", duration.Round(time.Millisecond)).
		Msg("[streams] hold producer during local nest handoff")
	return true
}

func SourceSchemeStatusForStream(name, scheme string) SourceSchemeStatus {
	stream := Get(name)
	if stream == nil {
		return SourceSchemeStatus{}
	}

	stream.mu.Lock()
	producers := append([]*Producer(nil), stream.producers...)
	stream.mu.Unlock()

	var status SourceSchemeStatus
	for _, producer := range producers {
		producerStatus := producer.sourceSchemeStatus(scheme)
		if !producerStatus.Handled {
			continue
		}
		status.Handled = true
		status.Medias += producerStatus.Medias
		status.Receivers += producerStatus.Receivers
		status.Packets += producerStatus.Packets
	}

	return status
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

	p.recoveringUntil = time.Time{}

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
	p.clearExecNestBackoffLocked()

	go p.worker(conn, workerID)
}

func (p *Producer) reconnectBackoff(retry int, err error) time.Duration {
	if err != nil && strings.Contains(err.Error(), execNestResetError) {
		timeout := execNestResetBackoff
		if retry < 3 {
			p.recoveringUntil = time.Now().Add(timeout)
			return timeout
		}
		if retry < 8 {
			timeout = 30 * time.Second
			p.recoveringUntil = time.Now().Add(timeout)
			return timeout
		}
		timeout = time.Minute
		p.recoveringUntil = time.Now().Add(timeout)
		return timeout
	}

	p.recoveringUntil = time.Time{}

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

func (p *Producer) markExecNestBackoffLocked() time.Duration {
	now := time.Now()
	if p.execNestLastFail.IsZero() || now.Sub(p.execNestLastFail) > execNestBackoffWindow {
		p.execNestFailures = 0
	}
	p.execNestFailures++
	p.execNestLastFail = now

	timeout := execNestResetBackoff
	switch {
	case p.execNestFailures >= 10:
		timeout = execNestBackoffMax
	case p.execNestFailures >= 6:
		timeout = execNestBackoffLong
	case p.execNestFailures >= 3:
		timeout = execNestBackoffMedium
	}
	p.recoveringUntil = now.Add(timeout)

	return timeout
}

func (p *Producer) recentExecNestFailuresLocked(now time.Time) int {
	if p.execNestLastFail.IsZero() || now.Sub(p.execNestLastFail) > execNestBackoffWindow {
		return 0
	}
	return p.execNestFailures
}

func (p *Producer) markExecNestDerivedStartLocked(now time.Time) int {
	if p.execNestLastStart.IsZero() || now.Sub(p.execNestLastStart) > execNestDerivedStartWindow {
		p.execNestStarts = 0
	}
	p.execNestStarts++
	p.execNestLastStart = now
	return p.execNestStarts
}

func (p *Producer) clearExecNestBackoffLocked() {
	p.execNestFailures = 0
	p.execNestLastFail = time.Time{}
}

func (p *Producer) hasReaders() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.hasReadersLocked()
}

func (p *Producer) hasReadersLocked() bool {
	for _, track := range p.receivers {
		if len(track.Senders()) > 0 {
			return true
		}
	}
	for _, track := range p.senders {
		if len(track.Senders()) > 0 {
			return true
		}
	}
	return false
}

func (p *Producer) deferStopDuringRecovery() bool {
	p.mu.Lock()

	now := time.Now()
	wait := p.recoveringUntil.Sub(now)
	if wait <= 0 {
		p.mu.Unlock()
		return false
	}

	if p.idleRecoverUntil.IsZero() || now.After(p.idleRecoverUntil) {
		p.idleRecoverUntil = now.Add(execNestIdleRecoveryMax)
	}

	stopAt := p.recoveringUntil
	if p.idleRecoverUntil.Before(stopAt) {
		stopAt = p.idleRecoverUntil
	}
	wait = time.Until(stopAt)
	if wait < 0 {
		wait = 0
	}
	wait += deferredStopPadding
	rawURL := safeProducerURL(p.url)
	p.mu.Unlock()

	log.Warn().
		Str("url", rawURL).
		Stringer("wait", wait.Round(time.Millisecond)).
		Msg("[streams] defer producer stop during local nest recovery")

	time.AfterFunc(wait, p.stopAfterDeferredRecovery)
	return true
}

func (p *Producer) stopAfterDeferredRecovery() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.hasReadersLocked() {
		p.idleRecoverUntil = time.Time{}
		return
	}

	now := time.Now()
	if wait := p.recoveringUntil.Sub(now); wait > 0 && now.Before(p.idleRecoverUntil) {
		if idleWait := p.idleRecoverUntil.Sub(now); idleWait < wait {
			wait = idleWait
		}
		if wait < 0 {
			wait = 0
		}
		time.AfterFunc(wait+deferredStopPadding, p.stopAfterDeferredRecovery)
		return
	}

	log.Warn().Str("url", safeProducerURL(p.url)).Msg("[streams] stop idle producer after local nest recovery")
	p.stopLocked()
}

func (p *Producer) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stopLocked()
}

func (p *Producer) stopLocked() {
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
	p.recoveringUntil = time.Time{}
	p.idleRecoverUntil = time.Time{}
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
