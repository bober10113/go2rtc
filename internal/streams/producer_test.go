package streams

import (
	"errors"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
)

func TestExecNestDerivedWarmupPolicyEscalatesAfterRecentFailures(t *testing.T) {
	timeout, stable, packets := execNestDerivedWarmupPolicy(0)
	if timeout != execNestDerivedWarmupTimeout || stable != execNestDerivedWarmupStable || packets != execNestDerivedWarmupMinPackets {
		t.Fatalf("base policy = (%s, %s, %d), want (%s, %s, %d)", timeout, stable, packets, execNestDerivedWarmupTimeout, execNestDerivedWarmupStable, execNestDerivedWarmupMinPackets)
	}

	timeout, stable, packets = execNestDerivedWarmupPolicy(2)
	if timeout != execNestDerivedFlapTimeout || stable != execNestDerivedFlapStable || packets != execNestDerivedFlapMinPackets {
		t.Fatalf("flap policy = (%s, %s, %d), want (%s, %s, %d)", timeout, stable, packets, execNestDerivedFlapTimeout, execNestDerivedFlapStable, execNestDerivedFlapMinPackets)
	}

	timeout, stable, packets = execNestDerivedWarmupPolicy(6)
	if timeout != execNestDerivedHardTimeout || stable != execNestDerivedHardStable || packets != execNestDerivedHardMinPackets {
		t.Fatalf("hard policy = (%s, %s, %d), want (%s, %s, %d)", timeout, stable, packets, execNestDerivedHardTimeout, execNestDerivedHardStable, execNestDerivedHardMinPackets)
	}
}

func TestRecentExecNestFailuresIgnoresStaleFailures(t *testing.T) {
	now := time.Now()
	prod := &Producer{
		execNestFailures: 4,
		execNestLastFail: now.Add(-execNestBackoffWindow -
			time.Second),
	}

	if got := prod.recentExecNestFailuresLocked(now); got != 0 {
		t.Fatalf("stale failures = %d, want 0", got)
	}

	prod.execNestLastFail = now.Add(-time.Minute)
	if got := prod.recentExecNestFailuresLocked(now); got != 4 {
		t.Fatalf("recent failures = %d, want 4", got)
	}
}

func TestExecNestShouldResetRawAfterDerivedFailure(t *testing.T) {
	if execNestShouldResetRawAfterDerivedFailure(execNestDerivedRawResetAfter - 1) {
		t.Fatalf("raw reset triggered before threshold")
	}

	if !execNestShouldResetRawAfterDerivedFailure(execNestDerivedRawResetAfter) {
		t.Fatalf("raw reset did not trigger at threshold")
	}
}

func TestExecNestShouldResetRawAfterDerivedSettleFailure(t *testing.T) {
	if execNestShouldResetRawAfterDerivedSettleFailure(execNestDerivedSettleResetAfter - 1) {
		t.Fatalf("settle raw reset triggered before threshold")
	}

	if !execNestShouldResetRawAfterDerivedSettleFailure(execNestDerivedSettleResetAfter) {
		t.Fatalf("settle raw reset did not trigger at threshold")
	}
}

func TestExecNestShouldClearBackoffAfterDerivedWarmup(t *testing.T) {
	if !execNestShouldClearBackoffAfterDerivedWarmup(0) {
		t.Fatalf("clean warmup should clear backoff")
	}

	if execNestShouldClearBackoffAfterDerivedWarmup(1) {
		t.Fatalf("flapping warmup should keep backoff history")
	}
}

func TestSourceSchemeHasMedia(t *testing.T) {
	if sourceSchemeHasMedia(SourceSchemeStatus{}) {
		t.Fatalf("empty status has media")
	}

	if sourceSchemeHasMedia(SourceSchemeStatus{Handled: true}) {
		t.Fatalf("handled status without media has media")
	}

	if sourceSchemeHasMedia(SourceSchemeStatus{Handled: true, Medias: 1}) {
		t.Fatalf("handled status without receiver/packets has media")
	}

	if sourceSchemeHasMedia(SourceSchemeStatus{Handled: true, Medias: 1, Receivers: 1}) {
		t.Fatalf("handled status without packets has media")
	}

	if sourceSchemeHasMedia(SourceSchemeStatus{Handled: true, Medias: 1, Receivers: 1, Packets: 1}) {
		t.Fatalf("handled status without byte flow has media")
	}

	if !sourceSchemeHasMedia(SourceSchemeStatus{Handled: true, Medias: 1, Receivers: 1, Packets: 1, Bytes: 64}) {
		t.Fatalf("handled status with media flow was not detected")
	}
}

func TestSourceSchemeStatusUsesSourceReceiverStats(t *testing.T) {
	codec := &core.Codec{Name: core.CodecH264}
	media := &core.Media{
		Kind:      core.KindVideo,
		Direction: core.DirectionRecvonly,
		Codecs:    []*core.Codec{codec},
	}
	localReceiver := core.NewReceiver(media, codec)
	sourceReceiver := core.NewReceiver(media, codec)
	sourceReceiver.Packets = 42
	sourceReceiver.Bytes = 1024

	prod := &Producer{
		url:       "nest:?device_id=redacted",
		conn:      &sourceStatsProducer{medias: []*core.Media{media}, receivers: []*core.Receiver{sourceReceiver}},
		receivers: []*core.Receiver{localReceiver},
	}

	status := prod.sourceSchemeStatus("nest")
	if !status.Handled {
		t.Fatalf("nest source was not handled")
	}
	if status.Medias != 1 {
		t.Fatalf("medias = %d, want 1", status.Medias)
	}
	if status.Receivers != 1 {
		t.Fatalf("receivers = %d, want 1", status.Receivers)
	}
	if status.Packets != 42 {
		t.Fatalf("packets = %d, want source receiver packets", status.Packets)
	}
	if status.Bytes != 1024 {
		t.Fatalf("bytes = %d, want source receiver bytes", status.Bytes)
	}
}

func TestSourceSchemeStatusForStreamAggregatesBytes(t *testing.T) {
	const streamName = "test_nest_raw_bytes"
	defer Delete(streamName)

	codec := &core.Codec{Name: core.CodecH264}
	media := &core.Media{
		Kind:      core.KindVideo,
		Direction: core.DirectionRecvonly,
		Codecs:    []*core.Codec{codec},
	}
	sourceReceiver := core.NewReceiver(media, codec)
	sourceReceiver.Packets = 7
	sourceReceiver.Bytes = 4096

	stream := &Stream{
		producers: []*Producer{{
			url:       "nest:?device_id=redacted",
			conn:      &sourceStatsProducer{medias: []*core.Media{media}, receivers: []*core.Receiver{sourceReceiver}},
			receivers: []*core.Receiver{core.NewReceiver(media, codec)},
		}},
	}

	streamsMu.Lock()
	streams[streamName] = stream
	streamsMu.Unlock()

	status := SourceSchemeStatusForStream(streamName, "nest")
	if status.Packets != 7 {
		t.Fatalf("packets = %d, want 7", status.Packets)
	}
	if status.Bytes != 4096 {
		t.Fatalf("bytes = %d, want 4096", status.Bytes)
	}
	if _, ok := localNestSourceAvailableForDerived(streamName); !ok {
		t.Fatalf("raw nest stream with packet and byte flow was not available")
	}
}

func TestLocalNestH264ReadinessFromRTP(t *testing.T) {
	prod := &Producer{}

	prod.observeLocalNestH264Payload([]byte{h264.NALUTypeSPS})
	prod.observeLocalNestH264Payload([]byte{h264.NALUTypePPS})
	prod.observeLocalNestH264Payload([]byte{h264.NALUTypeIFrame})

	if !prod.localNestH264SPS.Load() || !prod.localNestH264PPS.Load() || !prod.localNestH264Keyframe.Load() {
		t.Fatalf("H264 readiness was not detected from single NALU RTP payloads")
	}
}

func TestLocalNestH264ReadinessFromFragmentedIDR(t *testing.T) {
	prod := &Producer{}

	prod.observeLocalNestH264Payload([]byte{28, 0x80 | h264.NALUTypeIFrame})

	if !prod.localNestH264Keyframe.Load() {
		t.Fatalf("H264 keyframe was not detected from FU-A start packet")
	}
}

func TestResetLocalNestReadinessForConsumerHandoffKeepsParameterSets(t *testing.T) {
	prod := &Producer{}
	codec := &core.Codec{
		Name:     core.CodecH264,
		FmtpLine: "packetization-mode=1;profile-level-id=64001f;sprop-parameter-sets=Z2QAH6wkhAFAFuwEQAAAAwBAAAAMI8YMkg==,aO4yyLA=",
	}

	prod.observeLocalNestH264Payload([]byte{h264.NALUTypeSPS})
	prod.observeLocalNestH264Payload([]byte{h264.NALUTypePPS})
	prod.observeLocalNestH264Payload([]byte{h264.NALUTypeIFrame})
	prod.localNestVideoPacketNsec.Store(time.Now().UnixNano())

	prod.resetLocalNestReadinessForConsumerHandoff(codec)

	if !prod.localNestH264SPS.Load() || !prod.localNestH264PPS.Load() {
		t.Fatalf("handoff reset did not preserve codec SPS/PPS")
	}
	if prod.localNestH264Keyframe.Load() {
		t.Fatalf("handoff reset kept stale keyframe readiness")
	}
	if got := prod.localNestVideoPacketNsec.Load(); got != 0 {
		t.Fatalf("handoff reset kept stale packet timestamp %d", got)
	}
}

func TestExecNestDerivedWarmupSeverityUsesRecentStarts(t *testing.T) {
	if got := execNestDerivedWarmupSeverity(0, 1); got != 0 {
		t.Fatalf("severity for first start = %d, want 0", got)
	}

	if got := execNestDerivedWarmupSeverity(0, execNestDerivedStartFlapAfter); got != 2 {
		t.Fatalf("severity for flap starts = %d, want 2", got)
	}

	if got := execNestDerivedWarmupSeverity(0, execNestDerivedStartHardAfter); got != 6 {
		t.Fatalf("severity for hard starts = %d, want 6", got)
	}

	if got := execNestDerivedWarmupSeverity(4, 1); got != 4 {
		t.Fatalf("severity did not preserve higher failure count: %d", got)
	}
}

func TestExecNestDerivedSettleDelay(t *testing.T) {
	if got := execNestDerivedSettleDelay(0); got != 0 {
		t.Fatalf("base settle = %s, want 0", got)
	}
	if got := execNestDerivedSettleDelay(2); got != execNestDerivedFlapSettle {
		t.Fatalf("flap settle = %s, want %s", got, execNestDerivedFlapSettle)
	}
	if got := execNestDerivedSettleDelay(6); got != execNestDerivedHardSettle {
		t.Fatalf("hard settle = %s, want %s", got, execNestDerivedHardSettle)
	}
}

func TestMarkExecNestDerivedStartResetsAfterWindow(t *testing.T) {
	now := time.Now()
	prod := &Producer{}

	if got := prod.markExecNestDerivedStartLocked(now); got != 1 {
		t.Fatalf("first recent start = %d, want 1", got)
	}
	if got := prod.markExecNestDerivedStartLocked(now.Add(time.Minute)); got != 2 {
		t.Fatalf("second recent start = %d, want 2", got)
	}
	if got := prod.markExecNestDerivedStartLocked(now.Add(time.Minute + execNestDerivedStartWindow + time.Second)); got != 1 {
		t.Fatalf("stale recent starts were not reset: %d", got)
	}
}

func TestBeginLocalNestDerivedRecoveryHold(t *testing.T) {
	prod := &Producer{}

	done := prod.beginLocalNestDerivedRecoveryHold(time.Second)
	prod.mu.Lock()
	active := prod.localNestRecoveries
	heldUntil := prod.recoveringUntil
	prod.mu.Unlock()

	if active != 1 {
		t.Fatalf("active recoveries = %d, want 1", active)
	}
	if time.Until(heldUntil) <= 0 {
		t.Fatalf("recovery hold was not set in the future")
	}

	done()
	prod.mu.Lock()
	active = prod.localNestRecoveries
	prod.mu.Unlock()
	if active != 0 {
		t.Fatalf("active recoveries after done = %d, want 0", active)
	}
}

func TestBeginExecNestDerivedRecoverySingleFlight(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	defer resetExecNestDerivedRecoveryForTest()

	start := time.Now()
	first, owner, waiters, age := beginExecNestDerivedRecovery("nest_raw", start)
	if !owner {
		t.Fatalf("first recovery was not owner")
	}
	if waiters != 0 || age != 0 {
		t.Fatalf("first recovery waiters=%d age=%s, want 0", waiters, age)
	}

	second, owner, waiters, age := beginExecNestDerivedRecovery("nest_raw", start.Add(2*time.Second))
	if owner {
		t.Fatalf("duplicate recovery became owner")
	}
	if second != first {
		t.Fatalf("duplicate recovery did not join first call")
	}
	if waiters != 1 || age != 2*time.Second {
		t.Fatalf("duplicate recovery waiters=%d age=%s, want waiters=1 age=2s", waiters, age)
	}

	errDone := errors.New("done")
	finishExecNestDerivedRecovery("nest_raw", first, errDone)

	select {
	case <-second.done:
	default:
		t.Fatalf("duplicate recovery was not released")
	}
	if !errors.Is(second.err, errDone) {
		t.Fatalf("duplicate recovery err=%v, want %v", second.err, errDone)
	}

	next, owner, waiters, _ := beginExecNestDerivedRecovery("nest_raw", start.Add(3*time.Second))
	if !owner {
		t.Fatalf("new recovery after finish was not owner")
	}
	if next == first {
		t.Fatalf("new recovery reused finished call")
	}
	if waiters != 0 {
		t.Fatalf("new recovery waiters=%d, want 0", waiters)
	}
	finishExecNestDerivedRecovery("nest_raw", next, nil)
}

func resetExecNestDerivedRecoveryForTest() {
	execNestDerivedRecovery.Lock()
	defer execNestDerivedRecovery.Unlock()
	execNestDerivedRecovery.calls = map[string]*execNestDerivedRecoveryCall{}
}

type sourceStatsProducer struct {
	medias    []*core.Media
	receivers []*core.Receiver
}

func (p *sourceStatsProducer) GetMedias() []*core.Media {
	return p.medias
}

func (p *sourceStatsProducer) GetTrack(_ *core.Media, _ *core.Codec) (*core.Receiver, error) {
	return nil, core.ErrCantGetTrack
}

func (p *sourceStatsProducer) SourceReceivers() []*core.Receiver {
	return p.receivers
}

func (p *sourceStatsProducer) Start() error {
	return nil
}

func (p *sourceStatsProducer) Stop() error {
	return nil
}
