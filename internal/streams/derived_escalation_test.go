package streams

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

func TestDerivedEscalationRawActivityCannotVetoForever(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	name := t.Name()
	now := time.Now()
	for i, elapsed := range []time.Duration{0, 30 * time.Second, time.Minute} {
		reset, escalated := reserveExecNestDerivedRawReset(name, i+1, true, false, now.Add(elapsed))
		if want := i == 2; reset != want || escalated != want {
			t.Fatalf("attempt %d: reset=%v escalated=%v want=%v", i+1, reset, escalated, want)
		}
	}
}

func TestDerivedEscalationNeedsTimeAndRepeatedFailures(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	now := time.Now()
	for i := 0; i < 10; i++ {
		if reset, _ := reserveExecNestDerivedRawReset(t.Name(), i+1, true, true, now); reset {
			t.Fatal("rapid failures bypassed time threshold")
		}
	}
	if reset, _ := reserveExecNestDerivedRawReset(t.Name()+"single", 40, true, false, now); reset {
		t.Fatal("producer-local history bypassed shared failure threshold")
	}
}

func TestDerivedEscalationSharesWarmupAndSettleHistory(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	now := time.Now()
	name := t.Name()
	reserveExecNestDerivedRawReset(name, 1, true, false, now)
	reserveExecNestDerivedRawReset(name, 1, true, true, now.Add(30*time.Second))
	if reset, escalated := reserveExecNestDerivedRawReset(name, 1, true, false, now.Add(time.Minute)); !reset || !escalated {
		t.Fatal("producer recreation or gate change lost shared failure history")
	}
}

func TestDerivedEscalationCooldownAndCameraIsolation(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	now := time.Now()
	name := t.Name()
	if reset, escalated := reserveExecNestDerivedRawReset(name, 3, false, false, now); !reset || escalated {
		t.Fatal("inactive raw threshold changed")
	}
	for i := 1; i < 120; i++ {
		if reset, _ := reserveExecNestDerivedRawReset(name, 40, true, i%2 != 0, now.Add(time.Duration(i)*time.Second)); reset {
			t.Fatal("reset repeated inside cooldown")
		}
	}
	if reset, _ := reserveExecNestDerivedRawReset(name+"other", 3, false, false, now); !reset {
		t.Fatal("another camera inherited cooldown")
	}
	if reset, escalated := reserveExecNestDerivedRawReset(name, 40, true, false, now.Add(2*time.Minute)); !reset || !escalated {
		t.Fatal("ongoing failed media did not retry after cooldown")
	}
}

func TestDerivedEscalationVerifiedMediaClearsFailuresNotCooldown(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	now := time.Now()
	name := t.Name()
	reserveExecNestDerivedRawReset(name, 3, false, false, now)
	p := &Producer{}
	p.markLocalNestDerivedReady(name, "test verified media")
	noteExecNestDerivedStall(name, now.Add(time.Second), time.Minute)
	if reset, _ := reserveExecNestDerivedRawReset(name, 3, true, false, now.Add(2*time.Second)); reset {
		t.Fatal("verified media erased reset cooldown")
	}
	p.markLocalNestDerivedReady(name, "test verified media")
	if reset, _ := reserveExecNestDerivedRawReset(name, 40, true, false, now.Add(3*time.Minute)); reset {
		t.Fatal("verified media retained stale failure history")
	}
}

func TestDerivedEscalationExpiresOldFailures(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	now := time.Now()
	name := t.Name()
	reserveExecNestDerivedRawReset(name, 1, true, false, now)
	reserveExecNestDerivedRawReset(name, 2, true, false, now.Add(30*time.Second))
	if reset, _ := reserveExecNestDerivedRawReset(name, 40, true, false, now.Add(execNestBackoffWindow+time.Minute)); reset {
		t.Fatal("expired failures caused escalation")
	}
}

func TestDerivedEscalationConcurrentReservation(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	now := time.Now()
	name := t.Name()
	noteExecNestDerivedStall(name, now, time.Minute)
	var count atomic.Int32
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if reset, _ := reserveExecNestDerivedRawReset(name, 3, true, false, now); reset {
				count.Add(1)
			}
		}()
	}
	workers.Wait()
	if count.Load() != 1 {
		t.Fatalf("got %d reset owners, want 1", count.Load())
	}
}

func TestDerivedEscalationLongGapTrace(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	name := t.Name()
	gap := time.Now()
	// Observed sequence: recording loss, stall at +16s (stagnant for 10s),
	// then a failed settle at +74s despite raw packet growth.
	noteExecNestDerivedStall(name, gap.Add(16*time.Second), 10*time.Second)
	if reset, escalated := reserveExecNestDerivedRawReset(name, 2, true, true, gap.Add(74*time.Second)); !reset || !escalated {
		t.Fatal("trace waited for later completed probes instead of sustained failure")
	}
}

func TestDerivedEscalationInactiveTracePreservesRecovery(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	name := t.Name()
	now := time.Now()
	noteExecNestDerivedStall(name, now, time.Minute)
	if reset, _ := reserveExecNestDerivedRawReset(name, 2, true, true, now); !reset {
		t.Fatal("active escalation not reserved")
	}
	// The source subsequently becomes inactive. Do not impose the active-media
	// cooldown on the old reset path; Producer.reset retains its duplicate guard.
	if reset, escalated := reserveExecNestDerivedRawReset(name, 3, false, false, now.Add(20*time.Second)); !reset || escalated {
		t.Fatal("new cooldown delayed existing inactive-source recovery")
	}
}

func TestDerivedEscalationCapsNextProbe(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	name := t.Name()
	now := time.Now()
	if got := execNestDerivedRecoveryTimeout(name, now, 45*time.Second); got != 45*time.Second {
		t.Fatal("healthy startup timeout changed")
	}
	noteExecNestDerivedStall(name, now.Add(10*time.Second), 10*time.Second)
	if got := execNestDerivedRecoveryTimeout(name, now.Add(55*time.Second), 45*time.Second); got != 5*time.Second {
		t.Fatalf("probe timeout %s, want remaining 5s", got)
	}
	if got := execNestDerivedRecoveryTimeout(name, now.Add(65*time.Second), 45*time.Second); got != execNestDerivedWarmupCheck {
		t.Fatalf("overdue probe timeout %s", got)
	}
	reserveExecNestDerivedRawReset(name, 3, true, false, now.Add(65*time.Second))
	noteExecNestDerivedStall(name, now.Add(80*time.Second), 10*time.Second)
	if got := execNestDerivedRecoveryTimeout(name, now.Add(100*time.Second), 45*time.Second); got != 45*time.Second {
		t.Fatal("cooldown shortened probe despite escalation being unavailable")
	}
	clearExecNestDerivedFailures(name)
	if got := execNestDerivedRecoveryTimeout(name, now.Add(200*time.Second), 45*time.Second); got != 45*time.Second {
		t.Fatal("verified recovery retained probe deadline")
	}
}

func TestDerivedEscalationSettleFailureRequestsRawReset(t *testing.T) {
	resetExecNestDerivedRecoveryForTest()
	name := t.Name()
	codec := &core.Codec{Name: core.CodecH264}
	media := &core.Media{Kind: core.KindVideo, Codecs: []*core.Codec{codec}}
	receiver := core.NewReceiver(media, codec)
	receiver.Packets, receiver.Bytes = 100, 10000
	// Keep the mock out of running state: verify the real reset request without
	// launching a reconnect goroutine or contacting any camera.
	raw := &Producer{url: "nest:?device_id=test", conn: &sourceStatsProducer{
		medias: []*core.Media{media}, receivers: []*core.Receiver{receiver},
	}}
	streamsMu.Lock()
	streams[name] = &Stream{producers: []*Producer{raw}}
	streamsMu.Unlock()
	defer Delete(name)
	now := time.Now()
	reserveExecNestDerivedRawReset(name, 1, true, false, now.Add(-time.Minute))
	reserveExecNestDerivedRawReset(name, 2, true, false, now.Add(-30*time.Second))
	derived := &Producer{}
	err := derived.failLocalNestDerivedSettle(name, 0, 0,
		SourceSchemeStatus{Packets: 50, Bytes: 5000}, videoMediaStatus{}, "test stalled derived video")
	if err == nil || err.Error() != execNestResetError {
		t.Fatalf("unexpected failure result: %v", err)
	}
	if raw.lastReset.IsZero() {
		t.Fatal("active raw counters prevented settle failure from requesting reset")
	}
}
