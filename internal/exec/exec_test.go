package exec

import (
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/internal/streams"
)

func TestLocalNestRecoveryWindowEscalatesRepeatedInactiveFailures(t *testing.T) {
	if got := localNestRecoveryWindow(1, 0, true); got != localNestInactiveRecovery {
		t.Fatalf("first inactive failure wait = %s, want %s", got, localNestInactiveRecovery)
	}

	if got := localNestRecoveryWindow(3, 0, true); got != localNestRecoveryMedium {
		t.Fatalf("repeated inactive failure wait = %s, want %s", got, localNestRecoveryMedium)
	}

	if got := localNestRecoveryWindow(6, 0, true); got != localNestRecoveryLong {
		t.Fatalf("many inactive failures wait = %s, want %s", got, localNestRecoveryLong)
	}

	if got := localNestRecoveryWindow(10, 0, true); got != localNestRecoveryMax {
		t.Fatalf("max inactive failures wait = %s, want %s", got, localNestRecoveryMax)
	}
}

func TestLocalNestRecoveryWindowEscalatesInactiveProbeFailures(t *testing.T) {
	if got := localNestRecoveryWindow(1, 2, true); got != localNestFlapRecoveryMin {
		t.Fatalf("inactive probe failures wait = %s, want %s", got, localNestFlapRecoveryMin)
	}
}

func TestLocalNestRecoveryWindowKeepsProgressiveProbeBackoff(t *testing.T) {
	tests := []struct {
		probeFailures int
		want          time.Duration
	}{
		{probeFailures: 0, want: localNestRecoveryWindowBase},
		{probeFailures: 2, want: localNestFlapRecoveryMin},
		{probeFailures: 4, want: localNestFlapRecoveryLong},
		{probeFailures: 6, want: localNestFlapRecoveryMax},
	}
	for _, tt := range tests {
		if got := localNestRecoveryWindow(1, tt.probeFailures, true); got != tt.want {
			t.Fatalf("probe failures %d wait = %s, want %s", tt.probeFailures, got, tt.want)
		}
	}
}

func TestLocalNestRecoveryLogsAreRateLimitedPerEvent(t *testing.T) {
	localNestRecoveryLogs.Lock()
	localNestRecoveryLogs.state = map[string]map[string]localNestRecoveryLogState{}
	localNestRecoveryLogs.Unlock()

	now := time.Now()
	if allowed, suppressed := allowLocalNestRecoveryLog("cam_raw", "hold", now); !allowed || suppressed != 0 {
		t.Fatalf("first event allowed=%t suppressed=%d, want true/0", allowed, suppressed)
	}
	if allowed, _ := allowLocalNestRecoveryLog("cam_raw", "hold", now.Add(time.Second)); allowed {
		t.Fatal("duplicate event was not rate limited")
	}
	if allowed, suppressed := allowLocalNestRecoveryLog("cam_raw", "reset", now.Add(time.Second)); !allowed || suppressed != 0 {
		t.Fatalf("separate event allowed=%t suppressed=%d, want true/0", allowed, suppressed)
	}
	if allowed, suppressed := allowLocalNestRecoveryLog("cam_raw", "hold", now.Add(localNestRecoveryLogInterval)); !allowed || suppressed != 1 {
		t.Fatalf("event after interval allowed=%t suppressed=%d, want true/1", allowed, suppressed)
	}
}

func TestLocalNestStatusAvailableUsesMediaPresence(t *testing.T) {
	if localNestStatusAvailable(streams.SourceSchemeStatus{}) {
		t.Fatalf("empty status was available")
	}

	if localNestStatusAvailable(streams.SourceSchemeStatus{Handled: true}) {
		t.Fatalf("handled status without media was available")
	}

	if localNestStatusAvailable(streams.SourceSchemeStatus{Handled: true, Medias: 1}) {
		t.Fatalf("handled status without receiver/packets was available")
	}

	if localNestStatusAvailable(streams.SourceSchemeStatus{Handled: true, Medias: 1, Receivers: 1}) {
		t.Fatalf("handled status without packets was available")
	}

	if !localNestStatusAvailable(streams.SourceSchemeStatus{Handled: true, Medias: 1, Receivers: 1, Packets: 1}) {
		t.Fatalf("handled status with packet flow was not available")
	}
}

func TestLocalNestPublishTimeoutResetsWhenRawPacketsAreMissing(t *testing.T) {
	localNestRecovery.Lock()
	localNestRecovery.state = map[string]localNestRecoveryState{}
	localNestRecovery.Unlock()

	reset, attempts := markLocalNestPublishTimeout("cam_raw", streams.SourceSchemeStatus{
		Handled:   true,
		Medias:    1,
		Receivers: 1,
	})
	if !reset {
		t.Fatalf("publish timeout with zero raw packets did not reset")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestLocalNestPublishTimeoutResetsWhenRawPacketsContinue(t *testing.T) {
	localNestRecovery.Lock()
	localNestRecovery.state = map[string]localNestRecoveryState{}
	localNestRecovery.Unlock()

	status := streams.SourceSchemeStatus{
		Handled:   true,
		Medias:    1,
		Receivers: 1,
		Packets:   10,
	}

	reset, attempts := markLocalNestPublishTimeout("cam_raw", status)
	if !reset {
		t.Fatalf("first publish timeout with raw packets did not reset")
	}
	if attempts != 1 {
		t.Fatalf("first attempts = %d, want 1", attempts)
	}
	if wait, attempts := localNestPublishRecoveryWait("cam_raw"); wait <= 0 || attempts != 1 {
		t.Fatalf("publish recovery wait = %s attempts = %d, want active wait with one attempt", wait, attempts)
	}

	reset, attempts = markLocalNestPublishTimeout("cam_raw", status)
	if !reset {
		t.Fatalf("second publish timeout with raw packets did not reset")
	}
	if attempts != 2 {
		t.Fatalf("second attempts = %d, want 2", attempts)
	}
}
