package streams

import (
	"errors"
	"testing"
	"time"
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
